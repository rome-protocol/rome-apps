//! Dedicated low-priority blocking pool for read-path emulation (`eth_call` /
//! `estimateGas` / `rome_emulateCallAccounts`). Runs the synchronous, CPU-heavy
//! emulation OFF the main async runtime so a read burst can't starve the
//! write/confirm path. Sized to the machine's parallelism (correct under CPU
//! shares — no hard quota to misread) and niced low so writes preempt reads
//! under contention. Panic-isolated: a panicking emulation returns an error;
//! the worker survives and the write path is untouched.
//!
//! Intake is bounded (queue holds at most `workers * QUEUE_DEPTH_PER_WORKER`
//! pending jobs) but NOT reject-on-full: a caller hitting a momentarily-full
//! queue applies async backpressure — it waits for a slot up to [`SUBMIT_DEADLINE`]
//! before shedding with `read pool at capacity`. This absorbs legitimate
//! concurrent read bursts (which on a small box can exceed a per-worker queue)
//! while still bounding memory and shedding a sustained flood (fail-closed).

use std::{sync::Arc, thread, time::Duration};

type Job = Box<dyn FnOnce() + Send + 'static>;

/// Queue slots per worker. Bounds the intake queue's memory. With backpressure
/// this is the wait-vs-proceed threshold, not a hard reject line — callers past
/// it wait for a slot rather than being rejected.
const QUEUE_DEPTH_PER_WORKER: usize = 64;

/// Longest a caller waits for a queue slot before its request is shed. Sized to
/// let a legitimate transient burst drain (queue depth × per-emulation time)
/// while still bounding how long a request parks under a genuine sustained flood.
const SUBMIT_DEADLINE: Duration = Duration::from_secs(30);

/// Longest a caller waits for a running emulation's result. The emulator's own
/// opcode budget bounds the work, but RPC-heavy emulations can still run long;
/// past this the caller gets an error. The worker itself cannot be preempted
/// and finishes the job in the background.
const RUN_TIMEOUT: Duration = Duration::from_secs(30);

/// Worker stack size. Emulator call frames form a linked snapshot chain whose
/// depth walk and drop are recursive, and nesting depth is bounded only by
/// the opcode budget. The default 2 MiB stack can overflow on deeply nested
/// calls, and a stack overflow aborts the whole process (it is not a panic,
/// so `catch_unwind` cannot contain it). The reservation is virtual: pages are
/// only committed when actually touched.
const WORKER_STACK_SIZE: usize = 512 * 1024 * 1024;

/// Async poll interval while waiting for a queue slot. `tokio::time::sleep` —
/// yields to the runtime, never blocks a runtime worker thread.
const BACKPRESSURE_POLL: Duration = Duration::from_millis(5);

/// A bounded, low-priority worker pool for blocking emulation work.
pub struct ReadPool {
    tx: crossbeam_channel::Sender<Job>,
    workers: usize,
    capacity: usize,
}

impl ReadPool {
    /// Spawn a pool sized to available parallelism, each worker niced low, with
    /// an intake queue bounded to `workers * QUEUE_DEPTH_PER_WORKER`.
    pub fn new() -> std::io::Result<Arc<Self>> {
        let workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let capacity = workers.saturating_mul(QUEUE_DEPTH_PER_WORKER);
        Self::with_capacity(workers, capacity)
    }

    /// Spawn `workers` niced-low threads draining a job queue bounded to
    /// `capacity`. Split out from [`Self::new`] so tests can build a small,
    /// fully-saturable pool with known bounds.
    fn with_capacity(workers: usize, capacity: usize) -> std::io::Result<Arc<Self>> {
        let (tx, rx) = crossbeam_channel::bounded::<Job>(capacity);
        for _ in 0..workers {
            let rx = rx.clone();
            thread::Builder::new()
                .name("rome-read".into())
                .stack_size(WORKER_STACK_SIZE)
                .spawn(move || {
                    set_low_priority();
                    // Loop exits when the last Sender (the ReadPool) drops.
                    while let Ok(job) = rx.recv() {
                        // Per-job panic isolation: a panicking emulation must not
                        // kill the worker (shrinking the pool) or escape.
                        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job));
                    }
                })?;
        }
        Ok(Arc::new(Self {
            tx,
            workers,
            capacity,
        }))
    }

    /// Worker-thread count (= the machine's parallelism).
    pub fn worker_count(&self) -> usize {
        self.workers
    }

    /// Max pending jobs the intake queue holds before the pool sheds load.
    pub fn queue_capacity(&self) -> usize {
        self.capacity
    }

    /// Immediate, non-blocking enqueue (rejects on full). Retained as a test
    /// building block for deterministically filling the queue; the production
    /// path is [`Self::submit_with_backpressure`], which waits for a slot.
    #[cfg(test)]
    fn submit<F, R>(&self, f: F) -> anyhow::Result<tokio::sync::oneshot::Receiver<R>>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let job: Job = Box::new(move || {
            // If `f` panics, the worker's catch_unwind catches it and `tx` drops
            // un-sent, so the awaiting caller sees Err.
            let _ = tx.send(f());
        });
        self.tx.try_send(job).map_err(|e| match e {
            crossbeam_channel::TrySendError::Full(_) => anyhow::anyhow!("read pool at capacity"),
            crossbeam_channel::TrySendError::Disconnected(_) => {
                anyhow::anyhow!("read pool unavailable")
            }
        })?;
        Ok(rx)
    }

    /// Enqueue a job, applying async backpressure when the intake queue is full:
    /// wait (yielding to the runtime) for a slot until `deadline_after` elapses,
    /// then shed with `read pool at capacity`. Absorbs transient bursts (the
    /// common case — a legitimate client firing many concurrent reads) instead of
    /// rejecting them; only a SUSTAINED overload that keeps the queue full past
    /// the deadline is shed. Queue memory stays bounded by `capacity`; waiters are
    /// bounded by the server's max connections.
    async fn submit_with_backpressure<F, R>(
        &self,
        f: F,
        deadline_after: std::time::Duration,
    ) -> anyhow::Result<tokio::sync::oneshot::Receiver<R>>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let mut job: Job = Box::new(move || {
            // If `f` panics, the worker's catch_unwind catches it and `tx` drops
            // un-sent, so the awaiting caller sees Err.
            let _ = tx.send(f());
        });
        let deadline = tokio::time::Instant::now() + deadline_after;
        loop {
            match self.tx.try_send(job) {
                Ok(()) => return Ok(rx),
                Err(crossbeam_channel::TrySendError::Full(returned)) => {
                    if tokio::time::Instant::now() >= deadline {
                        return Err(anyhow::anyhow!("read pool at capacity"));
                    }
                    // Full right now — yield and retry until a worker frees a slot
                    // or the deadline passes. `try_send` hands the job back on
                    // Full, so nothing is lost across retries.
                    job = returned;
                    tokio::time::sleep(BACKPRESSURE_POLL).await;
                }
                Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
                    return Err(anyhow::anyhow!("read pool unavailable"));
                }
            }
        }
    }

    /// Run a blocking closure on the read pool and await its result on the
    /// caller's runtime. A panic in `f` is surfaced as an error — it never aborts
    /// a worker or reaches the write path. Applies backpressure when the intake
    /// queue is momentarily full (waits for a slot up to [`SUBMIT_DEADLINE`]),
    /// shedding with `read pool at capacity` only under sustained overload.
    /// Waits at most [`RUN_TIMEOUT`] for the result.
    pub async fn run<F, R>(&self, f: F) -> anyhow::Result<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.run_with_timeout(f, RUN_TIMEOUT).await
    }

    async fn run_with_timeout<F, R>(&self, f: F, timeout: Duration) -> anyhow::Result<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let rx = self.submit_with_backpressure(f, SUBMIT_DEADLINE).await?;
        match tokio::time::timeout(timeout, rx).await {
            Ok(result) => result.map_err(|_| anyhow::anyhow!("read pool task panicked")),
            Err(_) => Err(anyhow::anyhow!("emulation timed out")),
        }
    }
}

/// Best-effort: nice the calling (read-pool worker) thread so write/confirm
/// work on the main runtime preempts emulation under CPU contention. Per-thread
/// on Linux; effective only under CPU shares (no hard quota).
fn set_low_priority() {
    unsafe {
        libc::setpriority(libc::PRIO_PROCESS, 0, 10);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn runs_work_and_returns_result() {
        let pool = ReadPool::new().unwrap();
        assert_eq!(pool.run(|| 2 + 2).await.unwrap(), 4);
    }

    #[tokio::test]
    async fn slow_task_times_out_and_pool_survives() {
        let pool = ReadPool::with_capacity(1, 1).unwrap();
        let r = pool
            .run_with_timeout(|| std::thread::sleep(Duration::from_millis(500)), Duration::from_millis(50))
            .await;
        assert!(r.is_err(), "a task past the timeout must return Err");
        // The worker finishes the slow job, then serves new work.
        assert_eq!(pool.run(|| 7).await.unwrap(), 7);
    }

    /// Deep recursion that would overflow the default 2 MiB thread stack must
    /// complete on a read-pool worker.
    #[tokio::test]
    async fn deep_recursion_fits_worker_stack() {
        fn depth(n: u64) -> u64 {
            let pad = [n; 16];
            if n == 0 { 0 } else { std::hint::black_box(pad)[0].min(1) + depth(n - 1) }
        }
        let pool = ReadPool::with_capacity(1, 1).unwrap();
        assert_eq!(pool.run(|| depth(200_000)).await.unwrap(), 200_000);
    }

    #[tokio::test]
    async fn panicking_task_is_isolated_and_pool_survives() {
        let pool = ReadPool::new().unwrap();
        let r = pool.run(|| -> i32 { panic!("boom in emulation") }).await;
        assert!(
            r.is_err(),
            "a panicking task must return Err, not abort the pool"
        );
        // The pool still serves work after a panic.
        assert_eq!(pool.run(|| 7).await.unwrap(), 7);
    }

    #[tokio::test]
    async fn sized_to_available_parallelism() {
        let pool = ReadPool::new().unwrap();
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        assert_eq!(pool.worker_count(), cores);
    }

    /// Helper: saturate a pool (every worker parked + queue full). Returns the
    /// release sender (drop/send to free parked jobs) and the held receivers.
    fn saturate(
        pool: &Arc<ReadPool>,
    ) -> (
        crossbeam_channel::Sender<()>,
        Vec<tokio::sync::oneshot::Receiver<()>>,
    ) {
        use std::time::Duration;
        let (block_tx, block_rx) = crossbeam_channel::unbounded::<()>();
        let (started_tx, started_rx) = crossbeam_channel::unbounded::<()>();
        let mut held = Vec::new();
        // Occupy every worker thread.
        for _ in 0..pool.worker_count() {
            let started = started_tx.clone();
            let block = block_rx.clone();
            held.push(
                pool.submit(move || {
                    let _ = started.send(());
                    let _ = block.recv();
                })
                .expect("an idle worker must accept a job"),
            );
        }
        for _ in 0..pool.worker_count() {
            started_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("every worker should start its job");
        }
        // Fill the intake queue to its bound.
        for _ in 0..pool.queue_capacity() {
            let block = block_rx.clone();
            held.push(
                pool.submit(move || {
                    let _ = block.recv();
                })
                .expect("a free queue slot must accept a job"),
            );
        }
        (block_tx, held)
    }

    /// Backpressure: a caller hitting a momentarily-full intake must WAIT for a
    /// slot and then proceed — NOT be instantly rejected. Rejecting legitimate
    /// concurrent read bursts (a small CI box has few workers → small queue) was
    /// the regression this replaces. Saturate, start a `run` that must block,
    /// then free capacity and assert it completes with the real result.
    #[tokio::test]
    async fn run_backpressures_then_proceeds_when_a_slot_frees() {
        use std::time::Duration;
        let pool = ReadPool::with_capacity(1, 1).unwrap();
        let (block_tx, held) = saturate(&pool);

        // Full now. `run` must wait for a slot, not reject.
        let pool2 = pool.clone();
        let fut = tokio::spawn(async move { pool2.run(|| 99).await });

        // Let run() reach the full queue and begin waiting, then free everything.
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(block_tx); // release parked jobs → workers drain → slot frees

        let r = tokio::time::timeout(Duration::from_secs(10), fut)
            .await
            .expect("run must proceed once a slot frees, not wait forever")
            .expect("join");
        assert_eq!(
            r.unwrap(),
            99,
            "backpressured run must return the real result after waiting"
        );
        drop(held);
    }

    /// Sustained-overload backstop: if the queue stays full past the submit
    /// deadline, the request is shed (fail-closed) rather than waiting forever or
    /// growing the backlog. Uses a short deadline; never frees a slot.
    #[tokio::test]
    async fn sustained_overload_sheds_after_deadline() {
        use std::time::Duration;
        let pool = ReadPool::with_capacity(1, 1).unwrap();
        let (block_tx, held) = saturate(&pool);

        let start = tokio::time::Instant::now();
        let rx = pool
            .submit_with_backpressure(|| (), Duration::from_millis(150))
            .await;
        assert!(
            rx.is_err(),
            "a queue full past the deadline must shed with an error"
        );
        assert!(
            start.elapsed() >= Duration::from_millis(150),
            "shed must wait for the deadline (backpressure), not reject instantly"
        );
        drop(block_tx);
        drop(held);
    }
}
