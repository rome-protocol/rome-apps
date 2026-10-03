//! Concurrent-request coalescer for multi-`DoTx` batching (Phase 3, slice 2).
//!
//! Default-off: only constructed when `batching` is configured (the
//! `eth_sendRawTransaction` dispatch is slice 3). It groups concurrently
//! in-flight submits and hands each FIFO group to a [`BatchBackend`], which
//! overlay-emulates + packs the prefix + submits it as one Solana tx and
//! individually submits the remainder — one result per input, in order.
//!
//! **Confirmation invariant (#344):** a caller's `submit` future resolves only
//! after the backend returns, and the backend returns only after Solana
//! confirmation. The coalescer never hands back a hash before the tx is
//! confirmed — it only changes *how* concurrently-waiting txs reach Solana.
//!
//! Grouping is backlog-driven: block for the first job, then drain whatever is
//! already queued (up to `max_pack_size`). Under load this coalesces naturally;
//! at idle a lone tx is a group of one, so there is no added latency.

use {
    crate::error::{ApiError, Result},
    async_trait::async_trait,
    ethers::types::{Bytes, TxHash},
    rome_sdk::rome_evm_client::{tx::PackLimits, RomeEVMClient},
    std::{sync::Arc, time::Duration},
    tokio::sync::{mpsc, oneshot, Mutex},
};

/// Processes one FIFO group: pack the prefix that fits, submit it, individually
/// submit the remainder. Returns exactly one result per input rlp, in order.
/// Production impl is [`RealBatchBackend`] (wraps `send_pack`); tests use a mock.
#[async_trait]
pub trait BatchBackend: Send + Sync {
    async fn submit_group(&self, rlps: Vec<Bytes>) -> Vec<Result<TxHash>>;
}

/// Production [`BatchBackend`]: a thin wrapper over `RomeEVMClient::send_pack`,
/// mapping its `ProgramResult`s to the proxy's `ApiError`. All pack / fallback
/// logic lives in `send_pack` — nothing is re-implemented here.
pub struct RealBatchBackend {
    pub client: Arc<RomeEVMClient>,
    pub limits: PackLimits,
}

#[async_trait]
impl BatchBackend for RealBatchBackend {
    async fn submit_group(&self, rlps: Vec<Bytes>) -> Vec<Result<TxHash>> {
        self.client
            .send_pack(rlps, self.limits)
            .await
            .into_iter()
            .map(|r| r.map_err(ApiError::from))
            .collect()
    }
}

struct Job {
    rlp: Bytes,
    reply: oneshot::Sender<Result<TxHash>>,
}

/// Coalesces concurrent submits and routes each result back to its caller.
#[derive(Clone)]
pub struct Batcher {
    sender: mpsc::Sender<Job>,
}

impl Batcher {
    /// Spawn `concurrency` packer lanes draining one shared queue. `max_pack_size`
    /// caps txs handed to the backend per group; `concurrency` caps how many packs
    /// may be confirming on Solana at once (both floored at 1). `concurrency = 1`
    /// is the pre-Phase-4 single-lane behavior — packs serialize on confirmation.
    ///
    /// `fill_timeout_ms` is how long a *backlogged* forming pack waits to gather
    /// more txs before submitting (0 = off, the instant-drain behavior). A lone tx
    /// (no backlog) never waits, so idle traffic pays no added latency.
    pub fn new(
        backend: Arc<dyn BatchBackend>,
        max_pack_size: usize,
        concurrency: usize,
        fill_timeout_ms: u64,
        intake_capacity: usize,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(intake_capacity.max(1));
        let max = max_pack_size.max(1);
        let lanes = concurrency.max(1);
        let fill_timeout = Duration::from_millis(fill_timeout_ms);
        // One shared queue, `lanes` consumers. Each lane holds the lock only long
        // enough to collect its group, then releases it before the (slow) Solana
        // submit+confirm — so up to `lanes` packs confirm concurrently.
        let receiver = Arc::new(Mutex::new(receiver));
        for _ in 0..lanes {
            tokio::spawn(packer_loop(
                receiver.clone(),
                backend.clone(),
                max,
                fill_timeout,
            ));
        }
        Self { sender }
    }

    /// Enqueue a tx and await its (post-confirmation) result.
    pub async fn submit(&self, rlp: Bytes) -> Result<TxHash> {
        let (reply, rx) = oneshot::channel();
        self.sender
            .send(Job { rlp, reply })
            .await
            .map_err(|_| ApiError::Custom("batcher channel closed".to_string()))?;
        rx.await
            .map_err(|_| ApiError::Custom("batcher dropped the request".to_string()))?
    }
}

/// Block for the next job, then drain whatever is immediately queued, up to
/// `max`. Under backlog (more than one tx ready), optionally wait up to
/// `fill_timeout` for the pack to fill further. Returns `None` once the channel
/// is closed and drained.
async fn collect_group(
    rx: &mut mpsc::Receiver<Job>,
    max: usize,
    fill_timeout: Duration,
) -> Option<Vec<Job>> {
    let first = rx.recv().await?;
    let mut group = vec![first];
    // Instant drain of whatever is already queued.
    while group.len() < max {
        match rx.try_recv() {
            Ok(job) => group.push(job),
            Err(_) => break,
        }
    }
    // Fill window: only under backlog (the instant drain already found >1) and
    // only while room remains. A lone tx (idle) returns immediately, paying no
    // added latency. Bounded by a single deadline so the wait can't exceed it.
    if !fill_timeout.is_zero() && group.len() >= 2 && group.len() < max {
        let deadline = tokio::time::Instant::now() + fill_timeout;
        while group.len() < max {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some(job)) => group.push(job),
                _ => break, // window elapsed, or channel closed and drained
            }
        }
    }
    Some(group)
}

async fn packer_loop(
    rx: Arc<Mutex<mpsc::Receiver<Job>>>,
    backend: Arc<dyn BatchBackend>,
    max: usize,
    fill_timeout: Duration,
) {
    loop {
        // Collect the next group under the lock, then drop the lock before the
        // slow submit so sibling lanes can collect + confirm in parallel.
        let group = {
            let mut guard = rx.lock().await;
            match collect_group(&mut guard, max, fill_timeout).await {
                Some(group) => group,
                None => return, // channel closed and drained
            }
        };
        let rlps: Vec<Bytes> = group.iter().map(|j| j.rlp.clone()).collect();
        let results = backend.submit_group(rlps).await;
        let mut results = results.into_iter();
        for job in group {
            let res = results
                .next()
                .unwrap_or_else(|| Err(ApiError::Custom("backend returned too few results".to_string())));
            let _ = job.reply.send(res);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rlp(tag: u8) -> Bytes {
        Bytes::from(vec![tag; 16])
    }
    fn hash_of(rlp: &Bytes) -> TxHash {
        TxHash::from(ethers::utils::keccak256(rlp))
    }

    /// Returns `Ok(keccak(rlp))` per tx — a deterministic per-input result so
    /// fan-out correctness can be asserted regardless of how grouping happened.
    struct HashBackend;
    #[async_trait]
    impl BatchBackend for HashBackend {
        async fn submit_group(&self, rlps: Vec<Bytes>) -> Vec<Result<TxHash>> {
            rlps.iter().map(|r| Ok(hash_of(r))).collect()
        }
    }

    /// Fails any rlp whose first byte is 0xFF; others get `Ok(keccak)`.
    struct PoisonByteBackend;
    #[async_trait]
    impl BatchBackend for PoisonByteBackend {
        async fn submit_group(&self, rlps: Vec<Bytes>) -> Vec<Result<TxHash>> {
            rlps.iter()
                .map(|r| {
                    if r.first() == Some(&0xFF) {
                        Err(ApiError::Custom("backend rejected".to_string()))
                    } else {
                        Ok(hash_of(r))
                    }
                })
                .collect()
        }
    }

    #[tokio::test]
    async fn collect_group_drains_up_to_max() {
        let (tx, mut rx) = mpsc::channel(32);
        for i in 0..5 {
            let (reply, _r) = oneshot::channel();
            tx.send(Job { rlp: rlp(i), reply }).await.unwrap();
        }
        let g1 = collect_group(&mut rx, 3, Duration::ZERO).await.unwrap();
        assert_eq!(g1.len(), 3, "drains up to max");
        let g2 = collect_group(&mut rx, 3, Duration::ZERO).await.unwrap();
        assert_eq!(g2.len(), 2, "remaining queued jobs");
    }

    #[tokio::test]
    async fn collect_group_single_when_alone() {
        let (tx, mut rx) = mpsc::channel(32);
        let (reply, _r) = oneshot::channel();
        tx.send(Job { rlp: rlp(7), reply }).await.unwrap();
        let g = collect_group(&mut rx, 4, Duration::ZERO).await.unwrap();
        assert_eq!(g.len(), 1, "a lone job forms a group of one");
    }

    #[tokio::test]
    async fn routes_each_result_to_its_caller() {
        let batcher = Batcher::new(Arc::new(HashBackend), 4, 1, 0, 1024);
        let mut handles = Vec::new();
        for i in 0..6u8 {
            let b = batcher.clone();
            let r = rlp(i);
            handles.push(tokio::spawn(async move {
                let got = b.submit(r.clone()).await;
                (r, got)
            }));
        }
        for h in handles {
            let (r, got) = h.await.unwrap();
            assert_eq!(got.unwrap(), hash_of(&r), "each caller gets the hash of ITS rlp");
        }
    }

    #[tokio::test]
    async fn backend_error_reaches_the_right_caller() {
        let batcher = Batcher::new(Arc::new(PoisonByteBackend), 4, 1, 0, 1024);
        let good = rlp(1);
        let bad = Bytes::from(vec![0xFFu8; 16]);
        let (rg, rb) = tokio::join!(batcher.submit(good.clone()), batcher.submit(bad.clone()));
        assert_eq!(rg.unwrap(), hash_of(&good), "good tx confirmed");
        assert!(matches!(rb, Err(ApiError::Custom(_))), "bad tx surfaces the error");
    }

    #[tokio::test]
    async fn concurrency_runs_packs_in_parallel() {
        use tokio::sync::Barrier;

        // Returns only once TWO submit_group calls are simultaneously in flight,
        // so it can complete ONLY if two packer lanes run concurrently. A single
        // lane serializes the two groups → the size-2 barrier never releases.
        struct BarrierBackend {
            barrier: Arc<Barrier>,
        }
        #[async_trait]
        impl BatchBackend for BarrierBackend {
            async fn submit_group(&self, rlps: Vec<Bytes>) -> Vec<Result<TxHash>> {
                self.barrier.wait().await;
                rlps.iter().map(|r| Ok(hash_of(r))).collect()
            }
        }

        let backend = Arc::new(BarrierBackend {
            barrier: Arc::new(Barrier::new(2)),
        });
        // concurrency = 2; max_pack_size = 1 so each job is its own group.
        let batcher = Batcher::new(backend, 1, 2, 0, 1024);

        let a = rlp(1);
        let b = rlp(2);
        // A single lane would hang on the size-2 barrier; the 2s cap turns that
        // into a clean failure instead of a stuck test.
        let joined = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            tokio::join!(batcher.submit(a.clone()), batcher.submit(b.clone()))
        })
        .await
        .expect("two packer lanes must confirm packs concurrently (single-lane hangs the barrier)");

        assert_eq!(joined.0.unwrap(), hash_of(&a));
        assert_eq!(joined.1.unwrap(), hash_of(&b));
    }

    #[tokio::test]
    async fn concurrency_one_keeps_a_single_lane() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        // Records the peak number of simultaneously in-flight submit_group calls.
        struct PeakBackend {
            in_flight: AtomicUsize,
            peak: AtomicUsize,
        }
        #[async_trait]
        impl BatchBackend for PeakBackend {
            async fn submit_group(&self, rlps: Vec<Bytes>) -> Vec<Result<TxHash>> {
                let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                self.peak.fetch_max(now, Ordering::SeqCst);
                // Hold the slot briefly so any parallelism would overlap here.
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                self.in_flight.fetch_sub(1, Ordering::SeqCst);
                rlps.iter().map(|r| Ok(hash_of(r))).collect()
            }
        }

        let backend = Arc::new(PeakBackend {
            in_flight: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        });
        // concurrency = 1 → the pre-Phase-4 single-lane behavior.
        let batcher = Batcher::new(backend.clone(), 1, 1, 0, 1024);

        let mut handles = Vec::new();
        for i in 0..4u8 {
            let b = batcher.clone();
            handles.push(tokio::spawn(async move { b.submit(rlp(i)).await }));
        }
        for h in handles {
            h.await.unwrap().unwrap();
        }
        assert_eq!(
            backend.peak.load(Ordering::SeqCst),
            1,
            "concurrency=1 must keep exactly one pack in flight (single-lane)"
        );
    }

    #[tokio::test]
    async fn fill_window_gathers_late_arrival_under_backlog() {
        let (tx, mut rx) = mpsc::channel(32);
        // Two jobs already queued ⇒ backlog present at collect time.
        for i in 0..2u8 {
            let (reply, _r) = oneshot::channel();
            tx.send(Job { rlp: rlp(i), reply }).await.unwrap();
        }
        // A third arrives shortly after, within the fill window.
        let tx_late = tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let (reply, _r) = oneshot::channel();
            tx_late.send(Job { rlp: rlp(2), reply }).await.unwrap();
        });
        // max = 3 so the late third closes the pack and returns promptly. Without
        // the window the instant drain returns 2 and the third is missed.
        let group = collect_group(&mut rx, 3, Duration::from_secs(2)).await.unwrap();
        assert_eq!(
            group.len(),
            3,
            "fill window must gather the late arrival under backlog"
        );
    }

    #[tokio::test]
    async fn fill_window_skipped_at_idle_no_added_latency() {
        let (tx, mut rx) = mpsc::channel(32);
        let (reply, _r) = oneshot::channel();
        tx.send(Job { rlp: rlp(7), reply }).await.unwrap();
        // A lone tx (no backlog) must NOT wait the window, even a huge one.
        let start = std::time::Instant::now();
        let group = collect_group(&mut rx, 4, Duration::from_secs(10)).await.unwrap();
        assert_eq!(group.len(), 1, "a lone tx forms a group of one");
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "idle traffic must not pay the fill-window latency"
        );
    }
}
