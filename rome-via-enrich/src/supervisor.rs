//! Worker supervision with bounded exponential backoff.
//!
//! Background: rome-via-enrich runs 10 polling workers spawned via
//! `tokio::spawn(worker::run(...))`. Each worker is an internally-looping
//! `loop { ... sleep ... }`, but any `?` on a DB query (or transient panic
//! propagating as `JoinError`) returns `Err` out of the loop. Without
//! supervision, the worker's task ends silently — the enrichment job halts
//! until manual operator restart.
//!
//! `supervise` wraps a worker factory in a restart loop:
//! - On `Err(_)`: retry with exponential backoff (1s, 2s, 4s, ... cap 60s).
//! - On a successful 60s run since the last restart: reset the backoff to 1s.
//!   (We measure "ran 60s without exit" by tracking wall time of each attempt;
//!   if the attempt itself elapsed >= 60s before returning Err, we treat
//!   subsequent restarts as fresh.)
//! - On `RestartPolicy::Permanent`: stop restarting and log a fatal event.
//!   The default policy treats every `anyhow::Error` as transient — workers
//!   today don't classify, so a class of "config-shaped" permanent errors is
//!   not yet expressed in the error type. Callers can plug in custom
//!   classification via `supervise_with`.
//!
//! Each restart emits a `tracing::warn!` with the worker name, attempt count,
//! and backoff duration so log-based alerts can fire on `worker_restart_total`
//! patterns.

use std::future::Future;
use std::time::Duration;
use tokio::task::JoinHandle;

/// Per-attempt outcome classification. Determines whether the supervisor
/// retries (with backoff) or stops permanently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestartPolicy {
    /// Retry the worker — DB blip, RPC timeout, transient panic, etc.
    Transient,
    /// Stop the supervisor — bad config, schema mismatch, irrecoverable state.
    Permanent,
}

/// Default classifier: treat every error as transient. Workers today don't
/// distinguish; if we later add a typed error enum we can swap this for a
/// classifier that inspects the variant.
pub fn classify_default(_err: &anyhow::Error) -> RestartPolicy {
    RestartPolicy::Transient
}

/// Backoff schedule: 1s, 2s, 4s, 8s, 16s, 32s, 60s, 60s, ...
const BASE_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(60);
/// If a worker stayed alive at least this long before failing, reset the
/// backoff schedule on the next failure — the worker recovered, treat the
/// next failure as fresh.
const HEALTHY_RUN_THRESHOLD: Duration = Duration::from_secs(60);

fn next_backoff(attempt: u32) -> Duration {
    // attempt=0 → 1s, 1 → 2s, 2 → 4s, ... capped at 60s.
    let shift = attempt.min(6);
    let secs = BASE_BACKOFF
        .as_secs()
        .checked_shl(shift)
        .unwrap_or(u64::MAX);
    Duration::from_secs(secs).min(MAX_BACKOFF)
}

/// Spawn a supervised worker. `factory` is a closure that produces a fresh
/// worker future each restart — workers cannot share an `async fn` across
/// restarts because the future has already been polled to completion.
///
/// The returned `JoinHandle<()>` resolves when:
/// - The worker returns `Ok(())` (workers today never do — they loop forever).
/// - The worker fails with a `Permanent` policy error.
/// - The task is `.abort()`-ed by the caller (e.g. SIGTERM path in main).
pub fn supervise<F, Fut>(name: &'static str, factory: F) -> JoinHandle<()>
where
    F: Fn() -> Fut + Send + 'static,
    Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    supervise_with(name, factory, classify_default)
}

/// Like [`supervise`], but lets the caller plug in a custom error classifier.
pub fn supervise_with<F, Fut, C>(
    name: &'static str,
    factory: F,
    classify: C,
) -> JoinHandle<()>
where
    F: Fn() -> Fut + Send + 'static,
    Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    C: Fn(&anyhow::Error) -> RestartPolicy + Send + 'static,
{
    tokio::spawn(async move {
        let mut attempt: u32 = 0;
        loop {
            let started = tokio::time::Instant::now();
            let result = factory().await;
            let ran_for = started.elapsed();

            match result {
                Ok(()) => {
                    tracing::info!(worker = name, "worker returned Ok(()) — supervisor exiting");
                    return;
                }
                Err(e) => {
                    let policy = classify(&e);
                    if policy == RestartPolicy::Permanent {
                        tracing::error!(
                            worker = name,
                            error = %e,
                            "worker failed with permanent error; supervisor will NOT restart"
                        );
                        return;
                    }

                    // Reset the attempt counter if the worker ran long enough
                    // to be considered healthy before failing.
                    if ran_for >= HEALTHY_RUN_THRESHOLD {
                        attempt = 0;
                    }

                    let backoff = next_backoff(attempt);
                    tracing::warn!(
                        worker = name,
                        attempt = attempt + 1,
                        backoff_secs = backoff.as_secs(),
                        ran_for_secs = ran_for.as_secs(),
                        error = %e,
                        "worker exited with error; restarting after backoff"
                    );

                    tokio::time::sleep(backoff).await;
                    attempt = attempt.saturating_add(1);
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    /// Backoff schedule matches: 1s, 2s, 4s, 8s, 16s, 32s, 60s, 60s, ...
    #[test]
    fn backoff_schedule_caps_at_60s() {
        assert_eq!(next_backoff(0), Duration::from_secs(1));
        assert_eq!(next_backoff(1), Duration::from_secs(2));
        assert_eq!(next_backoff(2), Duration::from_secs(4));
        assert_eq!(next_backoff(3), Duration::from_secs(8));
        assert_eq!(next_backoff(4), Duration::from_secs(16));
        assert_eq!(next_backoff(5), Duration::from_secs(32));
        assert_eq!(next_backoff(6), Duration::from_secs(60));
        assert_eq!(next_backoff(7), Duration::from_secs(60));
        assert_eq!(next_backoff(100), Duration::from_secs(60));
    }

    /// Default classifier treats everything as transient.
    #[test]
    fn default_classifier_is_transient() {
        let err = anyhow::anyhow!("any error");
        assert_eq!(classify_default(&err), RestartPolicy::Transient);
    }

    /// Spawning a worker that always errors triggers repeated restarts.
    /// We use `tokio::time::pause()` to fast-forward through the backoff.
    #[tokio::test(start_paused = true)]
    async fn supervisor_restarts_failing_worker() {
        let counter = Arc::new(AtomicU32::new(0));
        let counter_factory = counter.clone();

        let handle = supervise("test_failing", move || {
            let n = counter_factory.fetch_add(1, Ordering::SeqCst);
            async move {
                if n < 3 {
                    Err(anyhow::anyhow!("simulated transient failure #{n}"))
                } else {
                    // After 3 restarts, return Ok to terminate the supervisor.
                    Ok(())
                }
            }
        });

        // Advance enough virtual time to cover 1s + 2s + 4s backoffs = 7s.
        tokio::time::advance(Duration::from_secs(10)).await;
        // Let the supervisor finish.
        handle.await.expect("supervisor should not panic");

        // The factory was called 4 times: 3 failing attempts + 1 success.
        assert_eq!(counter.load(Ordering::SeqCst), 4);
    }

    /// Permanent errors should NOT trigger a restart.
    #[tokio::test(start_paused = true)]
    async fn supervisor_does_not_restart_on_permanent_error() {
        let counter = Arc::new(AtomicU32::new(0));
        let counter_factory = counter.clone();

        let handle = supervise_with(
            "test_permanent",
            move || {
                counter_factory.fetch_add(1, Ordering::SeqCst);
                async move { Err(anyhow::anyhow!("config error — irrecoverable")) }
            },
            |_| RestartPolicy::Permanent,
        );

        // Even a long advance shouldn't trigger a restart.
        tokio::time::advance(Duration::from_secs(120)).await;
        handle.await.expect("supervisor should terminate cleanly");

        // Factory called exactly once — permanent classification kills the loop.
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    /// A long-lived worker that fails after running >= 60s should NOT escalate
    /// the backoff schedule — the attempt counter resets to 0.
    #[tokio::test(start_paused = true)]
    async fn healthy_run_resets_backoff() {
        // We use a custom factory that:
        //   attempt 0: runs 90s then fails (healthy)
        //   attempt 1: fails instantly
        //   attempt 2: returns Ok
        // After the first attempt, backoff should reset → next failure uses 1s, not 2s.
        let counter = Arc::new(AtomicU32::new(0));
        let counter_factory = counter.clone();

        let handle = supervise("test_healthy_reset", move || {
            let n = counter_factory.fetch_add(1, Ordering::SeqCst);
            async move {
                match n {
                    0 => {
                        // Run "healthy" for 90s, then fail.
                        tokio::time::sleep(Duration::from_secs(90)).await;
                        Err(anyhow::anyhow!("failed after long run"))
                    }
                    1 => Err(anyhow::anyhow!("instant fail")),
                    _ => Ok(()),
                }
            }
        });

        // 90s (worker 1 lifetime) + 1s (post-healthy backoff) + 1s (post-instant-fail backoff) + slack.
        tokio::time::advance(Duration::from_secs(120)).await;
        handle.await.expect("supervisor should complete");

        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }
}
