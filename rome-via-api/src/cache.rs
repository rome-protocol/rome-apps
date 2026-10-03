//! Small Redis-backed response cache for hot, expensive read aggregates.
//!
//! The explorer home page fires a handful of heavy chain-wide aggregates
//! (`stats/overview`, `throughput/current-tps`) on every load and again on each
//! SSE invalidation. Each is a multi-`COUNT` / windowed-`JOIN` over `evm_tx` +
//! `eth_block`, so under load they take seconds and pile up on the DB pool. These
//! tiles tolerate a few seconds of staleness, so we memoize the JSON response in
//! Redis with a short TTL: a cache hit is one GET instead of the full query set.
//!
//! No Redis configured ⇒ transparent pass-through (always computes). A malformed
//! or missing cache entry, or a failed SET, degrades to compute — the cache never
//! fails a request.
use std::future::Future;

use serde_json::Value;

use crate::{error::AppError, state::AppState};

/// Return the cached JSON for `key`, or run `compute`, cache it for `ttl_secs`,
/// and return it. All cache-layer errors degrade to a fresh compute.
///
/// **Stale-while-revalidate.** `ttl_secs` is the FRESHNESS window, not the
/// physical expiry: entries live `cache_envelope::physical_ttl(ttl_secs)` in
/// Redis, and a request that finds an aged entry returns it immediately while
/// a background task recomputes and re-fills (guarded by a `:lock` SET NX so
/// concurrent stale hits don't stampede the DB). Before this, a 10s TTL on a
/// query that costs 4–6s cold (throughput cadence / 24h timeseries, measured
/// on hadrian 2026-07-30) meant nearly every dashboard visit paid the scan;
/// now only the very first request after a long quiet period does.
pub async fn cached_json<F, Fut>(
    state: &AppState,
    key: &str,
    ttl_secs: u64,
    compute: F,
) -> Result<Value, AppError>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = Result<Value, AppError>> + Send,
{
    let now = chrono::Utc::now().timestamp();

    if let Some(mut redis) = state.redis.clone() {
        let cached: Result<Option<String>, _> = redis::AsyncCommands::get(&mut redis, key).await;
        if let Ok(Some(json_str)) = cached {
            if let Some((value, fresh)) = crate::cache_envelope::parse_envelope(&json_str, now) {
                if fresh {
                    tracing::debug!(key, "via read-cache hit (fresh)");
                    return Ok(value);
                }
                // Stale: serve instantly, refresh behind a lock so exactly one
                // task pays the scan.
                tracing::debug!(key, "via read-cache hit (stale) — background refresh");
                let lock_key = format!("{key}:lock");
                let key = key.to_string();
                let mut lock_redis = redis.clone();
                tokio::spawn(async move {
                    let lock: Result<bool, _> = redis::cmd("SET")
                        .arg(&lock_key)
                        .arg("1")
                        .arg("NX")
                        .arg("EX")
                        .arg(30)
                        .query_async(&mut lock_redis)
                        .await
                        .map(|v: Option<String>| v.is_some());
                    if !matches!(lock, Ok(true)) {
                        return; // another instance is already refreshing
                    }
                    match compute().await {
                        Ok(v) => {
                            let raw = crate::cache_envelope::envelope(
                                &v,
                                chrono::Utc::now().timestamp(),
                                ttl_secs,
                            );
                            let _: Result<String, _> = redis::AsyncCommands::set_ex(
                                &mut lock_redis,
                                &key,
                                raw,
                                crate::cache_envelope::physical_ttl(ttl_secs),
                            )
                            .await;
                        }
                        Err(e) => tracing::warn!(key, error = %e, "background cache refresh failed"),
                    }
                    let _: Result<i64, _> =
                        redis::AsyncCommands::del(&mut lock_redis, &lock_key).await;
                });
                return Ok(value);
            }
        }
    }

    let value = compute().await?;

    if let Some(mut redis) = state.redis.clone() {
        let raw = crate::cache_envelope::envelope(&value, now, ttl_secs);
        let _: Result<String, _> = redis::AsyncCommands::set_ex(
            &mut redis,
            key,
            raw,
            crate::cache_envelope::physical_ttl(ttl_secs),
        )
        .await;
    }
    Ok(value)
}
