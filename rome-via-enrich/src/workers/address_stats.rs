/// Address stats worker.
///
/// Tails `evm_tx` (via eth_block_txs) and updates `address_stats`:
/// - Increments tx_count for `from_addr`, `to_addr`, and the per-tx fee
///   recipient (extracted from `evm_tx_result.tx_result->'gas_report'->>'gas_recipient'`).
/// - Updates `first_seen` / `last_seen` timestamps.
/// - Sets `is_contract = true` if the address has emitted logs (heuristic).
///
/// Counting the fee recipient ensures addresses that only ever appear as the
/// gas-fee receiver (block proposers / validators) still report a non-zero
/// `tx_count` on the address summary card.
///
/// The is_contract heuristic: an address that appears as a log-emitting contract
/// (i.e., address field in receipt logs) is flagged as a contract. This avoids
/// expensive eth_getCode calls per address.
use sqlx::PgPool;
use std::time::Duration;
use tracing::{debug, warn};

/// Convert a Unix-epoch-seconds value (as read from `eth_block.params_block_timestamp`,
/// a NUMERIC column fetched as `FLOAT8`) into a UTC datetime for `first_seen` / `last_seen`.
///
/// `params_block_timestamp` is a numeric epoch, NOT an RFC3339 string — the same
/// decode `cross_chain.rs` uses (`params_block_timestamp::FLOAT8` →
/// `DateTime::from_timestamp(secs as i64, 0)`). A `None` epoch (block not yet
/// produced) maps to `None`; an out-of-range epoch also maps to `None`.
fn epoch_to_dt(secs: Option<f64>) -> Option<chrono::DateTime<chrono::Utc>> {
    secs.and_then(|e| chrono::DateTime::from_timestamp(e as i64, 0))
}

pub async fn run(
    pool: PgPool,
    chain_id: i64,
    poll_interval: Duration,
    batch_size: i64,
) -> anyhow::Result<()> {
    loop {
        let cursor: i64 = sqlx::query_scalar(
            "SELECT COALESCE(last_processed, 0)
             FROM rome_via.enrich_cursors
             WHERE chain_id = $1 AND worker = 'address_stats'",
        )
        .bind(chain_id)
        .fetch_optional(&pool)
        .await?
        .unwrap_or(0);

        // Fetch next batch of txs ordered by slot, keyed by eth_block_txs.
        // Also pull the per-tx fee recipient from the gas_report JSONB so we
        // count fee-only addresses (block proposers) under tx_count.
        let rows: Vec<(String, Option<String>, Option<String>, Option<String>, i64)> =
            sqlx::query_as(
                "SELECT t.tx_hash,
                        tx.from_addr,
                        tx.to_addr,
                        LOWER(r.tx_result->'gas_report'->>'gas_recipient') AS fee_recipient,
                        t.slot_number
                 FROM rome_via.eth_block_txs t
                 JOIN rome_via.evm_tx tx ON tx.tx_hash = t.tx_hash
                 LEFT JOIN rome_via.evm_tx_result r ON r.tx_hash = t.tx_hash
                 WHERE t.chain_id = $1 AND t.slot_number > $2
                 ORDER BY t.slot_number ASC, t.tx_idx ASC
                 LIMIT $3",
            )
            .bind(chain_id)
            .bind(cursor)
            .bind(batch_size)
            .fetch_all(&pool)
            .await
            .unwrap_or_default();

        if rows.is_empty() {
            tokio::time::sleep(poll_interval).await;
            continue;
        }

        // E-C1: defer the (possibly LIMIT-cut) top slot so a full batch never
        // skips a boundary slot's tail. Fetch query unchanged; only what we
        // process + persist changes.
        let slots: Vec<i64> = rows.iter().map(|r| r.4).collect();
        let plan = super::batch_cursor::plan_batch(&slots, batch_size as usize);
        if plan.giant_slot {
            warn!(worker = "address_stats", slot = slots[0],
                  "single slot fills an entire batch; tail beyond batch_size cannot be deferred (unexpected on one-block-per-slot chains)");
        }

        for (tx_hash, from_addr, to_addr, fee_recipient, _slot_number) in &rows[..plan.process_count] {
            // Get block timestamp for first_seen / last_seen.
            // `params_block_timestamp` is a NUMERIC epoch (rome-via-sync
            // migration 0004) — fetch it as FLOAT8 and convert via epoch_to_dt,
            // mirroring cross_chain.rs. (The old `::TEXT` + parse::<DateTime>
            // path always failed on a bare epoch, so first_seen/last_seen were
            // written NULL for every address.)
            let ts_epoch: Option<f64> = sqlx::query_scalar(
                "SELECT b.params_block_timestamp::FLOAT8
                 FROM rome_via.eth_block_txs t
                 JOIN rome_via.eth_block b
                     ON b.slot_number = t.slot_number
                     AND b.slot_block_idx = t.slot_block_idx
                 WHERE t.chain_id = $1 AND t.tx_hash = $2
                 LIMIT 1",
            )
            .bind(chain_id)
            .bind(tx_hash)
            .fetch_optional(&pool)
            .await
            .ok()
            .flatten();
            let ts: Option<chrono::DateTime<chrono::Utc>> = epoch_to_dt(ts_epoch);

            // De-dup so a tx where (from == to == fee_recipient) only counts as 1.
            let mut counted: std::collections::HashSet<&str> =
                std::collections::HashSet::new();

            for addr in [from_addr.as_deref(), to_addr.as_deref(), fee_recipient.as_deref()]
                .into_iter()
                .flatten()
            {
                if counted.insert(addr) {
                    upsert_address(&pool, chain_id, addr, ts).await;
                }
            }

            debug!(%tx_hash, "processed address stats");
        }

        // Mark contract addresses. An address is a contract if ANY of these
        // POSITIVE signals hold:
        //   1. it emitted a Transfer/log as the token contract (token_transfers
        //      .token_address), OR
        //   2. it is a known token (token_metadata), OR
        //   3. it carries a RESOLVED contract label (contract_labels with a
        //      non-NULL display_label — registry-labeled protocol infra:
        //      routers, factories, AavePool, Multicall3, the faucet, V3/V4).
        //
        // Branch 3 MUST require `display_label IS NOT NULL`. The contract_labels
        // worker also writes "seen, not a contract" marker rows (raw_name = "",
        // display_label NULL) for EOAs it examined as a tx recipient, purely to
        // avoid re-probing them. A bare `EXISTS(contract_labels)` counted those
        // as a contract signal and flipped plain EOAs to is_contract=true —
        // measured on hadrian 2026-07-31: 3,357 of 3,462 "contracts" were EOAs
        // with a NULL-label marker row (the two busiest wallets on the chain
        // among them), so most address pages rendered the contract shape for
        // user wallets. `display_label IS NOT NULL` keeps only real, resolved
        // contracts — zero false positives.
        //
        // Residual (accepted, tracked): an unlabeled, non-token, non-emitting
        // contract (e.g. an Arc allowlist module) has no positive indexed
        // signal and stays is_contract=false — it is indistinguishable from an
        // EOA without eth_getCode. That is the SAFE direction (a verified one
        // still renders its source panel) and far smaller than the 3,357
        // false-positives removed. A getCode-based has_code column is the
        // follow-up for full accuracy.
        //
        // Full chain-wide sweep (NOT cursor-bound), gated on
        // `is_contract = false`, so it only ever ADDS. Correcting the predicate
        // does NOT retroactively un-mark the already-flipped false positives —
        // migration 0230 does that one-time backfill.
        let _ = sqlx::query(
            "UPDATE rome_via.address_stats a
             SET is_contract = true
             WHERE a.chain_id = $1
               AND a.is_contract = false
               AND (
                   EXISTS (
                       SELECT 1 FROM rome_via.token_transfers tt
                       WHERE tt.chain_id = $1 AND tt.token_address = a.address
                   )
                   OR EXISTS (
                       SELECT 1 FROM rome_via.token_metadata tm
                       WHERE tm.chain_id = $1 AND tm.address = a.address
                   )
                   OR EXISTS (
                       SELECT 1 FROM rome_via.contract_labels cl
                       WHERE cl.chain_id = $1 AND cl.address = a.address
                         AND cl.display_label IS NOT NULL
                   )
                   OR EXISTS (
                       -- Ground truth: the label worker verified on-chain code
                       -- (getCode). Catches real contracts with no name/symbol/
                       -- token/label — the residual left by the signals above.
                       SELECT 1 FROM rome_via.contract_labels cl
                       WHERE cl.chain_id = $1 AND cl.address = a.address
                         AND cl.has_code = true
                   )
               )",
        )
        .bind(chain_id)
        .execute(&pool)
        .await;

        // Advance cursor (plain — no F6 valve/clamp; see upsert_address doc for why).
        let _ = sqlx::query(
            "INSERT INTO rome_via.enrich_cursors (chain_id, worker, last_processed, last_processed_at)
             VALUES ($1, 'address_stats', $2, NOW())
             ON CONFLICT (chain_id, worker) DO UPDATE
                 SET last_processed    = EXCLUDED.last_processed,
                     last_processed_at = NOW()",
        )
        .bind(chain_id)
        .bind(plan.new_cursor)
        .execute(&pool)
        .await;
    }
}

/// F6 (propagate-don't-swallow) is deliberately NOT applied here: `tx_count`'s
/// `+1` is non-idempotent, so cursor-clamp replay would re-increment rows
/// already counted on a prior pass — a double-count, which is worse than the
/// rare best-effort miss this worker already accepts. Full idempotency
/// (recompute-by-aggregation, or a processed-marker table) is disproportionate
/// for an approximate activity counter. `first_seen`/`last_seen` (LEAST/
/// GREATEST) and the `is_contract` sweep above are idempotent and unaffected.
async fn upsert_address(
    pool: &PgPool,
    chain_id: i64,
    address: &str,
    ts: Option<chrono::DateTime<chrono::Utc>>,
) {
    let res = sqlx::query(
        "INSERT INTO rome_via.address_stats
             (chain_id, address, tx_count, first_seen, last_seen)
         VALUES ($1, $2, 1, $3, $3)
         ON CONFLICT (chain_id, address) DO UPDATE
             SET tx_count   = address_stats.tx_count + 1,
                 first_seen = LEAST(address_stats.first_seen, EXCLUDED.first_seen),
                 last_seen  = GREATEST(address_stats.last_seen, EXCLUDED.last_seen)",
    )
    .bind(chain_id)
    .bind(address)
    .bind(ts)
    .execute(pool)
    .await;

    if let Err(e) = res {
        warn!(%address, error = %e, "failed to upsert address_stats");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // REGRESSION LOCK: `eth_block.params_block_timestamp` is a NUMERIC epoch
    // (rome-via-sync/migrations/0004_eth_block.up.sql), not an RFC3339 string.
    // The worker previously fetched it `::TEXT` and `.parse::<DateTime<Utc>>()`d
    // it — which ALWAYS failed on a bare epoch like "1780558086", so every
    // address got NULL first_seen/last_seen (tx_count still incremented). We now
    // mirror cross_chain.rs: fetch `::FLOAT8`, convert via `from_timestamp`.

    /// A real epoch round-trips: `.timestamp()` matches the input second.
    #[test]
    fn epoch_to_dt_converts_unix_seconds() {
        let dt = epoch_to_dt(Some(1_780_558_086.0)).expect("a valid epoch must convert");
        assert_eq!(dt.timestamp(), 1_780_558_086);
    }

    /// A missing timestamp (block not yet produced) stays None — no spurious row.
    #[test]
    fn epoch_to_dt_none_passthrough() {
        assert!(epoch_to_dt(None).is_none());
    }

    /// Fractional epochs truncate toward the whole second (sub-second precision
    /// is irrelevant for first_seen/last_seen).
    #[test]
    fn epoch_to_dt_truncates_fraction() {
        let dt = epoch_to_dt(Some(1_780_558_086.987)).expect("fractional epoch converts");
        assert_eq!(dt.timestamp(), 1_780_558_086);
    }

    /// The epoch 0 (Unix epoch) is a valid, non-None instant — distinct from the
    /// old parse path which would have rejected "0" as a datetime.
    #[test]
    fn epoch_to_dt_zero_is_some() {
        let dt = epoch_to_dt(Some(0.0)).expect("epoch 0 is a valid instant");
        assert_eq!(dt.timestamp(), 0);
    }

    /// A garbage/out-of-range epoch is handled gracefully (None, not panic).
    /// `from_timestamp` returns None outside chrono's representable range.
    #[test]
    fn epoch_to_dt_out_of_range_is_none() {
        // f64::MAX as i64 saturates to i64::MAX, which is far outside the
        // representable DateTime range → None.
        assert!(epoch_to_dt(Some(f64::MAX)).is_none());
    }
}
