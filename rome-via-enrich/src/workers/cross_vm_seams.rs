//! cross_vm_seams — maintains the feed of EVM<->Solana crossings.
//!
//! The Cross-chain screen needs "the crossings, newest first, paginated". That is a
//! feed, not the transaction feed with a predicate bolted on, so this worker maintains
//! it as one: a row per tx that sits on at least one seam, and nothing else.
//!
//! **Why this shape.** The predecessor (`tx_class`) classified every transaction, then
//! filtered. On hadrian that meant 3.09M rows growing at ~100/s to locate 114 crossings
//! — 1 in 27,000. Writing at chain rate is what made that worker unable to keep up, and
//! a rare predicate over a full shadow table is what made the planner's choice matter.
//! Here the worker still READS every transaction to decide, but writes only the
//! crossings, so its write volume is ~0.004% of the chain's and it cannot fall behind.
//!
//! **Late inputs, and why that needs a SECOND cursor.** Seam classification reads
//! `cross_chain_correlations` (rome tx type, Solana legs) and `method_signatures`
//! (decoded method), both written by other enrich workers. A tx examined before those
//! land can be misjudged — always by missing a seam, never by inventing one, since every
//! trigger is positive evidence. So recent history must be re-examined.
//!
//! "Advance forward" and "re-examine recent" are two jobs and must not share a cursor.
//! Re-reading from `cursor - RECHECK_SLOTS` with a `LIMIT` returns the OLDEST rows of
//! that window — at ~100 tx/s the window holds ~200k rows against a 500-row batch, so
//! the batch max never exceeds the cursor, the cursor freezes, and the worker re-reads
//! the same rows forever. Hence:
//!   * `cross_vm_seams` — the FORWARD cursor. Strictly `slot > cursor`, so it always
//!     advances; it drains a backlog because each pass starts past the last one.
//!   * `cross_vm_seams_recheck` — a wrapping sweep of the trailing window, one batch per
//!     poll, which can never starve or block the forward pass.
//!
//! Note what the recheck sweep is NOT: it is a sliding window over recent slots, so it
//! cannot repair rows older than that window. Anything that has to correct existing
//! history — adding a derived column, changing the classifier — needs a backfill
//! migration; assuming this sweep will get to it eventually is wrong (migration 0226
//! exists because 0225 made exactly that mistake).
use rome_via_classify::{classify, is_oracle_method, seams, ClassifyInput, SeamInput};
use serde_json::Value;
use sqlx::{PgPool, Row};
use std::time::Duration;

/// How far back the recheck sweep re-examines, so a tx whose enrichment landed after its
/// first look is picked up. Cheap here: re-examined non-crossings write nothing.
const RECHECK_SLOTS: i64 = 5_000;

/// Forward cursor: highest slot examined. Only ever moves up.
const FORWARD: &str = "cross_vm_seams";
/// Recheck cursor: position of the wrapping sweep through the trailing window.
const RECHECK: &str = "cross_vm_seams_recheck";

fn rows_query() -> &'static str {
    r#"
    SELECT et.tx_hash,
           ebt.slot_number,
           ebt.tx_idx,
           et.to_addr,
           et.value_wei::TEXT AS value_wei,
           et.tx_type_byte,
           COALESCE(et.origination, 'ecdsa') AS origination,
           et.solana_signer,
           et.method_id,
           et.input_len,
           COALESCE(ms.signature, et.method_id, '0x') AS method,
           etr.tx_result,
           COALESCE(ccc.rome_tx_type, 'Rhea') AS rome_tx_type,
           COALESCE(jsonb_array_length(ccc.solana_legs), 0) AS solana_leg_count,
           (ccc.cpi_program IS NOT NULL) AS has_cpi_target
    FROM rome_via.evm_tx et
    JOIN rome_via.eth_block_txs ebt
      ON ebt.tx_hash = et.tx_hash AND ebt.chain_id = et.chain_id
    LEFT JOIN rome_via.evm_tx_result etr
      ON etr.tx_hash = et.tx_hash AND etr.chain_id = et.chain_id
    LEFT JOIN rome_via.method_signatures ms ON ms.selector = et.method_id
    LEFT JOIN rome_via.cross_chain_correlations ccc
      ON ccc.tx_hash = et.tx_hash AND ccc.chain_id = et.chain_id
    WHERE et.chain_id = $1 AND ebt.slot_number > $2
    ORDER BY ebt.slot_number ASC, ebt.tx_idx ASC
    LIMIT $3
    "#
}

struct Crossing {
    tx_hash: String,
    slot_number: i64,
    tx_idx: i32,
    seams: Vec<String>,
    /// Oracle-keeper refresh(). A real crossing, but ~1.58M/year of infrastructure
    /// traffic against ~114 real ones, so the default read filters it out.
    is_oracle: bool,
}

/// Decide whether a transaction crosses a seam. Returns the seam labels, empty when it
/// is an ordinary EVM transaction (the overwhelmingly common case).
fn crossing_of(
    tx_type: &str,
    method: &str,
    to_addr: Option<&str>,
    value_wei: &str,
    tx_type_byte: Option<i16>,
    solana_leg_count: usize,
    tx_result: Option<&Value>,
    origination: &str,
    solana_signer: Option<&str>,
    input_len: Option<i32>,
    has_cpi_target: bool,
) -> Vec<String> {
    let to_lower = to_addr.map(|s| s.to_ascii_lowercase());
    let action_tags = classify(&ClassifyInput {
        tx_type,
        method,
        to: to_lower.as_deref(),
        value_wei,
        tx_type_byte,
        solana_leg_count,
        logs: tx_result.and_then(|v| v.get("logs")),
        input_len,
    });
    // Mirrors addresses.rs: a synthetic (Solana-controlled) sender is a non-ecdsa tx
    // carrying a solana_signer.
    let controlled_by_solana = origination != "ecdsa" && solana_signer.is_some();
    seams(&SeamInput {
        action_tags: &action_tags,
        to: to_lower.as_deref(),
        tx_type,
        origination,
        controlled_by_solana,
        tx_type_byte,
        has_cpi_target,
    })
    .iter()
    .map(|s| s.as_str().to_string())
    .collect()
}

async fn read_cursor(pool: &PgPool, chain_id: i64, worker: &str) -> anyhow::Result<i64> {
    let row = sqlx::query(
        "SELECT COALESCE(last_processed, 0) AS c FROM rome_via.enrich_cursors
         WHERE chain_id = $1 AND worker = $2",
    )
    .bind(chain_id).bind(worker)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| r.get::<i64, _>("c")).unwrap_or(0))
}

/// Persist the crossings from one batch and advance the cursor, atomically. A batch with
/// no crossings still advances the cursor — that is the common case.
async fn write_batch(pool: &PgPool, chain_id: i64, worker: &str, rows: &[Crossing], cursor: i64) -> anyhow::Result<()> {
    let mut tx = pool.begin().await?;
    for r in rows {
        sqlx::query(
            "INSERT INTO rome_via.cross_vm_seams
               (chain_id, tx_hash, slot_number, tx_idx, seams, is_oracle, updated_at)
             VALUES ($1,$2,$3,$4,$5,$6, now())
             ON CONFLICT (chain_id, tx_hash) DO UPDATE
               SET seams = EXCLUDED.seams, is_oracle = EXCLUDED.is_oracle, updated_at = now()",
        )
        .bind(chain_id).bind(&r.tx_hash).bind(r.slot_number).bind(r.tx_idx).bind(&r.seams).bind(r.is_oracle)
        .execute(&mut *tx).await?;
    }
    sqlx::query(
        "INSERT INTO rome_via.enrich_cursors (chain_id, worker, last_processed, last_processed_at)
         VALUES ($1, $2, $3, NOW())
         ON CONFLICT (chain_id, worker) DO UPDATE
           SET last_processed = EXCLUDED.last_processed, last_processed_at = NOW()",
    )
    .bind(chain_id).bind(worker).bind(cursor).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}

/// Examine one batch starting strictly after `from_slot`; persist any crossings and
/// advance `worker`'s cursor to the highest slot seen. Returns (rows examined, max slot).
async fn process_batch(
    pool: &PgPool,
    chain_id: i64,
    worker: &str,
    from_slot: i64,
    batch_size: i64,
) -> anyhow::Result<(usize, i64)> {
    // Propagate, never swallow: a silently-empty result is indistinguishable from
    // "caught up", so a failing query would leave the worker looking healthy while
    // doing nothing. Let it surface and let the supervisor restart with backoff.
    let rows = sqlx::query(rows_query())
        .bind(chain_id).bind(from_slot).bind(batch_size)
        .fetch_all(pool).await?;
    if rows.is_empty() {
        return Ok((0, from_slot));
    }
    let mut crossings = Vec::new();
    let mut max_slot = from_slot;
    for r in &rows {
        let slot: i64 = r.get("slot_number");
        max_slot = max_slot.max(slot);
        let tx_result: Option<Value> = r.try_get("tx_result").ok().flatten();
        let seam_list = crossing_of(
            &r.get::<String, _>("rome_tx_type"),
            &r.get::<String, _>("method"),
            r.try_get::<Option<String>, _>("to_addr").ok().flatten().as_deref(),
            &r.try_get::<Option<String>, _>("value_wei").ok().flatten().unwrap_or_else(|| "0".into()),
            r.try_get::<Option<i16>, _>("tx_type_byte").ok().flatten(),
            r.try_get::<i32, _>("solana_leg_count").unwrap_or(0) as usize,
            tx_result.as_ref(),
            &r.get::<String, _>("origination"),
            r.try_get::<Option<String>, _>("solana_signer").ok().flatten().as_deref(),
            r.try_get::<Option<i32>, _>("input_len").ok().flatten(),
            r.try_get::<bool, _>("has_cpi_target").unwrap_or(false),
        );
        if seam_list.is_empty() {
            continue; // ordinary EVM tx — the feed does not record it
        }
        crossings.push(Crossing {
            tx_hash: r.get("tx_hash"),
            slot_number: slot,
            tx_idx: r.get("tx_idx"),
            seams: seam_list,
            is_oracle: is_oracle_method(
                r.try_get::<Option<String>, _>("method_id").ok().flatten().as_deref(),
            ),
        });
    }
    write_batch(pool, chain_id, worker, &crossings, max_slot).await?;
    tracing::debug!(chain_id, worker, examined = rows.len(), crossings = crossings.len(), max_slot, "cross_vm_seams");
    Ok((rows.len(), max_slot))
}

pub async fn run(
    pool: PgPool,
    chain_id: i64,
    poll_interval: Duration,
    batch_size: i64,
) -> anyhow::Result<()> {
    loop {
        // ── forward: strictly `slot > cursor`, so the cursor always advances and a
        //    backlog drains. Bounded by the backlog, not by the poll interval. ──
        let mut cursor = read_cursor(&pool, chain_id, FORWARD).await?;
        loop {
            let (n, max_slot) = process_batch(&pool, chain_id, FORWARD, cursor, batch_size).await?;
            // No rows, or a batch that could not advance the slot cursor (every row in
            // it shares one slot) — either way there is nothing further to drain.
            if n == 0 || max_slot <= cursor {
                break;
            }
            cursor = max_slot;
            if (n as i64) < batch_size {
                break; // partial batch => caught up
            }
        }

        // ── recheck: one batch per poll over the trailing window, with its OWN cursor
        //    that wraps. Never blocks or starves the forward pass. ──
        let tip = read_cursor(&pool, chain_id, FORWARD).await?;
        let window_start = (tip - RECHECK_SLOTS).max(0);
        let mut recheck = read_cursor(&pool, chain_id, RECHECK).await?;
        if recheck < window_start || recheck >= tip {
            recheck = window_start; // wrap to the start of the window
        }
        let (n, max_slot) = process_batch(&pool, chain_id, RECHECK, recheck, batch_size).await?;
        if n == 0 || max_slot <= recheck {
            // Window exhausted — restart the sweep on the next poll.
            write_batch(&pool, chain_id, RECHECK, &[], window_start).await?;
        }

        tokio::time::sleep(poll_interval).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn ordinary_evm_traffic_is_not_recorded() {
        // The whole point of the feed: ~27,000 of these for every crossing, and none of
        // them produce a row.
        let s = crossing_of("Rhea", "transfer(address,uint256)", Some("0xabc"), "0", Some(2), 0,
            Some(&json!({"exit_reason":{"code":0}})), "ecdsa", None, None, false);
        assert!(s.is_empty(), "plain EVM tx must not enter the seam feed: {s:?}");
    }

    #[test]
    fn a_solana_originated_tx_is_recorded_on_the_sol_to_evm_seam() {
        let s = crossing_of("Rhea", "0x", Some("0xabc"), "0", None, 0, None, "solana_unsigned", Some("SoLsig"), None, false);
        assert_eq!(s, vec!["sol_to_evm"]);
    }

    #[test]
    fn a_romulus_tx_is_recorded_on_the_evm_to_sol_seam() {
        let s = crossing_of("Romulus", "0x", Some("0xabc"), "0", None, 2, None, "ecdsa", None, None, false);
        assert_eq!(s, vec!["evm_to_sol"]);
    }

    #[test]
    fn a_cpi_target_tx_is_recorded_on_the_evm_to_sol_seam() {
        // A Rhea, ecdsa tx that CPI'd a Solana program at depth-2 (has_cpi_target = true) —
        // e.g. a rome-dex / mango swap. It reached into Solana, so it is an evm_to_sol
        // crossing even without a Romulus label, a solana_cpi tag, or to == precompile.
        // This is the plumbing test for the ~33k CPI crossings restored to the feed.
        let s = crossing_of("Rhea", "0x", Some("0xabc"), "0", None, 0, None, "ecdsa", None, None, true);
        assert_eq!(s, vec!["evm_to_sol"]);
    }

    #[test]
    fn a_bridge_deposit_is_recorded_even_without_tags() {
        // 0x7E deposit type byte covers rows indexed before the tag existed.
        let s = crossing_of("Rhea", "0x", Some("0xabc"), "0", Some(0x7e), 0, None, "ecdsa", None, None, false);
        assert_eq!(s, vec!["bridge"]);
    }

    /// Regression: the forward pass must start STRICTLY after its cursor.
    ///
    /// The first cut of this worker read from `cursor - RECHECK_SLOTS` on the forward
    /// pass. At ~100 tx/s that window holds ~200k rows against a 500-row batch, so every
    /// batch was the oldest slice of the window, its max slot never exceeded the cursor,
    /// and the cursor froze. Worse, a full batch skipped the sleep, so it became a hot
    /// loop re-reading the same 500 rows forever while making no progress. The recheck
    /// gets its own wrapping cursor precisely so the forward pass can stay strict.
    #[test]
    fn forward_and_recheck_use_separate_cursors() {
        assert_ne!(FORWARD, RECHECK, "sharing one cursor freezes forward progress");
        assert_eq!(FORWARD, "cross_vm_seams");
        assert_eq!(RECHECK, "cross_vm_seams_recheck");
    }

    /// The recheck sweep stays inside the trailing window and wraps, so it is bounded
    /// and cannot run backwards forever or overtake the forward cursor.
    #[test]
    fn recheck_sweep_wraps_within_the_trailing_window() {
        let wrap = |tip: i64, pos: i64| {
            let start = (tip - RECHECK_SLOTS).max(0);
            if pos < start || pos >= tip { start } else { pos }
        };
        let tip = 100_000;
        let start = tip - RECHECK_SLOTS;
        assert_eq!(wrap(tip, 0), start, "stale cursor snaps to the window start");
        assert_eq!(wrap(tip, tip), start, "reaching the tip restarts the sweep");
        assert_eq!(wrap(tip, start + 10), start + 10, "mid-window position is kept");
        assert_eq!(wrap(500, 0), 0, "young chain: window floors at 0");
    }

    #[test]
    fn query_re_examines_a_trailing_window_in_feed_order() {
        let q = rows_query();
        assert!(q.contains("ebt.slot_number > $2"), "bounded by the cursor window");
        assert!(q.contains("ORDER BY ebt.slot_number ASC, ebt.tx_idx ASC"));
        assert!(q.contains("COALESCE(ccc.rome_tx_type, 'Rhea')"), "type resolved at write time");
    }

    /// Writing only crossings is what makes this worker able to keep up. Its predecessor
    /// wrote a row per transaction and was capped below the chain's tx rate.
    #[test]
    fn write_volume_is_a_tiny_fraction_of_the_chain() {
        // Measured on hadrian: 114 crossings out of 3,089,556 txs.
        let (crossings, total) = (114.0, 3_089_556.0);
        assert!(crossings / total < 0.001, "if crossings ever approach chain rate, revisit the shape");
    }
}
