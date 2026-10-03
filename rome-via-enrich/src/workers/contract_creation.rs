//! Contract-creation worker (B4).
//!
//! Surfaces a contract's **creator** and **creation transaction** on the
//! address page. A directly-deployed contract has a tx with `to = NULL` and
//! init code (`input_len > 0`); its `eth_getTransactionReceipt` carries the
//! created address in `contractAddress` and the deployer in `from`. This worker
//! walks those creation txs by slot, fetches each receipt, and records
//! `(creator, creation_tx)` on the created address's `contract_labels` row.
//!
//! ## Coverage
//! Direct deploys only. Factory-deployed contracts (SPL wrappers, Uniswap
//! pools) have `to = factory`, so the created address is in a log / internal
//! CREATE, not the top-level receipt — those aren't caught here. Token wrappers
//! already carry a creator via `factory_tokens` (the `TokenCreated` event), so
//! the residual miss is non-token factory-deployed contracts. Verified live on
//! hadrian 2026-07-31: a `to=NULL` deploy receipt returns
//! `contractAddress = 0x9e43…` (40KB of code) with `from` = the deployer.
//!
//! Cursor: `enrich_cursors` row `worker = 'contract_creation'`, advancing by
//! `eth_block_txs.slot_number` (evm_tx has no slot column). Starts at 0 so the
//! first deploy backfills history; re-runs are no-ops.

use std::time::Duration;

use sqlx::PgPool;
use tracing::{debug, info, warn};

/// Select creation-tx candidates after the cursor: `to = NULL` with init code,
/// joined to eth_block_txs for the slot keyset. Ordered so the cursor advances
/// monotonically. The `ix_evm_tx_to (chain_id, to_addr)` index serves the NULL
/// predicate; the created set is small (hundreds), so this is cheap.
fn creation_candidates_sql() -> &'static str {
    r#"
    SELECT ebt.slot_number, et.tx_hash, COALESCE(et.from_addr, et.from_address) AS from_addr
      FROM rome_via.evm_tx et
      JOIN rome_via.eth_block_txs ebt
        ON ebt.tx_hash = et.tx_hash AND ebt.chain_id = et.chain_id
     WHERE et.chain_id = $1
       AND et.to_addr IS NULL
       AND et.input_len > 0
       AND ebt.slot_number > $2
     ORDER BY ebt.slot_number ASC, et.tx_hash ASC
     LIMIT $3
    "#
}

/// Extract `(created_address, creator)` from an `eth_getTransactionReceipt`
/// result. `None` when the tx created no contract (`contractAddress` null/empty)
/// or the shape is unexpected. Both lowercased to match how addresses are
/// stored. Pure — unit-tested.
fn parse_creation(result: &serde_json::Value) -> Option<(String, String)> {
    let ca = result.get("contractAddress")?.as_str()?.trim();
    if ca.is_empty() || ca == "0x" {
        return None;
    }
    let from = result.get("from")?.as_str()?.trim();
    if from.is_empty() {
        return None;
    }
    Some((ca.to_lowercase(), from.to_lowercase()))
}

/// Fetch a tx receipt via the proxy's `eth_getTransactionReceipt`. Transport
/// failures (timeout / connection / non-2xx) propagate as `Err` (F6:
/// Transient — recovers, hold indefinitely). A missing `result` key or a
/// null result is `Ok(None)` (F6: Terminal — not reliably retrievable; see
/// the call site).
async fn eth_get_receipt(
    client: &reqwest::Client,
    proxy_url: &str,
    tx_hash: &str,
) -> anyhow::Result<Option<serde_json::Value>> {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getTransactionReceipt",
        "params": [tx_hash]
    });
    let resp: serde_json::Value = client
        .post(proxy_url)
        .json(&body)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let Some(result) = resp.get("result") else {
        return Ok(None);
    };
    if result.is_null() {
        return Ok(None);
    }
    Ok(Some(result.clone()))
}

pub async fn run(
    pool: PgPool,
    chain_id: i64,
    proxy_url: String,
    poll_interval: Duration,
    batch_size: i64,
) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;

    const MAX_HOLD: u32 = 30;
    let mut hold = super::rpc_verdict::HoldState::clear();

    loop {
        let cursor: i64 = sqlx::query_scalar(
            "SELECT COALESCE(last_processed, 0)
             FROM rome_via.enrich_cursors
             WHERE chain_id = $1 AND worker = 'contract_creation'",
        )
        .bind(chain_id)
        .fetch_optional(&pool)
        .await?
        .unwrap_or(0);

        let rows: Vec<(i64, String, Option<String>)> = sqlx::query_as(creation_candidates_sql())
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
        let slots: Vec<i64> = rows.iter().map(|r| r.0).collect();
        let plan = super::batch_cursor::plan_batch(&slots, batch_size as usize);
        if plan.giant_slot {
            warn!(worker = "contract_creation", slot = slots[0],
                  "single slot fills an entire batch; tail beyond batch_size cannot be deferred (unexpected on one-block-per-slot chains)");
        }

        let mut earliest_miss: Option<(i64, super::rpc_verdict::MissKind)> = None;

        for (slot_number, tx_hash, from_addr) in &rows[..plan.process_count] {
            let result = match eth_get_receipt(&client, &proxy_url, tx_hash).await {
                Ok(Some(r)) => r,
                // RPC Ok(None): result absent/null — not reliably retrievable
                // (mirrors cross_chain's fetch_tx_programs Ok(None) handling).
                // Terminal: won't resolve by waiting — give up and advance
                // past the stuck slot after MAX_HOLD polls rather than
                // wedging the worker on a permanently-missing receipt.
                Ok(None) => {
                    debug!(%tx_hash, "no receipt for creation candidate — deferring");
                    if earliest_miss.is_none() {
                        earliest_miss =
                            Some((*slot_number, super::rpc_verdict::MissKind::Terminal));
                    }
                    continue;
                }
                // RPC transport Err: transient (proxy hiccup / timeout) —
                // recovers, so hold indefinitely rather than force-advancing
                // past a receipt that will show up once the proxy recovers.
                Err(e) => {
                    warn!(%tx_hash, error = %e, "eth_getTransactionReceipt failed — deferring");
                    if earliest_miss.is_none() {
                        earliest_miss =
                            Some((*slot_number, super::rpc_verdict::MissKind::Transient));
                    }
                    continue;
                }
            };
            let Some((created, creator)) = parse_creation(&result) else {
                // to=NULL + init code but the receipt reports no contractAddress
                // (e.g. a reverted deploy). Nothing to record.
                continue;
            };
            // Record creator + creation_tx on the created contract's row. The
            // `from` on the tx is the canonical deployer; prefer the receipt's
            // `from` and fall back to the indexed from_addr.
            let creator = if !creator.is_empty() {
                creator
            } else {
                from_addr.clone().unwrap_or_default()
            };
            let res = sqlx::query(
                r#"
                INSERT INTO rome_via.contract_labels
                    (chain_id, address, creator, creation_tx, updated_at)
                VALUES ($1, $2, $3, $4, NOW())
                ON CONFLICT (chain_id, address) DO UPDATE
                  SET creator      = COALESCE(rome_via.contract_labels.creator, EXCLUDED.creator),
                      creation_tx  = COALESCE(rome_via.contract_labels.creation_tx, EXCLUDED.creation_tx),
                      updated_at   = NOW()
                "#,
            )
            .bind(chain_id)
            .bind(&created)
            .bind(&creator)
            .bind(tx_hash)
            .execute(&pool)
            .await;
            match res {
                Ok(_) => info!(address = %created, creator = %creator, tx = %tx_hash, "recorded contract creation"),
                Err(e) => {
                    warn!(address = %created, error = %e, "failed to record contract creation");
                    if earliest_miss.is_none() {
                        earliest_miss =
                            Some((*slot_number, super::rpc_verdict::db_miss_kind(&e)));
                    }
                }
            }
        }

        // F6: don't commit past a swallowed receipt-fetch/write miss; hold
        // the cursor just below the earliest miss so it re-processes next
        // poll.
        let outcome =
            super::rpc_verdict::valve_persist_cursor(plan.new_cursor, earliest_miss, hold, MAX_HOLD);
        hold = outcome.hold;
        if outcome.gave_up {
            warn!(worker = "contract_creation",
                "terminal miss unresolved for MAX_HOLD polls; advancing past stuck slot [F6]");
        }

        let _ = sqlx::query(
            "INSERT INTO rome_via.enrich_cursors (chain_id, worker, last_processed, last_processed_at)
             VALUES ($1, 'contract_creation', $2, NOW())
             ON CONFLICT (chain_id, worker) DO UPDATE
                 SET last_processed    = EXCLUDED.last_processed,
                     last_processed_at = NOW()",
        )
        .bind(chain_id)
        .bind(outcome.persist)
        .execute(&pool)
        .await;

        if earliest_miss.is_some() {
            tokio::time::sleep(poll_interval).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_creation_extracts_and_lowercases() {
        let r = serde_json::json!({
            "contractAddress": "0x9E43411436492188DEF8BFFCB7F869DC77516B2C",
            "from": "0x7283ED89F1AEBA4A4D024B6BC0C29659F85D4444",
            "status": "0x1"
        });
        assert_eq!(
            parse_creation(&r),
            Some((
                "0x9e43411436492188def8bffcb7f869dc77516b2c".to_string(),
                "0x7283ed89f1aeba4a4d024b6bc0c29659f85d4444".to_string()
            ))
        );
    }

    #[test]
    fn parse_creation_none_when_no_contract_created() {
        // A plain call receipt: contractAddress is null.
        let r = serde_json::json!({ "contractAddress": serde_json::Value::Null, "from": "0xabc" });
        assert_eq!(parse_creation(&r), None);
        let r2 = serde_json::json!({ "contractAddress": "0x", "from": "0xabc" });
        assert_eq!(parse_creation(&r2), None);
        // Missing from.
        let r3 = serde_json::json!({ "contractAddress": "0x9e43411436492188def8bffcb7f869dc77516b2c" });
        assert_eq!(parse_creation(&r3), None);
    }

    #[test]
    fn candidates_sql_filters_creations_and_keysets_by_slot() {
        let q = creation_candidates_sql();
        assert!(q.contains("to_addr IS NULL"), "must select creation txs");
        assert!(q.contains("input_len > 0"), "must require init code (exclude gap rows)");
        assert!(q.contains("slot_number > $2"), "must keyset by the cursor");
    }
}
