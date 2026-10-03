/// Meta-Hook Router Solana-tx indexer.
///
/// Watches `getSignaturesForAddress(<router>)` for every Solana tx that
/// invokes the Meta-Hook Router program, regardless of whether that tx is
/// linked to an indexed EVM tx. For each new sig:
///   * Parse the router's anchor log (`Meta-Hook Router: executing N hooks
///     for mint <MINT>`) for the mint + expected hook count.
///   * Run the shared `parse_hook_executions` parser to extract per-hook
///     outcomes.
///   * Upsert into `meta_hook_invocations` (the user-facing record) and into
///     `hook_executions` keyed by `tx_hash = 'sol:<sig>'` (so the existing
///     hook-execution endpoints surface them alongside EVM hooks).
///
/// Cursor: `slot_number` stored in `enrich_cursors` under worker
/// `'meta_hook_indexer'`. Each loop only processes signatures with
/// `slot > cursor` and advances.
///
/// Failure modes parameterised: an RPC hiccup on one sig logs a warning and
/// moves on; the cursor is only advanced past successfully ingested batches.
use sqlx::PgPool;
use std::time::Duration;
use tracing::{debug, info, warn};

use crate::workers::hook_executions::parse_hook_executions;

pub async fn run(
    pool: PgPool,
    chain_id: i64,
    solana_rpc_url: String,
    meta_hook_program_id: String,
    poll_interval: Duration,
    batch_size: i64,
) -> anyhow::Result<()> {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()?;
    let fetch_limit = batch_size.clamp(50, 1000) as usize;

    const MAX_HOLD: u32 = 30;
    let mut hold = super::rpc_verdict::HoldState::clear();

    info!(
        worker = "meta_hook_indexer",
        router = %meta_hook_program_id,
        fetch_limit,
        "starting"
    );

    loop {
        let cursor_slot: i64 = sqlx::query_scalar(
            "SELECT COALESCE(last_processed, 0)
             FROM rome_via.enrich_cursors
             WHERE chain_id = $1 AND worker = 'meta_hook_indexer'",
        )
        .bind(chain_id)
        .fetch_optional(&pool)
        .await?
        .unwrap_or(0);

        let sig_infos =
            match fetch_recent_signatures(&http, &solana_rpc_url, &meta_hook_program_id, fetch_limit)
                .await
            {
                Ok(s) => s,
                Err(e) => {
                    warn!(error = %e, "getSignaturesForAddress failed — retrying after poll");
                    tokio::time::sleep(poll_interval).await;
                    continue;
                }
            };

        // RPC returns newest-first. Filter to sigs above cursor, then process
        // oldest-first so the cursor advance is monotonic.
        let mut to_process: Vec<SignatureInfo> = sig_infos
            .into_iter()
            .filter(|s| s.slot > cursor_slot)
            .collect();
        to_process.sort_by_key(|s| s.slot);

        if to_process.is_empty() {
            tokio::time::sleep(poll_interval).await;
            continue;
        }

        let mut max_slot = cursor_slot;
        let mut processed = 0usize;
        let mut earliest_miss: Option<(i64, super::rpc_verdict::MissKind)> = None;

        for info_ in &to_process {
            if info_.slot > max_slot {
                max_slot = info_.slot;
            }

            match fetch_transaction(&http, &solana_rpc_url, &info_.signature).await {
                Ok(Some(tx_blob)) => {
                    match ingest_invocation(&pool, chain_id, info_, &tx_blob, &meta_hook_program_id)
                        .await
                    {
                        Ok(()) => processed += 1,
                        // F6: a swallowed ingest write must hold the cursor —
                        // otherwise this invocation's rows are lost forever.
                        Err(e) => {
                            warn!(sig = %info_.signature, error = %e, "ingest failed");
                            if earliest_miss.is_none() {
                                earliest_miss =
                                    Some((info_.slot, super::rpc_verdict::db_miss_kind(&e)));
                            }
                        }
                    }
                }
                // F6/E-N7: pruned = TERMINAL miss (never resolves) — hold
                // rather than advancing past a sig whose invocation is lost.
                Ok(None) => {
                    debug!(sig = %info_.signature, "tx not retrievable (pruned) — skipping");
                    if earliest_miss.is_none() {
                        earliest_miss = Some((info_.slot, super::rpc_verdict::MissKind::Terminal));
                    }
                }
                // F6/E-N7: transport Err = TRANSIENT miss (recovers) — held indefinitely.
                Err(e) => {
                    warn!(sig = %info_.signature, error = %e, "getTransaction failed — skipping");
                    if earliest_miss.is_none() {
                        earliest_miss = Some((info_.slot, super::rpc_verdict::MissKind::Transient));
                    }
                }
            }
        }

        // F6: don't commit past a miss (RPC or swallowed DB write). Hold the
        // cursor just below the earliest miss so it re-processes next poll;
        // the writes above are ON CONFLICT-idempotent, so replay is safe.
        let outcome =
            super::rpc_verdict::valve_persist_cursor(max_slot, earliest_miss, hold, MAX_HOLD);
        hold = outcome.hold;
        if outcome.gave_up {
            warn!(worker = "meta_hook_indexer",
                "terminal miss unresolved for MAX_HOLD polls; advancing past stuck slot [F6]");
        }

        sqlx::query(
            r#"
            INSERT INTO rome_via.enrich_cursors
                (chain_id, worker, last_processed, last_processed_at)
            VALUES ($1, 'meta_hook_indexer', $2, NOW())
            ON CONFLICT (chain_id, worker)
            DO UPDATE SET last_processed = EXCLUDED.last_processed,
                          last_processed_at = EXCLUDED.last_processed_at
            "#,
        )
        .bind(chain_id)
        .bind(outcome.persist)
        .execute(&pool)
        .await?;

        debug!(
            worker = "meta_hook_indexer",
            processed,
            new_cursor = outcome.persist,
            "batch done"
        );

        if earliest_miss.is_some() {
            tokio::time::sleep(poll_interval).await;
        }
    }
}

#[derive(Debug, Clone)]
struct SignatureInfo {
    signature: String,
    slot: i64,
    block_time: Option<i64>,
    err: Option<serde_json::Value>,
}

async fn fetch_recent_signatures(
    http: &reqwest::Client,
    rpc_url: &str,
    router: &str,
    limit: usize,
) -> anyhow::Result<Vec<SignatureInfo>> {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSignaturesForAddress",
        "params": [router, { "limit": limit }],
    });
    let resp: serde_json::Value = http
        .post(rpc_url)
        .json(&body)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let arr = resp
        .get("result")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let mut out = Vec::with_capacity(arr.len());
    for entry in arr {
        let Some(signature) = entry.get("signature").and_then(|v| v.as_str()) else {
            continue;
        };
        let slot = entry.get("slot").and_then(|v| v.as_i64()).unwrap_or(0);
        let block_time = entry.get("blockTime").and_then(|v| v.as_i64());
        let err = entry.get("err").cloned();
        out.push(SignatureInfo {
            signature: signature.to_string(),
            slot,
            block_time,
            err,
        });
    }
    Ok(out)
}

async fn fetch_transaction(
    http: &reqwest::Client,
    rpc_url: &str,
    sig: &str,
) -> anyhow::Result<Option<serde_json::Value>> {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [sig, {
            "commitment": "confirmed",
            "maxSupportedTransactionVersion": 1,
        }],
    });
    let resp: serde_json::Value = http
        .post(rpc_url)
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

async fn ingest_invocation(
    pool: &PgPool,
    chain_id: i64,
    info_: &SignatureInfo,
    tx: &serde_json::Value,
    meta_hook_program_id: &str,
) -> Result<(), sqlx::Error> {
    let empty_logs: Vec<serde_json::Value> = Vec::new();
    let log_messages: Vec<String> = tx
        .get("meta")
        .and_then(|m| m.get("logMessages"))
        .and_then(|l| l.as_array())
        .unwrap_or(&empty_logs)
        .iter()
        .filter_map(|v| v.as_str().map(String::from))
        .collect();

    let mint = extract_mint(&log_messages);
    let hook_count = extract_hook_count(&log_messages);
    let outcome = outcome_from_err(&info_.err);
    let fee_payer = extract_fee_payer(tx);
    let block_time = info_.block_time.or_else(|| tx.get("blockTime").and_then(|v| v.as_i64()));

    sqlx::query(
        r#"
        INSERT INTO rome_via.meta_hook_invocations
            (chain_id, sol_signature, slot_number, block_time,
             mint, hook_count, outcome, fee_payer)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (chain_id, sol_signature) DO UPDATE SET
            slot_number = EXCLUDED.slot_number,
            block_time  = EXCLUDED.block_time,
            mint        = EXCLUDED.mint,
            hook_count  = EXCLUDED.hook_count,
            outcome     = EXCLUDED.outcome,
            fee_payer   = EXCLUDED.fee_payer
        "#,
    )
    .bind(chain_id)
    .bind(&info_.signature)
    .bind(info_.slot)
    .bind(block_time)
    .bind(mint.as_deref())
    .bind(hook_count as i32)
    .bind(&outcome)
    .bind(fee_payer.as_deref())
    .execute(pool)
    .await?;

    let execs = parse_hook_executions(&log_messages, meta_hook_program_id);
    if execs.is_empty() {
        return Ok(());
    }
    let synthetic_hash = format!("sol:{}", info_.signature);
    let ts = block_time.and_then(|e| chrono::DateTime::from_timestamp(e, 0));

    // F6: propagate (rather than swallow) so the caller holds the cursor and
    // retries the whole signature — the INSERT is ON CONFLICT-idempotent, so
    // replaying an already-ingested hook execution is safe.
    for exec in execs {
        sqlx::query(
            r#"
            INSERT INTO rome_via.hook_executions
                (chain_id, tx_hash, hook_address, hook_kind, result, reason, gas_used, block_number, timestamp)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (chain_id, tx_hash, hook_address) DO UPDATE SET
                hook_kind = EXCLUDED.hook_kind,
                result    = EXCLUDED.result,
                reason    = EXCLUDED.reason,
                gas_used  = EXCLUDED.gas_used
            "#,
        )
        .bind(chain_id)
        .bind(&synthetic_hash)
        .bind(&exec.hook_address)
        .bind(&exec.hook_kind)
        .bind(&exec.result)
        .bind(exec.reason.as_deref())
        .bind(exec.gas_used)
        .bind(0_i64)
        .bind(ts)
        .execute(pool)
        .await?;
    }

    Ok(())
}

/// "Program log: Meta-Hook Router: executing N hooks for mint <MINT>"
fn extract_mint(logs: &[String]) -> Option<String> {
    const MARKER: &str = "for mint ";
    for line in logs {
        if let Some(idx) = line.find(MARKER) {
            let tail = &line[idx + MARKER.len()..];
            let mint = tail.trim().trim_end_matches(|c: char| c == ',' || c.is_whitespace());
            if !mint.is_empty() {
                return Some(mint.to_string());
            }
        }
    }
    None
}

fn extract_hook_count(logs: &[String]) -> usize {
    const PREFIX: &str = "Program log: Meta-Hook Router: executing ";
    for line in logs {
        if let Some(rest) = line.strip_prefix(PREFIX) {
            if let Some(space) = rest.find(' ') {
                if let Ok(n) = rest[..space].parse() {
                    return n;
                }
            }
        }
    }
    0
}

fn outcome_from_err(err: &Option<serde_json::Value>) -> String {
    match err {
        None | Some(serde_json::Value::Null) => "success".to_string(),
        Some(v) => {
            // Heuristic: if any inner string mentions revert/reject, call it reject.
            let s = v.to_string().to_lowercase();
            if s.contains("revert") || s.contains("reject") || s.contains("custom") {
                "reject".to_string()
            } else {
                "error".to_string()
            }
        }
    }
}

fn extract_fee_payer(tx: &serde_json::Value) -> Option<String> {
    tx.get("transaction")
        .and_then(|t| t.get("message"))
        .and_then(|m| m.get("accountKeys"))
        .and_then(|a| a.as_array())
        .and_then(|arr| arr.first())
        .and_then(|v| {
            // jsonParsed format gives {"pubkey": "...", ...}; legacy gives bare string.
            v.as_str()
                .map(String::from)
                .or_else(|| v.get("pubkey").and_then(|p| p.as_str().map(String::from)))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_mint_from_router_log() {
        let logs = vec![
            "Program MetaHk... invoke [2]".to_string(),
            "Program log: Meta-Hook Router: executing 2 hooks for mint 8WbfJq6DaDZHhR6f4jexmk4XMawZwX9P5EG7xUPHiCSm".to_string(),
        ];
        assert_eq!(
            extract_mint(&logs).as_deref(),
            Some("8WbfJq6DaDZHhR6f4jexmk4XMawZwX9P5EG7xUPHiCSm"),
        );
    }

    #[test]
    fn mint_returns_none_when_anchor_missing() {
        let logs = vec!["Program log: other".to_string()];
        assert!(extract_mint(&logs).is_none());
    }

    #[test]
    fn hook_count_from_anchor() {
        let logs = vec![
            "Program log: Meta-Hook Router: executing 3 hooks for mint X".to_string(),
        ];
        assert_eq!(extract_hook_count(&logs), 3);
    }

    #[test]
    fn hook_count_zero_when_missing() {
        assert_eq!(extract_hook_count(&[]), 0);
    }

    #[test]
    fn outcome_success_when_err_null() {
        assert_eq!(outcome_from_err(&None), "success");
        assert_eq!(outcome_from_err(&Some(serde_json::Value::Null)), "success");
    }

    #[test]
    fn outcome_reject_on_custom_error() {
        let v = Some(serde_json::json!({ "InstructionError": [0, { "Custom": 6001 }] }));
        assert_eq!(outcome_from_err(&v), "reject");
    }
}
