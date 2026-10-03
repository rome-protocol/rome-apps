//! DoTxBatch trace enrichment worker.
//!
//! Polls indexed Solana txs (`evm_tx_sol_tx`), fetches each one's
//! `meta.log_messages` directly from Solana RPC, calls
//! `rome_evm_client::indexer::parsers::parse_batch_trace`, and persists
//! the structured per-sub-call + per-CPI breakdown as JSONB.
//!
//! Non-batch txs are skipped (parse_batch_trace returns None when the
//! logs don't contain the "Atomic batch transaction" start marker).
//!
//! Surfaced via rome-via-api at `GET /api/v1/txs/{hash}/batch-trace`.

use {
    rome_sdk::rome_evm_client::indexer::parsers::parse_batch_trace,
    serde::Deserialize,
    serde_json::json,
    sqlx::PgPool,
    std::time::Duration,
    tracing::{debug, warn},
};

pub async fn run(
    pool: PgPool,
    chain_id: i64,
    solana_rpc_url: String,
    poll_interval: Duration,
    batch_size: i64,
) -> anyhow::Result<()> {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;

    // F1: bounded retry for unretrievable txs. Without this, a tx pruned beyond
    // RPC retention stays a candidate forever (anti-join has no cursor), hot-spins
    // the loop, and if enough pile up, blocks newer txs from ever being traced.
    let mut trace_attempts: std::collections::HashMap<String, u32> = std::collections::HashMap::new();
    const MAX_TRACE_ATTEMPTS: u32 = 30;

    loop {
        // Pull the next batch of indexed Solana txs that we haven't traced yet.
        // Order by slot ascending so the indexed_at timestamps reflect arrival
        // order (useful for "recent batches" queries).
        let rows: Vec<(String, String)> = sqlx::query_as(
            r#"
            SELECT ets.evm_tx_hash, ets.sol_signature
            FROM rome_via.evm_tx_sol_tx ets
            LEFT JOIN rome_via.batch_traces bt
                ON bt.chain_id = ets.chain_id AND bt.tx_hash = ets.evm_tx_hash
            WHERE ets.chain_id = $1 AND bt.tx_hash IS NULL
            ORDER BY ets.slot_number ASC
            LIMIT $2
            "#,
        )
        .bind(chain_id)
        .bind(batch_size)
        .fetch_all(&pool)
        .await?;

        if rows.is_empty() {
            tokio::time::sleep(poll_interval).await;
            continue;
        }

        let mut had_indeterminate = false;
        for (evm_tx_hash, sol_sig) in rows {
            match fetch_log_messages(&http, &solana_rpc_url, &sol_sig).await {
                Ok(fetched) => {
                    let logs_for_parse: &[String] = fetched.as_deref().unwrap_or(&[]);
                    if let Some(trace) = parse_batch_trace(logs_for_parse) {
                        // Persist. Use INSERT … ON CONFLICT DO NOTHING so a
                        // racy concurrent insert (e.g., manual backfill) is
                        // idempotent.
                        let json_trace = match serde_json::to_value(&trace) {
                            Ok(v) => v,
                            Err(e) => {
                                warn!(tx = %evm_tx_hash, error = %e,
                                    "failed to serialize BatchTrace");
                                continue;
                            }
                        };
                        if let Err(e) = sqlx::query(
                            r#"
                            INSERT INTO rome_via.batch_traces
                                (chain_id, tx_hash, trace)
                            VALUES ($1, $2, $3)
                            ON CONFLICT (chain_id, tx_hash) DO NOTHING
                            "#,
                        )
                        .bind(chain_id)
                        .bind(&evm_tx_hash)
                        .bind(&json_trace)
                        .execute(&pool)
                        .await
                        {
                            warn!(tx = %evm_tx_hash, error = %e,
                                "failed to persist batch trace");
                        } else {
                            debug!(tx = %evm_tx_hash,
                                sub_calls = trace.sub_calls.len(),
                                "indexed batch trace");
                            trace_attempts.remove(&evm_tx_hash);
                        }
                    } else if super::rpc_verdict::may_sentinel_not_a_batch(&fetched) {
                        // Non-batch tx — record a sentinel so we don't re-poll
                        // it forever. JSONB `null` doesn't match the table
                        // schema (trace is NOT NULL), so persist an empty
                        // marker object with a discriminator.
                        if let Err(e) = sqlx::query(
                            r#"
                            INSERT INTO rome_via.batch_traces
                                (chain_id, tx_hash, trace)
                            VALUES ($1, $2, $3)
                            ON CONFLICT (chain_id, tx_hash) DO NOTHING
                            "#,
                        )
                        .bind(chain_id)
                        .bind(&evm_tx_hash)
                        .bind(json!({"not_a_batch": true}))
                        .execute(&pool)
                        .await
                        {
                            warn!(tx = %evm_tx_hash, error = %e,
                                "failed to record non-batch sentinel");
                        }
                        trace_attempts.remove(&evm_tx_hash);
                    } else {
                        // Terminal indeterminate (pruned/null/meta-absent/truncated):
                        // retry, but after MAX_TRACE_ATTEMPTS record a bounded
                        // unretrievable sentinel so the anti-join stops returning it
                        // forever (prevents the pruned-backlog wedge). Keeps
                        // not_a_batch:true so existing consumers treat it as "no trace".
                        had_indeterminate = true;
                        let n = trace_attempts.entry(evm_tx_hash.clone()).or_insert(0);
                        *n += 1;
                        let reached = *n;
                        if super::rpc_verdict::should_force_advance(reached, MAX_TRACE_ATTEMPTS) {
                            if let Err(e) = sqlx::query(
                                r#"
                                INSERT INTO rome_via.batch_traces
                                    (chain_id, tx_hash, trace)
                                VALUES ($1, $2, $3)
                                ON CONFLICT (chain_id, tx_hash) DO NOTHING
                                "#,
                            )
                            .bind(chain_id)
                            .bind(&evm_tx_hash)
                            .bind(json!({"not_a_batch": true, "unretrievable": true}))
                            .execute(&pool)
                            .await
                            {
                                warn!(tx = %evm_tx_hash, error = %e,
                                    "failed to record unretrievable sentinel");
                            } else {
                                warn!(tx = %evm_tx_hash, attempts = reached,
                                    "batch trace unretrievable after MAX_TRACE_ATTEMPTS — recorded bounded sentinel [F1]");
                            }
                            trace_attempts.remove(&evm_tx_hash);
                        } else {
                            debug!(tx = %evm_tx_hash, attempts = reached,
                                "indeterminate Solana logs — will retry [E-N1/F1]");
                        }
                    }
                }
                Err(e) => {
                    // Transient transport error — retry indefinitely (never sentinel;
                    // a recovered fetch may still yield a real trace). [F1]
                    had_indeterminate = true;
                    warn!(tx = %evm_tx_hash, sig = %sol_sig, error = %e,
                        "failed to fetch Solana tx logs — will retry [F1]");
                }
            }
        }

        // F1: pace retries in real time (the anti-join has no cursor, so without
        // this the loop hot-spins the same failing batch under an RPC outage).
        if had_indeterminate {
            tokio::time::sleep(poll_interval).await;
        }
    }
}

#[derive(Deserialize)]
struct GetTxResponse {
    result: Option<GetTxResult>,
}

#[derive(Deserialize)]
struct GetTxResult {
    meta: Option<GetTxMeta>,
}

#[derive(Deserialize)]
struct GetTxMeta {
    #[serde(rename = "logMessages")]
    log_messages: Option<Vec<String>>,
}

async fn fetch_log_messages(
    http: &reqwest::Client,
    url: &str,
    sig: &str,
) -> anyhow::Result<Option<Vec<String>>> {
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [sig, {
            "encoding": "json",
            "commitment": "confirmed",
            "maxSupportedTransactionVersion": 1
        }]
    });

    let res: GetTxResponse = http
        .post(url)
        .json(&body)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    // result null/absent (pruned/unretrievable, or a JSON-RPC error body) → None,
    // so the caller never sentinels it. meta null → None (incomplete). Present →
    // Some(logs) (logMessages null → Some(empty), which the sentinel gate rejects). [E-N1]
    Ok(res
        .result
        .and_then(|r| r.meta)
        .map(|m| m.log_messages.unwrap_or_default()))
}
