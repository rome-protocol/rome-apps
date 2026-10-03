/// Hook execution tracking worker.
///
/// Fetches each indexed sol_signature from Solana RPC and parses the Meta-Hook
/// Router's log output to record one `hook_executions` row per actual hook
/// invocation. No registry heuristic — if a tx did not invoke the router, it
/// gets zero rows.
///
/// # Log shape parsed
/// ```text
/// Program <ROUTER_ID> invoke [N]
/// Program log: Meta-Hook Router: executing K hooks for mint <MINT>
///   Program <HOOK_PROG> invoke [N+1]
///   Program log: Instruction: MetaHook
///   Program log: Call: from <FROM_ADDR>, to <HOOK_EVM_ADDR>
///   Program <HOOK_PROG> consumed <G> of <BUDGET> compute units
///   Program <HOOK_PROG> success
/// Program log: Hook[i] slot=<j> result=<pass|reject> [reason]
/// ```
///
/// `hook_address` stored in the DB is the EVM hook contract (`HOOK_EVM_ADDR`) when
/// the hook is an EVM contract (invoked via the rome-evm program). For purely
/// native Solana hooks the Solana program ID is stored instead.
use sqlx::PgPool;
use std::time::Duration;
use tracing::{debug, warn};

pub async fn run(
    pool: PgPool,
    chain_id: i64,
    solana_rpc_url: String,
    meta_hook_program_id: String,
    poll_interval: Duration,
    batch_size: i64,
) -> anyhow::Result<()> {
    // One-shot: purge stale KYC placeholder rows from earlier heuristic runs.
    // This is idempotent — subsequent loops skip the deletion path.
    if let Err(e) = sqlx::query(
        "DELETE FROM rome_via.hook_executions
         WHERE chain_id = $1 AND hook_kind = 'evm_kyc'
           AND hook_address = 'HookKycProgram1111111111111111111111111111'",
    )
    .bind(chain_id)
    .execute(&pool)
    .await
    {
        warn!(error = %e, "failed to purge legacy KYC rows");
    }

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;

    let mut hold = super::rpc_verdict::HoldState::clear();
    const MAX_HOLD: u32 = 30;

    loop {
        let cursor: i64 = sqlx::query_scalar(
            "SELECT COALESCE(last_processed, 0)
             FROM rome_via.enrich_cursors
             WHERE chain_id = $1 AND worker = 'hook_executions'",
        )
        .bind(chain_id)
        .fetch_optional(&pool)
        .await?
        .unwrap_or(0);

        // Pull next batch of (sol_signature, evm_tx_hash, slot, block_number, ts).
        let rows: Vec<(String, String, i64, Option<i64>, Option<f64>)> = sqlx::query_as(
            r#"
            SELECT
                ets.sol_signature,
                ets.evm_tx_hash,
                ets.slot_number,
                eb.params_number,
                eb.params_block_timestamp::FLOAT8
            FROM rome_via.evm_tx_sol_tx ets
            JOIN rome_via.eth_block_txs ebt
                ON ebt.tx_hash = ets.evm_tx_hash
                AND ebt.chain_id = ets.chain_id
            JOIN rome_via.eth_block eb
                ON eb.slot_number = ebt.slot_number
                AND eb.slot_block_idx = ebt.slot_block_idx
                AND eb.chain_id = ebt.chain_id
            WHERE ets.chain_id = $1 AND ets.slot_number > $2
            ORDER BY ets.slot_number ASC, ets.evm_tx_hash ASC, ets.sol_signature ASC
            LIMIT $3
            "#,
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
        let slots: Vec<i64> = rows.iter().map(|r| r.2).collect();
        let plan = super::batch_cursor::plan_batch(&slots, batch_size as usize);
        if plan.giant_slot {
            warn!(worker = "hook_executions", slot = slots[0],
                  "single slot fills an entire batch; tail beyond batch_size cannot be deferred (unexpected on one-block-per-slot chains)");
        }

        let mut earliest_miss: Option<(i64, super::rpc_verdict::MissKind)> = None;

        for (sol_sig, evm_tx_hash, slot_number, block_number, ts_epoch) in &rows[..plan.process_count] {
            let logs = match fetch_tx_logs(&http, &solana_rpc_url, sol_sig).await {
                Ok(Some(l)) => l,
                // E-N7: pruned/unretrievable = TERMINAL miss — defer (hold cursor)
                // rather than committing past it with no hook rows written.
                Ok(None) => {
                    debug!(sol_sig, "tx not found on RPC (pruned) — deferring");
                    if earliest_miss.is_none() {
                        earliest_miss = Some((*slot_number, super::rpc_verdict::MissKind::Terminal));
                    }
                    continue;
                }
                // E-N7/F2: transport Err = TRANSIENT miss (recovers) — held indefinitely.
                Err(e) => {
                    warn!(sol_sig, error = %e, "RPC getTransaction failed — deferring");
                    if earliest_miss.is_none() {
                        earliest_miss = Some((*slot_number, super::rpc_verdict::MissKind::Transient));
                    }
                    continue;
                }
            };
            // E-N7/F4: empty logs (meta:null shape) — a real confirmed tx always
            // emits invoke/success lines, so empty means meta was absent and we
            // can't tell if the router fired. TERMINAL miss, defer.
            if logs.is_empty() {
                debug!(sol_sig, "empty Solana logs (meta absent) — deferring tx");
                if earliest_miss.is_none() {
                    earliest_miss = Some((*slot_number, super::rpc_verdict::MissKind::Terminal));
                }
                continue;
            }
            // E-N7: truncated logs are incomplete — the router marker or a hook line
            // could be beyond the cut. TERMINAL miss; defer rather than under-count.
            if super::rpc_verdict::logs_truncated(&logs) {
                debug!(sol_sig, "Solana logs truncated — deferring tx");
                if earliest_miss.is_none() {
                    earliest_miss = Some((*slot_number, super::rpc_verdict::MissKind::Terminal));
                }
                continue;
            }

            if !logs.iter().any(|l| l.contains(&meta_hook_program_id)) {
                // Fast path: no router invocation.
                continue;
            }

            let executions = parse_hook_executions(&logs, &meta_hook_program_id);
            let ts = ts_epoch.and_then(|e| chrono::DateTime::from_timestamp(e as i64, 0));
            let block_num = block_number.unwrap_or(0);

            for exec in executions {
                let insert = sqlx::query(
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
                .bind(evm_tx_hash)
                .bind(&exec.hook_address)
                .bind(&exec.hook_kind)
                .bind(&exec.result)
                .bind(exec.reason.as_deref())
                .bind(exec.gas_used)
                .bind(block_num)
                .bind(ts)
                .execute(&pool)
                .await;

                match insert {
                    Ok(_) => debug!(
                        sol_sig,
                        evm_tx_hash,
                        hook = exec.hook_address,
                        result = exec.result,
                        "recorded hook execution"
                    ),
                    Err(e) => {
                        warn!(
                            sol_sig,
                            evm_tx_hash,
                            hook = exec.hook_address,
                            error = %e,
                            "hook_execution insert failed"
                        );
                        // F6: a swallowed hook-row write must hold the cursor —
                        // otherwise this hook execution is lost forever.
                        if earliest_miss.is_none() {
                            earliest_miss =
                                Some((*slot_number, super::rpc_verdict::db_miss_kind(&e)));
                        }
                    }
                }
            }
        }

        // F6/E-N7: hold below the earliest miss (RPC or DB write); give up past
        // a TERMINAL stuck slot after MAX_HOLD (advance past only that slot);
        // hold a TRANSIENT miss indefinitely (outage stalls, not force-bleeds).
        let outcome =
            super::rpc_verdict::valve_persist_cursor(plan.new_cursor, earliest_miss, hold, MAX_HOLD);
        hold = outcome.hold;
        if outcome.gave_up {
            warn!(worker = "hook_executions",
                "terminal miss unresolved for MAX_HOLD polls; advancing past stuck slot [F6]");
        }
        let persist_cursor = outcome.persist;

        sqlx::query(
            r#"
            INSERT INTO rome_via.enrich_cursors (chain_id, worker, last_processed, last_processed_at)
            VALUES ($1, 'hook_executions', $2, NOW())
            ON CONFLICT (chain_id, worker)
            DO UPDATE SET last_processed = EXCLUDED.last_processed,
                          last_processed_at = EXCLUDED.last_processed_at
            "#,
        )
        .bind(chain_id)
        .bind(persist_cursor)
        .execute(&pool)
        .await?;

        tracing::debug!(
            worker = "hook_executions",
            processed = rows.len(),
            new_cursor = persist_cursor,
            "batch done"
        );

        // E-N7: pace retries in real time on any miss. [E-N7 F2]
        if earliest_miss.is_some() {
            tokio::time::sleep(poll_interval).await;
        }
    }
}

/// Call `getTransaction` and return `logMessages`, if the tx is retrievable.
async fn fetch_tx_logs(
    http: &reqwest::Client,
    rpc_url: &str,
    sig: &str,
) -> anyhow::Result<Option<Vec<String>>> {
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
    let logs = result
        .get("meta")
        .and_then(|m| m.get("logMessages"))
        .and_then(|l| l.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        .unwrap_or_default();
    Ok(Some(logs))
}

/// A parsed hook execution.
#[derive(Debug, PartialEq)]
pub struct ParsedHookExecution {
    pub hook_address: String,
    pub hook_kind: String,
    pub result: String,
    pub reason: Option<String>,
    pub gas_used: Option<i64>,
}

/// Extract hook executions from Solana log messages.
///
/// Walks the logs linearly and pairs each `Hook[i] ... result=X` tally with
/// the most recent inner CPI context (hook program, EVM target address,
/// compute consumed).
///
/// Anchor discipline: the router emits
/// `Meta-Hook Router: executing N hooks for mint ...` at the start of its
/// hook-execution path. That line is *only* present when the router is
/// actually iterating hooks — admin paths (initialize, register, etc.)
/// invoke the router without producing it, so its absence is not in itself
/// noteworthy. We warn loudly only when we observe a mismatch between the
/// anchor's declared count and the number of `Hook[N]` tallies we parsed,
/// since that indicates the grammar has drifted while *still* being the
/// execute-hooks path.
pub fn parse_hook_executions(
    logs: &[String],
    meta_hook_program_id: &str,
) -> Vec<ParsedHookExecution> {
    let expected_count = extract_router_hook_count(logs);

    let mut out = Vec::new();
    let mut router_active = false;
    let mut cur_hook_program: Option<String> = None;
    let mut cur_evm_target: Option<String> = None;
    let mut cur_compute: Option<i64> = None;
    let mut cur_reason: Option<String> = None;

    for line in logs {
        // Track router scope: we're inside the router between its invoke and success/failed.
        if let Some(prog) = line.strip_prefix("Program ") {
            if prog.starts_with(meta_hook_program_id) {
                if prog.contains(" invoke [") {
                    router_active = true;
                    continue;
                }
                if prog.ends_with(" success") || prog.contains(" failed") {
                    router_active = false;
                    cur_hook_program = None;
                    cur_evm_target = None;
                    cur_compute = None;
                    cur_reason = None;
                    continue;
                }
            }
        }

        if !router_active {
            continue;
        }

        // Inner CPI open: "Program <HOOK_PROG> invoke [3]"
        if let Some(rest) = line.strip_prefix("Program ") {
            if let Some(idx) = rest.find(" invoke [") {
                let prog = &rest[..idx];
                if prog != meta_hook_program_id {
                    cur_hook_program = Some(prog.to_string());
                    cur_evm_target = None;
                    cur_compute = None;
                    cur_reason = None;
                    continue;
                }
            }
            // Compute units line: "Program <X> consumed N of M compute units"
            if let Some(consumed_idx) = rest.find(" consumed ") {
                let after = &rest[consumed_idx + " consumed ".len()..];
                if let Some(of_idx) = after.find(" of ") {
                    if let Ok(n) = after[..of_idx].parse::<i64>() {
                        cur_compute = Some(n);
                    }
                }
                continue;
            }
            // Inner CPI failure: "Program <HOOK_PROG> failed: custom program error: 0x0"
            // — the hook reverted. Router aborts without emitting `Hook[N]`, so we must
            // synthesize the `reject` entry here using the reason captured earlier from
            // the "EVM callback reverted: ..." log. Match only the literal status-line
            // shape `" failed:"`; a trailing " failed" inside a Hook[N] reason line
            // like "... kyc failed" must not be treated as a CPI close.
            if let Some(failed_idx) = rest.find(" failed:") {
                let prog = &rest[..failed_idx];
                let is_current_hook = cur_hook_program
                    .as_deref()
                    .map(|p| p == prog)
                    .unwrap_or(false);
                if is_current_hook && prog != meta_hook_program_id {
                    let hook_address = cur_evm_target
                        .clone()
                        .or_else(|| cur_hook_program.clone())
                        .unwrap_or_default();
                    let hook_kind = if cur_evm_target.is_some() {
                        "evm_custom".to_string()
                    } else {
                        "native".to_string()
                    };
                    if !hook_address.is_empty() {
                        out.push(ParsedHookExecution {
                            hook_address,
                            hook_kind,
                            result: "reject".to_string(),
                            reason: cur_reason.clone().or_else(|| {
                                Some(rest[failed_idx..].trim_start_matches(" failed").trim_start_matches(':').trim().to_string())
                            }),
                            gas_used: cur_compute,
                        });
                    }
                    cur_hook_program = None;
                    cur_evm_target = None;
                    cur_compute = None;
                    cur_reason = None;
                }
                continue;
            }
        }

        // EVM target address from rome-evm's internal log.
        if let Some(call) = line.strip_prefix("Program log: Call: ") {
            // "from <A>, to <B>"
            if let Some(to_idx) = call.find(", to ") {
                let to = &call[to_idx + ", to ".len()..];
                let target = to.trim().trim_end_matches(',').to_string();
                cur_evm_target = Some(format_evm_address(&target));
            }
            continue;
        }

        // Revert trail captured for the *next* failure: "Program log: EVM callback
        // reverted: Revert(Reverted)" (or similar). Take the tail after the last colon.
        if let Some(revert_tail) = line.strip_prefix("Program log: EVM callback reverted: ") {
            cur_reason = Some(revert_tail.trim().to_string());
            continue;
        }

        // Terminal marker (success path): "Program log: Hook[i] slot=j result=pass|reject [reason]"
        if let Some(hook_info) = line.strip_prefix("Program log: Hook[") {
            let result = parse_hook_result(hook_info);
            let (result_word, reason) = result;

            let hook_address = cur_evm_target
                .clone()
                .or_else(|| cur_hook_program.clone())
                .unwrap_or_default();

            let hook_kind = if cur_evm_target.is_some() {
                "evm_custom".to_string()
            } else {
                "native".to_string()
            };

            if !hook_address.is_empty() {
                out.push(ParsedHookExecution {
                    hook_address,
                    hook_kind,
                    result: result_word,
                    reason: reason.or(cur_reason.clone()),
                    gas_used: cur_compute,
                });
            }

            // Reset per-hook state but stay in router scope.
            cur_evm_target = None;
            cur_compute = None;
            cur_reason = None;
        }
    }

    if let Some(expected) = expected_count {
        // The router aborts on the first reject, so expected>parsed is the
        // normal outcome when the last parsed entry is a reject — not a parser
        // bug. Warn only when the shortfall isn't explained by that.
        let aborted_on_reject = out
            .last()
            .map(|e| e.result == "reject")
            .unwrap_or(false);
        if out.len() != expected && !(out.len() < expected && aborted_on_reject) {
            warn!(
                expected,
                parsed = out.len(),
                "hook execution count mismatch vs router anchor — parser may have missed entries"
            );
        }
    }

    out
}

/// Scan logs for the Meta-Hook Router anchor line and extract `N` from
/// `Meta-Hook Router: executing N hooks for mint <X>`.
fn extract_router_hook_count(logs: &[String]) -> Option<usize> {
    const PREFIX: &str = "Program log: Meta-Hook Router: executing ";
    for line in logs {
        if let Some(rest) = line.strip_prefix(PREFIX) {
            if let Some(space) = rest.find(' ') {
                if let Ok(n) = rest[..space].parse() {
                    return Some(n);
                }
            }
        }
    }
    None
}

/// Parse "N] slot=M result=pass|reject [trailing reason]" into (result, reason).
fn parse_hook_result(s: &str) -> (String, Option<String>) {
    let Some(eq_idx) = s.find("result=") else {
        return ("unknown".to_string(), None);
    };
    let after = &s[eq_idx + "result=".len()..];
    let mut it = after.splitn(2, char::is_whitespace);
    let word = it.next().unwrap_or("unknown").trim().to_string();
    let reason = it.next().map(|r| r.trim().to_string()).filter(|r| !r.is_empty());
    (word, reason)
}

/// Normalise an EVM address log token into a `0x`-prefixed lowercase hex string.
fn format_evm_address(raw: &str) -> String {
    let s = raw.trim().trim_end_matches(|c: char| c == ',' || c.is_whitespace());
    if s.starts_with("0x") || s.starts_with("0X") {
        s.to_lowercase()
    } else {
        format!("0x{}", s.to_lowercase())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ROUTER: &str = "MetaHk1111111111111111111111111111111111111";

    fn lines(s: &str) -> Vec<String> {
        s.trim().lines().map(String::from).collect()
    }

    #[test]
    fn parses_two_pass_hooks() {
        let logs = lines(
            "Program TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb invoke [1]
Program log: Instruction: TransferChecked
Program MetaHk1111111111111111111111111111111111111 invoke [2]
Program log: Meta-Hook Router: executing 2 hooks for mint MINT
Program CmobH2vR6aUtQ8x4xd1LYNiH6k2G7PFT5StTgWqvy2VU invoke [3]
Program log: Instruction: MetaHook
Program log: Call: from 42d1e8f9e3f66d9b62e97274b842012be93f8537, to 3034c72c4857cba3500a461236d36ca207d7c984
Program log: Execute
Program CmobH2vR6aUtQ8x4xd1LYNiH6k2G7PFT5StTgWqvy2VU consumed 82909 of 1321937 compute units
Program CmobH2vR6aUtQ8x4xd1LYNiH6k2G7PFT5StTgWqvy2VU success
Program log: Hook[0] slot=0 result=pass
Program CmobH2vR6aUtQ8x4xd1LYNiH6k2G7PFT5StTgWqvy2VU invoke [3]
Program log: Instruction: MetaHook
Program log: Call: from 42d1e8f9e3f66d9b62e97274b842012be93f8537, to 54c4c8efe05ea40c0de1ef2608adf32a3c2e019c
Program log: Execute
Program CmobH2vR6aUtQ8x4xd1LYNiH6k2G7PFT5StTgWqvy2VU consumed 84261 of 1235910 compute units
Program CmobH2vR6aUtQ8x4xd1LYNiH6k2G7PFT5StTgWqvy2VU success
Program log: Hook[1] slot=1 result=pass
Program MetaHk1111111111111111111111111111111111111 success",
        );
        let out = parse_hook_executions(&logs, ROUTER);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].hook_address, "0x3034c72c4857cba3500a461236d36ca207d7c984");
        assert_eq!(out[0].result, "pass");
        assert_eq!(out[0].hook_kind, "evm_custom");
        assert_eq!(out[0].gas_used, Some(82909));
        assert_eq!(out[1].hook_address, "0x54c4c8efe05ea40c0de1ef2608adf32a3c2e019c");
        assert_eq!(out[1].result, "pass");
        assert_eq!(out[1].gas_used, Some(84261));
    }

    #[test]
    fn no_router_invocation_yields_no_executions() {
        let logs = lines(
            "Program ComputeBudget111111111111111111111111111111 invoke [1]
Program ComputeBudget111111111111111111111111111111 success
Program TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb invoke [1]
Program log: Instruction: Transfer
Program TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb success",
        );
        assert!(parse_hook_executions(&logs, ROUTER).is_empty());
    }

    #[test]
    fn parses_reject_with_reason() {
        let logs = lines(
            "Program MetaHk1111111111111111111111111111111111111 invoke [2]
Program log: Meta-Hook Router: executing 1 hooks for mint X
Program CmobH2vR6aUtQ8x4xd1LYNiH6k2G7PFT5StTgWqvy2VU invoke [3]
Program log: Call: from aaaa, to deadbeef00000000000000000000000000000000
Program CmobH2vR6aUtQ8x4xd1LYNiH6k2G7PFT5StTgWqvy2VU success
Program log: Hook[0] slot=0 result=reject revert: kyc failed
Program MetaHk1111111111111111111111111111111111111 success",
        );
        let out = parse_hook_executions(&logs, ROUTER);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].result, "reject");
        assert_eq!(out[0].reason.as_deref(), Some("revert: kyc failed"));
    }

    #[test]
    fn falls_back_to_program_id_when_no_evm_call() {
        let logs = lines(
            "Program MetaHk1111111111111111111111111111111111111 invoke [2]
Program NativeHook11111111111111111111111111111111 invoke [3]
Program NativeHook11111111111111111111111111111111 consumed 10 of 1000 compute units
Program NativeHook11111111111111111111111111111111 success
Program log: Hook[0] slot=0 result=pass
Program MetaHk1111111111111111111111111111111111111 success",
        );
        let out = parse_hook_executions(&logs, ROUTER);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].hook_address, "NativeHook11111111111111111111111111111111");
        assert_eq!(out[0].hook_kind, "native");
        assert_eq!(out[0].gas_used, Some(10));
    }

    #[test]
    fn parses_pass_then_fail_sequence() {
        // Real log shape observed from sig 4xPWJVkN... — hook 0 passes, hook 1
        // reverts via the EVM callback path (no Hook[1] tally emitted).
        let logs = lines(
            "Program MetaHk1111111111111111111111111111111111111 invoke [2]
Program log: Meta-Hook Router: executing 2 hooks for mint MINT
Program CmobH2vR6aUtQ8x4xd1LYNiH6k2G7PFT5StTgWqvy2VU invoke [3]
Program log: Instruction: MetaHook
Program log: Call: from 42d1, to f19346fb1deecdb8672cdeafe34c5038714c9a30
Program CmobH2vR6aUtQ8x4xd1LYNiH6k2G7PFT5StTgWqvy2VU consumed 76866 of 1323437 compute units
Program CmobH2vR6aUtQ8x4xd1LYNiH6k2G7PFT5StTgWqvy2VU success
Program log: Hook[0] slot=0 result=pass
Program CmobH2vR6aUtQ8x4xd1LYNiH6k2G7PFT5StTgWqvy2VU invoke [3]
Program log: Instruction: MetaHook
Program log: Call: from 42d1, to 6de0d5c3abf60be76e38392934994882d99bb187
Program log: EVM callback reverted: Revert(Reverted)
Program log: Error: EvmCallbackReverted
Program CmobH2vR6aUtQ8x4xd1LYNiH6k2G7PFT5StTgWqvy2VU consumed 66728 of 1243453 compute units
Program CmobH2vR6aUtQ8x4xd1LYNiH6k2G7PFT5StTgWqvy2VU failed: custom program error: 0x0
Program MetaHk1111111111111111111111111111111111111 consumed 165297 of 1342022 compute units
Program MetaHk1111111111111111111111111111111111111 failed: custom program error: 0x0",
        );
        let out = parse_hook_executions(&logs, ROUTER);
        assert_eq!(out.len(), 2, "should record pass + reject");
        assert_eq!(out[0].hook_address, "0xf19346fb1deecdb8672cdeafe34c5038714c9a30");
        assert_eq!(out[0].result, "pass");
        assert_eq!(out[1].hook_address, "0x6de0d5c3abf60be76e38392934994882d99bb187");
        assert_eq!(out[1].result, "reject");
        assert_eq!(out[1].reason.as_deref(), Some("Revert(Reverted)"));
        assert_eq!(out[1].gas_used, Some(66728));
    }

    #[test]
    fn parses_single_fail_no_pass() {
        // Real log shape observed from sig 5Qxc32c... — hook 0 reverts; router
        // aborts immediately without attempting hook 1. Both expected hooks are
        // counted by anchor_count but only one entry is emitted (the actual
        // failure). Mismatch is OK — we still surface the real failure.
        let logs = lines(
            "Program MetaHk1111111111111111111111111111111111111 invoke [2]
Program log: Meta-Hook Router: executing 2 hooks for mint MINT
Program CmobH2vR6aUtQ8x4xd1LYNiH6k2G7PFT5StTgWqvy2VU invoke [3]
Program log: Instruction: MetaHook
Program log: Call: from 42d1, to f19346fb1deecdb8672cdeafe34c5038714c9a30
Program log: EVM callback reverted: Revert(Reverted)
Program log: Error: EvmCallbackReverted
Program CmobH2vR6aUtQ8x4xd1LYNiH6k2G7PFT5StTgWqvy2VU consumed 66196 of 1323437 compute units
Program CmobH2vR6aUtQ8x4xd1LYNiH6k2G7PFT5StTgWqvy2VU failed: custom program error: 0x0
Program MetaHk1111111111111111111111111111111111111 consumed 84781 of 1342022 compute units
Program MetaHk1111111111111111111111111111111111111 failed: custom program error: 0x0",
        );
        let out = parse_hook_executions(&logs, ROUTER);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].hook_address, "0xf19346fb1deecdb8672cdeafe34c5038714c9a30");
        assert_eq!(out[0].result, "reject");
        assert_eq!(out[0].reason.as_deref(), Some("Revert(Reverted)"));
    }

    #[test]
    fn anchor_count_extracted_from_router_log() {
        let logs = lines(
            "Program log: Meta-Hook Router: executing 3 hooks for mint MINT",
        );
        assert_eq!(extract_router_hook_count(&logs), Some(3));
    }

    #[test]
    fn anchor_missing_when_no_router_log() {
        let logs = lines("Program log: Meta-Hook Router: executing FOO hooks for mint MINT");
        assert_eq!(extract_router_hook_count(&logs), None);
    }
}
