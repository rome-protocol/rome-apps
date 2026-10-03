/// Transaction list and detail handlers.
///
/// # Phase 2d RLP denormalization
/// - `from`: uses `from_addr` (signature-recovered at sync time); falls back to `from_address`
///   (Hercules-stored) if `from_addr` is NULL (pre-denorm rows or decode failure).
/// - `to`: uses `to_addr` column; NULL for contract creation or unindexed rows.
/// - `value`: uses `value_wei` cast to TEXT; defaults to "0" for NULL.
/// - `method`: uses `method_id` column; defaults to "0x" for NULL (plain transfer or unindexed).
/// - `type`: hardcoded "Rhea" (cross-chain classification is Phase 4).
/// - `status`: derived from `evm_tx_result.tx_result->'exit_reason'` JSONB.
use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
    Json,
};
use serde::Deserialize;
use sqlx::Row;

use crate::{
    api::models::{DecodedTransfer, Page, SolanaLeg, Tx, TxHookExecution, TxLog, TxStatus, TxType},
    classify::{classify, ClassifyInput},
    cursor::{decode, encode, TxCursor},
    error::AppError,
    state::AppState,
};
use rome_via_classify::{revert_reason, ORACLE_SELECTORS};

const DEFAULT_LIMIT: i64 = 20;
const MAX_LIMIT: i64 = 100;

/// Query parameters for GET /api/v1/txs.
/// The SELECT + joins that build a full `Tx` row. Shared so the paginated feed and the
/// by-hash fetch (used by the cross-VM seam feed) agree on exactly what a Tx is —
/// there is one definition of the row, and callers supply only the WHERE/ORDER/LIMIT.
const TX_SELECT: &str = r#"
        SELECT
            et.tx_hash,
            -- from: prefer signature-recovered from_addr; fall back to Hercules from_address
            COALESCE(et.from_addr, et.from_address) AS effective_from,
            et.to_addr,
            et.value_wei::TEXT                       AS value_wei_str,
            et.method_id,
            et.tx_type_byte,
            et.gas_limit,
            et.input_len,
            et.nonce,
            -- Phase 3: decode method selector to human-readable signature
            COALESCE(ms.signature, et.method_id)     AS method_decoded,
            ebt.slot_number,
            ebt.tx_idx,
            eb.params_number                         AS block_number,
            eb.params_block_timestamp::TEXT          AS timestamp_raw,
            etr.tx_result                            AS tx_result_json,
            jsonb_array_length(COALESCE(etr.tx_result->'logs', '[]'::jsonb)) AS logs_count,
            ccc.rome_tx_type                         AS cross_chain_type,
            ccc.solana_legs                          AS solana_legs_json,
            -- CPI target: calldata-decoded (et.*_calldata, from rlp_decode's
            -- deterministic depth-1 decode) is the primary source for program id —
            -- supersedes the cross_chain log-scraper's plumbing-filter blind spot
            -- (a direct invoke() of a filtered/unmatched program used to yield NULL).
            -- Label prefers the registry-curated ccc value (broader coverage than
            -- the calldata decoder's small native-program map), falling back to the
            -- calldata native label. Instruction prefers ccc's log-derived human
            -- name (broader today), falling back to calldata — S3 (IDL cache) will
            -- flip instruction to calldata-primary once Anchor names resolve.
            COALESCE(et.cpi_program_calldata, ccc.cpi_program)             AS cpi_program,
            COALESCE(ccc.cpi_program_label, et.cpi_program_label_calldata) AS cpi_program_label,
            COALESCE(ccc.cpi_instruction, et.cpi_instruction_calldata)     AS cpi_instruction,
            COALESCE(et.origination, 'ecdsa')        AS origination,
            et.solana_signer,
            cl.display_label                         AS to_label,
            cl.display_label_detail                  AS to_label_detail
        FROM rome_via.evm_tx et
        JOIN rome_via.eth_block_txs ebt
            ON ebt.tx_hash = et.tx_hash AND ebt.chain_id = et.chain_id
        JOIN rome_via.eth_block eb
            ON eb.slot_number = ebt.slot_number
            AND eb.slot_block_idx = ebt.slot_block_idx
            AND eb.chain_id = ebt.chain_id
        LEFT JOIN rome_via.evm_tx_result etr
            ON etr.tx_hash = et.tx_hash AND etr.chain_id = et.chain_id
        LEFT JOIN rome_via.method_signatures ms
            ON ms.selector = et.method_id
        LEFT JOIN rome_via.cross_chain_correlations ccc
            ON ccc.tx_hash = et.tx_hash AND ccc.chain_id = et.chain_id
        LEFT JOIN rome_via.contract_labels cl
            ON cl.chain_id = et.chain_id AND cl.address = et.to_addr
"#;

#[derive(Debug, Deserialize)]
pub struct TxListQuery {
    /// Opaque cursor token from a previous response's `nextCursor`.
    pub cursor: Option<String>,
    /// Number of items per page (default 20, max 100).
    pub limit: Option<i64>,
    /// Filter by EVM block number.
    pub block: Option<i64>,
    /// Filter by sender address (0x-prefixed).
    pub from: Option<String>,
    /// Filter by recipient address (0x-prefixed). Phase 3: requires RLP decode.
    pub to: Option<String>,
    /// Filter by origination: "ecdsa", "solana_unsigned", or "solana_ed25519".
    pub origination: Option<String>,
    /// Include oracle-keeper refresh() txs. Default true (backward-compatible);
    /// "false" excludes the high-volume oracle refresh noise from the feed.
    pub include_oracle: Option<String>,
}



struct RawRow {
    tx_hash: String,
    effective_from: Option<String>,
    to_addr: Option<String>,
    value_wei_str: Option<String>,
    method_decoded: Option<String>,
    tx_type_byte: Option<i16>,
    gas_limit: Option<i64>,
    input_len: Option<i32>,
    nonce: Option<i64>,
    slot_number: i64,
    tx_idx: i32,
    block_number: Option<i64>,
    timestamp_raw: Option<String>,
    tx_result_json: Option<serde_json::Value>,
    logs_count: Option<i32>,
    cross_chain_type: Option<String>,
    solana_legs_json: Option<serde_json::Value>,
    cpi_program: Option<String>,
    cpi_program_label: Option<String>,
    cpi_instruction: Option<String>,
    origination: Option<String>,
    solana_signer: Option<String>,
    to_label: Option<String>,
    to_label_detail: Option<String>,
}

/// Decode the raw pg rows into `RawRow`, truncating the limit+1 has-more probe.
/// Shared by the tx feed and the cross-VM seam feed.
fn decode_raw_rows(raw_rows: Vec<sqlx::postgres::PgRow>, limit: usize) -> Vec<RawRow> {
    let rows: Vec<RawRow> = raw_rows
        .into_iter()
        .take(limit)
        .map(|r| RawRow {
            tx_hash: r.try_get("tx_hash").unwrap_or_default(),
            effective_from: r.try_get("effective_from").ok().flatten(),
            to_addr: r.try_get("to_addr").ok().flatten(),
            value_wei_str: r.try_get("value_wei_str").ok().flatten(),
            method_decoded: r.try_get("method_decoded").ok().flatten(),
            tx_type_byte: r.try_get("tx_type_byte").ok().flatten(),
            gas_limit: r.try_get("gas_limit").ok().flatten(),
            input_len: r.try_get("input_len").ok().flatten(),
            nonce: r.try_get("nonce").ok().flatten(),
            slot_number: r.try_get("slot_number").unwrap_or(0),
            tx_idx: r.try_get("tx_idx").unwrap_or(0),
            block_number: r.try_get("block_number").ok().flatten(),
            timestamp_raw: r.try_get("timestamp_raw").ok().flatten(),
            tx_result_json: r.try_get("tx_result_json").ok().flatten(),
            logs_count: r.try_get("logs_count").ok().flatten(),
            cross_chain_type: r.try_get("cross_chain_type").ok().flatten(),
            solana_legs_json: r.try_get("solana_legs_json").ok().flatten(),
            cpi_program: r.try_get("cpi_program").ok().flatten(),
            cpi_program_label: r.try_get("cpi_program_label").ok().flatten(),
            cpi_instruction: r.try_get("cpi_instruction").ok().flatten(),
            origination: r.try_get("origination").ok().flatten(),
            solana_signer: r.try_get("solana_signer").ok().flatten(),
            to_label: r.try_get("to_label").ok().flatten(),
            to_label_detail: r.try_get("to_label_detail").ok().flatten(),
        })
        .collect();
    rows
}

/// Turn decoded `RawRow`s into full `Tx` values (hook executions batched in).
/// Shared by the paginated tx feed and the cross-VM seam feed so there is exactly
/// one definition of how a Tx is assembled.
async fn rows_to_txs(state: &AppState, chain_id: i64, rows: Vec<RawRow>) -> Vec<Tx> {
    // Fetch hook executions for all tx hashes in this batch.
    let tx_hashes: Vec<String> = rows.iter().map(|r| r.tx_hash.clone()).collect();
    let hook_exec_pairs = fetch_hook_executions_batch(&state.db, chain_id, &tx_hashes).await;

    // Group hook executions by tx_hash.
    let mut hook_map: std::collections::HashMap<String, Vec<TxHookExecution>> =
        std::collections::HashMap::new();
    for (tx_hash, exec) in hook_exec_pairs {
        hook_map.entry(tx_hash).or_default().push(exec);
    }

    let items: Vec<Tx> = rows
        .into_iter()
        .map(|r| {
            let status = derive_status(r.tx_result_json.as_ref());
            let timestamp = r
                .timestamp_raw
                .as_deref()
                .and_then(|s: &str| s.parse::<f64>().ok())
                .map(|epoch| {
                    chrono::DateTime::from_timestamp(epoch as i64, 0)
                        .unwrap_or_default()
                        .format("%Y-%m-%dT%H:%M:%SZ")
                        .to_string()
                });
            let hook_executions = hook_map.remove(&r.tx_hash).unwrap_or_default();
            let (gas_used, gas_price, gas_recipient, priority_fee, base) =
                extract_gas_report(r.tx_result_json.as_ref());
            let solana_legs = parse_solana_legs(r.solana_legs_json.as_ref());
            let tx_type = match r.cross_chain_type.as_deref() {
                Some("Romulus") => TxType::Romulus,
                Some("Remus") => TxType::Remus,
                _ => TxType::Rhea,
            };
            let method = r.method_decoded.unwrap_or_else(|| "0x".to_string());
            let value = r.value_wei_str.unwrap_or_else(|| "0".to_string());
            let contract_address = created_contract_address(
                r.to_addr.as_deref(),
                r.effective_from.as_deref(),
                r.nonce,
                r.input_len,
            );
            let to_lower = r.to_addr.as_ref().map(|s| s.to_ascii_lowercase());
            let logs_json = r.tx_result_json.as_ref().and_then(|v| v.get("logs"));
            let action_tags = classify(&ClassifyInput {
                tx_type: tx_type.as_str(),
                method: &method,
                to: to_lower.as_deref(),
                value_wei: &value,
                tx_type_byte: r.tx_type_byte,
                solana_leg_count: solana_legs.len(),
                logs: logs_json,
                input_len: r.input_len,
            });
            Tx {
                hash: r.tx_hash,
                status,
                tx_type,
                method,
                from: r.effective_from.unwrap_or_default(),
                to: r.to_addr,
                value,
                gas_used,
                gas_price,
                gas_recipient,
                priority_fee,
                base,
                gas_limit: r.gas_limit,
                tx_type_byte: r.tx_type_byte,
                timestamp,
                block_number: r.block_number,
                hook_executions,
                logs: Vec::new(),
                logs_count: r.logs_count,
                action_tags,
                solana_legs,
                origination: r.origination.unwrap_or_else(|| "ecdsa".into()),
                solana_signer: r.solana_signer,
                to_label: r.to_label,
                to_label_detail: r.to_label_detail,
                // Decoded transfers are populated on the detail endpoint only.
                transfers: Vec::new(),
                // Settling Solana signature is detail-endpoint-only (the list
                // doesn't SELECT it — avoids a per-row subquery on the list).
                solana_settlement_sig: None,
                cpi_program: r.cpi_program,
                cpi_program_label: r.cpi_program_label,
                cpi_instruction: r.cpi_instruction,
                contract_address,
                input_len: r.input_len,
                // Revert reason is detail-endpoint-only, like `logs`/`transfers`.
                revert_reason: None,
            }
        })
        .collect();
    items
}

/// GET /api/v1/txs — paginated list of transactions, newest first.
///
/// Ordered by (slot_number DESC, tx_idx DESC). Supports optional filters: `block`, `from`.
#[utoipa::path(
    get,
    path = "/api/v1/txs",
    params(
        ("cursor" = Option<String>, Query, description = "Pagination cursor from previous response"),
        ("limit" = Option<i64>, Query, description = "Items per page (default 20, max 100)"),
        ("block" = Option<i64>, Query, description = "Filter by block number"),
        ("from" = Option<String>, Query, description = "Filter by sender address"),
        ("to" = Option<String>, Query, description = "Filter by recipient (Phase 3)"),
        ("origination" = Option<String>, Query, description = "Filter by origination: 'ecdsa', 'solana_unsigned', or 'solana_ed25519'"),
        ("include_oracle" = Option<String>, Query, description = "Include oracle refresh() txs (default true); 'false' excludes them")
    ),
    responses(
        (status = 200, description = "Paginated transaction list", body = inline(Page<Tx>)),
        (status = 400, description = "Bad request / invalid cursor", body = crate::error::ProblemJson),
        (status = 500, description = "Internal error", body = crate::error::ProblemJson)
    ),
    tag = "txs"
)]
pub async fn list_txs(
    State(state): State<AppState>,
    Query(params): Query<TxListQuery>,
) -> Result<impl IntoResponse, AppError> {
    // Reject `limit` outside [1, MAX_LIMIT] with 400 rather than silently
    // truncate — callers can't distinguish a truncated page from "no more
    // data" except via `hasMore`, which is easy to overlook. Closes
    // rome-via#66.
    let limit = match params.limit {
        None => DEFAULT_LIMIT,
        Some(l) if l < 1 => {
            return Err(AppError::BadRequest(format!("limit must be >= 1 (got {l})")));
        }
        Some(l) if l > MAX_LIMIT => {
            return Err(AppError::BadRequest(format!(
                "limit exceeds max of {MAX_LIMIT} (got {l})"
            )));
        }
        Some(l) => l,
    };
    let chain_id = state.chain_id;

    // `to` filter: the `to_addr` column is now populated for new rows (Phase 2d).
    // However, old rows (pre-migration, pre-backfill) have NULL to_addr, so a `to` filter
    // would silently miss them. Returning 400 until the backfill tool (Phase 3) runs.
    if params.to.is_some() {
        return Err(AppError::BadRequest(
            "`to` filter requires a full backfill — run `rome-via-enrich rebuild --table evm_tx` first".to_string(),
        ));
    }

    // Decode cursor.
    let (last_slot, last_tx_idx) = if let Some(tok) = &params.cursor {
        let cur: TxCursor = decode(tok, &state.cursor_secret)?;
        if cur.chain_id != chain_id {
            return Err(AppError::ChainNotSupported(format!(
                "cursor is for chain_id {} but serving {}",
                cur.chain_id, chain_id
            )));
        }
        (cur.last_slot, cur.last_tx_idx)
    } else {
        (i64::MAX, i32::MAX)
    };

    // Validate filters before hitting the DB.
    validate_origination(params.origination.as_deref())?;
    validate_include_oracle(params.include_oracle.as_deref())?;

    let raw_rows = sqlx::query(
        &format!(
            "{TX_SELECT}
        WHERE et.chain_id = $1
          AND (ebt.slot_number < $2 OR (ebt.slot_number = $2 AND ebt.tx_idx < $3))
          AND ($4::BIGINT IS NULL OR eb.params_number = $4)
          AND ($5::VARCHAR IS NULL OR COALESCE(et.from_addr, et.from_address) ILIKE $5)
          AND ($6::VARCHAR IS NULL OR COALESCE(et.origination,'ecdsa') = $6)
          -- Oracle de-noise: when include_oracle='false', drop every keeper refresh
          -- (ORACLE_SELECTORS: legacy refresh() + PriceBook refreshAll()). NULL-safe —
          -- a tx with no method_id (plain value transfer) is kept, not dropped.
          AND ($8::VARCHAR IS DISTINCT FROM 'false' OR NOT COALESCE(et.method_id = ANY($9), false))
        ORDER BY ebt.slot_number DESC, ebt.tx_idx DESC
        LIMIT $7
        "
        ),
    )
    .bind(chain_id)
    .bind(last_slot)
    .bind(last_tx_idx)
    .bind(params.block)
    .bind(params.from.clone())
    .bind(params.origination.clone())
    .bind(limit + 1)
    .bind(params.include_oracle.clone())
    .bind(ORACLE_SELECTORS)
    .fetch_all(&state.db)
    .await?;

    let has_more = raw_rows.len() as i64 > limit;

    // Keep slot/tx_idx data for cursor building alongside tx data.

    let rows = decode_raw_rows(raw_rows, limit as usize);

    let next_cursor = if has_more {
        rows.last().and_then(|last| {
            let cur = TxCursor {
                chain_id,
                last_slot: last.slot_number,
                last_tx_idx: last.tx_idx,
            };
            encode(&cur, &state.cursor_secret).ok()
        })
    } else {
        None
    };

    let items = rows_to_txs(&state, chain_id, rows).await;

    Ok(Json(Page {
        has_more,
        next_cursor,
        items,
    }))
}

/// Internal: fetch a Tx from the local DB by hash. Used by cross_chain handler.
pub async fn get_tx_internal(state: &AppState, hash: &str) -> Result<Tx, AppError> {
    let chain_id = state.chain_id;

    let row = sqlx::query(
        r#"
        SELECT
            et.tx_hash,
            COALESCE(et.from_addr, et.from_address) AS effective_from,
            et.to_addr,
            et.value_wei::TEXT                       AS value_wei_str,
            et.method_id,
            et.tx_type_byte,
            et.gas_limit,
            et.input_len,
            et.nonce,
            COALESCE(ms.signature, et.method_id)     AS method_decoded,
            ebt.slot_number,
            ebt.tx_idx,
            eb.params_number                         AS block_number,
            eb.params_block_timestamp::TEXT          AS timestamp_raw,
            etr.tx_result                            AS tx_result_json,
            ccc.rome_tx_type                         AS cross_chain_type,
            ccc.solana_legs                          AS solana_legs_json,
            COALESCE(et.origination, 'ecdsa')        AS origination,
            et.solana_signer,
            cl.display_label                         AS to_label,
            cl.display_label_detail                  AS to_label_detail,
            -- Settling Solana signature for this EVM tx. evm_tx_sol_tx is
            -- populated for every tx by sync; a scalar subquery surfaces it
            -- WITHOUT fanning out the row (a tx can map to >1 sig — pick the
            -- lowest deterministically). Columns per rome-via-sync migration
            -- 0008 (chain_id, evm_tx_hash, sol_signature).
            (SELECT est.sol_signature
               FROM rome_via.evm_tx_sol_tx est
              WHERE est.chain_id = et.chain_id
                AND est.evm_tx_hash = et.tx_hash
              ORDER BY est.sol_signature
              LIMIT 1)                               AS solana_settlement_sig,
            -- CPI target: see TX_SELECT's comment above for the calldata-vs-log
            -- COALESCE policy (program id = calldata primary; label/instruction =
            -- ccc primary, calldata fallback).
            COALESCE(et.cpi_program_calldata, ccc.cpi_program)             AS cpi_program,
            COALESCE(ccc.cpi_program_label, et.cpi_program_label_calldata) AS cpi_program_label,
            COALESCE(ccc.cpi_instruction, et.cpi_instruction_calldata)     AS cpi_instruction
        FROM rome_via.evm_tx et
        JOIN rome_via.eth_block_txs ebt
            ON ebt.tx_hash = et.tx_hash AND ebt.chain_id = et.chain_id
        JOIN rome_via.eth_block eb
            ON eb.slot_number = ebt.slot_number
            AND eb.slot_block_idx = ebt.slot_block_idx
            AND eb.chain_id = ebt.chain_id
        LEFT JOIN rome_via.evm_tx_result etr
            ON etr.tx_hash = et.tx_hash AND etr.chain_id = et.chain_id
        LEFT JOIN rome_via.method_signatures ms
            ON ms.selector = et.method_id
        LEFT JOIN rome_via.cross_chain_correlations ccc
            ON ccc.tx_hash = et.tx_hash AND ccc.chain_id = et.chain_id
        LEFT JOIN rome_via.contract_labels cl
            ON cl.chain_id = et.chain_id AND cl.address = et.to_addr
        WHERE et.chain_id = $1
          AND et.tx_hash = $2
        LIMIT 1
        "#,
    )
    .bind(chain_id)
    .bind(hash)
    .fetch_optional(&state.db)
    .await?;

    let r = row.ok_or_else(|| AppError::NotFound(format!("transaction {hash} not found")))?;

    let tx_result_json: Option<serde_json::Value> = r.try_get("tx_result_json").ok().flatten();
    let status = derive_status(tx_result_json.as_ref());
    let timestamp_raw: Option<String> = r.try_get("timestamp_raw").ok().flatten();
    let timestamp = timestamp_raw
        .as_deref()
        .and_then(|s: &str| s.parse::<f64>().ok())
        .map(|epoch| {
            chrono::DateTime::from_timestamp(epoch as i64, 0)
                .unwrap_or_default()
                .format("%Y-%m-%dT%H:%M:%SZ")
                .to_string()
        });

    // Cross-chain type from the correlations row joined above; default Rhea if no row yet.
    let tx_type = match r
        .try_get::<Option<String>, _>("cross_chain_type")
        .ok()
        .flatten()
        .as_deref()
    {
        Some("Romulus") => TxType::Romulus,
        Some("Remus") => TxType::Remus,
        _ => TxType::Rhea,
    };

    // Solana legs from the same correlations row.
    let solana_legs_json: Option<serde_json::Value> =
        r.try_get("solana_legs_json").ok().flatten();
    let solana_legs = parse_solana_legs(solana_legs_json.as_ref());

    // Fetch hook executions for this tx.
    let hook_executions = fetch_hook_executions_for_tx(&state.db, chain_id, hash).await;

    // Extract EVM logs from tx_result JSONB (`logs` array of {address, topics, data}).
    let logs = extract_logs(tx_result_json.as_ref());

    // Decoded ERC-20 token movements ("value moved") — detail-endpoint only,
    // mirroring `logs`. Keyed by (chain_id, tx_hash); lane-agnostic.
    let transfers = fetch_decoded_transfers(&state.db, chain_id, hash).await;

    // Extract per-tx fee fields from tx_result.gas_report JSONB.
    let (gas_used, gas_price, gas_recipient, priority_fee, base) =
        extract_gas_report(tx_result_json.as_ref());

    let method = r.try_get::<Option<String>, _>("method_decoded")
        .ok()
        .flatten()
        .unwrap_or_else(|| "0x".to_string());
    let value = r.try_get::<Option<String>, _>("value_wei_str")
        .ok()
        .flatten()
        .unwrap_or_else(|| "0".to_string());
    let to_addr: Option<String> = r.try_get::<Option<String>, _>("to_addr").ok().flatten();
    let to_lower = to_addr.as_ref().map(|s| s.to_ascii_lowercase());
    let from_addr: Option<String> = r.try_get::<Option<String>, _>("effective_from").ok().flatten();
    let nonce: Option<i64> = r.try_get("nonce").ok().flatten();
    let tx_type_byte: Option<i16> = r.try_get("tx_type_byte").ok().flatten();
    let input_len: Option<i32> = r.try_get("input_len").ok().flatten();
    let contract_address =
        created_contract_address(to_addr.as_deref(), from_addr.as_deref(), nonce, input_len);
    let logs_json = tx_result_json.as_ref().and_then(|v| v.get("logs"));
    let action_tags = classify(&ClassifyInput {
        tx_type: tx_type.as_str(),
        method: &method,
        to: to_lower.as_deref(),
        value_wei: &value,
        tx_type_byte,
        solana_leg_count: solana_legs.len(),
        logs: logs_json,
        input_len,
    });

    Ok(Tx {
        hash: r.try_get("tx_hash").unwrap_or_else(|_| hash.to_string()),
        status,
        tx_type,
        method,
        from: from_addr.unwrap_or_default(),
        to: to_addr,
        value,
        gas_used,
        gas_price,
        gas_recipient,
        priority_fee,
        base,
        gas_limit: r.try_get("gas_limit").ok().flatten(),
        tx_type_byte,
        timestamp,
        block_number: r.try_get("block_number").ok().flatten(),
        hook_executions,
        logs_count: Some(logs.len() as i32),
        logs,
        action_tags,
        solana_legs,
        origination: r.try_get::<Option<String>, _>("origination")
            .ok()
            .flatten()
            .unwrap_or_else(|| "ecdsa".into()),
        solana_signer: r.try_get::<Option<String>, _>("solana_signer").ok().flatten(),
        to_label: r.try_get::<Option<String>, _>("to_label").ok().flatten(),
        to_label_detail: r.try_get::<Option<String>, _>("to_label_detail").ok().flatten(),
        transfers,
        solana_settlement_sig: r
            .try_get::<Option<String>, _>("solana_settlement_sig")
            .ok()
            .flatten(),
        cpi_program: r.try_get::<Option<String>, _>("cpi_program").ok().flatten(),
        cpi_program_label: r.try_get::<Option<String>, _>("cpi_program_label").ok().flatten(),
        cpi_instruction: r.try_get::<Option<String>, _>("cpi_instruction").ok().flatten(),
        contract_address,
        input_len,
        revert_reason: revert_reason(tx_result_json.as_ref()),
    })
}

/// Fetch decoded ERC-20 token movements for a tx from `rome_via.token_transfers`,
/// joined to `rome_via.token_metadata` for symbol/decimals. Ordered by log_index.
/// Best-effort: a query error (e.g. table not yet migrated) yields an empty vec
/// rather than failing the whole tx-detail response.
async fn fetch_decoded_transfers(
    db: &sqlx::PgPool,
    chain_id: i64,
    tx_hash: &str,
) -> Vec<DecodedTransfer> {
    let rows = sqlx::query(
        r#"
        SELECT tt.log_index,
               tt.token_address,
               tt.from_addr,
               tt.to_addr,
               tt.amount::text AS amount_raw,
               tm.symbol,
               tm.decimals
        FROM rome_via.token_transfers tt
        LEFT JOIN rome_via.token_metadata tm
            ON tm.chain_id = tt.chain_id AND tm.address = tt.token_address
        WHERE tt.chain_id = $1 AND tt.tx_hash = $2
        ORDER BY tt.log_index
        "#,
    )
    .bind(chain_id)
    .bind(tx_hash)
    .fetch_all(db)
    .await;

    let rows = match rows {
        Ok(rows) => rows,
        Err(e) => {
            tracing::warn!(chain_id, tx_hash, error = %e, "failed to fetch decoded transfers");
            return Vec::new();
        }
    };

    rows.into_iter()
        .filter_map(|row| {
            let amount_raw: String = row.try_get("amount_raw").ok().flatten()?;
            let decimals: Option<i16> = row.try_get("decimals").ok().flatten();
            let amount_display = decimals.map(|d| format_amount(&amount_raw, d));
            Some(DecodedTransfer {
                token_address: row.try_get("token_address").ok().flatten()?,
                symbol: row.try_get("symbol").ok().flatten(),
                decimals,
                from: row
                    .try_get::<Option<String>, _>("from_addr")
                    .ok()
                    .flatten()
                    .unwrap_or_default(),
                to: row
                    .try_get::<Option<String>, _>("to_addr")
                    .ok()
                    .flatten()
                    .unwrap_or_default(),
                amount_raw,
                amount_display,
            })
        })
        .collect()
}

/// Shift a raw base-unit integer string by `decimals` decimal places to produce
/// a human-readable decimal string (e.g. `("1999999", 6) -> "1.999999"`).
///
/// Big-decimal math: token amounts routinely exceed `u64`/`u128` (a `uint256`
/// is up to 78 digits), so we never parse into a native integer. Trailing
/// fractional zeros are stripped (`("1000000", 6) -> "1"`, not `"1.000000"`)
/// and an integer value renders with no decimal point. A negative `decimals`
/// (not expected from the schema) is treated as 0. On the unreachable parse-
/// failure path we return the raw string unchanged rather than panicking.
pub fn format_amount(raw: &str, decimals: i16) -> String {
    use bigdecimal::num_bigint::BigInt;
    use bigdecimal::BigDecimal;
    use std::str::FromStr;

    // Parse the raw base-unit integer as an arbitrary-precision BigInt (a
    // uint256 is up to 78 digits — never fits u128). On the unreachable
    // parse-failure path, return the raw string unchanged rather than panic.
    let Ok(mantissa) = BigInt::from_str(raw) else {
        return raw.to_string();
    };
    // `BigDecimal::new(mantissa, scale)` represents `mantissa / 10^scale`,
    // i.e. it MOVES the decimal point `scale` places left — exactly the
    // base-unit → display shift. (`with_scale` would keep the value and only
    // change precision, which is the wrong operation here.) A negative
    // `decimals` (not expected from the SMALLINT column) clamps to 0.
    let scale = decimals.max(0) as i64;
    // `normalized()` drops trailing fractional zeros and redundant scale,
    // giving "1" for an exact integer and "1.999999" otherwise.
    BigDecimal::new(mantissa, scale).normalized().to_string()
}

/// Parse `cross_chain_correlations.solana_legs` JSONB array into typed `SolanaLeg`s.
pub(crate) fn parse_solana_legs(v: Option<&serde_json::Value>) -> Vec<SolanaLeg> {
    let Some(arr) = v.and_then(|x| x.as_array()) else {
        return Vec::new();
    };
    arr.iter()
        .filter_map(|leg| {
            Some(SolanaLeg {
                sol_chain: leg.get("solChain")?.as_str()?.to_string(),
                sol_signature: leg.get("solSignature")?.as_str()?.to_string(),
            })
        })
        .collect()
}

/// Extracted fee fields from a `tx_result.gas_report`:
/// `(gas_used, gas_price, gas_recipient, priority_fee, base)`.
/// All numeric fields are decimal-string-encoded; recipient is 0x-prefixed
/// lowercase hex. `gas_used` is the TOTAL fee; `priority_fee` is the priority
/// portion (lamports, from the PRIORITY_FEE marker; `"0"` when absent — legacy /
/// no-bid txs); `base = gas_used − priority_fee` is derived here so it stays
/// consistent even on rows whose JSONB predates the split (no `base`/`priority_fee`
/// key). Lamport-scale priority fits `u128`; an unparseable pair yields `base=None`.
pub(crate) fn extract_gas_report(
    tx_result: Option<&serde_json::Value>,
) -> (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
) {
    let Some(report) = tx_result.and_then(|v| v.get("gas_report")) else {
        return (None, None, None, None, None);
    };
    let gas_used = report.get("gas_value").and_then(json_value_to_decimal_string);
    let gas_price = report.get("gas_price").and_then(json_value_to_decimal_string);
    let gas_recipient = report
        .get("gas_recipient")
        .and_then(|v| v.as_str())
        .map(|s| s.to_lowercase());
    // priority_fee defaults to "0" when the key is absent (pre-priority rows) so
    // the UI always has a number to show; base derives from total − priority.
    let priority_fee = report
        .get("priority_fee")
        .and_then(json_value_to_decimal_string)
        .or_else(|| Some("0".to_string()));
    let base = derive_base(gas_used.as_deref(), priority_fee.as_deref());
    (gas_used, gas_price, gas_recipient, priority_fee, base)
}

/// `base = total − priority` over decimal strings. Lamport-scale priority and the
/// settled total both fit `u128`; returns `None` if either side won't parse (so the
/// caller omits `base` rather than emit a wrong number). Saturating — never panics.
fn derive_base(total: Option<&str>, priority: Option<&str>) -> Option<String> {
    let total: u128 = total?.parse().ok()?;
    let priority: u128 = priority.unwrap_or("0").parse().ok()?;
    Some(total.saturating_sub(priority).to_string())
}

/// Convert a JSON value that may be a hex string, decimal string, or number
/// (typical ethers U256 serialization is `"0x..."`) into a base-10 decimal string.
fn json_value_to_decimal_string(v: &serde_json::Value) -> Option<String> {
    if let Some(n) = v.as_u64() {
        return Some(n.to_string());
    }
    let s = v.as_str()?;
    if let Some(hex) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        return hex_to_decimal(hex);
    }
    // Already decimal? Validate it's all digits.
    if !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) {
        return Some(s.to_string());
    }
    None
}

/// Convert a hex (no `0x`) string up to 64 chars (U256) into a decimal string.
fn hex_to_decimal(hex: &str) -> Option<String> {
    if hex.is_empty() {
        return Some("0".to_string());
    }
    let padded;
    let h = if hex.len() % 2 == 1 {
        padded = format!("0{}", hex);
        padded.as_str()
    } else {
        hex
    };
    let mut bytes: Vec<u8> = Vec::with_capacity(h.len() / 2);
    for i in (0..h.len()).step_by(2) {
        bytes.push(u8::from_str_radix(&h[i..i + 2], 16).ok()?);
    }
    if bytes.iter().all(|&b| b == 0) {
        return Some("0".to_string());
    }
    let mut out: Vec<u8> = Vec::new();
    while bytes.iter().any(|&b| b != 0) {
        let mut rem: u32 = 0;
        for b in bytes.iter_mut() {
            let cur = rem * 256 + *b as u32;
            *b = (cur / 10) as u8;
            rem = cur % 10;
        }
        out.push(b'0' + rem as u8);
    }
    out.reverse();
    String::from_utf8(out).ok()
}

/// Parse the `logs` array inside a `tx_result` JSONB into typed `TxLog`s.
/// Returns empty vec on missing/malformed data — logs are best-effort.
fn extract_logs(tx_result: Option<&serde_json::Value>) -> Vec<TxLog> {
    let Some(arr) = tx_result.and_then(|v| v.get("logs")).and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    arr.iter()
        .filter_map(|entry| {
            let address = entry.get("address")?.as_str()?.to_string();
            let topics = entry
                .get("topics")?
                .as_array()?
                .iter()
                .filter_map(|t| t.as_str().map(String::from))
                .collect();
            let data = entry
                .get("data")
                .and_then(|d| d.as_str())
                .unwrap_or("0x")
                .to_string();
            Some(TxLog { address, topics, data })
        })
        .collect()
}

/// GET /api/v1/txs/:hash — single transaction by hash.
#[utoipa::path(
    get,
    path = "/api/v1/txs/{hash}",
    params(
        ("hash" = String, Path, description = "Transaction hash (0x-prefixed)")
    ),
    responses(
        (status = 200, description = "Transaction detail", body = Tx),
        (status = 404, description = "Transaction not found", body = crate::error::ProblemJson),
        (status = 500, description = "Internal error", body = crate::error::ProblemJson)
    ),
    tag = "txs"
)]
pub async fn get_tx_by_hash(
    State(state): State<AppState>,
    Path(hash): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let tx = get_tx_internal(&state, &hash).await?;
    Ok(Json(tx))
}

/// GET /api/v1/txs/:hash/batch-trace — per-sub-call + per-CPI breakdown for
/// a DoTxBatch transaction. Populated asynchronously by the
/// `batch_trace` enrich worker (rome-via-enrich), which calls
/// `parse_batch_trace` against the tx's Solana log_messages.
///
/// Response shape (see rome_evm_client::indexer::parsers::BatchTrace):
/// ```json
/// {
///   "sub_calls": [
///     {
///       "index": 0,
///       "cpis": [
///         { "program_id": "Tokenkeg...", "depth": 2, "success": true,
///           "logs": ["SPL: TransferChecked"] }
///       ],
///       "evm_lines": ["Call: from aabb..., to ccdd..."]
///     }
///   ]
/// }
/// ```
///
/// Returns `{"not_a_batch": true}` if the tx exists but isn't a DoTxBatch.
/// Returns 404 if the worker hasn't indexed the tx yet (or the tx doesn't
/// exist on chain).
#[utoipa::path(
    get,
    path = "/api/v1/txs/{hash}/batch-trace",
    params(
        ("hash" = String, Path, description = "Transaction hash (0x-prefixed)")
    ),
    responses(
        (status = 200, description = "Batch trace JSON"),
        (status = 404, description = "Trace not yet indexed", body = crate::error::ProblemJson),
        (status = 500, description = "Internal error", body = crate::error::ProblemJson)
    ),
    tag = "txs"
)]
pub async fn get_batch_trace(
    State(state): State<AppState>,
    Path(hash): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let row: Option<(serde_json::Value,)> = sqlx::query_as(
        "SELECT trace FROM rome_via.batch_traces
         WHERE chain_id = $1 AND tx_hash = $2",
    )
    .bind(state.chain_id)
    .bind(&hash)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| AppError::Internal(e.to_string()))?;

    match row {
        Some((trace,)) => Ok(Json(trace)),
        None => Err(AppError::NotFound(format!(
            "batch trace for {} not yet indexed",
            hash
        ))),
    }
}

/// The Solana legs of an EVM tx, ordered by (slot_number, tx_idx, instr_idx) =
/// execution order across AND within Solana blocks. For an iterative tx this is
/// the full set of Solana txs that executed it; for a simple tx it's one leg.
pub async fn fetch_sol_legs(
    db: &sqlx::PgPool,
    chain_id: i64,
    hash: &str,
) -> Result<Vec<crate::api::models::SolTxLeg>, sqlx::Error> {
    let rows: Vec<(String, i64, i32, i32)> = sqlx::query_as(
        "SELECT sol_signature, slot_number, tx_idx, instr_idx
           FROM rome_via.evm_tx_sol_tx
          WHERE chain_id = $1 AND evm_tx_hash = $2
          ORDER BY slot_number, tx_idx, instr_idx",
    )
    .bind(chain_id)
    .bind(hash)
    .fetch_all(db)
    .await?;

    Ok(rows
        .into_iter()
        .map(
            |(sol_signature, slot_number, tx_idx, instr_idx)| crate::api::models::SolTxLeg {
                sol_signature,
                slot_number,
                tx_idx,
                instr_idx,
            },
        )
        .collect())
}

/// GET /api/v1/txs/:hash/sol-legs — the ordered Solana legs of an (iterative)
/// EVM tx. Each entry is a Solana tx signature plus its (slot, tx_idx,
/// instr_idx) ordinal; the list is in execution order. Empty list when the tx
/// has no mirrored legs yet.
pub async fn get_sol_legs(
    State(state): State<AppState>,
    Path(hash): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let legs = fetch_sol_legs(&state.db, state.chain_id, &hash)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;
    Ok(Json(legs))
}

/// Fetch hook executions for a batch of tx hashes.
async fn fetch_hook_executions_batch(
    db: &sqlx::PgPool,
    chain_id: i64,
    tx_hashes: &[String],
) -> Vec<(String, TxHookExecution)> {
    if tx_hashes.is_empty() {
        return vec![];
    }

    // Left-join hooks_registry to pick up the human-readable name. Multiple
    // registry entries per (chain_id, hook_address) can exist when a hook is
    // registered for multiple mints — MIN() across them is a harmless tiebreak
    // since the name is intrinsic to the contract.
    let rows: Vec<(String, String, String, String, Option<String>, Option<i64>, Option<String>)> =
        sqlx::query_as(
            r#"
            SELECT he.tx_hash, he.hook_address, he.hook_kind, he.result, he.reason, he.gas_used,
                   (SELECT MIN(name) FROM rome_via.hooks_registry hr
                    WHERE hr.chain_id = he.chain_id AND hr.hook_address = he.hook_address)
                       AS hook_name
            FROM rome_via.hook_executions he
            WHERE he.chain_id = $1 AND he.tx_hash = ANY($2)
            ORDER BY he.tx_hash, he.hook_address
            "#,
        )
        .bind(chain_id)
        .bind(tx_hashes)
        .fetch_all(db)
        .await
        .unwrap_or_default();

    rows.into_iter()
        .map(|(tx_hash, hook_address, hook_kind, result, reason, gas_used, hook_name)| {
            (
                tx_hash,
                TxHookExecution {
                    hook_address,
                    hook_kind,
                    result,
                    reason,
                    gas_used,
                    hook_name,
                },
            )
        })
        .collect()
}

/// Fetch hook executions for a single tx.
async fn fetch_hook_executions_for_tx(
    db: &sqlx::PgPool,
    chain_id: i64,
    tx_hash: &str,
) -> Vec<TxHookExecution> {
    let rows: Vec<(String, String, String, Option<String>, Option<i64>, Option<String>)> = sqlx::query_as(
        r#"
        SELECT he.hook_address, he.hook_kind, he.result, he.reason, he.gas_used,
               (SELECT MIN(name) FROM rome_via.hooks_registry hr
                WHERE hr.chain_id = he.chain_id AND hr.hook_address = he.hook_address)
                   AS hook_name
        FROM rome_via.hook_executions he
        WHERE he.chain_id = $1 AND he.tx_hash = $2
        ORDER BY he.hook_address
        "#,
    )
    .bind(chain_id)
    .bind(tx_hash)
    .fetch_all(db)
    .await
    .unwrap_or_default();

    rows.into_iter()
        .map(|(hook_address, hook_kind, result, reason, gas_used, hook_name)| TxHookExecution {
            hook_address,
            hook_kind,
            result,
            reason,
            gas_used,
            hook_name,
        })
        .collect()
}

/// Derive transaction status from the `tx_result` JSONB column.
///
/// `tx_result` is a serialized `TxResult` struct. The `exit_reason` object
/// has `{ "code": 0|1|..., "reason": "Succeed(Stopped)" | "Revert(..)" | ... }`.
/// - `code == 0` → success
/// - any other code → failed
/// - null (no result row yet) → pending
pub(crate) fn derive_status(tx_result: Option<&serde_json::Value>) -> TxStatus {
    let Some(v) = tx_result else { return TxStatus::Pending };
    let Some(exit) = v.get("exit_reason") else { return TxStatus::Failed };
    // Primary signal: numeric `code` (0 = success).
    if let Some(code) = exit.get("code").and_then(|c| c.as_i64()) {
        return if code == 0 { TxStatus::Success } else { TxStatus::Failed };
    }
    // Fallback: string `reason` starts with "Succeed".
    if let Some(reason) = exit.get("reason").and_then(|r| r.as_str()) {
        return if reason.starts_with("Succeed") { TxStatus::Success } else { TxStatus::Failed };
    }
    TxStatus::Failed
}

/// Validate the `origination` filter parameter.
///
/// Returns `Ok(())` for `None`, `"ecdsa"`, or `"solana_unsigned"`.
/// Returns `Err(AppError::BadRequest)` for any other value.
pub(crate) fn validate_origination(o: Option<&str>) -> Result<(), AppError> {
    match o {
        None | Some("ecdsa") | Some("solana_unsigned") | Some("solana_ed25519") => Ok(()),
        Some(other) => Err(AppError::BadRequest(format!(
            "origination must be 'ecdsa', 'solana_unsigned', or 'solana_ed25519' (got '{other}')"
        ))),
    }
}

/// Validate the `include_oracle` query param: `None`, `"true"`, or `"false"`.
pub(crate) fn validate_include_oracle(b: Option<&str>) -> Result<(), AppError> {
    match b {
        None | Some("true") | Some("false") => Ok(()),
        Some(other) => Err(AppError::BadRequest(format!(
            "include_oracle must be 'true' or 'false' (got '{other}')"
        ))),
    }
}

/// Derive the CREATE-created contract address from a deployment tx's sender +
/// nonce (standard `keccak256(rlp([from, nonce]))[12..]`) — never stored, always
/// computed on read so historical rows are retroactively correct with no backfill.
/// `None` unless this is a creation (`to` absent) with both `from` and a
/// non-negative `nonce` present AND non-empty calldata (`input_len > 0`) — a
/// `to`-null row with empty calldata is a historical indexing gap, not a
/// creation, and must not get a fabricated address. Reuses `rome_sdk`'s
/// `calc_contract_address` so the explorer agrees byte-for-byte with the
/// indexer's own derivation.
fn created_contract_address(
    to: Option<&str>,
    from: Option<&str>,
    nonce: Option<i64>,
    input_len: Option<i32>,
) -> Option<String> {
    if to.is_some() {
        return None;
    }
    if !input_len.is_some_and(|n| n > 0) {
        return None;
    }
    let from = from?;
    let nonce = nonce?;
    if nonce < 0 {
        return None;
    }
    let from_addr: ethers::types::Address = from.parse().ok()?;
    let nonce_u256 = ethers::types::U256::from(nonce as u64);
    let addr = rome_sdk::rome_evm_client::indexer::calc_contract_address(&from_addr, &None, &nonce_u256)?;
    Some(format!("{:?}", addr))
}

#[cfg(test)]
mod created_contract_address_tests {
    use super::*;

    #[test]
    fn created_contract_address_derives_from_sender_and_nonce() {
        // Creation: to absent, init code present (input_len>0) → live rubicon vector
        // (on-chain receipt): 0x17a6…c583 nonce 8 → 0x5023aa0a…023a.
        assert_eq!(
            created_contract_address(None, Some("0x17a60218c6e6b2a049f08da1d1fc3cf09ff0c583"), Some(8), Some(2000)),
            Some("0x5023aa0a484700f7183b7422be83203ab5da023a".to_string())
        );
        // Not a creation: `to` present ⇒ None regardless of from/nonce.
        assert_eq!(
            created_contract_address(Some("0xdac17f958d2ee523a2206206994597c13d831ec7"),
                                     Some("0x17a60218c6e6b2a049f08da1d1fc3cf09ff0c583"), Some(8), Some(2000)),
            None
        );
        // Gap row: to absent but EMPTY calldata (input_len 0) is NOT a creation —
        // must not fabricate a derived address for it.
        assert_eq!(
            created_contract_address(None, Some("0x17a60218c6e6b2a049f08da1d1fc3cf09ff0c583"), Some(8), Some(0)),
            None
        );
        // Unknown input_len (decode failed) ⇒ None (never fabricate).
        assert_eq!(
            created_contract_address(None, Some("0x17a60218c6e6b2a049f08da1d1fc3cf09ff0c583"), Some(8), None),
            None
        );
        // Missing from/nonce ⇒ None.
        assert_eq!(created_contract_address(None, None, Some(8), Some(2000)), None);
        assert_eq!(created_contract_address(None, Some("0x17a60218c6e6b2a049f08da1d1fc3cf09ff0c583"), None, Some(2000)), None);
    }
}

#[cfg(test)]
mod gas_report_tests {
    use super::*;

    #[test]
    fn hex_to_decimal_basic() {
        assert_eq!(hex_to_decimal("5208").as_deref(), Some("21000"));
        assert_eq!(hex_to_decimal("0").as_deref(), Some("0"));
        assert_eq!(hex_to_decimal("").as_deref(), Some("0"));
        assert_eq!(hex_to_decimal("ff").as_deref(), Some("255"));
        // Odd length is left-padded.
        assert_eq!(hex_to_decimal("a").as_deref(), Some("10"));
    }

    #[test]
    fn json_value_to_decimal_handles_hex_string() {
        let v = serde_json::json!("0x5208");
        assert_eq!(json_value_to_decimal_string(&v).as_deref(), Some("21000"));
    }

    #[test]
    fn json_value_to_decimal_handles_decimal_string() {
        let v = serde_json::json!("21000");
        assert_eq!(json_value_to_decimal_string(&v).as_deref(), Some("21000"));
    }

    #[test]
    fn json_value_to_decimal_handles_number() {
        let v = serde_json::json!(21000u64);
        assert_eq!(json_value_to_decimal_string(&v).as_deref(), Some("21000"));
    }

    #[test]
    fn extract_gas_report_full_shape() {
        // ethers-style serialization of a TxResult.gas_report WITH the priority split.
        let v = serde_json::json!({
            "gas_report": {
                "gas_value": "0x5208",      // 21000 (total)
                "gas_price": "0x77359400",
                "gas_recipient": "0xABCDef0000000000000000000000000000000000",
                "priority_fee": "0x1b58"    // 7000
            }
        });
        let (gu, gp, gr, pf, base) = extract_gas_report(Some(&v));
        assert_eq!(gu.as_deref(), Some("21000"));
        assert_eq!(gp.as_deref(), Some("2000000000"));
        assert_eq!(
            gr.as_deref(),
            Some("0xabcdef0000000000000000000000000000000000")
        );
        assert_eq!(pf.as_deref(), Some("7000"), "priority parsed");
        assert_eq!(base.as_deref(), Some("14000"), "base = 21000 - 7000");
    }

    #[test]
    fn extract_gas_report_no_priority_key_is_zero_full_base() {
        // Legacy row (synced pre-priority): no priority_fee key under gas_report.
        let v = serde_json::json!({
            "gas_report": { "gas_value": "0x5208", "gas_price": "0x1", "gas_recipient": null }
        });
        let (gu, gp, gr, pf, base) = extract_gas_report(Some(&v));
        assert_eq!(gu.as_deref(), Some("21000"));
        assert_eq!(gp.as_deref(), Some("1"));
        assert!(gr.is_none());
        assert_eq!(pf.as_deref(), Some("0"), "absent priority defaults to 0");
        assert_eq!(base.as_deref(), Some("21000"), "base == full total");
    }

    #[test]
    fn extract_gas_report_missing_block() {
        let v = serde_json::json!({ "exit_reason": { "code": 0 } });
        let (gu, gp, gr, pf, base) = extract_gas_report(Some(&v));
        assert!(gu.is_none() && gp.is_none() && gr.is_none());
        assert!(pf.is_none() && base.is_none(), "no gas_report → all None");
    }
}

#[cfg(test)]
mod origination_tests {
    use super::*;

    // TDD: write the test first, then the implementation above makes it pass.

    #[test]
    fn validate_origination_accepts_none() {
        assert!(validate_origination(None).is_ok());
    }

    #[test]
    fn validate_origination_accepts_ecdsa() {
        assert!(validate_origination(Some("ecdsa")).is_ok());
    }

    #[test]
    fn validate_origination_accepts_solana_unsigned() {
        assert!(validate_origination(Some("solana_unsigned")).is_ok());
    }

    #[test]
    fn validate_origination_rejects_unknown_value() {
        let err = validate_origination(Some("foo")).expect_err("should reject 'foo'");
        match err {
            AppError::BadRequest(msg) => {
                assert!(msg.contains("foo"), "message should include the bad value");
                assert!(msg.contains("ecdsa"), "message should mention valid values");
            }
            other => panic!("expected BadRequest, got {other:?}"),
        }
    }

    #[test]
    fn validate_origination_rejects_empty_string() {
        let err = validate_origination(Some("")).expect_err("should reject empty string");
        assert!(matches!(err, AppError::BadRequest(_)));
    }
}

#[cfg(test)]
mod format_amount_tests {
    use super::*;

    // TDD: these are written RED — `format_amount` does not exist yet.

    #[test]
    fn shifts_six_decimals_with_fraction() {
        // The canonical "value moved" example: 1.999999 wUSDC.
        assert_eq!(format_amount("1999999", 6), "1.999999");
    }

    #[test]
    fn whole_number_strips_trailing_fraction() {
        // 1_000_000 raw / 10^6 = exactly 1. Lock the contract to "1"
        // (trailing zeros stripped) — NOT "1.0" / "1.000000".
        assert_eq!(format_amount("1000000", 6), "1");
    }

    #[test]
    fn leading_zero_for_sub_unit_values() {
        // 10_000_000 raw / 10^9 = 0.01 — must render the leading zero.
        assert_eq!(format_amount("10000000", 9), "0.01");
    }

    #[test]
    fn zero_decimals_is_passthrough() {
        // decimals=0 → the raw integer is the display value, unchanged.
        assert_eq!(format_amount("42", 0), "42");
        assert_eq!(format_amount("0", 0), "0");
    }

    #[test]
    fn zero_amount_renders_zero() {
        assert_eq!(format_amount("0", 6), "0");
    }

    #[test]
    fn huge_78_digit_value_does_not_overflow() {
        // A 78-digit raw integer (well beyond u128::MAX ~ 3.4e38, i.e. 39 digits)
        // shifted by 18 decimals. This MUST be big-decimal/string math — a u128
        // or u64 path would overflow and panic/wrap. We assert the exact shifted
        // string: 78 digits, decimal point inserted 18 places from the right →
        // 60 integer digits + "." + 18 fraction digits.
        let raw = "1".to_string() + &"2".repeat(77); // 78 chars, no trailing zeros
        let out = format_amount(&raw, 18);
        let (int_part, frac_part) = out.split_once('.').expect("should have a decimal point");
        assert_eq!(int_part.len(), 60, "integer part should be 78-18=60 digits");
        assert_eq!(frac_part.len(), 18, "fraction part should be 18 digits");
        // Round-trip: removing the point yields the original 78-digit integer.
        assert_eq!(format!("{int_part}{frac_part}"), raw);
    }

    #[test]
    fn shifts_more_decimals_than_digits() {
        // 5 raw / 10^6 = 0.000005 — decimals exceed the integer's digit count.
        assert_eq!(format_amount("5", 6), "0.000005");
    }

}

#[derive(Debug, Deserialize)]
pub struct CrossVmQuery {
    pub cursor: Option<String>,
    pub limit: Option<i64>,
    /// Restrict to one seam: "evm_to_sol", "sol_to_evm" or "bridge". Omit for all
    /// crossings.
    pub seam: Option<String>,
    /// Include oracle-keeper refresh() crossings. **Defaults to "false"** — note this is
    /// the opposite of /txs, deliberately: on /txs the keeper is a small fraction of a
    /// large feed, but it is ~99.99% of the crossings, so including it by default would
    /// bury every real crossing. Pass "true" to see keeper traffic.
    pub include_oracle: Option<String>,
}

/// Validate the `seam` filter. Unknown values are rejected rather than silently
/// returning an empty page — an empty page is indistinguishable from "no crossings",
/// which is exactly the confusion this feed exists to remove.
pub(crate) fn validate_seam(s: Option<&str>) -> Result<(), AppError> {
    match s {
        None | Some("evm_to_sol") | Some("sol_to_evm") | Some("bridge") => Ok(()),
        Some(other) => Err(AppError::BadRequest(format!(
            "invalid seam '{other}' (expected evm_to_sol, sol_to_evm or bridge)"
        ))),
    }
}

/// GET /api/v1/cross-vm — the feed of EVM<->Solana crossings, newest first.
///
/// Reads `rome_via.cross_vm_seams`, which contains a row ONLY for transactions that
/// cross a seam (~1 in 27,000 on a busy chain). Because the feed table holds just the
/// crossings, this is an ordered scan of a small table rather than a rare predicate over
/// the whole chain — there is no plan for the planner to get wrong, and the page is
/// chain-wide rather than a slice of recent activity.
#[utoipa::path(
    get,
    path = "/api/v1/cross-vm",
    params(
        ("limit" = Option<i64>, Query, description = "Items per page (default 20, max 100)"),
        ("cursor" = Option<String>, Query, description = "Opaque keyset cursor from a previous page's next_cursor"),
        ("seam" = Option<String>, Query, description = "Filter to one seam: 'evm_to_sol', 'sol_to_evm', or 'bridge'; omit for all"),
        ("include_oracle" = Option<String>, Query, description = "Include oracle-keeper refresh crossings — default 'false' (opposite of /txs, since the keeper is ~99.99% of crossings); 'true' to show")
    ),
    responses(
        (status = 200, description = "Cross-VM transactions, newest first", body = Page<Tx>),
        (status = 400, description = "Invalid seam or limit", body = crate::error::ProblemJson),
        (status = 500, description = "Internal error", body = crate::error::ProblemJson)
    ),
    tag = "cross-vm"
)]
pub async fn list_cross_vm(
    State(state): State<AppState>,
    Query(params): Query<CrossVmQuery>,
) -> Result<impl IntoResponse, AppError> {
    let chain_id = state.chain_id;
    let limit = match params.limit {
        None => 20,
        Some(n) if (1..=100).contains(&n) => n,
        Some(n) => return Err(AppError::BadRequest(format!("limit {n} out of range (1-100)"))),
    };
    validate_seam(params.seam.as_deref())?;
    validate_include_oracle(params.include_oracle.as_deref())?;
    // Opt-in, unlike /txs — see the field doc for why the defaults differ.
    let with_oracle = params.include_oracle.as_deref() == Some("true");

    let (last_slot, last_tx_idx) = if let Some(tok) = params.cursor.as_deref() {
        let cur: TxCursor = crate::cursor::decode(tok, &state.cursor_secret)?;
        if cur.chain_id != chain_id {
            return Err(AppError::ChainNotSupported(format!(
                "cursor is for chain {} but this API serves {}", cur.chain_id, chain_id
            )));
        }
        (cur.last_slot, cur.last_tx_idx)
    } else {
        (i64::MAX, i32::MAX)
    };

    let raw_rows = sqlx::query(
        &format!(
            "{TX_SELECT}
        JOIN rome_via.cross_vm_seams cvs
            ON cvs.chain_id = et.chain_id AND cvs.tx_hash = et.tx_hash
        WHERE et.chain_id = $1
          AND (cvs.slot_number < $2 OR (cvs.slot_number = $2 AND cvs.tx_idx < $3))
          AND ($4::VARCHAR IS NULL OR $4 = ANY(cvs.seams))
          -- $6 false => `NOT is_oracle`, which is exactly the partial index
          -- ix_cross_vm_seams_feed_no_oracle, so the default read stays an ordered scan
          -- of the dense non-keeper subset.
          AND ($6::BOOL OR NOT cvs.is_oracle)
        ORDER BY cvs.slot_number DESC, cvs.tx_idx DESC
        LIMIT $5
        "
        ),
    )
    .bind(chain_id)
    .bind(last_slot)
    .bind(last_tx_idx)
    .bind(params.seam.clone())
    .bind(limit + 1)
    .bind(with_oracle)
    .fetch_all(&state.db)
    .await?;

    let has_more = raw_rows.len() as i64 > limit;
    let rows = decode_raw_rows(raw_rows, limit as usize);
    let next_cursor = rows.last().and_then(|last| {
        let cur = TxCursor { chain_id, last_slot: last.slot_number, last_tx_idx: last.tx_idx };
        if has_more { crate::cursor::encode(&cur, &state.cursor_secret).ok() } else { None }
    });
    let items = rows_to_txs(&state, chain_id, rows).await;
    Ok(Json(Page { has_more, next_cursor, items }))
}

#[cfg(test)]
mod cross_vm_tests {
    use super::*;

    /// The seam feed hides keeper traffic by DEFAULT, the opposite of /txs.
    ///
    /// On /txs the oracle keeper is a small slice of a large feed, so hiding it by
    /// default would silently drop data. In the seam feed it is ~99.99% of the rows
    /// (~1.58M/year of refresh() against ~114 real crossings), so including it by
    /// default buries every real crossing and makes the screen useless — which is the
    /// bug the feed exists to fix. The asymmetry is deliberate; this pins it.
    #[test]
    fn seam_feed_excludes_oracle_unless_explicitly_asked() {
        let want = |v: Option<&str>| v == Some("true");
        assert!(!want(None), "default must hide keeper traffic");
        assert!(!want(Some("false")));
        assert!(want(Some("true")), "explicit opt-in shows it");
    }

    #[test]
    fn seam_feed_rejects_an_unparseable_include_oracle() {
        assert!(validate_include_oracle(Some("yes")).is_err());
        assert!(validate_include_oracle(Some("true")).is_ok());
        assert!(validate_include_oracle(None).is_ok());
    }
}
