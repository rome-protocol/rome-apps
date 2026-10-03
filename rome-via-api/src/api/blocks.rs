use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
    Json,
};
use serde::Deserialize;
use sqlx::Row;

use crate::{
    api::models::{Block, Page},
    cursor::{decode, encode, BlockCursor},
    error::AppError,
    state::AppState,
};

const DEFAULT_LIMIT: i64 = 20;
const MAX_LIMIT: i64 = 100;

/// Query parameters for GET /api/v1/blocks.
#[derive(Debug, Deserialize)]
pub struct BlockListQuery {
    /// Opaque cursor token from a previous response's `nextCursor`.
    pub cursor: Option<String>,
    /// Number of items per page (default 20, max 100).
    pub limit: Option<i64>,
}

/// GET /api/v1/blocks — paginated list of blocks, newest first.
#[utoipa::path(
    get,
    path = "/api/v1/blocks",
    params(
        ("cursor" = Option<String>, Query, description = "Pagination cursor from previous response"),
        ("limit" = Option<i64>, Query, description = "Items per page (default 20, max 100)")
    ),
    responses(
        (status = 200, description = "Paginated block list", body = inline(Page<Block>)),
        (status = 400, description = "Bad request / invalid cursor", body = crate::error::ProblemJson),
        (status = 500, description = "Internal error", body = crate::error::ProblemJson)
    ),
    tag = "blocks"
)]
pub async fn list_blocks(
    State(state): State<AppState>,
    Query(params): Query<BlockListQuery>,
) -> Result<impl IntoResponse, AppError> {
    let limit = params
        .limit
        .unwrap_or(DEFAULT_LIMIT)
        .clamp(1, MAX_LIMIT);
    let chain_id = state.chain_id;

    // Decode cursor to get the last block number seen (exclusive upper bound).
    let last_number: i64 = if let Some(tok) = &params.cursor {
        let cur: BlockCursor = decode(tok, &state.cursor_secret)?;
        if cur.chain_id != chain_id {
            return Err(AppError::ChainNotSupported(format!(
                "cursor is for chain_id {} but serving {}",
                cur.chain_id, chain_id
            )));
        }
        cur.last_number
    } else {
        i64::MAX
    };

    // Fetch limit+1 rows so we can detect whether there is a next page.
    let rows = sqlx::query(
        r#"
        SELECT
            eb.params_number            AS number,
            eb.slot_number              AS slot,
            eb.slot_block_idx           AS slot_block_idx,
            eb.params_blockhash         AS hash,
            eb.params_parent_hash       AS parent_hash,
            eb.block_gas_used::TEXT     AS gas_used,
            eb.gas_recipient            AS gas_recipient,
            eb.params_block_timestamp::TEXT AS timestamp_raw,
            (
                SELECT COUNT(*)::BIGINT
                FROM rome_via.eth_block_txs ebt
                WHERE ebt.slot_number = eb.slot_number
                  AND ebt.slot_block_idx = eb.slot_block_idx
                  AND ebt.chain_id = eb.chain_id
            ) AS tx_count,
            -- Sum of per-tx gas-limit ceilings for txs in this block. Useful as
            -- a denominator for "block utilization of requested ceilings" —
            -- way more meaningful than the chain's static block ceiling.
            (
                SELECT SUM(et.gas_limit)::TEXT
                FROM rome_via.evm_tx et
                JOIN rome_via.eth_block_txs ebt2
                    ON ebt2.tx_hash = et.tx_hash AND ebt2.chain_id = et.chain_id
                WHERE et.chain_id = eb.chain_id
                  AND ebt2.slot_number = eb.slot_number
                  AND ebt2.slot_block_idx = eb.slot_block_idx
            ) AS gas_limit_sum,
            -- Per-block tx-type distribution (Rhea/Remus/Romulus). Returns a
            -- JSONB object so the API can hydrate a single TypeCounts struct
            -- without three extra round-trips.
            (
                SELECT jsonb_build_object(
                    'rhea',    COUNT(*) FILTER (WHERE rome_tx_type = 'Rhea'),
                    'remus',   COUNT(*) FILTER (WHERE rome_tx_type = 'Remus'),
                    'romulus', COUNT(*) FILTER (WHERE rome_tx_type = 'Romulus')
                )
                FROM rome_via.cross_chain_correlations ccc
                WHERE ccc.chain_id = eb.chain_id
                  AND ccc.block_number = eb.params_number
            ) AS type_counts_json
        FROM rome_via.eth_block eb
        WHERE eb.chain_id = $1
          AND eb.params_number < $2
        ORDER BY eb.params_number DESC
        LIMIT $3
        "#,
    )
    .bind(chain_id)
    .bind(last_number)
    .bind(limit + 1)
    .fetch_all(&state.db)
    .await?;

    let has_more = rows.len() as i64 > limit;

    let items: Vec<Block> = rows
        .into_iter()
        .take(limit as usize)
        .map(|r| {
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
            Block {
                number: r.try_get::<Option<i64>, _>("number").ok().flatten().unwrap_or(0),
                slot: r.try_get("slot").unwrap_or(0),
                slot_block_idx: r.try_get("slot_block_idx").unwrap_or(0),
                hash: r.try_get("hash").ok().flatten(),
                parent_hash: r.try_get("parent_hash").ok().flatten(),
                tx_count: r.try_get::<Option<i64>, _>("tx_count").ok().flatten().unwrap_or(0),
                gas_used: r.try_get::<Option<String>, _>("gas_used").ok().flatten().unwrap_or_else(|| "0".to_string()),
                gas_limit_sum: r.try_get::<Option<String>, _>("gas_limit_sum").ok().flatten(),
                type_counts: parse_type_counts(r.try_get::<Option<serde_json::Value>, _>("type_counts_json").ok().flatten()),
                gas_recipient: r.try_get::<Option<String>, _>("gas_recipient").ok().flatten(),
                timestamp,
            }
        })
        .collect();

    let next_cursor = if has_more {
        items.last().and_then(|b| {
            let cur = BlockCursor {
                chain_id,
                last_number: b.number,
            };
            encode(&cur, &state.cursor_secret).ok()
        })
    } else {
        None
    };

    Ok(Json(Page {
        has_more,
        next_cursor,
        items,
    }))
}

/// GET /api/v1/blocks/:number — single block by EVM block number.
#[utoipa::path(
    get,
    path = "/api/v1/blocks/{number}",
    params(
        ("number" = i64, Path, description = "EVM block number")
    ),
    responses(
        (status = 200, description = "Block detail", body = Block),
        (status = 404, description = "Block not found", body = crate::error::ProblemJson),
        (status = 500, description = "Internal error", body = crate::error::ProblemJson)
    ),
    tag = "blocks"
)]
pub async fn get_block_by_number(
    State(state): State<AppState>,
    Path(number): Path<i64>,
) -> Result<impl IntoResponse, AppError> {
    let chain_id = state.chain_id;

    let row = sqlx::query(
        r#"
        SELECT
            eb.params_number            AS number,
            eb.slot_number              AS slot,
            eb.slot_block_idx           AS slot_block_idx,
            eb.params_blockhash         AS hash,
            eb.params_parent_hash       AS parent_hash,
            eb.block_gas_used::TEXT     AS gas_used,
            eb.gas_recipient            AS gas_recipient,
            eb.params_block_timestamp::TEXT AS timestamp_raw,
            (
                SELECT COUNT(*)::BIGINT
                FROM rome_via.eth_block_txs ebt
                WHERE ebt.slot_number = eb.slot_number
                  AND ebt.slot_block_idx = eb.slot_block_idx
                  AND ebt.chain_id = eb.chain_id
            ) AS tx_count,
            -- Sum of per-tx gas-limit ceilings for txs in this block. Useful as
            -- a denominator for "block utilization of requested ceilings" —
            -- way more meaningful than the chain's static block ceiling.
            (
                SELECT SUM(et.gas_limit)::TEXT
                FROM rome_via.evm_tx et
                JOIN rome_via.eth_block_txs ebt2
                    ON ebt2.tx_hash = et.tx_hash AND ebt2.chain_id = et.chain_id
                WHERE et.chain_id = eb.chain_id
                  AND ebt2.slot_number = eb.slot_number
                  AND ebt2.slot_block_idx = eb.slot_block_idx
            ) AS gas_limit_sum,
            -- Per-block tx-type distribution (Rhea/Remus/Romulus). Returns a
            -- JSONB object so the API can hydrate a single TypeCounts struct
            -- without three extra round-trips.
            (
                SELECT jsonb_build_object(
                    'rhea',    COUNT(*) FILTER (WHERE rome_tx_type = 'Rhea'),
                    'remus',   COUNT(*) FILTER (WHERE rome_tx_type = 'Remus'),
                    'romulus', COUNT(*) FILTER (WHERE rome_tx_type = 'Romulus')
                )
                FROM rome_via.cross_chain_correlations ccc
                WHERE ccc.chain_id = eb.chain_id
                  AND ccc.block_number = eb.params_number
            ) AS type_counts_json
        FROM rome_via.eth_block eb
        WHERE eb.chain_id = $1
          AND eb.params_number = $2
        LIMIT 1
        "#,
    )
    .bind(chain_id)
    .bind(number)
    .fetch_optional(&state.db)
    .await?;

    let r = row.ok_or_else(|| AppError::NotFound(format!("block {number} not found")))?;

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

    Ok(Json(Block {
        number: r.try_get::<Option<i64>, _>("number").ok().flatten().unwrap_or(number),
        slot: r.try_get("slot").unwrap_or(0),
        slot_block_idx: r.try_get("slot_block_idx").unwrap_or(0),
        hash: r.try_get("hash").ok().flatten(),
        parent_hash: r.try_get("parent_hash").ok().flatten(),
        tx_count: r.try_get::<Option<i64>, _>("tx_count").ok().flatten().unwrap_or(0),
        gas_used: r.try_get::<Option<String>, _>("gas_used").ok().flatten().unwrap_or_else(|| "0".to_string()),
        gas_limit_sum: r.try_get::<Option<String>, _>("gas_limit_sum").ok().flatten(),
        type_counts: parse_type_counts(r.try_get::<Option<serde_json::Value>, _>("type_counts_json").ok().flatten()),
        gas_recipient: r.try_get::<Option<String>, _>("gas_recipient").ok().flatten(),
        timestamp,
    }))
}

/// Parse the `jsonb_build_object(...)` we emit for `type_counts_json`.
/// Returns None if the JSON is null/missing (block has no cross-chain rows
/// yet — counts unknown). Returns Some(zeros) if all three filters returned 0.
fn parse_type_counts(v: Option<serde_json::Value>) -> Option<crate::api::models::TypeCounts> {
    let v = v?;
    let obj = v.as_object()?;
    let g = |k: &str| obj.get(k).and_then(|x| x.as_i64()).unwrap_or(0);
    Some(crate::api::models::TypeCounts {
        rhea: g("rhea"),
        remus: g("remus"),
        romulus: g("romulus"),
    })
}
