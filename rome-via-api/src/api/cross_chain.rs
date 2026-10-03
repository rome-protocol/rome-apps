/// Cross-chain correlation endpoints.
///
/// GET /api/v1/cross-chain         — list correlations
/// GET /api/v1/txs/:hash?chain=X   — fetch tx; if chain != local, use RPC fallback
use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
    Json,
};
use serde::Deserialize;
use sqlx::Row;

use crate::{
    api::models::{CrossChainCorrelation, EvmLeg, Page, SolanaLeg},
    error::AppError,
    rpc_fallback,
    state::AppState,
};

const DEFAULT_LIMIT: i64 = 20;
const MAX_LIMIT: i64 = 100;

/// Query parameters for GET /api/v1/cross-chain.
#[derive(Debug, Deserialize)]
pub struct CrossChainQuery {
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub tx_type: Option<String>, // "Rhea" | "Romulus"
}

/// GET /api/v1/cross-chain — paginated list of cross-chain correlations.
#[utoipa::path(
    get,
    path = "/api/v1/cross-chain",
    params(
        ("limit" = Option<i64>, Query, description = "Items per page (default 20, max 100)"),
        ("offset" = Option<i64>, Query, description = "Offset for pagination"),
        ("tx_type" = Option<String>, Query, description = "Filter by type: Rhea | Romulus")
    ),
    responses(
        (status = 200, description = "Paginated cross-chain correlations", body = inline(Page<CrossChainCorrelation>)),
        (status = 500, description = "Internal error", body = crate::error::ProblemJson)
    ),
    tag = "cross-chain"
)]
pub async fn list_cross_chain(
    State(state): State<AppState>,
    Query(params): Query<CrossChainQuery>,
) -> Result<impl IntoResponse, AppError> {
    let limit = params.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let offset = params.offset.unwrap_or(0).max(0);
    let chain_id = state.chain_id;

    let rows = sqlx::query(
        r#"
        SELECT tx_hash, rome_tx_type, evm_legs, solana_legs, block_number, timestamp
        FROM rome_via.cross_chain_correlations
        WHERE chain_id = $1
          AND ($2::TEXT IS NULL OR rome_tx_type = $2)
        ORDER BY block_number DESC
        LIMIT $3 OFFSET $4
        "#,
    )
    .bind(chain_id)
    .bind(params.tx_type.as_deref())
    .bind(limit + 1)
    .bind(offset)
    .fetch_all(&state.db)
    .await?;

    let has_more = rows.len() as i64 > limit;

    let items: Vec<CrossChainCorrelation> = rows
        .into_iter()
        .take(limit as usize)
        .map(|r| {
            let tx_hash: String = r.try_get("tx_hash").unwrap_or_default();
            let rome_tx_type: String = r.try_get("rome_tx_type").unwrap_or_default();
            let evm_legs_json: serde_json::Value =
                r.try_get("evm_legs").unwrap_or(serde_json::json!([]));
            let solana_legs_json: serde_json::Value =
                r.try_get("solana_legs").unwrap_or(serde_json::json!([]));
            let timestamp: Option<chrono::DateTime<chrono::Utc>> =
                r.try_get("timestamp").ok().flatten();

            let evm_legs: Vec<EvmLeg> = evm_legs_json
                .as_array()
                .unwrap_or(&vec![])
                .iter()
                .filter_map(|v| {
                    Some(EvmLeg {
                        chain_id: v.get("chainId")?.as_i64()?,
                        block_number: v.get("blockNumber")?.as_i64(),
                        tx_hash: v.get("txHash")?.as_str()?.to_string(),
                    })
                })
                .collect();

            let solana_legs: Vec<SolanaLeg> = solana_legs_json
                .as_array()
                .unwrap_or(&vec![])
                .iter()
                .filter_map(|v| {
                    Some(SolanaLeg {
                        sol_chain: v.get("solChain")?.as_str()?.to_string(),
                        sol_signature: v.get("solSignature")?.as_str()?.to_string(),
                    })
                })
                .collect();

            let timestamp_str = timestamp
                .map(|t| t.format("%Y-%m-%dT%H:%M:%SZ").to_string());

            CrossChainCorrelation {
                rome_tx_hash: tx_hash,
                rome_tx_type,
                evm_legs,
                solana_legs,
                timestamp: timestamp_str,
            }
        })
        .collect();

    Ok(Json(Page {
        items,
        next_cursor: if has_more {
            Some((offset + limit).to_string())
        } else {
            None
        },
        has_more,
    }))
}

/// Query parameters for GET /api/v1/txs/:hash (extended with chain).
#[derive(Debug, Deserialize)]
pub struct TxWithChainQuery {
    /// If provided and different from local chain_id, use RPC fallback.
    pub chain: Option<i64>,
}

/// GET /api/v1/txs/:hash?chain=<chain_id>
///
/// If chain == local chain_id (or absent), serves from DB.
/// If chain is a foreign chain_id, uses RPC fallback.
/// Response includes `source: "indexed" | "live"`.
#[utoipa::path(
    get,
    path = "/api/v1/txs/{hash}/rpc",
    params(
        ("hash" = String, Path, description = "Transaction hash (0x-prefixed)"),
        ("chain" = Option<i64>, Query, description = "Chain ID (foreign chain → RPC fallback)")
    ),
    responses(
        (status = 200, description = "Transaction with source field", body = crate::api::models::TxWithSource),
        (status = 404, description = "Not found", body = crate::error::ProblemJson),
        (status = 503, description = "Foreign proxy unreachable", body = crate::error::ProblemJson)
    ),
    tag = "cross-chain"
)]
pub async fn get_tx_with_chain(
    State(state): State<AppState>,
    Path(hash): Path<String>,
    Query(params): Query<TxWithChainQuery>,
) -> Result<impl IntoResponse, AppError> {
    let requested_chain = params.chain.unwrap_or(state.chain_id);

    if requested_chain == state.chain_id {
        // Local chain — delegate to normal tx handler but wrap with source.
        let tx = crate::api::txs::get_tx_internal(&state, &hash).await?;
        let with_source = crate::api::models::TxWithSource {
            tx,
            source: "indexed".to_string(),
        };
        return Ok(Json(with_source));
    }

    // Foreign chain — RPC fallback.
    let tx = rpc_fallback::fetch_foreign_tx(&state, requested_chain, &hash).await?;
    let with_source = crate::api::models::TxWithSource {
        tx,
        source: "live".to_string(),
    };
    Ok(Json(with_source))
}
