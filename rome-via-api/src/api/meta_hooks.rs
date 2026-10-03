/// Meta-Hook Router invocations.
///
/// Surfaces Solana txs that invoked the rome Meta-Hook Router program
/// (Token-2022 transfer_hook path). These are *not* EVM txs — they're pure
/// Solana, but each invocation executes rome-evm bytecode for every attached
/// hook, so the per-hook outcomes live in `hook_executions` under the
/// synthetic key `tx_hash = 'sol:<sol_signature>'`.
use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
    Json,
};
use serde::Deserialize;
use sqlx::Row;

use crate::{
    api::models::{MetaHookInvocation, MetaHookInvocationDetail, Page, TxHookExecution},
    error::AppError,
    state::AppState,
};

const DEFAULT_LIMIT: i64 = 20;
const MAX_LIMIT: i64 = 100;

#[derive(Debug, Deserialize)]
pub struct MetaHookListQuery {
    pub cursor: Option<String>,
    pub limit: Option<i64>,
    pub mint: Option<String>,
}

/// GET /api/v1/meta-hooks — paginated newest-first list of router invocations.
#[utoipa::path(
    get,
    path = "/api/v1/meta-hooks",
    params(
        ("cursor" = Option<String>, Query, description = "Slot-number cursor from a previous response"),
        ("limit" = Option<i64>, Query, description = "Items per page (default 20, max 100)"),
        ("mint" = Option<String>, Query, description = "Filter by mint address"),
    ),
    responses(
        (status = 200, description = "Page of meta-hook invocations", body = inline(Page<MetaHookInvocation>)),
        (status = 400, description = "Bad request", body = crate::error::ProblemJson),
        (status = 500, description = "Internal error", body = crate::error::ProblemJson),
    ),
    tag = "meta-hooks"
)]
pub async fn list_invocations(
    State(state): State<AppState>,
    Query(params): Query<MetaHookListQuery>,
) -> Result<impl IntoResponse, AppError> {
    let limit = params.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let chain_id = state.chain_id;
    let cursor_slot: i64 = params
        .cursor
        .as_deref()
        .and_then(|s| s.parse().ok())
        .unwrap_or(i64::MAX);

    let rows = sqlx::query(
        r#"
        SELECT
            sol_signature, slot_number, block_time,
            mint, hook_count, outcome, fee_payer
        FROM rome_via.meta_hook_invocations
        WHERE chain_id = $1
          AND slot_number < $2
          AND ($3::TEXT IS NULL OR mint = $3)
        ORDER BY slot_number DESC
        LIMIT $4
        "#,
    )
    .bind(chain_id)
    .bind(cursor_slot)
    .bind(params.mint.as_deref())
    .bind(limit + 1)
    .fetch_all(&state.db)
    .await?;

    let has_more = rows.len() as i64 > limit;
    let items: Vec<MetaHookInvocation> = rows
        .iter()
        .take(limit as usize)
        .map(row_to_invocation)
        .collect();

    let next_cursor = if has_more {
        items.last().map(|i| i.slot_number.to_string())
    } else {
        None
    };

    Ok(Json(Page {
        items,
        next_cursor,
        has_more,
    }))
}

/// GET /api/v1/meta-hooks/:sig — detail with hook executions.
#[utoipa::path(
    get,
    path = "/api/v1/meta-hooks/{sig}",
    params(("sig" = String, Path, description = "Solana signature (base58)")),
    responses(
        (status = 200, description = "Invocation detail", body = MetaHookInvocationDetail),
        (status = 404, description = "Not found", body = crate::error::ProblemJson),
        (status = 500, description = "Internal error", body = crate::error::ProblemJson),
    ),
    tag = "meta-hooks"
)]
pub async fn get_invocation(
    State(state): State<AppState>,
    Path(sig): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let chain_id = state.chain_id;
    let row = sqlx::query(
        r#"
        SELECT sol_signature, slot_number, block_time,
               mint, hook_count, outcome, fee_payer
        FROM rome_via.meta_hook_invocations
        WHERE chain_id = $1 AND sol_signature = $2
        "#,
    )
    .bind(chain_id)
    .bind(&sig)
    .fetch_optional(&state.db)
    .await?;

    let invocation = row
        .as_ref()
        .map(row_to_invocation)
        .ok_or_else(|| AppError::NotFound(format!("meta-hook invocation {sig} not found")))?;

    let synthetic_hash = format!("sol:{sig}");
    let hooks: Vec<(String, String, String, Option<String>, Option<i64>, Option<String>)> = sqlx::query_as(
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
    .bind(&synthetic_hash)
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();

    let hook_executions = hooks
        .into_iter()
        .map(|(hook_address, hook_kind, result, reason, gas_used, hook_name)| TxHookExecution {
            hook_address,
            hook_kind,
            result,
            reason,
            gas_used,
            hook_name,
        })
        .collect();

    Ok(Json(MetaHookInvocationDetail {
        invocation,
        hook_executions,
    }))
}

fn row_to_invocation(r: &sqlx::postgres::PgRow) -> MetaHookInvocation {
    MetaHookInvocation {
        sol_signature: r.try_get("sol_signature").unwrap_or_default(),
        slot_number: r.try_get("slot_number").unwrap_or(0),
        block_time: r.try_get("block_time").ok().flatten(),
        mint: r.try_get("mint").ok().flatten(),
        hook_count: r.try_get("hook_count").unwrap_or(0),
        outcome: r.try_get("outcome").unwrap_or_default(),
        fee_payer: r.try_get("fee_payer").ok().flatten(),
    }
}
