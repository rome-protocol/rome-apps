/// Hooks registry endpoint.
///
/// GET /api/v1/hooks — list registered hooks (from hooks_registry table).
use axum::{
    extract::{Query, State},
    response::IntoResponse,
    Json,
};
use serde::Deserialize;
use sqlx::Row;

use crate::{
    api::models::{HookEntry, Page},
    error::AppError,
    state::AppState,
};

const DEFAULT_LIMIT: i64 = 50;
const MAX_LIMIT: i64 = 200;

/// Query parameters for GET /api/v1/hooks.
#[derive(Debug, Deserialize)]
pub struct HooksQuery {
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    /// Filter by token address.
    pub token: Option<String>,
}

/// GET /api/v1/hooks — list hooks registered on this chain.
#[utoipa::path(
    get,
    path = "/api/v1/hooks",
    params(
        ("limit" = Option<i64>, Query, description = "Items per page (default 50, max 200)"),
        ("offset" = Option<i64>, Query, description = "Offset for pagination"),
        ("token" = Option<String>, Query, description = "Filter by token address")
    ),
    responses(
        (status = 200, description = "Paginated hooks list", body = inline(Page<HookEntry>)),
        (status = 500, description = "Internal error", body = crate::error::ProblemJson)
    ),
    tag = "hooks"
)]
pub async fn list_hooks(
    State(state): State<AppState>,
    Query(params): Query<HooksQuery>,
) -> Result<impl IntoResponse, AppError> {
    let limit = params.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let offset = params.offset.unwrap_or(0).max(0);
    let chain_id = state.chain_id;

    let rows = sqlx::query(
        r#"
        SELECT token_address, hook_address, hook_kind, registered_at, registered_tx_hash, name
        FROM rome_via.hooks_registry
        WHERE chain_id = $1
          AND ($2::VARCHAR IS NULL OR token_address ILIKE $2)
        ORDER BY registered_at DESC
        LIMIT $3 OFFSET $4
        "#,
    )
    .bind(chain_id)
    .bind(params.token.as_deref())
    .bind(limit + 1)
    .bind(offset)
    .fetch_all(&state.db)
    .await?;

    let has_more = rows.len() as i64 > limit;

    let items: Vec<HookEntry> = rows
        .into_iter()
        .take(limit as usize)
        .map(|r| {
            let registered_at: Option<chrono::DateTime<chrono::Utc>> =
                r.try_get("registered_at").ok().flatten();
            HookEntry {
                token_address: r.try_get("token_address").unwrap_or_default(),
                hook_address: r.try_get("hook_address").unwrap_or_default(),
                hook_kind: r.try_get("hook_kind").unwrap_or_default(),
                registered_at: registered_at
                    .map(|t| t.format("%Y-%m-%dT%H:%M:%SZ").to_string())
                    .unwrap_or_default(),
                registered_tx_hash: r.try_get("registered_tx_hash").ok().flatten(),
                name: r.try_get("name").ok().flatten(),
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
