//! Axum router + shared `AppState` for the REST layer.
//!
//! `AppState` wraps the Postgres pool, the `TxBuilder`, and the CDN base URL in
//! an `Arc` so handlers can cheaply clone a shared view of the backend.

use std::sync::Arc;

use axum::routing::{get, post};
use axum::Router;
use sqlx::PgPool;

use crate::tx_builder::TxBuilder;

/// Inner state held behind an `Arc` so every handler clones a cheap pointer.
pub struct AppStateInner {
    pub pool: PgPool,
    pub tx_builder: TxBuilder,
    pub cdn_url: String,
}

/// The state type handed to every handler. Clone-cheap.
pub type AppState = Arc<AppStateInner>;

/// Assemble the complete REST router with all eight endpoints from the M3B
/// plan's endpoint surface.
pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(super::handlers::health))
        .route("/ready", get(super::handlers::ready))
        .route("/apps", get(super::handlers::list_apps))
        .route("/apps/:id", get(super::handlers::get_app))
        .route("/apps/:id/metrics", get(super::handlers::get_metrics))
        .route(
            "/apps/:id/capabilities/:name/quote",
            post(super::handlers::quote),
        )
        .route(
            "/apps/:id/capabilities/:name/execute",
            post(super::handlers::execute),
        )
        .route("/docs/openapi.json", get(super::openapi::openapi_json))
        .with_state(state)
}

/// Assemble the REST router with the MCP streamable-HTTP transport merged
/// on top. Used by `main.rs` when `CARDO_MCP_MODE=both` (the default) — one
/// listener serves both surfaces. The REST sub-router gets its state via
/// `build_router`; the MCP sub-router carries its own adapter state, so
/// merging is safe.
pub fn build_router_with_mcp(
    state: AppState,
    mcp_handler: Arc<crate::mcp::McpHandler>,
    allowed_hosts: Vec<String>,
) -> Router {
    build_router(state).merge(crate::mcp::transport::http::build_mcp_router_with_allowed_hosts(
        mcp_handler,
        allowed_hosts,
    ))
}

/// MCP-only router, for the `CARDO_MCP_MODE=http` flavour where we want to
/// serve just the agent surface (no REST). Useful for a standalone
/// `mcp.cardo.rome.builders` deployment.
pub fn build_mcp_only_router(
    mcp_handler: Arc<crate::mcp::McpHandler>,
    allowed_hosts: Vec<String>,
) -> Router {
    crate::mcp::transport::http::build_mcp_router_with_allowed_hosts(mcp_handler, allowed_hosts)
}
