/// API module — assembles the full axum Router for rome-via-api.
///
/// Routes:
///   GET /healthz             — liveness probe
///   GET /readyz              — readiness probe (pings DB)
///   GET /api/v1/stats/overview
///   GET /api/v1/blocks
///   GET /api/v1/blocks/:number
///   GET /api/v1/txs
///   GET /api/v1/txs/:hash
///   GET /api/v1/tokens               — Phase 3
///   GET /api/v1/tokens/:address      — Phase 3
///   GET /api/v1/tokens/:address/holders   — Phase 3
///   GET /api/v1/tokens/:address/transfers — Phase 3
///   GET /api/v1/addresses/:address   — Phase 3
///   GET /api/v1/addresses/:address/txs — Phase 3
///   GET /api/v1/search               — Phase 3
///   GET /api/v1/cross-chain          — Phase 4: cross-chain correlations list
///   GET /api/v1/cross-vm             — the EVM<->Solana seam feed (crossings only)
///   GET /api/v1/txs/:hash/rpc        — Phase 4: tx with chain=foreign_id RPC fallback
///   GET /api/v1/hooks                — Phase 4: hooks registry list
///   GET /api/v1/stream/home          — Phase 4: SSE combined stream
///   GET /api/v1/stream/blocks        — Phase 4: SSE block events
///   GET /api/v1/stream/txs           — Phase 4: SSE tx events
///   GET /api/v1/stream/address/:addr — Phase 4: SSE address tx events
///   GET /api/v1/openapi.json — OpenAPI spec (served by SwaggerUi automatically)
///   GET /api/v1/docs         — Swagger UI
pub mod address_kind;
pub mod addresses;
pub mod audit;
pub mod blocks;
pub mod cross_chain;
pub mod health;
pub mod hooks;
pub mod meta_hooks;
pub mod models;
pub mod openapi;
pub mod search;
pub mod stats;
pub mod throughput;
pub mod tokens;
pub mod txs;

use axum::{routing::get, Router};
use tower_http::{
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};
use utoipa::OpenApi as _;
use utoipa_swagger_ui::SwaggerUi;

use crate::sse;
use crate::state::AppState;

use self::openapi::ApiDoc;

/// Build the complete axum router with all API routes, middleware, and OpenAPI docs.
pub fn router(state: AppState) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([
            axum::http::Method::GET,
            axum::http::Method::OPTIONS,
        ])
        .allow_headers(Any);

    let api_v1 = Router::new()
        // Phase 2 routes
        .route("/stats/overview", get(stats::overview))
        .route("/blocks", get(blocks::list_blocks))
        .route("/blocks/:number", get(blocks::get_block_by_number))
        .route("/txs", get(txs::list_txs))
        .route("/txs/:hash", get(txs::get_tx_by_hash))
        .route("/txs/:hash/batch-trace", get(txs::get_batch_trace))
        .route("/txs/:hash/sol-legs", get(txs::get_sol_legs))
        // Phase 3 routes
        .route("/tokens", get(tokens::list_tokens))
        .route("/tokens/:address", get(tokens::get_token))
        .route("/tokens/:address/holders", get(tokens::list_token_holders))
        .route("/tokens/:address/transfers", get(tokens::list_token_transfers))
        .route("/tokens/:address/gate-events", get(tokens::list_token_gate_events))
        .route("/addresses", get(addresses::list_addresses))
        .route("/addresses/:address", get(addresses::get_address))
        .route("/addresses/:address/txs", get(addresses::list_address_txs))
        .route("/addresses/:address/tokens", get(addresses::list_address_tokens))
        .route("/search", get(search::search))
        // Phase 4 routes
        .route("/cross-chain", get(cross_chain::list_cross_chain))
        // The cross-VM seam feed: only txs that cross EVM<->Solana, newest first.
        .route("/cross-vm", get(txs::list_cross_vm))
        .route("/txs/:hash/rpc", get(cross_chain::get_tx_with_chain))
        .route("/hooks", get(hooks::list_hooks))
        .route("/meta-hooks", get(meta_hooks::list_invocations))
        .route("/meta-hooks/:sig", get(meta_hooks::get_invocation))
        // Phase 4 SSE routes
        .route("/stream/home", get(sse::stream_home))
        .route("/stream/blocks", get(sse::stream_blocks))
        .route("/stream/txs", get(sse::stream_txs))
        .route("/stream/address/:address", get(sse::stream_address))
        .route("/throughput/current-tps", get(throughput::current_tps))
        .route("/throughput/peak-tps", get(throughput::peak_tps_handler))
        .route("/throughput/top-blocks", get(throughput::top_blocks))
        .route("/throughput/timeseries", get(throughput::timeseries))
        .route("/throughput/histogram", get(throughput::histogram))
        .route("/throughput/cadence", get(throughput::cadence))
        .route("/throughput/record", get(throughput::record))
        // Public "Audit" tab surface — a read-only browse over the PII-free
        // Tier-1 `audit.chain_event` record, public like every other endpoint.
        .merge(audit::routes())
        .with_state(state.clone());

    let health_routes = Router::new()
        .route("/healthz", get(health::healthz))
        .route("/readyz", get(health::readyz))
        .with_state(state);

    let swagger = SwaggerUi::new("/api/v1/docs")
        .url("/api/v1/openapi.json", ApiDoc::openapi());

    Router::new()
        .nest("/api/v1", api_v1)
        .merge(health_routes)
        .merge(swagger)
        .layer(TraceLayer::new_for_http())
        .layer(cors)
}
