//! OpenAPI v3 document for the Rome App Distribution Portal REST surface.
//!
//! The handler bodies live in `handlers.rs`. The `_op` wrapper functions below
//! exist only to host `#[utoipa::path(...)]` attributes — this keeps the
//! OpenAPI registration in one place without littering the real handlers with
//! derive macros. Every wrapper's signature corresponds 1:1 to a real handler;
//! if a new endpoint is added, add a matching wrapper here and register it in
//! `paths(...)`.

use axum::response::{IntoResponse, Json};
use utoipa::OpenApi;

use super::types::{
    AppListResponse, AppSummary, ExecuteRequest, MetricsResponse, QuoteRequest, QuoteResponse,
    UnsignedTxResponse,
};

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Rome App Distribution Portal",
        version = "0.1.0",
        description = "Catalog + execution API for the Rome App Distribution Portal. Lists apps ingested from the signed manifest CDN and exposes simulate/execute over capability ABIs."
    ),
    paths(
        health_op,
        ready_op,
        list_apps_op,
        get_app_op,
        get_metrics_op,
        quote_op,
        execute_op,
    ),
    components(schemas(
        AppSummary,
        AppListResponse,
        QuoteRequest,
        QuoteResponse,
        ExecuteRequest,
        UnsignedTxResponse,
        MetricsResponse,
    )),
    tags(
        (name = "explorer", description = "Rome App Distribution Portal — catalog + exec API")
    )
)]
pub struct ApiDoc;

/// GET /docs/openapi.json — serialized OpenAPI v3 document.
pub async fn openapi_json() -> impl IntoResponse {
    Json(ApiDoc::openapi())
}

// ---------------------------------------------------------------------------
// utoipa path wrappers. Bodies are intentionally empty — utoipa only reads the
// `#[utoipa::path]` attribute to build the spec, not the function body.
// ---------------------------------------------------------------------------

#[utoipa::path(
    get,
    path = "/health",
    responses((status = 200, description = "Liveness ok")),
    tag = "explorer"
)]
#[allow(dead_code)]
pub fn health_op() {}

#[utoipa::path(
    get,
    path = "/ready",
    responses(
        (status = 200, description = "All backends reachable"),
        (status = 503, description = "At least one backend unreachable"),
    ),
    tag = "explorer"
)]
#[allow(dead_code)]
pub fn ready_op() {}

#[utoipa::path(
    get,
    path = "/apps",
    params(
        ("tier" = Option<String>, Query, description = "Filter by tier (`featured` or `long-tail`)"),
        ("status" = Option<String>, Query, description = "Filter by status (`live`, `coming-soon`, `deprecated`)"),
        ("category" = Option<String>, Query, description = "Filter by category (array-membership)"),
        ("q" = Option<String>, Query, description = "Free-text search over name + description"),
        ("limit" = Option<u32>, Query, description = "Items per page (default 50, max 200)"),
        ("offset" = Option<u32>, Query, description = "Pagination offset (default 0)"),
    ),
    responses((status = 200, body = AppListResponse)),
    tag = "explorer"
)]
#[allow(dead_code)]
pub fn list_apps_op() {}

#[utoipa::path(
    get,
    path = "/apps/{id}",
    params(("id" = String, Path, description = "App id")),
    responses(
        (status = 200, description = "Full manifest JSON"),
        (status = 404, description = "App not found"),
    ),
    tag = "explorer"
)]
#[allow(dead_code)]
pub fn get_app_op() {}

#[utoipa::path(
    get,
    path = "/apps/{id}/metrics",
    params(("id" = String, Path, description = "App id")),
    responses(
        (status = 200, body = MetricsResponse),
        (status = 404, description = "App not found"),
    ),
    tag = "explorer"
)]
#[allow(dead_code)]
pub fn get_metrics_op() {}

#[utoipa::path(
    post,
    path = "/apps/{id}/capabilities/{name}/quote",
    params(
        ("id" = String, Path, description = "App id"),
        ("name" = String, Path, description = "Capability name"),
    ),
    request_body = QuoteRequest,
    responses(
        (status = 200, body = QuoteResponse),
        (status = 404, description = "App or capability not found"),
        (status = 422, description = "Simulation failed"),
    ),
    tag = "explorer"
)]
#[allow(dead_code)]
pub fn quote_op() {}

#[utoipa::path(
    post,
    path = "/apps/{id}/capabilities/{name}/execute",
    params(
        ("id" = String, Path, description = "App id"),
        ("name" = String, Path, description = "Capability name"),
    ),
    request_body = ExecuteRequest,
    responses(
        (status = 200, body = UnsignedTxResponse, description = "Unsigned EIP-1559 tx for the client to sign and broadcast"),
        (status = 404, description = "App or capability not found"),
    ),
    tag = "explorer"
)]
#[allow(dead_code)]
pub fn execute_op() {}
