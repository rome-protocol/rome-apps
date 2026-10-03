//! REST handlers for the Rome App Distribution Portal.
//!
//! Endpoint surface (see router.rs for wiring):
//!   GET  /health                                       — liveness
//!   GET  /ready                                        — Postgres + CDN probe
//!   GET  /apps                                         — list with filters
//!   GET  /apps/:id                                     — full manifest
//!   GET  /apps/:id/metrics                             — metrics cache
//!   POST /apps/:id/capabilities/:name/quote            — simulate
//!   POST /apps/:id/capabilities/:name/execute          — build unsigned tx
//!   GET  /docs/openapi.json                            — served by openapi.rs
//!
//! Security invariant (see tx_builder.rs): `execute` returns unsigned tx data
//! only — the server never signs and never sees a private key.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde_json::{json, Value};
use sqlx::Row;

use super::{
    attribution::ClientKind,
    encoding::encode_inputs_as_hex,
    error::AppError,
    query,
    router::AppState,
    types::{
        AppListQuery, AppListResponse, ExecuteRequest, MetricsResponse, QuoteRequest,
        QuoteResponse, UnsignedTxResponse,
    },
};

/// Liveness probe. Always 200 — success does not depend on any backend.
pub async fn health() -> impl IntoResponse {
    (StatusCode::OK, Json(json!({"status": "ok"})))
}

/// Readiness probe: pings Postgres with a trivial query and the CDN base URL
/// with a HEAD request. Returns 503 if either is unreachable.
pub async fn ready(State(state): State<AppState>) -> impl IntoResponse {
    let pg_ok = sqlx::query("SELECT 1")
        .execute(&state.pool)
        .await
        .is_ok();

    let cdn_ok = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .build()
    {
        Ok(client) => client
            .head(&state.cdn_url)
            .send()
            .await
            .map(|r| r.status().is_success() || r.status().is_redirection())
            .unwrap_or(false),
        Err(_) => false,
    };

    if pg_ok && cdn_ok {
        (
            StatusCode::OK,
            Json(json!({"postgres": "ok", "cdn": "ok"})),
        )
            .into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"postgres": pg_ok, "cdn": cdn_ok})),
        )
            .into_response()
    }
}

/// List apps with optional filters + pagination. Delegates to
/// `rest::query::list_apps` — the same SQL path the MCP tool uses.
pub async fn list_apps(
    State(state): State<AppState>,
    Query(q): Query<AppListQuery>,
    kind: ClientKind,
) -> Result<Json<AppListResponse>, AppError> {
    let resp = query::list_apps(&state.pool, &q)
        .await
        .map_err(AppError::Internal)?;
    tracing::info!(
        client_kind = kind.as_str(),
        limit = resp.limit,
        offset = resp.offset,
        "list_apps"
    );
    Ok(Json(resp))
}

/// Return the full manifest JSON for a single app. Shares SQL with MCP's
/// `describe_app` tool via `rest::query::get_app`.
pub async fn get_app(
    State(state): State<AppState>,
    Path(id): Path<String>,
    kind: ClientKind,
) -> Result<Json<Value>, AppError> {
    let maybe = query::get_app(&state.pool, &id)
        .await
        .map_err(AppError::Internal)?;
    let raw = maybe.ok_or_else(|| AppError::AppNotFound(id.clone()))?;
    tracing::info!(client_kind = kind.as_str(), id = %id, "get_app");
    Ok(Json(raw))
}

/// Read metrics from the manifest's `metrics_cache` sub-object. Missing
/// fields default to zeros so the response shape is always stable; M3D adds
/// a dedicated metrics table that will replace this read. Shares SQL + slice
/// logic with MCP's `get_metrics` tool via `rest::query::get_metrics`.
pub async fn get_metrics(
    State(state): State<AppState>,
    Path(id): Path<String>,
    kind: ClientKind,
) -> Result<Json<MetricsResponse>, AppError> {
    let maybe = query::get_metrics(&state.pool, &id)
        .await
        .map_err(AppError::Internal)?;
    let resp = maybe.ok_or_else(|| AppError::AppNotFound(id.clone()))?;
    tracing::info!(client_kind = kind.as_str(), id = %id, "get_metrics");
    Ok(Json(resp))
}

/// Simulate a capability call via `TxBuilder::simulate`. Returns
/// `QuoteResponse` with `return_data` hex-encoded under `outputs`.
pub async fn quote(
    State(state): State<AppState>,
    Path((app_id, name)): Path<(String, String)>,
    kind: ClientKind,
    Json(req): Json<QuoteRequest>,
) -> Result<Json<QuoteResponse>, AppError> {
    let (contract_addr, _abi) = resolve_capability(&state.pool, &app_id, &name).await?;
    let calldata_hex = encode_inputs_as_hex(&req.inputs)?;

    let sim = state
        .tx_builder
        .simulate(&req.from, &contract_addr, &calldata_hex, "0")
        .await
        .map_err(|e| AppError::SimulationFailed(e.to_string()))?;

    let outputs = json!({
        "return_data": format!("0x{}", hex::encode(&sim.return_data))
    });

    tracing::info!(
        client_kind = kind.as_str(),
        app_id = %app_id,
        capability = %name,
        "quote"
    );
    Ok(Json(QuoteResponse {
        outputs,
        gas_used: sim.gas_used.to_string(),
        cu_used: sim.cu_used,
    }))
}

/// Build an unsigned EIP-1559 tx via `TxBuilder::build_unsigned`. The client
/// signs + broadcasts — this server never touches a private key.
pub async fn execute(
    State(state): State<AppState>,
    Path((app_id, name)): Path<(String, String)>,
    kind: ClientKind,
    Json(req): Json<ExecuteRequest>,
) -> Result<Json<UnsignedTxResponse>, AppError> {
    let (contract_addr, _abi) = resolve_capability(&state.pool, &app_id, &name).await?;
    let calldata_hex = encode_inputs_as_hex(&req.inputs)?;
    let gas_limit_opt = req.gas_limit.as_deref();

    let ut = state
        .tx_builder
        .build_unsigned(
            &req.from,
            &contract_addr,
            &calldata_hex,
            "0",
            gas_limit_opt,
        )
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("build_unsigned: {e}")))?;

    tracing::info!(
        client_kind = kind.as_str(),
        app_id = %app_id,
        capability = %name,
        "execute"
    );
    Ok(Json(UnsignedTxResponse {
        to: format!("0x{:x}", ut.to),
        data: format!("0x{}", hex::encode(&ut.data)),
        value: ut.value.to_string(),
        gas_limit: ut.gas_limit.to_string(),
        chain_id: ut.chain_id,
        nonce: ut.nonce,
        max_fee_per_gas: ut.max_fee_per_gas.to_string(),
        max_priority_fee_per_gas: ut.max_priority_fee_per_gas.to_string(),
    }))
}

/// Load the contract address and ABI string for a given (app_id, capability)
/// pair. Joins `app_capabilities` with `apps` — each capability inherits the
/// app's `contract_addr` for now (v1 simplification).
async fn resolve_capability(
    pool: &sqlx::PgPool,
    app_id: &str,
    name: &str,
) -> Result<(String, String), AppError> {
    let row = sqlx::query(
        "SELECT a.contract_addr AS contract_addr, c.abi AS abi \
         FROM app_capabilities c JOIN apps a ON a.id = c.app_id \
         WHERE c.app_id = $1 AND c.name = $2",
    )
    .bind(app_id)
    .bind(name)
    .fetch_optional(pool)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("resolve_capability: {e}")))?;
    let row = row.ok_or_else(|| AppError::CapabilityNotFound {
        app_id: app_id.into(),
        name: name.into(),
    })?;
    Ok((row.get("contract_addr"), row.get("abi")))
}

