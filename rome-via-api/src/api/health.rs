use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
};

use crate::state::AppState;

/// GET /healthz — liveness probe. Always returns 200 "ok" if the process is running.
#[utoipa::path(
    get,
    path = "/healthz",
    responses(
        (status = 200, description = "Service is alive", body = String)
    ),
    tag = "health"
)]
pub async fn healthz() -> Response {
    (StatusCode::OK, "ok").into_response()
}

/// GET /readyz — readiness probe. Pings the DB with `SELECT 1`; returns 503 if unreachable.
#[utoipa::path(
    get,
    path = "/readyz",
    responses(
        (status = 200, description = "Service is ready (DB reachable)", body = String),
        (status = 503, description = "Service not ready (DB unreachable)", body = String)
    ),
    tag = "health"
)]
pub async fn readyz(State(state): State<AppState>) -> Response {
    match sqlx::query("SELECT 1").execute(&state.db).await {
        Ok(_) => (StatusCode::OK, "ok").into_response(),
        Err(e) => {
            tracing::warn!(error = %e, "readyz: database ping failed");
            (StatusCode::SERVICE_UNAVAILABLE, "db unavailable").into_response()
        }
    }
}
