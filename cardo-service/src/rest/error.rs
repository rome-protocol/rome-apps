use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("app not found: {0}")]
    AppNotFound(String),
    #[error("capability not found on {app_id}: {name}")]
    CapabilityNotFound { app_id: String, name: String },
    #[error("invalid request: {0}")]
    BadRequest(String),
    #[error("simulation failed: {0}")]
    SimulationFailed(String),
    #[error("database unavailable")]
    DatabaseUnavailable,
    #[error("internal error: {0}")]
    Internal(#[from] anyhow::Error),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            AppError::AppNotFound(_) => (StatusCode::NOT_FOUND, self.to_string()),
            AppError::CapabilityNotFound { .. } => (StatusCode::NOT_FOUND, self.to_string()),
            AppError::BadRequest(_) => (StatusCode::BAD_REQUEST, self.to_string()),
            AppError::SimulationFailed(_) => (StatusCode::UNPROCESSABLE_ENTITY, self.to_string()),
            AppError::DatabaseUnavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                "database unavailable".into(),
            ),
            AppError::Internal(e) => {
                tracing::error!(error = ?e, "internal error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error".into(),
                )
            }
        };
        (status, axum::Json(json!({ "error": message }))).into_response()
    }
}
