use axum::{
    http::{HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use utoipa::ToSchema;

/// RFC 7807 Problem Details response body.
/// Serialized with `Content-Type: application/problem+json`.
#[derive(Debug, Serialize, ToSchema)]
pub struct ProblemJson {
    /// A URI that identifies the problem type.
    #[serde(rename = "type")]
    pub problem_type: String,

    /// Short, human-readable summary of the problem.
    pub title: String,

    /// HTTP status code.
    pub status: u16,

    /// Optional human-readable explanation specific to this occurrence.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,

    /// Optional URI reference that identifies the specific occurrence.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instance: Option<String>,
}

/// Application error variants. Each maps to a specific HTTP status + problem type.
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("not found: {0}")]
    NotFound(String),

    #[error("bad request: {0}")]
    BadRequest(String),

    #[error("cursor invalid: {0}")]
    CursorInvalid(String),

    #[error("chain not supported: {0}")]
    ChainNotSupported(String),

    /// Phase 4: foreign rollup Proxy unreachable (→ 503).
    #[error("foreign rollup RPC unreachable: {0}")]
    ForeignProxyUnreachable(String),

    #[error("internal error: {0}")]
    Internal(String),

    #[error("internal error: {0}")]
    InternalError(#[from] anyhow::Error),

    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, problem_type, title, detail) = match &self {
            AppError::NotFound(msg) => (
                StatusCode::NOT_FOUND,
                "https://romeprotocol.xyz/errors/not-found",
                "Not Found",
                Some(msg.clone()),
            ),
            AppError::BadRequest(msg) => (
                StatusCode::BAD_REQUEST,
                "https://romeprotocol.xyz/errors/bad-request",
                "Bad Request",
                Some(msg.clone()),
            ),
            AppError::CursorInvalid(msg) => (
                StatusCode::BAD_REQUEST,
                "https://romeprotocol.xyz/errors/cursor-invalid",
                "Cursor Invalid",
                Some(msg.clone()),
            ),
            AppError::ChainNotSupported(msg) => (
                StatusCode::BAD_REQUEST,
                "https://romeprotocol.xyz/errors/chain-not-supported",
                "Chain Not Supported",
                Some(msg.clone()),
            ),
            AppError::ForeignProxyUnreachable(msg) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "https://romeprotocol.xyz/errors/foreign-proxy-unreachable",
                "Foreign rollup RPC unreachable",
                Some(msg.clone()),
            ),
            AppError::Internal(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "https://romeprotocol.xyz/errors/internal-error",
                "Internal Server Error",
                Some(msg.clone()),
            ),
            AppError::InternalError(_) | AppError::Database(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "https://romeprotocol.xyz/errors/internal-error",
                "Internal Server Error",
                None,
            ),
        };

        // Log internal errors at error level (don't expose details to clients).
        if matches!(&self, AppError::InternalError(_) | AppError::Database(_) | AppError::Internal(_)) {
            tracing::error!(error = %self, "internal error in handler");
        }

        let body = ProblemJson {
            problem_type: problem_type.to_string(),
            title: title.to_string(),
            status: status.as_u16(),
            detail,
            instance: None,
        };

        let mut response = (status, Json(body)).into_response();
        response.headers_mut().insert(
            "Content-Type",
            HeaderValue::from_static("application/problem+json"),
        );
        response
    }
}
