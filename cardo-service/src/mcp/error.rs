//! MCP error mapping for cardo-service.
//!
//! The JSON-RPC 2.0 spec reserves `-32099..-32000` for implementation-defined
//! server errors. MCP piggy-backs on this to carry application-level failure
//! kinds (tool not found, app not found, etc.). We pick `-32010..-32020` for
//! Rome-specific app-level errors and keep `-32099..-32099` reserved for
//! potential future use.
//!
//! Two surfaces:
//!
//! - `McpError::code(&self)` — JSON-RPC code an error maps to.
//! - `McpError::to_json(&self)` — `{code, message}` object (suitable for the
//!   `error` field of a JSON-RPC response).
//!
//! These are the wire-format primitives. A later adapter layer lifts
//! `McpError` into rmcp's `ErrorData` when responding through the
//! `ServerHandler` trait.

use serde_json::{json, Value};
use thiserror::Error;

/// Error kinds the MCP handler can emit.
#[derive(Debug, Error)]
pub enum McpError {
    /// Request could not be parsed as valid JSON.
    #[error("parse error: {0}")]
    ParseError(String),

    /// Request parsed but is malformed (missing `jsonrpc` field, etc.).
    #[error("invalid request: {0}")]
    InvalidRequest(String),

    /// Unknown JSON-RPC method.
    #[error("method not found: {0}")]
    MethodNotFound(String),

    /// `params` failed validation against the method's schema.
    #[error("invalid params: {0}")]
    InvalidParams(String),

    /// Unexpected server-side failure. Full detail goes to logs, the wire
    /// message is kept short.
    #[error("internal error: {0}")]
    Internal(#[from] anyhow::Error),

    /// `tools/call` named a tool the registry doesn't know.
    #[error("tool not found: {0}")]
    ToolNotFound(String),

    /// Manifest-level lookup: no app with that id.
    #[error("app not found: {0}")]
    AppNotFound(String),

    /// Manifest-level lookup: app exists but capability name is unknown.
    #[error("capability not found on {app_id}: {name}")]
    CapabilityNotFound { app_id: String, name: String },

    /// `TxBuilder::simulate` returned an error (e.g. revert).
    #[error("simulation failed: {0}")]
    SimulationFailed(String),
}

impl McpError {
    /// Map to the corresponding JSON-RPC 2.0 error code.
    ///
    /// Standard codes (-32700..-32603) come from JSON-RPC 2.0 §5.1.
    /// App-level codes (-32010..-32020) live in the server-error range.
    pub fn code(&self) -> i32 {
        match self {
            Self::ParseError(_) => -32700,
            Self::InvalidRequest(_) => -32600,
            Self::MethodNotFound(_) => -32601,
            Self::InvalidParams(_) => -32602,
            Self::Internal(_) => -32603,
            Self::ToolNotFound(_) => -32010,
            Self::AppNotFound(_) => -32011,
            Self::CapabilityNotFound { .. } => -32012,
            Self::SimulationFailed(_) => -32020,
        }
    }

    /// Serialize to `{code, message}` — the shape an `error` object wants in a
    /// JSON-RPC response. `data` is omitted in v1; add it here if clients ever
    /// ask for structured diagnostics.
    pub fn to_json(&self) -> Value {
        json!({
            "code": self.code(),
            "message": self.to_string(),
        })
    }
}
