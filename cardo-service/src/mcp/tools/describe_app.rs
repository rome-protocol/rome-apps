//! MCP `describe_app` tool. Delegates to `rest::query::get_app`.

use serde_json::Value;
use sqlx::PgPool;

use crate::mcp::error::McpError;
use crate::rest::query;

/// Arguments: `{"id": "<app-id>"}`.
pub async fn call(pool: &PgPool, args: &Value) -> Result<Value, McpError> {
    let id = args
        .get("id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| McpError::InvalidParams("missing or non-string `id`".into()))?
        .to_string();

    let maybe = query::get_app(pool, &id).await.map_err(McpError::Internal)?;
    let raw = maybe.ok_or_else(|| McpError::AppNotFound(id.clone()))?;

    tracing::info!(
        client_kind = "agent",
        tool = "describe_app",
        app_id = %id,
        "tools/call"
    );
    Ok(raw)
}
