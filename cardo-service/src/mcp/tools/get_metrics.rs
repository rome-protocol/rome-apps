//! MCP `get_metrics` tool. Delegates to `rest::query::get_metrics`.

use serde_json::{json, Value};
use sqlx::PgPool;

use crate::mcp::error::McpError;
use crate::rest::query;

/// Arguments: `{"app_id": "<app-id>"}`.
pub async fn call(pool: &PgPool, args: &Value) -> Result<Value, McpError> {
    let app_id = args
        .get("app_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| McpError::InvalidParams("missing or non-string `app_id`".into()))?
        .to_string();

    let maybe = query::get_metrics(pool, &app_id)
        .await
        .map_err(McpError::Internal)?;
    let resp = maybe.ok_or_else(|| McpError::AppNotFound(app_id.clone()))?;

    tracing::info!(
        client_kind = "agent",
        tool = "get_metrics",
        app_id = %app_id,
        "tools/call"
    );

    // Return the flat MetricsResponse shape the REST handler emits.
    Ok(json!({
        "tx_7d": resp.tx_7d,
        "unique_callers_7d": resp.unique_callers_7d,
        "agent_share_7d": resp.agent_share_7d,
        "gas_usd_7d": resp.gas_usd_7d,
        "as_of": resp.as_of,
    }))
}
