//! MCP `list_apps` tool. Delegates to `rest::query::list_apps` so MCP and REST
//! share their SQL.

use serde_json::{json, Value};
use sqlx::PgPool;

use crate::mcp::error::McpError;
use crate::rest::query;
use crate::rest::types::AppListQuery;

/// Run a list_apps query. Arguments match the tool descriptor in
/// `mcp::registry::Registry::static_tools`.
pub async fn call(pool: &PgPool, args: &Value) -> Result<Value, McpError> {
    // Reject non-object args before hitting serde — `from_value` on a scalar
    // returns the same `Error::invalid_type` either way but the message is
    // clearer when we handle it here.
    if !args.is_object() {
        return Err(McpError::InvalidParams(
            "args must be a JSON object".into(),
        ));
    }
    let q: AppListQuery = serde_json::from_value(args.clone())
        .map_err(|e| McpError::InvalidParams(format!("bad list_apps args: {e}")))?;

    let resp = query::list_apps(pool, &q)
        .await
        .map_err(McpError::Internal)?;

    tracing::info!(
        client_kind = "agent",
        tool = "list_apps",
        limit = resp.limit,
        offset = resp.offset,
        "tools/call"
    );

    // Serialize `AppListResponse` to a `Value` so the handler can return it
    // without knowing its concrete type.
    Ok(json!({
        "apps": resp.apps,
        "total": resp.total,
        "limit": resp.limit,
        "offset": resp.offset,
    }))
}
