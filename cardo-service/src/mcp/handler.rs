//! MCP request handler: dispatches JSON-RPC frames to the right tool or
//! protocol method. Holds `AppState` + `Registry` per plan.
//!
//! # Shape of [`handle`]
//!
//! `handle(&self, request: Value) -> Option<Value>`.
//!
//! - Notifications (no `id` field per JSON-RPC 2.0) return `None`.
//! - Requests return `Some({jsonrpc, id, result | error})`.
//!
//! # Testability
//!
//! Some dispatch arms (`initialize`, `ping`, unknown-method, notification)
//! don't need `AppState`. They're exposed as free functions
//! (`dispatch_initialize`, `dispatch_unknown`, etc.) so Task 3 tests can run
//! without a Postgres pool or a real `TxBuilder`. `handle` just thread them
//! through. Tasks 4-6 land the state-carrying arms with a seeded DB harness.

use crate::mcp::error::McpError;
use crate::mcp::registry::Registry;
use crate::rest::router::AppState;
use serde_json::{json, Value};

/// MCP dispatch entry point. Shared between stdio and HTTP transports; both
/// call `handle` with a decoded JSON frame.
pub struct McpHandler {
    state: AppState,
    registry: Registry,
}

impl McpHandler {
    pub fn new(state: AppState) -> Self {
        Self {
            state,
            registry: Registry::new(),
        }
    }

    /// Shared `AppState` handed to tool implementations. Used by the
    /// `ServerHandler` adapter to thread state through rmcp's trait surface.
    pub(crate) fn state_ref(&self) -> &AppState {
        &self.state
    }

    /// Static + dynamic tool registry. Used by the `ServerHandler` adapter.
    pub(crate) fn registry_ref(&self) -> &Registry {
        &self.registry
    }

    /// Primary dispatch. See module docs.
    pub async fn handle(&self, request: Value) -> Option<Value> {
        // Notification short-circuit: JSON-RPC 2.0 specifies that a request
        // without an `id` field is a notification; server returns no response.
        if dispatch_notification(&request).is_none() {
            return None;
        }

        let id = request.get("id").cloned();
        let method = request
            .get("method")
            .and_then(|m| m.as_str())
            .unwrap_or("");

        let params = request.get("params").cloned();

        match method {
            "initialize" => Some(dispatch_initialize(id)),
            "ping" => Some(wrap_result(id, json!({}))),
            "tools/list" => Some(dispatch_tools_list(&self.state, &self.registry, id).await),
            "tools/call" => Some(dispatch_tools_call(&self.state, params, id).await),
            other => Some(dispatch_unknown(id, other)),
        }
    }
}

// ----- Free dispatch helpers ------------------------------------------------
//
// These are pub so tests can exercise them without constructing an `AppState`
// (the static arms don't touch state) and so the transport layer can call
// them directly if it ever needs to bypass `McpHandler`.

/// `initialize` result. Returns `{protocolVersion, serverInfo, capabilities}`.
/// Wrapped in a full JSON-RPC response envelope when `id` is provided.
pub fn dispatch_initialize(id: Option<Value>) -> Value {
    let result = json!({
        "protocolVersion": "2025-06-18",
        "serverInfo": {
            "name": "cardo-service",
            "version": env!("CARGO_PKG_VERSION"),
        },
        "capabilities": {
            "tools": {
                "listChanged": false
            }
        }
    });
    wrap_result(id, result)
}

/// Task 3: static-only `tools/list`. Used in tests to avoid the DB path.
/// Production `handle` uses `dispatch_tools_list` which merges static +
/// dynamic.
pub fn dispatch_tools_list_static(id: Option<Value>) -> Value {
    let reg = Registry::new();
    let tools = reg.static_tools();
    wrap_result(id, json!({ "tools": tools }))
}

/// Production `tools/list` — merges static + dynamic tools. Task 5 plugs in
/// the dynamic query.
pub async fn dispatch_tools_list(
    state: &AppState,
    registry: &Registry,
    id: Option<Value>,
) -> Value {
    let tools = registry.list_tools(&state.pool).await;
    wrap_result(id, json!({ "tools": tools }))
}

/// `tools/call` dispatch. Routes tool names to their implementation modules.
/// Filled in Task 4 (read tools) and Tasks 5/6 (capability + write tools).
pub async fn dispatch_tools_call(
    state: &AppState,
    params: Option<Value>,
    id: Option<Value>,
) -> Value {
    let Some(params) = params else {
        return wrap_error(id, &McpError::InvalidParams("missing params".into()));
    };
    let name = match params.get("name").and_then(|n| n.as_str()) {
        Some(n) => n.to_string(),
        None => {
            return wrap_error(
                id,
                &McpError::InvalidParams("missing tools/call `name`".into()),
            );
        }
    };
    // MCP spec: `arguments` is an optional JSON object. Missing ⇒ default {}.
    let args = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));

    let result = match name.as_str() {
        "list_apps" => crate::mcp::tools::list_apps::call(&state.pool, &args).await,
        "describe_app" => crate::mcp::tools::describe_app::call(&state.pool, &args).await,
        "get_metrics" => crate::mcp::tools::get_metrics::call(&state.pool, &args).await,
        "quote" => crate::mcp::tools::quote::call(state, &args).await,
        "execute" => crate::mcp::tools::execute::call(state, &args).await,
        n if n.contains('.') => crate::mcp::tools::capability::call(state, n, &args).await,
        _ => Err(McpError::ToolNotFound(name.clone())),
    };

    match result {
        Ok(v) => wrap_result(id, v),
        Err(e) => wrap_error(id, &e),
    }
}

/// Return a JSON-RPC error envelope for an unknown method.
pub fn dispatch_unknown(id: Option<Value>, method: &str) -> Value {
    wrap_error(id, &McpError::MethodNotFound(method.to_string()))
}

/// Detect notification shape (no `id` field in JSON-RPC) and return a signal
/// (via `Some(Value::Null)`) that the caller should skip a response. Used by
/// `handle` and exposed to Task 3 tests.
///
/// The test helper variant returns `None` when the frame IS a notification
/// (meaning: no response should be emitted). See `dispatch_notification` in
/// the test module.
pub fn dispatch_notification(req: &Value) -> Option<Value> {
    // `handle` uses this to short-circuit dispatch for notifications. Returns
    // `None` for notification (so `handle` can return `None`). Returns
    // `Some(req)` otherwise so `handle` continues dispatching.
    if req.get("id").is_none() {
        None
    } else {
        // Not a notification — caller should proceed with normal dispatch.
        // We pass back the request unchanged so callers can reuse this helper
        // without consuming the value.
        Some(req.clone())
    }
}

fn wrap_result(id: Option<Value>, result: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id.unwrap_or(Value::Null),
        "result": result,
    })
}

fn wrap_error(id: Option<Value>, err: &McpError) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id.unwrap_or(Value::Null),
        "error": err.to_json(),
    })
}
