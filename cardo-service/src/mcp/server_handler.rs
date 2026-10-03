//! rmcp `ServerHandler` adapter — bridges our `serde_json::Value`-flavoured
//! [`McpHandler`] into the concrete request/response types rmcp expects.
//!
//! # Why a separate adapter
//!
//! Our `McpHandler::handle(Value) -> Option<Value>` is the pure dispatch
//! surface exercised by unit tests in `mcp_handler_test`. rmcp's transports
//! (stdio, streamable HTTP) want a type that implements [`rmcp::ServerHandler`]
//! with concrete methods like `list_tools`, `call_tool`, `initialize`, each
//! taking strongly-typed `#[non_exhaustive]` param structs.
//!
//! Rather than fork the dispatch into two parallel implementations, this
//! module defines [`ServerHandlerAdapter`], a thin wrapper that:
//!
//! 1. Implements rmcp's methods by serializing the params to `Value`,
//! 2. Shapes a JSON-RPC `request` envelope around them,
//! 3. Calls `McpHandler::handle(request)`,
//! 4. Parses the `result`/`error` envelope back into rmcp's types.
//!
//! One dispatch path, same behaviour, both entry points.

use std::borrow::Cow;
use std::sync::Arc;

use rmcp::ServerHandler;
use rmcp::model::{
    CallToolRequestParams, CallToolResult, ErrorCode, Implementation, InitializeRequestParams,
    InitializeResult, JsonObject, ListToolsResult, PaginatedRequestParams, ProtocolVersion,
    ServerCapabilities, Tool, ToolsCapability,
};
use rmcp::service::{RequestContext, RoleServer};
use rmcp::ErrorData as RmcpError;
use serde_json::{json, Value};

use crate::mcp::handler::{
    dispatch_tools_call, dispatch_tools_list, McpHandler,
};

/// Clone-cheap adapter that plugs our [`McpHandler`] into the rmcp wire loop.
/// Carries an `Arc<McpHandler>` so `ServiceExt::serve` (which owns the value)
/// can share it across the service lifetime without moving the handler.
#[derive(Clone)]
pub struct ServerHandlerAdapter {
    handler: Arc<McpHandler>,
}

impl ServerHandlerAdapter {
    pub fn new(handler: Arc<McpHandler>) -> Self {
        Self { handler }
    }
}

impl ServerHandler for ServerHandlerAdapter {
    fn get_info(&self) -> InitializeResult {
        // `InitializeResult` / `ServerCapabilities` / `Implementation` are all
        // `#[non_exhaustive]` in rmcp 1.5.0 — we build them from `Default` and
        // then assign the fields we care about. The rest stay at their
        // defaults (None / empty) which matches our capability surface: tools
        // only, no prompts, no resources, no tasks.
        let mut capabilities = ServerCapabilities::default();
        let mut tools_cap = ToolsCapability::default();
        tools_cap.list_changed = Some(false);
        capabilities.tools = Some(tools_cap);

        let mut server_info = Implementation::default();
        server_info.name = "cardo-service".to_string();
        server_info.version = env!("CARGO_PKG_VERSION").to_string();
        server_info.description = Some(
            "Rome Protocol Cardo app catalog — MCP server surface.".to_string(),
        );

        let mut result = InitializeResult::new(capabilities);
        result.protocol_version = ProtocolVersion::V_2025_06_18;
        result.server_info = server_info;
        result
    }

    async fn initialize(
        &self,
        _request: InitializeRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<InitializeResult, RmcpError> {
        // Preserve rmcp's default side-effect: record peer info on first call.
        if context.peer.peer_info().is_none() {
            context.peer.set_peer_info(_request);
        }
        Ok(self.get_info())
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, RmcpError> {
        // Run our shared dispatch path — it queries Postgres through the
        // registry exactly the same way the Value-only handler does.
        let envelope = dispatch_tools_list(
            self.handler.state_ref(),
            self.handler.registry_ref(),
            Some(json!(0)),
        )
        .await;

        extract_result(envelope)
            .and_then(|result| {
                let raw_tools = result
                    .get("tools")
                    .cloned()
                    .unwrap_or_else(|| Value::Array(vec![]));
                let arr = match raw_tools {
                    Value::Array(a) => a,
                    other => {
                        return Err(RmcpError::internal_error(
                            format!("tools field not an array: {other}"),
                            None,
                        ));
                    }
                };
                let tools: Vec<Tool> = arr
                    .into_iter()
                    .map(tool_from_json)
                    .collect::<Result<_, _>>()?;
                Ok(ListToolsResult {
                    meta: None,
                    next_cursor: None,
                    tools,
                })
            })
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, RmcpError> {
        // Re-assemble the params into the shape `dispatch_tools_call` expects:
        // `{name, arguments}`.
        let params = json!({
            "name": request.name,
            "arguments": request.arguments.unwrap_or_default(),
        });
        let envelope =
            dispatch_tools_call(self.handler.state_ref(), Some(params), Some(json!(0))).await;

        let result = extract_result(envelope)?;
        // `CallToolResult::structured` populates `content` + `structuredContent`
        // + `is_error = false` in one go; matches our wire expectations.
        Ok(CallToolResult::structured(result))
    }
}

/// Convert a `registry` tool descriptor (a `{name, description, inputSchema, ...}`
/// JSON object) into the concrete `Tool` struct rmcp wants. Unknown fields are
/// dropped — the registry is the source of truth for MCP tool shape, and rmcp's
/// Tool struct is `#[non_exhaustive]` so we can only populate what we need.
fn tool_from_json(value: Value) -> Result<Tool, RmcpError> {
    let Value::Object(map) = value else {
        return Err(RmcpError::internal_error(
            "tool descriptor is not an object".to_string(),
            None,
        ));
    };

    let name = map
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            RmcpError::internal_error("tool descriptor missing `name`".to_string(), None)
        })?
        .to_owned();
    let description = map
        .get("description")
        .and_then(|v| v.as_str())
        .map(|s| Cow::<'static, str>::Owned(s.to_owned()));
    let input_schema = match map.get("inputSchema") {
        Some(Value::Object(o)) => Arc::new(o.clone()),
        _ => Arc::new(JsonObject::new()),
    };
    let output_schema = match map.get("outputSchema") {
        Some(Value::Object(o)) => Some(Arc::new(o.clone())),
        _ => None,
    };

    // `Tool` is `#[non_exhaustive]` — build from `Default` + field assignment.
    let mut tool = Tool::default();
    tool.name = Cow::Owned(name);
    tool.description = description;
    tool.input_schema = input_schema;
    tool.output_schema = output_schema;
    Ok(tool)
}

/// Extract the `result` field from a JSON-RPC envelope, or map an `error`
/// entry into an [`RmcpError`] of the same code. The envelope is what our
/// `dispatch_*` helpers return.
fn extract_result(envelope: Value) -> Result<Value, RmcpError> {
    if let Some(err) = envelope.get("error") {
        let code = err
            .get("code")
            .and_then(|c| c.as_i64())
            .map(|c| ErrorCode(c as i32))
            .unwrap_or(ErrorCode::INTERNAL_ERROR);
        let message = err
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("unknown MCP error")
            .to_owned();
        return Err(RmcpError::new(code, message, None));
    }
    let result = envelope.get("result").cloned().unwrap_or(Value::Null);
    Ok(result)
}

#[cfg(test)]
mod tests {
    //! Smoke tests for the adapter layer. Keep these pure-logic (no rmcp
    //! transport harness) — the Value round-trip is what matters.
    use super::*;

    #[test]
    fn tool_from_json_maps_common_fields() {
        let input = json!({
            "name": "describe_app",
            "description": "desc",
            "inputSchema": {"type": "object"}
        });
        let tool = tool_from_json(input).expect("should parse");
        assert_eq!(tool.name, "describe_app");
        assert_eq!(tool.description.as_deref(), Some("desc"));
        assert_eq!(
            tool.input_schema.get("type").and_then(|v| v.as_str()),
            Some("object")
        );
    }

    #[test]
    fn tool_from_json_rejects_non_object() {
        let err = tool_from_json(json!("not an object")).unwrap_err();
        assert_eq!(err.code, ErrorCode::INTERNAL_ERROR);
    }

    #[test]
    fn extract_result_unwraps_success() {
        let env = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {"ok": true}
        });
        let v = extract_result(env).unwrap();
        assert_eq!(v, json!({"ok": true}));
    }

    #[test]
    fn extract_result_maps_error() {
        let env = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "error": {"code": -32010, "message": "tool not found"}
        });
        let err = extract_result(env).unwrap_err();
        assert_eq!(err.code, ErrorCode(-32010));
        assert_eq!(err.message, "tool not found");
    }
}
