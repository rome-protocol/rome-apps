//! Tests for the MCP error mapping. These assert that:
//!
//! 1. JSON-RPC 2.0 standard codes map to the right variants (-32700..-32603).
//! 2. Rome-specific MCP app-level codes land inside the MCP app-level range
//!    (-32099..-32001).
//! 3. `to_json()` emits the on-wire shape clients expect (`{code, message}`).
//!
//! These tests intentionally avoid touching the ServerHandler / rmcp wire types
//! — they exercise the pure mapping function so later tasks can layer the
//! handler plumbing on top without re-asserting the codes.

use cardo_service::mcp::McpError;
use serde_json::Value;

#[test]
fn code_mapping_matches_jsonrpc_spec() {
    assert_eq!(McpError::ParseError("x".into()).code(), -32700);
    assert_eq!(McpError::InvalidRequest("x".into()).code(), -32600);
    assert_eq!(McpError::MethodNotFound("x".into()).code(), -32601);
    assert_eq!(McpError::InvalidParams("x".into()).code(), -32602);
    assert_eq!(
        McpError::Internal(anyhow::anyhow!("boom")).code(),
        -32603
    );
}

#[test]
fn tool_level_codes_are_in_mcp_range() {
    // MCP app-level error codes are in the JSON-RPC server-error range:
    // -32099..-32000 is reserved for implementation-defined server errors per
    // the JSON-RPC 2.0 spec. Our app-level codes live in -32099..-32001 so the
    // agent can distinguish them from transport-level errors.
    for e in [
        McpError::ToolNotFound("x".into()),
        McpError::AppNotFound("x".into()),
        McpError::CapabilityNotFound {
            app_id: "a".into(),
            name: "n".into(),
        },
        McpError::SimulationFailed("x".into()),
    ] {
        let c = e.code();
        assert!(
            (-32099..=-32001).contains(&c),
            "code {c} for {e:?} not in MCP app range"
        );
    }
}

#[test]
fn error_json_shape() {
    let v: Value = McpError::MethodNotFound("foo".into()).to_json();
    assert_eq!(v["code"], -32601);
    let m = v["message"].as_str().expect("message should be a string");
    assert!(m.contains("foo"), "message missing method: {m}");
}
