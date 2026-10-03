//! Tests for the MCP dispatch layer.
//!
//! Tasks 3–6 accumulate tests into this file.
//!
//! - Task 3 (unit, no DB): `initialize`, `tools/list` static, unknown method,
//!   notification. Uses the pure dispatch helpers.
//! - Task 4 (seeded DB, `#[ignore]`): `list_apps`, `describe_app`,
//!   `get_metrics` top-level tools. Shared SQL extracted to `rest::query`.
//! - Task 5 (seeded DB + capability row): dynamic per-capability tools.
//! - Task 6 (live stack, `#[ignore]`): `quote` + `execute` security grep.

mod common;

use cardo_service::mcp::handler::{
    dispatch_initialize, dispatch_notification, dispatch_tools_list_static, dispatch_unknown,
};
use cardo_service::mcp::registry::Registry;
use serde_json::{json, Value};

#[test]
fn initialize_returns_server_info() {
    let resp = dispatch_initialize(Some(json!(1)));
    let result = &resp["result"];
    assert_eq!(result["protocolVersion"], "2025-06-18");

    let server_name = result["serverInfo"]["name"]
        .as_str()
        .expect("serverInfo.name should be a string");
    // Server name advertises Rome by containing the "cardo" brand identifier.
    assert!(
        server_name.contains("cardo"),
        "serverInfo.name '{server_name}' does not advertise Rome (expected to contain 'cardo')"
    );
    assert_eq!(resp["id"], json!(1));
    assert_eq!(resp["jsonrpc"], "2.0");
}

#[test]
fn tools_list_returns_five_static_tools() {
    let reg = Registry::new();
    let tools = reg.static_tools();
    assert_eq!(
        tools.len(),
        5,
        "expected exactly 5 static tools, got {}: {:?}",
        tools.len(),
        tools.iter().map(|t| t["name"].as_str()).collect::<Vec<_>>()
    );
    let names: Vec<&str> = tools
        .iter()
        .map(|t| t["name"].as_str().expect("tool.name is string"))
        .collect();
    for expected in [
        "list_apps",
        "describe_app",
        "get_metrics",
        "quote",
        "execute",
    ] {
        assert!(
            names.contains(&expected),
            "missing static tool '{expected}': have {names:?}"
        );
    }
}

#[test]
fn static_tool_descriptors_have_schema_fields() {
    let reg = Registry::new();
    for t in reg.static_tools() {
        let name = t["name"].as_str().expect("name");
        assert!(t["description"].is_string(), "{name} missing description");
        assert!(
            t["inputSchema"].is_object(),
            "{name} missing inputSchema object"
        );
    }
}

#[test]
fn tools_list_dispatch_wraps_static_tools() {
    let resp = dispatch_tools_list_static(Some(json!(2)));
    let tools = resp["result"]["tools"]
        .as_array()
        .expect("result.tools should be an array");
    assert_eq!(tools.len(), 5);
    assert_eq!(resp["id"], json!(2));
}

#[test]
fn unknown_method_returns_method_not_found() {
    let resp = dispatch_unknown(Some(json!(7)), "not_a_method");
    assert_eq!(resp["error"]["code"], -32601);
    assert_eq!(resp["id"], json!(7));
}

#[test]
fn notification_returns_no_response() {
    // A JSON-RPC notification is a request without an `id`. The spec requires
    // the server to not respond at all — our `handle` path signals that by
    // returning None.
    let out: Option<Value> = dispatch_notification(&json!({
        "jsonrpc": "2.0",
        "method": "initialized"
    }));
    assert!(out.is_none());
}

// -----------------------------------------------------------------------------
// Task 4: top-level read tools via shared SQL — seeded DB tests.
// -----------------------------------------------------------------------------
//
// These tests round-trip through the MCP tool functions, which delegate to
// `rest::query::*`. Same SQL path as the REST handlers, same seeded fixture.

#[tokio::test]
#[ignore] // requires CARDO_POSTGRES_URL
async fn list_apps_returns_rows() {
    let (pool, _manifest) = common::db_harness().await;

    let args = json!({});
    let result = cardo_service::mcp::tools::list_apps::call(&pool, &args)
        .await
        .expect("list_apps call");
    let apps = result["apps"].as_array().expect("apps should be an array");
    assert!(!apps.is_empty(), "expected >=1 seeded app, got none");
    let ids: Vec<&str> = apps
        .iter()
        .map(|a| a["id"].as_str().unwrap_or(""))
        .collect();
    assert!(
        ids.contains(&"test-app"),
        "seeded test-app missing from response: {ids:?}"
    );
}

#[tokio::test]
#[ignore] // requires CARDO_POSTGRES_URL
async fn describe_app_returns_manifest() {
    let (pool, _manifest) = common::db_harness().await;

    let args = json!({"id": "test-app"});
    let result = cardo_service::mcp::tools::describe_app::call(&pool, &args)
        .await
        .expect("describe_app call");
    assert_eq!(result["id"], "test-app");
}

#[tokio::test]
#[ignore] // requires CARDO_POSTGRES_URL
async fn describe_app_not_found_returns_app_not_found_code() {
    let (pool, _manifest) = common::db_harness().await;

    let args = json!({"id": "does-not-exist-app"});
    let err = cardo_service::mcp::tools::describe_app::call(&pool, &args)
        .await
        .expect_err("missing app should error");
    // -32011 per error.rs. Check through `to_json` so we assert the on-wire
    // shape, not just the Rust variant.
    let json = err.to_json();
    assert_eq!(json["code"], -32011);
}

#[tokio::test]
#[ignore] // requires CARDO_POSTGRES_URL
async fn get_metrics_returns_cache_or_zeros() {
    let (pool, _manifest) = common::db_harness().await;

    let args = json!({"app_id": "test-app"});
    let result = cardo_service::mcp::tools::get_metrics::call(&pool, &args)
        .await
        .expect("get_metrics call");
    assert!(result.get("tx_7d").is_some(), "missing tx_7d");
    assert!(result.get("as_of").is_some(), "missing as_of");
}

// -----------------------------------------------------------------------------
// Task 5: dynamic per-capability tools (seeded DB).
// -----------------------------------------------------------------------------

#[tokio::test]
#[ignore] // requires CARDO_POSTGRES_URL
async fn tools_list_includes_capability_tools_from_postgres() {
    let (pool, _manifest) = common::db_harness().await;
    let tools = Registry::new().list_tools(&pool).await;
    let names: Vec<&str> = tools
        .iter()
        .map(|t| t["name"].as_str().unwrap_or(""))
        .collect();
    // The test-app fixture has one capability named "ping"; tool name is
    // "test-app.ping".
    assert!(
        names.contains(&"test-app.ping"),
        "dynamic capability tool missing: {names:?}"
    );
    // Static tools must still be present.
    for expected in ["list_apps", "describe_app"] {
        assert!(
            names.contains(&expected),
            "static tool '{expected}' dropped: {names:?}"
        );
    }
}

#[test]
fn capability_tool_rejects_missing_dot() {
    // A tool name without a '.' must not reach the capability dispatch. This
    // is a pure parser check — no DB needed.
    let err = cardo_service::mcp::tools::capability::parse_tool_name("no_dot_here")
        .expect_err("should reject");
    assert_eq!(err.to_json()["code"], -32010); // ToolNotFound
}

#[test]
fn capability_tool_accepts_dotted_name() {
    let (app, cap) = cardo_service::mcp::tools::capability::parse_tool_name("my-app.my-cap")
        .expect("should parse");
    assert_eq!(app, "my-app");
    assert_eq!(cap, "my-cap");
}

#[tokio::test]
#[ignore] // requires CARDO_POSTGRES_URL
async fn capability_tool_unknown_app_returns_capability_not_found() {
    let (pool, _manifest) = common::db_harness().await;
    let err =
        cardo_service::mcp::tools::capability::resolve(&pool, "no-such-app", "no-such-cap")
            .await
            .expect_err("unknown capability should error");
    assert_eq!(err.to_json()["code"], -32012);
}

// -----------------------------------------------------------------------------
// Task 6: quote + execute top-level tools (security invariant).
// -----------------------------------------------------------------------------

#[tokio::test]
async fn quote_tool_rejects_missing_app_id() {
    let err = cardo_service::mcp::tools::quote::validate_args(&json!({
        "capability": "ping",
        "from": "0x0000000000000000000000000000000000000000",
        "inputs": {}
    }))
    .expect_err("missing app_id should fail");
    assert_eq!(err.to_json()["code"], -32602);
}

#[tokio::test]
async fn execute_tool_rejects_missing_app_id() {
    let err = cardo_service::mcp::tools::execute::validate_args(&json!({
        "capability": "do_it",
        "from": "0x0000000000000000000000000000000000000000",
        "inputs": {}
    }))
    .expect_err("missing app_id should fail");
    assert_eq!(err.to_json()["code"], -32602);
}

#[test]
fn execute_tool_response_has_no_signing_keywords() {
    // Build the exact JSON the execute tool returns (via the shared shape
    // function) and grep for forbidden keywords. This is the M3C security
    // invariant — reviewer: if this test breaks because you added a
    // signature-shaped field to the response, reject the PR.
    let dummy = cardo_service::mcp::tools::execute::build_unsigned_response_shape(
        "0x0000000000000000000000000000000000000000",
        &[],
        "0",
        "21000",
        200200,
        0,
        "1000000000",
        "1",
    );
    let body = serde_json::to_string(&dummy).unwrap().to_lowercase();

    // Forbidden keywords: any of these appearing in the wire response means
    // the unsigned-only invariant broke.
    for forbidden in [
        "signature",
        "signatures",
        "private",
        "secret",
        "mnemonic",
        "signed_tx",
        "privkey",
    ] {
        assert!(
            !body.contains(forbidden),
            "execute response leaks forbidden keyword '{forbidden}': {body}"
        );
    }
    // Positive check: the expected unsigned-tx fields are all present.
    for expected in [
        "to",
        "data",
        "value",
        "gas_limit",
        "chain_id",
        "nonce",
        "max_fee_per_gas",
        "max_priority_fee_per_gas",
    ] {
        assert!(
            body.contains(expected),
            "execute response missing expected field '{expected}': {body}"
        );
    }
}

#[test]
fn quote_tool_returns_simulation_outcome_shape() {
    // Same shape check, applied to quote's response shape.
    let dummy = cardo_service::mcp::tools::quote::build_simulation_response_shape(&[0xde, 0xad], "12345", None);
    let body = serde_json::to_string(&dummy).unwrap();
    let body_lc = body.to_lowercase();
    // return_data present + 0x-prefixed.
    let rd = dummy["outputs"]["return_data"]
        .as_str()
        .expect("outputs.return_data should be a string");
    assert!(rd.starts_with("0x"), "return_data should be 0x-prefixed, got: {rd}");
    // gas_used is a string, not a number.
    let gu = dummy["gas_used"]
        .as_str()
        .expect("gas_used should be a string");
    assert!(!gu.is_empty());
    // No signing keywords leak here either.
    for forbidden in ["signature", "private", "secret", "mnemonic"] {
        assert!(
            !body_lc.contains(forbidden),
            "quote response leaks '{forbidden}': {body}"
        );
    }
}

#[tokio::test]
async fn list_apps_rejects_non_object_args() {
    let args = json!("not an object");
    let err = cardo_service::mcp::tools::list_apps::call(&dummy_pool(), &args)
        .await
        .expect_err("list_apps should reject non-object args");
    assert_eq!(err.to_json()["code"], -32602, "expected InvalidParams");
}

#[tokio::test]
async fn describe_app_missing_id_returns_invalid_params() {
    let args = json!({});
    let err = cardo_service::mcp::tools::describe_app::call(&dummy_pool(), &args)
        .await
        .expect_err("describe_app should require id");
    assert_eq!(err.to_json()["code"], -32602);
}

/// A pool constructed without a live DB. Any query will error on first use —
/// tests using this pool must fail at the argument-validation step, before
/// any query runs. `connect_lazy` requires a running Tokio reactor, hence
/// callers must be `#[tokio::test]`.
fn dummy_pool() -> sqlx::PgPool {
    sqlx::PgPool::connect_lazy("postgres://invalid:invalid@127.0.0.1:1/none")
        .expect("connect_lazy never returns Err")
}
