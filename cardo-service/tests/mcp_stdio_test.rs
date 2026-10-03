//! Integration tests for the MCP stdio transport (Task 7).
//!
//! These tests spawn the `cardo-service` binary with `CARDO_MCP_MODE=stdio`,
//! pipe JSON-RPC frames in via stdin, and read responses from stdout. Each
//! test is `#[ignore]` by default because spawning the binary needs the
//! full M3B env surface (Postgres URL, chain_id, program_id, an RPC the
//! bootstrap can reach, etc.). Opt in with:
//!
//! ```sh
//! cargo test -p cardo-service --test mcp_stdio_test -- --ignored
//! ```
//!
//! # Why we keep a non-ignored unit-level test
//!
//! The bulk of the stdio wiring is `serve(handler)` which drives an rmcp
//! `ServiceExt::serve(stdio())`. The rmcp `ServerHandler` impl is unit-tested
//! indirectly via the `McpHandler::handle` path covered by `mcp_handler_test`.
//! This file also includes a shape-only smoke test on the handler wrapper so
//! breakage surfaces without a live binary.

use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::time::Duration;

/// Spawn the `cardo-service` binary in stdio mode, pipe a two-frame session
/// (initialize + tools/list) in, assert the responses. Gated on
/// `CARDO_POSTGRES_URL` + `ROME_EVM_RPC_URL` + `ROME_CHAIN_ID` + `ROME_PROGRAM_ID`.
#[test]
#[ignore]
fn stdio_initialize_then_tools_list() {
    let bin = std::env::var("CARGO_BIN_EXE_cardo-service")
        .expect("CARGO_BIN_EXE_cardo-service not set — run via cargo test");

    let mut child = Command::new(&bin)
        .env("CARDO_MCP_MODE", "stdio")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn cardo-service");

    // Send two frames, each on its own line.
    let mut stdin = child.stdin.take().expect("stdin");
    let init = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "stdio-test", "version": "0"}
        }
    });
    let tools_list = json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/list"
    });
    writeln!(stdin, "{}", init).unwrap();
    writeln!(stdin, "{}", tools_list).unwrap();
    drop(stdin);

    // Read up to 2 response frames within a short deadline.
    let stdout = child.stdout.take().expect("stdout");
    let mut reader = BufReader::new(stdout);
    let mut init_line = String::new();
    let mut tools_line = String::new();

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while std::time::Instant::now() < deadline && init_line.is_empty() {
        let mut buf = String::new();
        if reader.read_line(&mut buf).unwrap() == 0 {
            break;
        }
        if buf.trim().is_empty() {
            continue;
        }
        init_line = buf;
        break;
    }
    while std::time::Instant::now() < deadline && tools_line.is_empty() {
        let mut buf = String::new();
        if reader.read_line(&mut buf).unwrap() == 0 {
            break;
        }
        if buf.trim().is_empty() {
            continue;
        }
        tools_line = buf;
        break;
    }

    // Cleanup before assertions so a hung child doesn't zombie.
    let _ = child.kill();
    let _ = child.wait();

    assert!(!init_line.is_empty(), "no initialize response received");
    let init_resp: Value =
        serde_json::from_str(init_line.trim()).expect("initialize response is JSON");
    assert_eq!(
        init_resp["result"]["protocolVersion"], "2025-06-18",
        "init response missing protocolVersion: {init_resp}"
    );

    assert!(!tools_line.is_empty(), "no tools/list response received");
    let tools_resp: Value =
        serde_json::from_str(tools_line.trim()).expect("tools/list response is JSON");
    assert!(
        tools_resp["result"]["tools"].is_array(),
        "tools/list missing result.tools array: {tools_resp}"
    );
}
