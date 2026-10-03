//! MCP stdio transport.
//!
//! Thin wrapper around rmcp's [`rmcp::transport::stdio`] pair. Caller passes
//! in an [`McpHandler`]; we wrap it in the [`ServerHandlerAdapter`] and hand
//! it to `ServiceExt::serve` along with `(stdin, stdout)` as the transport.
//!
//! # Framing
//!
//! rmcp's stdio transport is newline-delimited JSON (one message per LF line),
//! which matches the MCP spec rev 2025-06-18 §Transport — stdio.
//!
//! # Why rely on rmcp rather than drive the loop ourselves
//!
//! The plan's pseudocode outlines a hand-rolled loop around `BufReader::lines`
//! + `serde_json::from_str`. Implementing that ourselves would re-create
//! behaviour rmcp's `serve_inner` already does correctly: per-request spawn,
//! concurrent response serialization, drain-on-close, cancellation-token
//! propagation. Sticking with `ServiceExt::serve` keeps us behaviourally
//! identical to every other rmcp MCP server — which is what we want if a
//! client ever treats us like a plain MCP server.

use std::sync::Arc;

use anyhow::Result;
use rmcp::{transport::stdio, ServiceExt};

use crate::mcp::handler::McpHandler;
use crate::mcp::server_handler::ServerHandlerAdapter;

/// Serve MCP over stdin/stdout until EOF or the process is cancelled.
///
/// Returns after the rmcp service loop exits — typically because stdin
/// closed (EOF) or the [`ServiceExt::serve`] future was cancelled from
/// the caller.
pub async fn serve(handler: Arc<McpHandler>) -> Result<()> {
    tracing::info!("MCP stdio transport: starting");
    let adapter = ServerHandlerAdapter::new(handler);
    let service = adapter
        .serve(stdio())
        .await
        .map_err(|e| anyhow::anyhow!("rmcp serve failed: {e}"))?;
    // Block here until the service loop exits (EOF on stdin or external
    // cancellation). `.waiting()` consumes the `RunningService` and returns
    // the `QuitReason`.
    let reason = service
        .waiting()
        .await
        .map_err(|e| anyhow::anyhow!("rmcp service join error: {e}"))?;
    tracing::info!(?reason, "MCP stdio transport: stopped");
    Ok(())
}
