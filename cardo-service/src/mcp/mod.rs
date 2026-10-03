//! MCP server surface for `cardo-service`.
//!
//! # Task 1 recon outcome (recorded per plan §Task 1)
//!
//! **Path:** SDK (`rmcp` crate, pinned `=1.5.0`).
//!
//! All five rubric items hold against rmcp 1.5.0:
//! - Server role: `ServerHandler` trait (this module implements it on
//!   [`handler::McpHandler`]).
//! - stdio transport: `rmcp::transport::stdio::stdio()` + `ServiceExt::serve`.
//! - Streamable HTTP: `rmcp::transport::streamable_http_server::StreamableHttpService`
//!   behind the `transport-streamable-http-server` feature.
//! - Runtime tool registration: we implement `ServerHandler::list_tools` and
//!   `ServerHandler::call_tool` directly (skipping the `#[tool_router]` macro)
//!   so the tool list can be enumerated from Postgres on each `tools/list`.
//! - Semver-stable (≥1.0): rmcp is at 1.5.0 and owned by the official
//!   `modelcontextprotocol/rust-sdk` organization.
//!
//! **Spec rev targeted:** `2025-06-18` (rmcp const
//! `ProtocolVersion::V_2025_06_18`).
//!
//! # Why we still have a pure-Value [`handler::McpHandler::handle`]
//!
//! rmcp's `ServerHandler` trait is the wire-level entry point the transports
//! use. It takes concrete rmcp types (`CallToolRequestParams`, etc.) which are
//! `#[non_exhaustive]` — not cheap to fabricate from outside the crate. For
//! unit tests we keep a pure `Value -> Option<Value>` entry point; the
//! `ServerHandler` adapter then just parses the in-flight rmcp types into
//! `serde_json::Value` and delegates. Shared code path, same behaviour, fully
//! testable in-process without the rmcp transport harness.

pub mod attribution;
pub mod error;
pub mod handler;
pub mod registry;
pub mod server_handler;
pub mod tools;
pub mod transport;

pub use error::McpError;
pub use handler::McpHandler;
pub use server_handler::ServerHandlerAdapter;
