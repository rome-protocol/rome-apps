//! MCP transport adapters — stdio (Task 7) and streamable HTTP (Task 8). Both
//! funnel their decoded requests through [`super::handler::McpHandler`] so the
//! dispatch logic never forks. See each sub-module for the transport-specific
//! details.

pub mod http;
pub mod stdio;
