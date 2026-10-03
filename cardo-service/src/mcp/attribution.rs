//! Attribution constant for MCP requests.
//!
//! MCP traffic is agent-native by convention — unlike REST, which may be
//! called by a human browsing `cardo.rome.builders`. The log line suffix
//! `client_kind=agent` is how the observability layer splits the two surfaces
//! in dashboards.
//!
//! MCP tool implementations (`mcp::tools::*`) already emit `client_kind =
//! "agent"` inline; this constant exists so the rare code path outside those
//! tools can stay consistent.

/// The attribution string every MCP request carries in its tracing span,
/// regardless of transport (stdio or streamable HTTP). REST uses a
/// negotiated `X-Client-Kind` header; MCP doesn't — the transport itself
/// is the signal.
pub const MCP_CLIENT_KIND: &str = "agent";
