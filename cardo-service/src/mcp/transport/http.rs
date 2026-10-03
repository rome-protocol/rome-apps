//! MCP streamable-HTTP transport.
//!
//! Mounts a `POST /mcp` (and ancillary `GET` / `DELETE`) on an Axum router
//! via rmcp's [`StreamableHttpService`]. The resulting sub-router is meant to
//! be `.merge()`-ed into the existing REST router, **not** run standalone —
//! that keeps one Axum process, one bind address, one listener.
//!
//! # Why streamable HTTP, not plain POST-only JSON-RPC
//!
//! The plan's pseudocode sketched `POST /mcp` as a bare request/response
//! endpoint. MCP spec rev 2025-06-18 renames the "HTTP+SSE" transport to
//! "Streamable HTTP": the single `POST /mcp` endpoint returns either SSE
//! frames (for long-running tool calls) or JSON (for quick ones), and also
//! accepts `GET` for server-initiated notifications + `DELETE` for session
//! termination. Using rmcp's `StreamableHttpService` gets all of that for
//! free — we just mount its tower `Service` on Axum.
//!
//! # Session mode
//!
//! We run in **stateful** mode by default (rmcp default). Each MCP session
//! — typically one LLM conversation — gets its own `mcp-session-id` cookie
//! allocated by `LocalSessionManager`. Sessions don't carry any server-side
//! user state; they just keep SSE streams alive.
//!
//! # DNS rebinding
//!
//! rmcp's default `allowed_hosts` list is `["localhost", "127.0.0.1", "::1"]`.
//! Production deployments behind a public hostname (e.g. `mcp.cardo.rome.builders`)
//! need to override this — see [`build_mcp_router_with_allowed_hosts`].

use std::sync::Arc;

use axum::Router;
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::{StreamableHttpServerConfig, StreamableHttpService};

use crate::mcp::handler::McpHandler;
use crate::mcp::server_handler::ServerHandlerAdapter;

/// Build the MCP streamable-HTTP sub-router mounted at `/mcp`. Meant to be
/// `.merge()`-ed with the REST router in `build_router`.
///
/// The router shares no state with REST (the adapter captures the handler it
/// needs via an `Arc`). The merge in `rest::router::build_router` is what
/// puts them on the same listener.
pub fn build_mcp_router(handler: Arc<McpHandler>) -> Router {
    // Default allowed_hosts is loopback only. Production callers should go
    // through `build_mcp_router_with_allowed_hosts` and pass their public
    // hostname explicitly.
    build_mcp_router_with_allowed_hosts(
        handler,
        vec!["localhost".into(), "127.0.0.1".into(), "::1".into()],
    )
}

/// Variant that lets the caller override the DNS-rebinding `allowed_hosts`
/// list — useful for production (`mcp.cardo.rome.builders`) or for tests that
/// send a `Host:` header other than loopback.
pub fn build_mcp_router_with_allowed_hosts(
    handler: Arc<McpHandler>,
    allowed_hosts: Vec<String>,
) -> Router {
    // Delegate to the generic `router_from_factory` so tests can exercise the
    // mount without building an `McpHandler` (and therefore an `AppState`).
    router_from_factory(
        move || Ok(ServerHandlerAdapter::new(handler.clone())),
        allowed_hosts,
    )
}

/// Lower-level helper — mount any rmcp `ServerHandler` factory on `/mcp`.
/// Exposed so tests can drop in a stub handler without the full
/// `AppState` + `TxBuilder` + `RomeEVMClient` graph.
pub fn router_from_factory<S, F>(factory: F, allowed_hosts: Vec<String>) -> Router
where
    S: rmcp::ServerHandler + Send + 'static,
    F: Fn() -> Result<S, std::io::Error> + Send + Sync + 'static,
{
    let session_manager = Arc::new(LocalSessionManager::default());
    // `StreamableHttpServerConfig` is `#[non_exhaustive]` — start from
    // `Default` and chain the builder method that overrides allowed_hosts.
    let config = StreamableHttpServerConfig::default().with_allowed_hosts(allowed_hosts);
    let service = StreamableHttpService::new(factory, session_manager, config);
    // `StreamableHttpService` implements `tower::Service` and exposes
    // `handle(Request) -> Response` under the hood. Axum 0.7 wraps any tower
    // Service that takes `Request<Body>` via `Router::new().route_service`.
    Router::new().route_service("/mcp", service)
}
