//! MCP tool implementations.
//!
//! Each sub-module is one tool function. Every top-level read tool
//! (`list_apps`, `describe_app`, `get_metrics`) takes `&PgPool`. State-heavy
//! tools (`quote`, `execute`, per-capability `capability`) take `&AppState`.
//!
//! # Why not all `&AppState`
//!
//! The plan's stub was `call(state: &AppState, args: &Value)`. We narrowed it
//! on read tools to `&PgPool` so unit tests exercise them without constructing
//! a full `AppState` (which needs a live `RomeEVMClient`). Same behavior,
//! less plumbing. Tools that need the tx_builder still take `&AppState`
//! (Tasks 5+6).

pub mod capability;
pub mod describe_app;
pub mod execute;
pub mod get_metrics;
pub mod list_apps;
pub mod quote;
