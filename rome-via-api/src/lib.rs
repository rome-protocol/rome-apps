// rome-via-api: REST API server for Rome Via block explorer.
// Phase 4 — Cross-chain correlations, SSE, RPC fallback + Phases 2-3 API.

pub mod api;
pub mod cache;
pub mod cache_envelope;
// The classifier moved to the shared rome-via-classify crate so rome-via-enrich can
// persist exactly what the API reports. Re-exported so existing `crate::classify::…`
// call sites keep working.
pub use rome_via_classify::classify;
pub mod cli;
pub mod config;
pub mod cursor;
pub mod error;
pub mod rpc_fallback;
pub mod server;
pub mod sse;
pub mod state;
pub mod throughput_calc;
