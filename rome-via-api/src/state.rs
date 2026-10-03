use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::sse::IpConnections;

/// Type alias for the governor rate limiter used per foreign chain.
pub type ChainRateLimiter = Arc<governor::RateLimiter<
    governor::state::NotKeyed,
    governor::state::InMemoryState,
    governor::clock::DefaultClock,
>>;

/// Shared application state injected into all axum handlers via `State<AppState>`.
#[derive(Clone)]
pub struct AppState {
    /// Database connection pool for rome_via Postgres.
    pub db: PgPool,

    /// Chain ID served by this instance (i64 for sqlx BIGINT binding).
    pub chain_id: i64,

    /// HMAC secret for signing/verifying cursor tokens (raw bytes).
    pub cursor_secret: Vec<u8>,

    /// Proxy JSON-RPC URL for live balance / code lookups.
    pub proxy_url: String,

    /// Optional Redis connection manager for RPC fallback cache (Phase 4).
    pub redis: Option<redis::aio::ConnectionManager>,

    /// Map of chain_id → foreign Proxy URL for RPC fallback (Phase 4).
    /// Key is i64 chain_id; value is the Proxy base URL.
    pub foreign_proxies: Arc<HashMap<i64, String>>,

    /// Per-chain outbound rate limiters (10 rps per chain) for RPC fallback.
    /// Key is i64 chain_id.
    pub rate_limiters: Arc<HashMap<i64, ChainRateLimiter>>,

    /// Per-IP SSE connection counter (max 5 concurrent SSE/IP).
    pub sse_connections: IpConnections,
}

impl AppState {
    /// Create a new AppState with initialized SSE connection tracker.
    pub fn new(
        db: PgPool,
        chain_id: i64,
        cursor_secret: Vec<u8>,
        proxy_url: String,
        redis: Option<redis::aio::ConnectionManager>,
        foreign_proxies: Arc<HashMap<i64, String>>,
        rate_limiters: Arc<HashMap<i64, ChainRateLimiter>>,
    ) -> Self {
        Self {
            db,
            chain_id,
            cursor_secret,
            proxy_url,
            redis,
            foreign_proxies,
            rate_limiters,
            sse_connections: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}
