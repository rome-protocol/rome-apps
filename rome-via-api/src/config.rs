use std::net::SocketAddr;

/// Configuration for rome-via-api REST server.
///
/// Loaded from a TOML file pointed to by `ROME_VIA_API_CONFIG` env var or `-c` CLI flag.
#[derive(serde::Deserialize, Clone)]
pub struct ViaApiConfig {
    /// Chain ID served (e.g. 121220 for monti_spl devnet).
    #[serde(default = "default_chain_id")]
    pub chain_id: u64,

    /// Connection URL for the rome_via Postgres database (read-only queries).
    pub db_url: String,

    /// Address to bind the HTTP server.
    #[serde(default = "default_bind_addr")]
    pub bind_addr: SocketAddr,

    /// Max DB connections in the sqlx pool.
    #[serde(default = "default_pool_max_connections")]
    pub pool_max_connections: u32,

    /// HMAC secret used to sign cursor tokens.
    /// Rotating this secret invalidates all outstanding pagination cursors (acceptable).
    #[serde(default = "default_cursor_secret")]
    pub cursor_secret: String,

    /// Proxy JSON-RPC URL for live balance / code lookups.
    #[serde(default = "default_proxy_url")]
    pub proxy_url: String,

    /// Optional Redis URL for RPC fallback cache (Phase 4).
    /// If not set, fallback responses are served without caching.
    pub redis_url: Option<String>,

    /// Map of chain_id → Proxy URL for foreign rollup RPC fallback (Phase 4).
    /// Key is the chain_id as a string; value is the Proxy base URL.
    #[serde(default)]
    pub foreign_proxies: std::collections::HashMap<String, String>,
}

/// Hand-written so logging the config never prints credentials (`db_url`,
/// `cursor_secret`, `redis_url` may all carry secrets).
impl std::fmt::Debug for ViaApiConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ViaApiConfig")
            .field("chain_id", &self.chain_id)
            .field("db_url", &"<redacted>")
            .field("bind_addr", &self.bind_addr)
            .field("pool_max_connections", &self.pool_max_connections)
            .field("cursor_secret", &"<redacted>")
            .field("proxy_url", &self.proxy_url)
            .field("redis_url", &self.redis_url.as_ref().map(|_| "<redacted>"))
            .field("foreign_proxies", &self.foreign_proxies)
            .finish()
    }
}

fn default_chain_id() -> u64 {
    121220
}
fn default_bind_addr() -> SocketAddr {
    "0.0.0.0:8090".parse().unwrap()
}
fn default_pool_max_connections() -> u32 {
    20
}
fn default_cursor_secret() -> String {
    "dev-only-cursor-secret-change-in-prod".to_string()
}

fn default_proxy_url() -> String {
    "http://localhost:9090".to_string()
}

impl ViaApiConfig {
    /// Parse a TOML config file from disk.
    pub async fn load(path: &std::path::Path) -> anyhow::Result<Self> {
        let bytes = tokio::fs::read(path)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read config file {:?}: {e}", path))?;
        let text = std::str::from_utf8(&bytes)
            .map_err(|e| anyhow::anyhow!("Config file is not valid UTF-8: {e}"))?;
        toml::from_str(text).map_err(|e| anyhow::anyhow!("Failed to parse TOML config: {e}"))
    }
}
