use anyhow::{Context, Result};
use std::env;

#[derive(Clone)]
pub struct Config {
    pub cdn_url: String,
    pub postgres_url: String,
    pub poll_interval_secs: u64,
}

/// Hand-written so logging the config never prints the DB credentials.
impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("cdn_url", &self.cdn_url)
            .field("postgres_url", &"<redacted>")
            .field("poll_interval_secs", &self.poll_interval_secs)
            .finish()
    }
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let cdn_url = env::var("CARDO_CDN_URL")
            .context("CARDO_CDN_URL not set")?;
        let postgres_url = env::var("CARDO_POSTGRES_URL")
            .context("CARDO_POSTGRES_URL not set")?;
        let poll_interval_secs = env::var("CARDO_POLL_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(300);
        Ok(Self { cdn_url, postgres_url, poll_interval_secs })
    }
}
