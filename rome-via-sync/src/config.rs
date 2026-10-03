use std::net::SocketAddr;

/// Configuration for rome-via-sync polling loop.
///
/// Loaded from a TOML file pointed to by `ROME_VIA_SYNC_CONFIG` env var or `-c` CLI flag.
#[derive(serde::Deserialize, Debug, Clone)]
pub struct SyncConfig {
    /// Chain ID to tag all mirrored rows with (e.g. 121220 for monti_spl devnet).
    #[serde(default = "default_chain_id")]
    pub chain_id: u64,

    /// Connection URL for the source Hercules Postgres database (read-only).
    pub source_db_url: String,

    /// Connection URL for the target rome_via_db Postgres database (read-write).
    pub target_db_url: String,

    /// How often to poll Hercules for new rows, in MILLISECONDS.
    ///
    /// Overrides `poll_interval_seconds` when set. Sub-second matters: the sync is
    /// the producer of the live feed, so its wake cadence is a hard floor on how
    /// often the explorer's counter can move. Measured on hadrian 2026-07-28 with
    /// a 2s interval, the all-time counter changed every 2.05s median — it drained
    /// fully, slept, then wrote the next ~27 txs in one go. The chain produces a
    /// block every ~0.68s, so a 2s producer cadence discards most of the real
    /// granularity before anything downstream can show it.
    ///
    /// The idle backoff still applies on top, so a quiet chain does not pay for a
    /// fast base — it backs off geometrically to `max_idle_poll_interval_seconds`
    /// and snaps back the moment there is work.
    #[serde(default)]
    pub poll_interval_ms: Option<u64>,

    /// How often to poll Hercules for new rows (seconds).
    #[serde(default = "default_poll_interval_seconds")]
    pub poll_interval_seconds: u64,

    /// Ceiling for the idle backoff. After consecutive wakes that find nothing, the
    /// poll interval grows geometrically from `poll_interval_seconds` up to this, and
    /// snaps back the moment there is work. Bounds how long a long-quiet chain can
    /// take to notice its first new block.
    #[serde(default = "default_max_idle_poll_interval_seconds")]
    pub max_idle_poll_interval_seconds: u64,

    /// Max rows to copy per table per poll cycle.
    #[serde(default = "default_batch_size")]
    pub batch_size: i64,


    /// Address for the HTTP health server.
    #[serde(default = "default_health_addr")]
    pub health_addr: SocketAddr,
}


fn default_chain_id() -> u64 {
    121220
}
/// 30s ceiling: a chain quiet long enough to reach it is one nobody is watching
/// closely, and 30s of staleness there is a fair trade for dropping ~16 queries per
/// 2s against a database shared with three other chains.
fn default_max_idle_poll_interval_seconds() -> u64 {
    30
}

impl SyncConfig {
    /// Effective base poll interval in milliseconds.
    ///
    /// `poll_interval_ms` wins when set; otherwise the legacy seconds field is
    /// converted, so existing deployed configs keep their exact behaviour.
    pub fn poll_interval_base_ms(&self) -> u64 {
        self.poll_interval_ms
            .unwrap_or_else(|| self.poll_interval_seconds.saturating_mul(1_000))
    }

    /// Idle-backoff ceiling in milliseconds.
    pub fn max_idle_poll_interval_ms(&self) -> u64 {
        self.max_idle_poll_interval_seconds.saturating_mul(1_000)
    }
}

fn default_poll_interval_seconds() -> u64 {
    2
}
fn default_batch_size() -> i64 {
    // Rows per table per drain pass. The run loop now drains to empty each wake,
    // so this is a per-pass chunk size (fewer round-trips while catching up), not
    // a throughput cap as it was before.
    5000
}
fn default_health_addr() -> SocketAddr {
    "0.0.0.0:8091".parse().unwrap()
}

impl SyncConfig {
    /// Parse a TOML config file from disk.
    pub async fn load(path: &std::path::Path) -> anyhow::Result<Self> {
        let bytes = tokio::fs::read(path).await
            .map_err(|e| anyhow::anyhow!("Failed to read config file {:?}: {e}", path))?;
        let text = std::str::from_utf8(&bytes)
            .map_err(|e| anyhow::anyhow!("Config file is not valid UTF-8: {e}"))?;
        toml::from_str(text)
            .map_err(|e| anyhow::anyhow!("Failed to parse TOML config: {e}"))
    }
}

#[cfg(test)]
mod poll_interval_tests {
    use super::*;
    fn cfg(ms: Option<u64>, secs: u64) -> SyncConfig {
        let mut c: SyncConfig = serde_json::from_str(
            r#"{"source_db_url":"x","target_db_url":"y","chain_id":1}"#,
        ).expect("minimal config");
        c.poll_interval_ms = ms; c.poll_interval_seconds = secs; c
    }
    #[test]
    fn falls_back_to_the_legacy_seconds_field() {
        assert_eq!(cfg(None, 2).poll_interval_base_ms(), 2_000);
    }
    #[test]
    fn milliseconds_win_when_set() {
        assert_eq!(cfg(Some(400), 2).poll_interval_base_ms(), 400);
    }
    #[test]
    fn sub_second_is_representable() {
        assert_eq!(cfg(Some(250), 2).poll_interval_base_ms(), 250);
        assert!(cfg(Some(400), 2).poll_interval_base_ms() < 1_000);
    }
    #[test]
    fn idle_ceiling_converts_to_millis() {
        let c = cfg(Some(400), 2);
        assert_eq!(c.max_idle_poll_interval_ms(), c.max_idle_poll_interval_seconds * 1_000);
    }
}
