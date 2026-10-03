//! Mirrors `rome-via-sync`'s `Cli` (`rome-via-sync/src/cli.rs`) exactly.

use std::path::PathBuf;

use crate::config::AuditConfig;

#[derive(clap::Parser)]
#[command(version, about, long_about = None)]
pub struct Cli {
    /// Path to the config file (TOML). Falls back to ROME_AUDIT_CONFIG env var.
    #[clap(short = 'c', long)]
    pub config: Option<PathBuf>,
}

impl Cli {
    /// Get the path to the config file
    /// - from cli flag
    /// - from ROME_AUDIT_CONFIG env var
    pub fn get_config_path(&self) -> anyhow::Result<PathBuf> {
        self.config
            .clone()
            .or_else(|| std::env::var("ROME_AUDIT_CONFIG").ok().map(PathBuf::from))
            .ok_or_else(|| anyhow::anyhow!("Config file path not found"))
    }

    /// Load and parse the AuditConfig from the config file.
    pub async fn load_config(&self) -> anyhow::Result<AuditConfig> {
        AuditConfig::load(&self.get_config_path()?).await
    }
}
