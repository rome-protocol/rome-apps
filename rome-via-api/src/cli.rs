use std::path::PathBuf;

#[derive(clap::Parser)]
#[command(version, about, long_about = None)]
pub struct Cli {
    /// Path to the config file
    #[clap(short = 'c', long)]
    pub config: Option<PathBuf>,
}

impl Cli {
    /// Get the path to the config file
    /// - from cli flag
    /// - from ROME_VIA_API_CONFIG env var
    pub fn get_config_path(&self) -> anyhow::Result<PathBuf> {
        self.config
            .clone()
            .or_else(|| std::env::var("ROME_VIA_API_CONFIG").ok().map(PathBuf::from))
            .ok_or_else(|| anyhow::anyhow!("Config file path not found"))
    }
}
