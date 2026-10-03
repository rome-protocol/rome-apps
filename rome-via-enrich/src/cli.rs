use std::path::PathBuf;

#[derive(clap::Parser)]
#[command(version, about, long_about = None)]
pub struct Cli {
    /// Path to the config file
    #[clap(short = 'c', long)]
    pub config: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(clap::Subcommand)]
pub enum Command {
    /// One-shot maintenance ops (run then exit; NOT the daemon).
    Maintenance {
        #[command(subcommand)]
        op: MaintenanceOp,
    },
}

#[derive(clap::Subcommand)]
pub enum MaintenanceOp {
    /// Re-extract token_transfers from evm_tx_result and heal amounts / insert
    /// swallowed-era rows. Bounded to slot ≤ the holders cursor. Dry-run unless --apply.
    ReextractTransfers {
        #[clap(long)]
        apply: bool,
    },
    /// Heal token_holders.balance rows to on-chain balanceOf truth via a
    /// per-pair optimistic-concurrency algorithm (safe against the live
    /// holders worker — see maintenance::backfill_balances). Dry-run unless
    /// --apply; optionally scoped to one --token.
    BackfillBalances {
        #[clap(long)]
        apply: bool,
        #[clap(long)]
        token: Option<String>,
        #[clap(long, default_value_t = 6)]
        concurrency: usize,
    },
    /// Flag existing oracle-keeper crossings (cross_vm_seams.is_oracle) on this chain —
    /// the opt-in replacement for the retired auto-migration 0235 (keeps other chains
    /// forward-only). Idempotent; keyed on the shared ORACLE_SELECTORS. Dry-run unless --apply.
    BackfillOracleFlag {
        #[clap(long)]
        apply: bool,
    },
}

impl Cli {
    /// Get the path to the config file
    /// - from cli flag
    /// - from ROME_VIA_ENRICH_CONFIG env var
    pub fn get_config_path(&self) -> anyhow::Result<PathBuf> {
        self.config
            .clone()
            .or_else(|| {
                std::env::var("ROME_VIA_ENRICH_CONFIG")
                    .ok()
                    .map(PathBuf::from)
            })
            .ok_or_else(|| anyhow::anyhow!("Config file path not found"))
    }
}
