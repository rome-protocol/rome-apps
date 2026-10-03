//! rome-audit binary entrypoint — mirrors `rome-via-sync/src/main.rs`
//! exactly: dotenv + `Otel::init_from_env` + `Cli::parse` + `Config::load`,
//! connect the two `PgPool`s (source = Hercules, target = wherever the
//! `audit` schema lives), run migrations on target, start the health
//! server, spawn the run loop, wait for SIGTERM/SIGINT.

use anyhow::anyhow;
use clap::Parser;
use rome_obs::Otel;
use tokio::signal::unix::{signal, SignalKind};
use tokio_util::sync::CancellationToken;

use rome_audit::cli::Cli;
use rome_audit::server::start_health_server;

fn parse_hex20_addr(s: &str) -> anyhow::Result<[u8; 20]> {
    let bytes = hex::decode(s.trim_start_matches("0x"))
        .map_err(|e| anyhow!("resolve.tokens entry {s:?} is not valid hex: {e}"))?;
    bytes
        .try_into()
        .map_err(|v: Vec<u8>| anyhow!("resolve.tokens entry {s:?} must be 20 bytes, got {}", v.len()))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    Otel::init_from_env("rome-audit").map_err(|e| anyhow!(e.to_string()))?;

    let cli = Cli::parse();
    let config_path = cli.get_config_path()?;
    let cfg = rome_audit::config::AuditConfig::load(&config_path).await?;

    tracing::info!(chain_id = cfg.chain_id, health_addr = %cfg.health_addr, "rome-audit starting");

    // Connect to source (Hercules) and target (wherever `audit` lives) Postgres —
    // same two-pool shape as rome-via-sync.
    let source = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect(&cfg.source_db_url)
        .await
        .map_err(|e| anyhow!("Failed to connect to source (Hercules) DB: {e}"))?;

    let target = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect(&cfg.target_db_url)
        .await
        .map_err(|e| anyhow!("Failed to connect to target DB: {e}"))?;

    tracing::info!("running sqlx migrations on target DB");
    let mut migrator = sqlx::migrate!("./migrations");
    migrator.set_ignore_missing(true);
    migrator
        .run(&target)
        .await
        .map_err(|e| anyhow!("Migration failed: {e}"))?;
    tracing::info!("migrations applied");

    let health_addr_str = cfg.health_addr.to_string();
    let lag_probe = Some(rome_audit::server::LagProbe {
        source: source.clone(),
        target: target.clone(),
        chain_id: cfg.chain_id as i64,
    });
    let (addr, health_shutdown_tx) = start_health_server(&health_addr_str, lag_probe).await?;
    tracing::info!(?addr, "rome-audit health endpoint listening");

    // P5a Tier-3 overlay ingest server — boots iff `[overlay]` is present in
    // config; absent ⇒ byte-identical to P1-P4 (no overlay server, no
    // `src/overlay/` code runs at all).
    let overlay_shutdown_tx = match &cfg.overlay {
        Some(ov) => {
            let state =
                rome_audit::overlay::OverlayState::new(target.clone(), ov.ingest_secret.clone().into_bytes());
            let (overlay_addr, tx) = rome_audit::overlay::start_overlay_server(&ov.listen_addr, state).await?;
            tracing::info!(?overlay_addr, "rome-audit overlay ingest endpoint listening");
            Some(tx)
        }
        None => None,
    };

    let token = CancellationToken::new();
    let run_token = token.clone();

    // Source resolution (address → SourceKind): `[resolve]` present ⇒ the
    // live `resolve(token)` pipeline (P3c); absent ⇒ P1's empty static map
    // (the worker still advances the watermark, nothing registered to
    // decode).
    let mode = match &cfg.resolve {
        Some(rs) => {
            let provider = ethers::providers::Provider::<ethers::providers::Http>::try_from(
                rs.rpc_url.as_str(),
            )
            .map_err(|e| anyhow!("invalid resolve.rpc_url: {e}"))?;
            // Log-history reads come from the Hercules `source` DB (the
            // proxy caps `eth_getLogs` at 12,000 blocks, so a `[Earliest,
            // Latest]` walk can never complete); STATE reads stay on live
            // RPC via the wrapped `EthersResolverRpc`. See
            // `resolve::hercules_rpc`'s module doc.
            let inner = rome_audit::resolve::EthersResolverRpc::new(std::sync::Arc::new(provider));
            let rpc: std::sync::Arc<dyn rome_audit::resolve::ResolverRpc> = std::sync::Arc::new(
                rome_audit::resolve::HerculesResolverRpc::new(source.clone(), inner),
            );
            let registry: std::sync::Arc<dyn rome_audit::resolve::RegistrySource> =
                std::sync::Arc::new(
                    rome_audit::resolve::ConfigRegistrySource::from_section(&rs.registry)
                        .map_err(|e| anyhow!("invalid resolve.registry: {e}"))?,
                );
            let tokens: Vec<[u8; 20]> = rs
                .tokens
                .iter()
                .map(|t| parse_hex20_addr(t))
                .collect::<anyhow::Result<_>>()?;
            rome_audit::run::SourceMode::Resolved {
                registry,
                rpc,
                tokens,
                reresolve_interval: std::time::Duration::from_secs(rs.reresolve_interval_secs),
                discovery_enabled: rs.discovery.is_some(),
            }
        }
        None => rome_audit::run::SourceMode::Static(std::collections::BTreeMap::new()),
    };

    let run_cfg = cfg.clone();
    let run_handle = tokio::spawn(async move {
        rome_audit::run::run(source, target, run_cfg, mode, run_token).await
    });

    let mut sigterm = signal(SignalKind::terminate())
        .map_err(|e| anyhow!("failed to install SIGTERM handler: {e}"))?;
    let mut sigint = signal(SignalKind::interrupt())
        .map_err(|e| anyhow!("failed to install SIGINT handler: {e}"))?;

    tokio::select! {
        _ = sigterm.recv() => {
            tracing::info!("SIGTERM received, shutting down rome-audit...");
        }
        _ = sigint.recv() => {
            tracing::info!("SIGINT received, shutting down rome-audit...");
        }
        result = run_handle => {
            match result {
                Ok(Ok(())) => tracing::info!("run loop exited cleanly"),
                Ok(Err(e)) => tracing::error!(?e, "run loop exited with error"),
                Err(e) => tracing::error!(?e, "run task panicked"),
            }
        }
    }

    token.cancel();
    let _ = health_shutdown_tx.send(());
    if let Some(tx) = overlay_shutdown_tx {
        let _ = tx.send(());
    }

    Ok(())
}
