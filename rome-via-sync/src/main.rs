use anyhow::anyhow;
use clap::Parser;
use rome_obs::Otel;
use tokio::signal::unix::{signal, SignalKind};
use tokio_util::sync::CancellationToken;

use rome_via_sync::cli::Cli;
use rome_via_sync::config::SyncConfig;
use rome_via_sync::server::start_health_server;
use rome_via_sync::sync;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    Otel::init_from_env("rome-via-sync").map_err(|e| anyhow!(e.to_string()))?;

    let cli = Cli::parse();
    let config_path = cli.get_config_path()?;
    let cfg = SyncConfig::load(&config_path).await?;

    tracing::info!(chain_id = cfg.chain_id, health_addr = %cfg.health_addr, "rome-via-sync starting");

    // Connect to source (Hercules) and target (rome_via_db) Postgres.
    let source = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect(&cfg.source_db_url)
        .await
        .map_err(|e| anyhow!("Failed to connect to source DB: {e}"))?;

    let target = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect(&cfg.target_db_url)
        .await
        .map_err(|e| anyhow!("Failed to connect to target DB: {e}"))?;

    // Run sqlx migrations against target DB.
    // ignore_missing=true because rome-via-enrich shares the same
    // _sqlx_migrations table and owns versions 100+ (its migrations live in
    // rome-via-enrich/migrations). Without this, sync panics when it sees
    // enrich's versions tracked but not present in its own folder.
    tracing::info!("running sqlx migrations on target DB");
    let mut migrator = sqlx::migrate!("./migrations");
    migrator.set_ignore_missing(true);
    migrator
        .run(&target)
        .await
        .map_err(|e| anyhow!("Migration failed: {e}"))?;
    tracing::info!("migrations applied");

    // Start health endpoint.
    let health_addr_str = cfg.health_addr.to_string();
    let (addr, health_shutdown_tx) = start_health_server(&health_addr_str).await?;
    tracing::info!(?addr, "rome-via-sync health endpoint listening");

    // Cancellation token shared between sync loop and signal handlers.
    let token = CancellationToken::new();
    let sync_token = token.clone();

    // Spawn the polling sync loop.
    let sync_cfg = cfg.clone();
    let sync_handle = tokio::spawn(async move {
        sync::run(source, target, sync_cfg, sync_token).await
    });

    // Wait for SIGTERM or SIGINT.
    let mut sigterm = signal(SignalKind::terminate())
        .map_err(|e| anyhow!("failed to install SIGTERM handler: {e}"))?;
    let mut sigint = signal(SignalKind::interrupt())
        .map_err(|e| anyhow!("failed to install SIGINT handler: {e}"))?;

    tokio::select! {
        _ = sigterm.recv() => {
            tracing::info!("SIGTERM received, shutting down rome-via-sync...");
        }
        _ = sigint.recv() => {
            tracing::info!("SIGINT received, shutting down rome-via-sync...");
        }
        result = sync_handle => {
            match result {
                Ok(Ok(())) => tracing::info!("sync loop exited cleanly"),
                Ok(Err(e)) => tracing::error!(?e, "sync loop exited with error"),
                Err(e) => tracing::error!(?e, "sync task panicked"),
            }
        }
    }

    // Cancel the sync loop and shut down the health server.
    token.cancel();
    let _ = health_shutdown_tx.send(());

    Ok(())
}
