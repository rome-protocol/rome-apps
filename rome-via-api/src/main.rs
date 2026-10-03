use anyhow::{anyhow, Context};
use clap::Parser;
use governor::{Quota, RateLimiter};
use rome_obs::Otel;
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::Arc;
use tokio::signal::unix::{signal, SignalKind};

use rome_via_api::{api, cli::Cli, config::ViaApiConfig, state::{AppState, ChainRateLimiter}};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    Otel::init_from_env("rome-via-api").map_err(|e| anyhow!(e.to_string()))?;

    let cli = Cli::parse();
    let config_path = cli.get_config_path()?;

    let config = ViaApiConfig::load(&config_path)
        .await
        .with_context(|| format!("Failed to load config from {config_path:?}"))?;

    tracing::info!(
        chain_id = config.chain_id,
        bind_addr = %config.bind_addr,
        redis_enabled = config.redis_url.is_some(),
        "rome-via-api starting"
    );

    // Build connection pool.
    let pool = PgPoolOptions::new()
        .max_connections(config.pool_max_connections)
        .connect(&config.db_url)
        .await
        .with_context(|| format!("Failed to connect to DB: {}", config.db_url))?;

    // Optional Redis connection manager.
    let redis = if let Some(ref url) = config.redis_url {
        let client = redis::Client::open(url.as_str())
            .with_context(|| format!("Failed to create Redis client for {url}"))?;
        let mgr = redis::aio::ConnectionManager::new(client)
            .await
            .with_context(|| format!("Failed to connect to Redis at {url}"))?;
        tracing::info!("Redis cache connected");
        Some(mgr)
    } else {
        tracing::info!("No redis_url configured — RPC fallback cache disabled");
        None
    };

    // Build per-chain rate limiters (10 rps per foreign chain).
    let quota = Quota::per_second(NonZeroU32::new(10).unwrap());
    let mut rate_limiters: HashMap<i64, ChainRateLimiter> = HashMap::new();
    let mut foreign_proxies: HashMap<i64, String> = HashMap::new();
    for (chain_id_str, proxy_url) in &config.foreign_proxies {
        if let Ok(cid) = chain_id_str.parse::<i64>() {
            foreign_proxies.insert(cid, proxy_url.clone());
            rate_limiters.insert(cid, Arc::new(RateLimiter::direct(quota)));
        }
    }

    let state = AppState::new(
        pool,
        config.chain_id as i64,
        config.cursor_secret.into_bytes(),
        config.proxy_url.clone(),
        redis,
        Arc::new(foreign_proxies),
        Arc::new(rate_limiters),
    );

    // Build axum router.
    let app = api::router(state);

    // Bind and serve.
    let listener = tokio::net::TcpListener::bind(config.bind_addr)
        .await
        .with_context(|| format!("Failed to bind to {}: port may already be in use", config.bind_addr))?;

    tracing::info!(addr = %config.bind_addr, "rome-via-api listening");

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let server_handle = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .with_graceful_shutdown(async {
            let _ = shutdown_rx.await;
        })
        .await
        .map_err(|e| anyhow!("server error: {e}"))
    });

    // Wait for shutdown signal.
    let mut sigterm = signal(SignalKind::terminate())
        .map_err(|e| anyhow!("failed to install SIGTERM handler: {e}"))?;
    let mut sigint = signal(SignalKind::interrupt())
        .map_err(|e| anyhow!("failed to install SIGINT handler: {e}"))?;

    tokio::select! {
        _ = sigterm.recv() => {
            tracing::info!("SIGTERM received, shutting down rome-via-api...");
        }
        _ = sigint.recv() => {
            tracing::info!("SIGINT received, shutting down rome-via-api...");
        }
        result = server_handle => {
            tracing::error!(?result, "rome-via-api server exited unexpectedly");
        }
    }

    // Trigger graceful shutdown and drain in-flight requests.
    let _ = shutdown_tx.send(());

    Ok(())
}
