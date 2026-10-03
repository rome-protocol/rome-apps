mod api;
mod batcher;
mod batching;
mod cli;
mod config;
mod metrics;
mod proxy;
mod read_pool;
mod error;

use self::cli::Cli;
use anyhow::anyhow;
use clap::Parser;
use rome_obs::Otel;
use tokio::signal;


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // load any .env and init the logger
    dotenv::dotenv().ok();

    Otel::init_from_env("proxy").map_err(|e| anyhow!(e.to_string()))?;

    let config = Cli::parse().load_config().await?;
    let (_server, clock_jh, price_jh, metrics_jh) = config.init().await?;

    match (price_jh, metrics_jh) {
        (Some(price_jh), Some(metrics_jh)) => tokio::select! {
            res = clock_jh => anyhow::bail!("SolanaClockIndexer exited: {:?}", res),
            res = price_jh => anyhow::bail!("PriceManager exited: {:?}", res),
            res = metrics_jh => anyhow::bail!("MetricsServer exited: {:?}", res),
            _ = signal::ctrl_c() => anyhow::bail!("Shutdown proxy.."),
        },
        (Some(price_jh), None) => tokio::select! {
            res = clock_jh => anyhow::bail!("SolanaClockIndexer exited: {:?}", res),
            res = price_jh => anyhow::bail!("PriceManager exited: {:?}", res),
            _ = signal::ctrl_c() => anyhow::bail!("Shutdown proxy.."),
        },
        (None, Some(metrics_jh)) => tokio::select! {
            res = clock_jh => anyhow::bail!("SolanaClockIndexer exited: {:?}", res),
            res = metrics_jh => anyhow::bail!("MetricsServer exited: {:?}", res),
            _ = signal::ctrl_c() => anyhow::bail!("Shutdown proxy.."),
        },
        (None, None) => tokio::select! {
            res = clock_jh => anyhow::bail!("SolanaClockIndexer exited: {:?}", res),
            _ = signal::ctrl_c() => anyhow::bail!("Shutdown proxy.."),
        },
    }
}
