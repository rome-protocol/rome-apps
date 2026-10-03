use self::cli::Cli;
use self::config::HerculesConfig;
use anyhow::anyhow;
use clap::Parser;
use dotenv::dotenv;
use rome_obs::Otel;
use rome_sdk::rome_evm_client::error::RomeEvmError::Custom;
use tokio::signal::unix::{signal, SignalKind};
use tokio_util::sync::CancellationToken;

mod api;
mod cli;
mod config;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();

    Otel::init_from_env("hercules").map_err(|e| anyhow!(e.to_string()))?;

    let config: HerculesConfig = Cli::parse().load_config().await?;

    let cancellation_token = CancellationToken::new();
    let (server_handle, mut indexer_handles) = config.init(cancellation_token.clone()).await?;

    let server_handle2 = server_handle.clone();
    indexer_handles.push(tokio::spawn(async move {
        let mut sigterm = signal(SignalKind::terminate())
            .map_err(|_| Custom("Failed to install SIGTERM handler".to_string()))?;
        let mut sigint = signal(SignalKind::interrupt())
            .map_err(|_| Custom("Failed to install SIGINT handler".to_string()))?;

        tokio::select! {
            _ = sigterm.recv() => {
                tracing::info!("SIGTERM received, shutting down...");
            }
            _ = sigint.recv() => {
                tracing::info!("Ctrl-C (SIGINT) received, shutting down...");
            }
            res = server_handle2.stopped() => {
                tracing::info!("Admin server stopped: {:?}", res);
            }
        }
        Ok(())
    }));

    let (res, _, remaining) = futures::future::select_all(indexer_handles).await;
    // Final report
    match res {
        Ok(Ok(_)) => tracing::info!("Hercules exited."),
        Ok(Err(e)) => tracing::info!("Hercules exited with error: {}", e),
        Err(e) => tracing::error!("Hercules exited with error {}", e),
    }

    server_handle.stop().ok();
    cancellation_token.cancel();

    // Wait for all tasks to stop
    tracing::info!("Waiting for remaining tasks to stop...");
    for res in futures::future::join_all(remaining).await {
        match res {
            Ok(Ok(_)) => (),
            Ok(Err(e)) => tracing::info!("{}", e),
            Err(e) => tracing::info!("{}", e),
        }
    }

    Ok(())
}
