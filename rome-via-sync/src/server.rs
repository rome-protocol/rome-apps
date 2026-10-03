// Phase 2 TODO: extract to rome-via-common when DB/config types land.
use std::net::SocketAddr;

use anyhow::Context;
use axum::{routing::get, Router};
use tokio::sync::oneshot;

/// Boot the health server on `addr` (e.g. "0.0.0.0:8091" or "127.0.0.1:0" for ephemeral port).
/// Returns the actual bound `SocketAddr` and a shutdown sender.
/// Call `shutdown_tx.send(())` to trigger graceful shutdown of the server.
/// The server runs in a background tokio task; this function returns immediately.
pub async fn start_health_server(addr: &str) -> anyhow::Result<(SocketAddr, oneshot::Sender<()>)> {
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind health server on {addr}: port may already be in use"))?;

    let bound = listener
        .local_addr()
        .context("failed to read local address from listener")?;

    let app = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/readyz", get(|| async { "ok" }));

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("rome-via-sync health server exited unexpectedly");
    });

    Ok((bound, shutdown_tx))
}
