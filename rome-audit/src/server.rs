//! Minimal health server — mirrors `rome-via-sync`'s
//! `start_health_server` (`rome-via-sync/src/server.rs`) exactly, so a
//! deployment set up like `rome-via-sync`/`rome-via-enrich` (readiness
//! probe on `/healthz`/`/readyz`) is a drop-in fit.

use std::net::SocketAddr;

use anyhow::Context;
use axum::{routing::get, Json, Router};
use sqlx::PgPool;
use tokio::sync::oneshot;

/// Durability follow-up to the finality-prefix fix (`ingest::watermark`):
/// exposes `finalized_tip - verified_through_slot` on `/healthz` so a wedge
/// like the one that fix resolves is visible on a dashboard/alert BEFORE it
/// becomes a 31h silent stall again. Reads are best-effort and non-fatal —
/// `/healthz` always returns 200; the lag is `null` if either read fails.
#[derive(Clone)]
pub struct LagProbe {
    /// Hercules source pool — read-only usage, same as the ingest pipeline.
    pub source: PgPool,
    /// The pool `audit.ingest_watermark` lives in.
    pub target: PgPool,
    pub chain_id: i64,
}

async fn healthz(probe: Option<LagProbe>) -> Json<serde_json::Value> {
    let lag = match &probe {
        Some(p) => {
            let tip = crate::ingest::hercules_reads::finalized_tip(&p.source)
                .await
                .ok()
                .flatten();
            let watermark = crate::ingest::pipeline::load_watermark(&p.target, p.chain_id)
                .await
                .ok();
            match (tip, watermark) {
                (Some(tip), Some(verified_through)) => Some(tip - verified_through),
                _ => None,
            }
        }
        None => None,
    };
    Json(serde_json::json!({ "status": "ok", "ingest_lag_slots": lag }))
}

/// Boot the health server on `addr`. `lag_probe` is `None` in every existing
/// call site's absence of wiring (status-code-identical `/healthz` response (200; the body shape changed, deliberately): `"ok"`
/// becomes `{"status":"ok","ingest_lag_slots":null}` either way — still a
/// plain 200, so a readiness probe that only checks the status code
/// is unaffected). Returns the actual bound `SocketAddr` and a shutdown
/// sender; the server runs in a background tokio task.
pub async fn start_health_server(
    addr: &str,
    lag_probe: Option<LagProbe>,
) -> anyhow::Result<(SocketAddr, oneshot::Sender<()>)> {
    let listener = tokio::net::TcpListener::bind(addr).await.with_context(|| {
        format!("failed to bind health server on {addr}: port may already be in use")
    })?;

    let bound = listener
        .local_addr()
        .context("failed to read local address from listener")?;

    let app = Router::new()
        .route("/healthz", get(move || healthz(lag_probe.clone())))
        .route("/readyz", get(|| async { "ok" }));

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("rome-audit health server exited unexpectedly");
    });

    Ok((bound, shutdown_tx))
}
