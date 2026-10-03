//! P5a — the Bloom→audit Tier-3 overlay INGEST server (IMPL-PLAN §5.1 +
//! SPEC §0.2/§14.2/§9.0-c/§4.3/§11).
//!
//! **RECEIVE-ONLY, a recorder never an enforcer (§0.2).** Nothing under
//! this module makes an outbound HTTP call, a vendor call, a Solana/EVM
//! RPC call, a chain write, runs a poller, or runs a reconcile job. It only
//! accepts pushed rows over `POST /overlay/identity` and
//! `POST /overlay/evidence`, HMAC-verified before any parse (see
//! [`auth::verify`]).
//!
//! **Forward-only (§5.1 build directive).** Migration `0010_tier3_overlay`
//! creates empty tables; this server never scans, backfills, or
//! read-modifies anything at boot. The feed starts from the first
//! post-deploy request — `overlay_boot_touches_nothing` in
//! `tests/overlay_db.rs` proves boot alone changes zero rows.

pub mod auth;
pub mod canonical;
pub mod routes;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use sqlx::PgPool;
use tokio::sync::oneshot;

/// Shared state for the overlay server.
///
/// `now` is injectable (not `SystemTime::now()` inline) so tests can assert
/// exact `received_at`/`created_at`/`added_at` stamps and exercise the auth
/// skew boundary deterministically — see `tests/overlay_db.rs`.
#[derive(Clone)]
pub struct OverlayState {
    pub pool: PgPool,
    pub secret: Vec<u8>,
    pub now: Arc<dyn Fn() -> i64 + Send + Sync>,
}

impl OverlayState {
    /// Production constructor — clock is the real wall clock.
    pub fn new(pool: PgPool, secret: Vec<u8>) -> Self {
        Self {
            pool,
            secret,
            now: Arc::new(real_now),
        }
    }

    /// Test constructor — clock is caller-controlled.
    pub fn with_clock(pool: PgPool, secret: Vec<u8>, now: Arc<dyn Fn() -> i64 + Send + Sync>) -> Self {
        Self { pool, secret, now }
    }
}

fn real_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_secs() as i64
}

/// Boots the overlay ingest server on `addr`. Returns the actual bound
/// `SocketAddr` (mirrors [`crate::server::start_health_server`] exactly, so
/// binding on `127.0.0.1:0` in tests works the same way) and a shutdown
/// sender; the server runs in a background tokio task.
pub async fn start_overlay_server(
    addr: &str,
    state: OverlayState,
) -> anyhow::Result<(SocketAddr, oneshot::Sender<()>)> {
    let listener = tokio::net::TcpListener::bind(addr).await.with_context(|| {
        format!("failed to bind overlay server on {addr}: port may already be in use")
    })?;

    let bound = listener
        .local_addr()
        .context("failed to read local address from listener")?;

    let app = routes::router(state);

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("rome-audit overlay server exited unexpectedly");
    });

    Ok((bound, shutdown_tx))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// LOW-4: `real_now()` has zero coverage otherwise — every one of the
    /// 300+ other tests in this crate injects a fake clock via `with_clock`,
    /// so a secs-vs-millis (or any other unit) bug in the ONE place that
    /// calls the real wall clock would ship green and then reject 100% of
    /// real Bloom requests at the ±300s auth skew gate.
    #[test]
    fn real_now_is_unix_seconds_not_millis() {
        let ours = real_now();
        let theirs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_secs() as i64;
        assert!(
            (ours - theirs).abs() <= 5,
            "real_now() must return unix SECONDS — got {ours}, an independent SystemTime read got {theirs}"
        );
    }
}
