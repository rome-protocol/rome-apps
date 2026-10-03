/// SSE (Server-Sent Events) handlers.
///
/// Streams Postgres LISTEN/NOTIFY events to browser clients.
/// Channels:
///   rome_via_new_block — emitted by rome-via-sync after eth_block inserts
///   rome_via_new_tx    — emitted by rome-via-sync after evm_tx inserts
///
/// Endpoints:
///   GET /api/v1/stream/home    — combined block + tx stream (HomeDashboard live badges)
///   GET /api/v1/stream/blocks  — block events only
///   GET /api/v1/stream/txs     — tx events only
///   GET /api/v1/stream/address/:address — tx events (v1: all txs; client-side filtering)
///
/// Per-IP connection limit (5 concurrent SSE/IP) enforced via a shared RwLock<HashMap>.
use axum::{
    extract::{ConnectInfo, Path, State},
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse,
    },
};
use futures::stream::{self, Stream};
use sqlx::postgres::PgListener;
use std::{
    collections::HashMap,
    convert::Infallible,
    net::{IpAddr, SocketAddr},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::{mpsc, RwLock};
use tracing::warn;

use crate::{error::AppError, state::AppState};

/// Per-IP SSE connection counter (max 20 concurrent SSE connections per IP).
pub type IpConnections = Arc<RwLock<HashMap<IpAddr, usize>>>;

const MAX_SSE_PER_IP: usize = 20;

/// Check and increment per-IP connection count. Returns Err if at limit.
async fn acquire_connection(ip: IpAddr, connections: &IpConnections) -> Result<(), AppError> {
    let mut map = connections.write().await;
    let count = map.entry(ip).or_insert(0);
    if *count >= MAX_SSE_PER_IP {
        return Err(AppError::BadRequest(format!(
            "Too many SSE connections from {ip} (max {MAX_SSE_PER_IP})"
        )));
    }
    *count += 1;
    Ok(())
}

/// Decrement per-IP connection count.
async fn release_connection(ip: IpAddr, connections: IpConnections) {
    let mut map = connections.write().await;
    let count = map.entry(ip).or_insert(1);
    if *count > 0 {
        *count -= 1;
    }
    if *count == 0 {
        map.remove(&ip);
    }
}

/// Wraps a `Pin<Box<dyn Stream>>` and decrements the per-IP connection counter when
/// dropped (on client disconnect or server shutdown). Using a boxed inner stream
/// ensures `Pin<&mut Self>` can call `poll_next` on the inner without requiring
/// the inner stream itself to be `Unpin`.
struct SseGuardedStream {
    inner: Pin<Box<dyn Stream<Item = Result<Event, Infallible>> + Send>>,
    ip: IpAddr,
    connections: IpConnections,
}

impl Stream for SseGuardedStream {
    type Item = Result<Event, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl Drop for SseGuardedStream {
    fn drop(&mut self) {
        let ip = self.ip;
        let connections = self.connections.clone();
        // Spawn a detached task — `release_connection` is async, so we must
        // run it outside the synchronous drop context.
        tokio::spawn(async move {
            release_connection(ip, connections).await;
        });
    }
}

/// Build an SSE stream listening on the given Postgres NOTIFY channels.
async fn sse_for_channels(
    state: AppState,
    ip: IpAddr,
    channels: Vec<String>,
) -> Result<Sse<impl futures::Stream<Item = Result<Event, Infallible>>>, AppError> {
    let connections = state.sse_connections.clone();
    acquire_connection(ip, &connections).await?;

    let db = state.db.clone();
    let (tx, rx) = mpsc::channel::<(String, String)>(64);

    // Spawn a background task that listens on Postgres and forwards to the mpsc channel.
    // The guarded stream (below) owns the decrement; this task does NOT call
    // release_connection — dropping the stream handles cleanup.
    tokio::spawn(async move {
        let mut listener = match PgListener::connect_with(&db).await {
            Ok(l) => l,
            Err(e) => {
                warn!(error = %e, "PgListener connect failed");
                return;
            }
        };
        for ch in &channels {
            if let Err(e) = listener.listen(ch).await {
                warn!(channel = %ch, error = %e, "PgListener listen failed");
            }
        }

        loop {
            match listener.recv().await {
                Ok(notification) => {
                    let ev = (
                        notification.channel().to_string(),
                        notification.payload().to_string(),
                    );
                    // If receiver is gone (stream dropped), stop.
                    if tx.send(ev).await.is_err() {
                        break;
                    }
                }
                Err(e) => {
                    warn!(error = %e, "PgListener recv error");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    });

    // Convert mpsc receiver to a Stream of SSE Events.
    // Box + pin it so `SseGuardedStream` doesn't need to carry non-`Unpin` bounds.
    let event_stream: Pin<Box<dyn Stream<Item = Result<Event, Infallible>> + Send>> =
        Box::pin(stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|(channel, payload)| {
                let event = Event::default().event(channel).data(payload);
                (Ok::<Event, Infallible>(event), rx)
            })
        }));

    // Wrap in the guard so the counter is decremented when the client disconnects.
    let guarded = SseGuardedStream {
        inner: event_stream,
        ip,
        connections,
    };

    Ok(Sse::new(guarded).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("ping"),
    ))
}

/// GET /api/v1/stream/home — combined block + tx stream.
pub async fn stream_home(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> Result<impl IntoResponse, AppError> {
    sse_for_channels(
        state,
        addr.ip(),
        vec!["rome_via_new_block".to_string(), "rome_via_new_tx".to_string()],
    )
    .await
}

/// GET /api/v1/stream/blocks — block events only.
pub async fn stream_blocks(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> Result<impl IntoResponse, AppError> {
    sse_for_channels(state, addr.ip(), vec!["rome_via_new_block".to_string()]).await
}

/// GET /api/v1/stream/txs — all tx events.
pub async fn stream_txs(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> Result<impl IntoResponse, AppError> {
    sse_for_channels(state, addr.ip(), vec!["rome_via_new_tx".to_string()]).await
}

/// GET /api/v1/stream/address/:address — tx events (v1: all txs; client-side filter by address).
pub async fn stream_address(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Path(_address): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    // v1: all tx events are emitted; UI filters by address client-side.
    // TODO(Phase 4.5): server-side address filtering via DB lookup per event.
    sse_for_channels(state, addr.ip(), vec!["rome_via_new_tx".to_string()]).await
}
