//! Prometheus RED metrics for the Proxy JSON-RPC server.
//!
//! Exposes a `/metrics` endpoint on a separate Axum HTTP server (so we don't
//! mix it into the jsonrpsee RPC port) and emits per-method rate / errors /
//! duration via a jsonrpsee `RpcServiceT` middleware.
//!
//! Metric shape (label cardinality bounded by the closed `eth_*` / `rome_*`
//! method set, ~30):
//!
//! - `rome_proxy_rpc_requests_total{method, status}` — counter, status ∈
//!   {"success", "error"}.
//! - `rome_proxy_rpc_duration_seconds{method}` — histogram, latency in
//!   seconds. Buckets tuned for sub-second RPC: 1ms .. 10s.
//! - `rome_proxy_rpc_in_flight{method}` — gauge, in-flight requests per method.

use std::{future::Future, net::SocketAddr, pin::Pin, sync::Arc, time::Instant};

use anyhow::Context;
use axum::{
    extract::State,
    http::{header, StatusCode},
    response::IntoResponse,
    routing::get,
    Router,
};
use jsonrpsee::{
    server::middleware::rpc::RpcServiceT,
    types::Request,
    MethodResponse,
};
use once_cell::sync::Lazy;
use prometheus::{
    register_histogram_vec, register_int_counter_vec, register_int_gauge_vec, Encoder, HistogramVec,
    IntCounterVec, IntGaugeVec, TextEncoder,
};
use tokio::task::JoinHandle;

/// Latency buckets in seconds tuned for JSON-RPC: 1ms, 5ms, 10ms, 25ms, 50ms,
/// 100ms, 250ms, 500ms, 1s, 2.5s, 5s, 10s.
const DURATION_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

static RPC_REQUESTS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "rome_proxy_rpc_requests_total",
        "Total JSON-RPC requests handled by the Proxy, labelled by method and status.",
        &["method", "status"]
    )
    .expect("register rome_proxy_rpc_requests_total")
});

static RPC_DURATION_SECONDS: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        prometheus::HistogramOpts::new(
            "rome_proxy_rpc_duration_seconds",
            "JSON-RPC request latency in seconds, labelled by method."
        )
        .buckets(DURATION_BUCKETS.to_vec()),
        &["method"]
    )
    .expect("register rome_proxy_rpc_duration_seconds")
});

static RPC_IN_FLIGHT: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "rome_proxy_rpc_in_flight",
        "JSON-RPC requests currently being handled, labelled by method.",
        &["method"]
    )
    .expect("register rome_proxy_rpc_in_flight")
});

/// jsonrpsee RPC middleware that records per-method RED metrics.
#[derive(Clone)]
pub struct MetricsMiddleware<S> {
    pub service: S,
}

impl<'a, S> RpcServiceT<'a> for MetricsMiddleware<S>
where
    S: RpcServiceT<'a> + Send + Sync + Clone + 'static,
{
    type Future = Pin<Box<dyn Future<Output = MethodResponse> + Send + 'a>>;

    fn call(&self, req: Request<'a>) -> Self::Future {
        let method = req.method.to_string();
        let service = self.service.clone();

        // Increment in-flight before await; decrement on drop or completion.
        RPC_IN_FLIGHT.with_label_values(&[&method]).inc();
        let start = Instant::now();

        Box::pin(async move {
            let response = service.call(req).await;

            let elapsed = start.elapsed().as_secs_f64();
            let status = if response.is_success() { "success" } else { "error" };
            RPC_REQUESTS_TOTAL
                .with_label_values(&[&method, status])
                .inc();
            RPC_DURATION_SECONDS
                .with_label_values(&[&method])
                .observe(elapsed);
            RPC_IN_FLIGHT.with_label_values(&[&method]).dec();

            response
        })
    }
}

/// Render the default Prometheus registry as Prometheus exposition text.
fn gather_text() -> Result<String, prometheus::Error> {
    let mut buf = Vec::new();
    let metrics = prometheus::gather();
    TextEncoder::new().encode(&metrics, &mut buf)?;
    String::from_utf8(buf)
        .map_err(|e| prometheus::Error::Msg(format!("non-UTF8 prometheus output: {e}")))
}

#[derive(Clone, Default)]
struct MetricsState;

async fn metrics_handler(State(_): State<Arc<MetricsState>>) -> impl IntoResponse {
    match gather_text() {
        Ok(body) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
            body,
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
            format!("metrics gather failed: {err}"),
        )
            .into_response(),
    }
}

async fn health_handler() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

/// Spawn the Axum metrics HTTP server on the given address and return its
/// JoinHandle. Serves `GET /metrics` (Prometheus exposition) and
/// `GET /healthz` (200 OK).
pub fn spawn_metrics_server(addr: SocketAddr) -> JoinHandle<anyhow::Result<()>> {
    // Force lazy initialization of the metric handles up-front so that
    // subsequent calls in the middleware don't pay the lock cost twice.
    Lazy::force(&RPC_REQUESTS_TOTAL);
    Lazy::force(&RPC_DURATION_SECONDS);
    Lazy::force(&RPC_IN_FLIGHT);

    tokio::spawn(async move {
        let app = Router::new()
            .route("/metrics", get(metrics_handler))
            .route("/healthz", get(health_handler))
            .with_state(Arc::new(MetricsState));

        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .with_context(|| format!("Unable to bind metrics server on {addr}"))?;
        tracing::info!("Starting the metrics server at {addr}");
        axum::serve(listener, app)
            .await
            .context("metrics server exited")?;
        Ok(())
    })
}
