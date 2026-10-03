use crate::api::{EthServer, RomeServer};
use crate::batcher::Batcher;
use crate::metrics::MetricsMiddleware;
use anyhow::Context;
use ethers::types::{Address, H256};
use jsonrpsee::server::{
    middleware::rpc::RpcServiceBuilder, BatchRequestConfig, ServerBuilder, ServerHandle,
};
use jsonrpsee::RpcModule;
use rome_jito_bundler::{BundlerConfig, JitoBundleClient};
use rome_sdk::rome_evm_client::RomeEVMClient;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Composite handle pairing a primed [`JitoBundleClient`] with the
/// operator-supplied [`BundlerConfig`]. Stored on [`Proxy`] as
/// `Option<ProxyBundler>` so the dispatch path in
/// `eth_sendRawTransaction` (JB6.2) can consult both atomically: client for
/// HTTP submission, config for the `evm_priority_fee_threshold_wei` gate
/// and the `default_tip_strategy` consumed by
/// [`RomeEVMClient::send_transaction_via_bundle`].
#[derive(Clone)]
pub struct ProxyBundler {
    pub client: Arc<JitoBundleClient>,
    pub config: BundlerConfig,
}

/// State for a single installed filter (eth_newFilter / eth_newBlockFilter).
#[derive(Clone)]
pub(crate) enum FilterState {
    /// Log filter — tracks the block at which it was installed and the last polled block.
    Logs {
        from_block: u64,
        last_block: u64,
        addresses: Vec<Address>,
        topics: Vec<Option<Vec<H256>>>,
    },
    /// Block filter — tracks the last polled block number.
    NewBlocks { last_block: u64 },
    /// Pending-transaction filter stub — always returns empty (no mempool).
    PendingTransactions,
}

/// Max concurrent installed filters before `eth_newFilter` is refused. Bounds
/// the in-memory registry against a client that loops `eth_newFilter` without
/// ever calling `eth_uninstallFilter` — the map is process memory, so an
/// unbounded registry is an OOM DoS. op-geth (retired) used to front the RPC
/// and bound this; the proxy is now the sole ingress and must bound it itself.
pub(crate) const MAX_FILTERS: usize = 8192;
/// Idle TTL: a filter not polled within this window is evicted. Mirrors the
/// standard node behavior (geth auto-uninstalls idle filters) — covers the
/// common case of a client that polls then disconnects without uninstalling.
pub(crate) const FILTER_TTL: Duration = Duration::from_secs(300);

/// An installed filter plus the last time it was created or polled (for TTL
/// eviction). Wraps [`FilterState`] so the registry can be bounded by age.
#[derive(Clone)]
pub(crate) struct Filter {
    pub(crate) state: FilterState,
    pub(crate) last_touched: Instant,
}

/// Evict filters idle past `ttl`. Pure over the map so it is unit-testable
/// without a live `Proxy`.
pub(crate) fn evict_expired_filters(map: &mut HashMap<u64, Filter>, now: Instant, ttl: Duration) {
    map.retain(|_, f| now.duration_since(f.last_touched) < ttl);
}

/// Evict expired, then insert `state` iff under `max`. Returns the new filter
/// id, or `None` when at capacity (caller maps to an RPC error). Fail-closed:
/// a full registry refuses new filters rather than growing without bound.
pub(crate) fn insert_bounded_filter(
    map: &mut HashMap<u64, Filter>,
    counter: &AtomicU64,
    state: FilterState,
    now: Instant,
    max: usize,
    ttl: Duration,
) -> Option<u64> {
    // Reclaim idle slots first so a full-but-stale registry still admits.
    evict_expired_filters(map, now, ttl);
    if map.len() >= max {
        return None; // fail-closed: refuse rather than grow past the cap
    }
    let id = counter.fetch_add(1, Ordering::Relaxed);
    map.insert(id, Filter { state, last_touched: now });
    Some(id)
}

#[derive(Clone)]
pub struct Proxy {
    pub rome_evm_client: Arc<RomeEVMClient>,
    /// In-memory filter store for eth_newFilter / eth_getFilterChanges / etc.
    /// Bounded by [`MAX_FILTERS`] + [`FILTER_TTL`] (see [`insert_bounded_filter`]).
    pub(crate) filters: Arc<RwLock<HashMap<u64, Filter>>>,
    /// Monotonically increasing counter used to generate filter IDs.
    pub(crate) filter_counter: Arc<AtomicU64>,
    /// Optional Jito bundle plumbing.
    ///
    /// **Default-off invariant**: when the proxy config omits the
    /// `jito_bundler` section, this is `None` — no `JitoBundleClient` is
    /// constructed at startup, no HTTP calls are made, and no bundle code
    /// path is reachable.
    ///
    /// When `Some`, [`ProxyBundler`] carries both the primed client and the
    /// operator-supplied [`BundlerConfig`] together — `eth_sendRawTransaction`
    /// reads the config for the `evm_priority_fee_threshold_wei` gate and
    /// the `default_tip_strategy` (passed through to
    /// [`RomeEVMClient::send_transaction_via_bundle`]) when promotion fires.
    pub(crate) bundler: Option<ProxyBundler>,
    /// Optional dedicated low-priority pool for read-path emulation. `None` =
    /// run emulation inline on the async workers (today's behavior); `Some` =
    /// offload to the pool so a read burst can't starve the write/confirm path.
    pub(crate) read_pool: Option<Arc<crate::read_pool::ReadPool>>,
    /// Optional multi-`DoTx` batch coalescer. **Default-off**: `None` when the
    /// `batching` config section is omitted ⇒ `eth_sendRawTransaction` takes the
    /// original per-tx path. When `Some`, concurrent submits are coalesced via
    /// the [`Batcher`] (whose backend calls `RomeEVMClient::send_pack`).
    pub(crate) batcher: Option<Batcher>,
    /// eth_getLogs guardrail: max block span one query may request. **Default
    /// -off**: `None` when `get_logs_max_block_range` is omitted from the
    /// proxy config ⇒ any span is accepted (today's behavior). When `Some`,
    /// over-span queries get the standard -32005 paginate-please error
    /// instead of fanning a chain-history scan into the storage layer.
    pub(crate) get_logs_max_block_range: Option<u64>,
}

impl Proxy {
    /// Create a new instance of the [Proxy] without bundle plumbing.
    /// Equivalent to [`Self::new_with_bundler(client, None)`]; preserved as a
    /// public constructor for any external caller (e.g., test harnesses) so
    /// adding the bundler parameter doesn't break source compatibility.
    /// Internal startup wires through [`Self::new_with_bundler`] directly.
    #[allow(dead_code)] // public API; no in-crate callers post JB6 wiring
    pub fn new(rome_evm_client: Arc<RomeEVMClient>) -> Self {
        Self::new_with_bundler(rome_evm_client, None, None, None, None)
    }

    /// Create a new instance of the [Proxy] with optional Jito bundle
    /// plumbing. When `bundler` is `None`, behavior is byte-identical to
    /// [`Self::new`]. When `Some`, the [`ProxyBundler`] is stored on the
    /// proxy and consulted by `eth_sendRawTransaction` (JB6.2 dispatch).
    pub fn new_with_bundler(
        rome_evm_client: Arc<RomeEVMClient>,
        bundler: Option<ProxyBundler>,
        read_pool: Option<Arc<crate::read_pool::ReadPool>>,
        batcher: Option<Batcher>,
        get_logs_max_block_range: Option<u64>,
    ) -> Self {
        Self {
            rome_evm_client,
            filters: Arc::new(RwLock::new(HashMap::new())),
            filter_counter: Arc::new(AtomicU64::new(1)),
            bundler,
            read_pool,
            batcher,
            get_logs_max_block_range,
        }
    }

    /// Pure guard for the eth_getLogs block span. `None` cap = uncapped
    /// (default-off). An inverted range (`from > to`) is zero blocks, not a
    /// violation — it falls through and returns empty like today. On
    /// violation returns the message for the -32005 response.
    pub(crate) fn validate_get_logs_range(
        cap: Option<u64>,
        from: u64,
        to: u64,
    ) -> std::result::Result<(), String> {
        let Some(cap) = cap else { return Ok(()) };
        let span = to.saturating_sub(from).saturating_add(1);
        if from <= to && span > cap {
            return Err(format!(
                "eth_getLogs block range too wide: {span} blocks requested, max {cap} — paginate the query"
            ));
        }
        Ok(())
    }

    /// Run a synchronous read-path emulation closure on the dedicated read pool
    /// if one is configured, else inline on the async worker (today's behavior).
    /// When offloaded, a panic or pool error surfaces as an `ApiError`; the
    /// write/confirm path (separate threads) is never affected.
    pub(crate) async fn offload<F, R, E>(&self, f: F) -> crate::error::Result<R>
    where
        F: FnOnce() -> std::result::Result<R, E> + Send + 'static,
        R: Send + 'static,
        E: Into<crate::error::ApiError> + Send + 'static,
    {
        match &self.read_pool {
            Some(pool) => pool
                .run(f)
                .await
                .map_err(|e| crate::error::ApiError::custom(format!("read pool: {e}")))?
                .map_err(Into::into),
            None => f().map_err(Into::into),
        }
    }

    /// Pure routing predicate: `true` iff a single-rollup `RheaTx` arriving
    /// via `eth_sendRawTransaction` SHOULD be auto-promoted to the bundle
    /// path because (a) the bundler is configured and (b) the operator has
    /// set an `evm_priority_fee_threshold_wei` AND (c) the EVM tx's
    /// `maxPriorityFeePerGas` is at or above that threshold.
    ///
    /// JB6.2 plugs this predicate into `eth_sendRawTransaction`. Default-off
    /// invariant: returns `false` whenever any of the three conditions
    /// fails — most importantly, when `bundler.is_none()`.
    pub(crate) fn should_auto_promote(
        bundler: Option<&ProxyBundler>,
        tx_priority_fee_wei: u128,
    ) -> bool {
        match bundler.and_then(|b| b.config.evm_priority_fee_threshold_wei) {
            Some(threshold) => tx_priority_fee_wei >= threshold,
            None => false,
        }
    }

    /// Pure routing predicate: `true` iff `eth_sendRawTransaction` should route
    /// through the batch coalescer (the operator wired a `batching` config and a
    /// [`Batcher`] was constructed at startup). Default-off: `None` ⇒ `false` ⇒
    /// the original per-tx submit path, byte-identical to today.
    pub(crate) fn should_batch(batcher: Option<&Batcher>) -> bool {
        batcher.is_some()
    }

    /// Start the RPC server.
    pub async fn start_rpc_server(
        self,
        host: SocketAddr,
        max_connections: Option<u32>,
        max_batch_size: Option<u32>,
    ) -> anyhow::Result<ServerHandle> {
        let max_conn = max_connections.unwrap_or_else(default_max_connections);
        let max_batch = max_batch_size.unwrap_or(DEFAULT_MAX_BATCH_SIZE);
        tracing::info!(
            "Starting the RPC server at {host} (max_connections: {max_conn}, max_batch_size: {max_batch})"
        );

        let metrics_layer = RpcServiceBuilder::new()
            .layer_fn(|service| MetricsMiddleware { service });

        // `max_connections` is a global connection cap and `max_batch_size`
        // bounds the calls one request can fan out to. Per-IP rate limiting
        // is expected at the reverse proxy in front of this server.
        let rpc = ServerBuilder::default()
            .max_connections(max_conn)
            .set_batch_request_config(BatchRequestConfig::Limit(max_batch))
            .set_rpc_middleware(metrics_layer)
            .build(host)
            .await
            .context("Unable to start the RPC server")?;

        let mut module = RpcModule::new(());
        module.merge(EthServer::into_rpc(self.clone())).unwrap();
        module.merge(RomeServer::into_rpc(self)).unwrap();

        Ok(rpc.start(module))
    }
}

/// Batch-size cap when the operator hasn't set `max_batch_size`. High enough
/// for indexers (e.g. Blockscout) and client-side batching (viem defaults to
/// 1000) while still bounding a single request's emulation fan-out.
pub(crate) const DEFAULT_MAX_BATCH_SIZE: u32 = 1000;

/// Inbound connection cap when the operator hasn't set `max_connections`:
/// half the process fd limit (leaving headroom for outbound RPC / DB /
/// fresh-per-call read connections), floored at 100. Derived from the actual
/// limit — no machine-specific magic number.
fn derive_max_connections(fd_soft_limit: u64) -> u32 {
    let half = (fd_soft_limit / 2).min(u32::MAX as u64) as u32;
    half.max(100)
}

/// Read the process `RLIMIT_NOFILE` soft limit and derive the inbound cap.
/// Falls back to the floor (100) if the limit can't be read.
fn default_max_connections() -> u32 {
    let mut rlim = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: valid resource id and a fully-initialized rlimit out-param.
    let rc = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) };
    if rc != 0 {
        return 100;
    }
    derive_max_connections(rlim.rlim_cur as u64)
}

#[cfg(test)]
mod tests {
    //! Tests cover the **default-off invariant** at the predicate level:
    //! `Proxy::should_auto_promote` returns `false` whenever the bundler is
    //! absent, the threshold is absent, or the tx's `maxPriorityFeePerGas`
    //! falls below the threshold. JB6.2 plugs this predicate into
    //! `eth_sendRawTransaction`; pinning the boolean logic here means the
    //! handler-side wiring is mechanical on top of an already-tested
    //! decision function.
    //!
    //! We construct a [`ProxyBundler`] via a hidden test seam
    //! (`build_wiremock_backed_bundler`) when needed, but most tests
    //! exercise the `None`-bundler branches and don't require a live client.
    use super::*;

    /// Default-off: with no bundler configured, no tx (regardless of its
    /// priority fee) is auto-promoted. This is the load-bearing invariant
    /// for proxy configs that omit the `jito_bundler` section entirely.
    #[test]
    fn no_bundler_means_no_auto_promotion() {
        // Even with a sky-high priority fee, no bundler => no promotion.
        assert!(!Proxy::should_auto_promote(None, u128::MAX));
        assert!(!Proxy::should_auto_promote(None, 0));
    }

    /// Default-off: with no batcher configured, no tx is coalesced — the
    /// load-bearing invariant for configs that omit the `batching` section.
    #[test]
    fn no_batcher_means_no_batching() {
        assert!(!Proxy::should_batch(None));
    }

    /// Default-off: with no `get_logs_max_block_range` configured, any span —
    /// including genesis-to-tip — passes. Deploy-order safety: this code can
    /// ship before the genesis-scanning clients are redeployed, and the cap
    /// flips on later via config alone.
    #[test]
    fn get_logs_range_uncapped_when_config_omitted() {
        assert!(Proxy::validate_get_logs_range(None, 0, u64::MAX).is_ok());
    }

    /// Capped: spans at the cap pass, one block past it errors with a message
    /// naming the span, the cap, and asking the caller to paginate.
    #[test]
    fn get_logs_range_cap_boundary() {
        // 10_000-block cap: [0, 9999] is exactly 10_000 blocks -> ok.
        assert!(Proxy::validate_get_logs_range(Some(10_000), 0, 9_999).is_ok());
        // [0, 10_000] is 10_001 blocks -> err.
        let err = Proxy::validate_get_logs_range(Some(10_000), 0, 10_000)
            .expect_err("span over cap must error");
        assert!(err.contains("10001"), "message must name the span: {err}");
        assert!(err.contains("10000"), "message must name the cap: {err}");
    }

    /// Inverted range (from > to) is not a cap violation — it yields zero
    /// blocks and falls through to the storage layer (which returns empty),
    /// matching today's behavior.
    #[test]
    fn get_logs_range_inverted_is_not_a_violation() {
        assert!(Proxy::validate_get_logs_range(Some(10), 50, 40).is_ok());
    }

    /// `bundler.is_some()` but `threshold.is_none()` is the
    /// "instantiated-but-not-routed" production-safety case (operator opts
    /// into bundle plumbing without enabling per-tx auto-promotion). MUST
    /// return `false` for every priority-fee value.
    #[tokio::test]
    async fn bundler_without_threshold_means_no_auto_promotion() {
        let bundler = build_wiremock_backed_bundler(None).await;
        // No threshold => no promotion regardless of fee.
        assert!(!Proxy::should_auto_promote(Some(&bundler), 0));
        assert!(!Proxy::should_auto_promote(Some(&bundler), u128::MAX));
    }

    /// When both bundler + threshold are configured, promotion is gated on
    /// `tx_priority_fee_wei >= threshold`.
    #[tokio::test]
    async fn bundler_with_threshold_promotes_at_or_above_threshold() {
        let threshold = 1_000_000_000u128; // 1 gwei
        let bundler = build_wiremock_backed_bundler(Some(threshold)).await;

        // Below threshold: no promotion.
        assert!(!Proxy::should_auto_promote(Some(&bundler), threshold - 1));
        // At threshold: promote.
        assert!(Proxy::should_auto_promote(Some(&bundler), threshold));
        // Above threshold: promote.
        assert!(Proxy::should_auto_promote(Some(&bundler), threshold + 1));
    }

    /// Smoke-build a `JitoBundleClient` against an unreachable URL. We
    /// deliberately use a URL that won't respond — the test asserts the
    /// constructor fails fast (no panic, no hang) so the predicate-level
    /// tests above don't accidentally exercise live HTTP. If
    /// `JitoBundleClient::new` ever changes to lazy-init, this test pins
    /// that contract change for review.
    #[tokio::test]
    async fn jito_bundle_client_new_fails_fast_on_unreachable_url() {
        use rome_jito_bundler::{
            BundlerConfig, Cluster, FallbackPolicy, LandingTarget, TipStrategy,
        };
        let cfg = BundlerConfig {
            cluster: Cluster::Devnet,
            block_engine_url: Some(
                "http://127.0.0.1:1/api/v1/bundles".to_string(),
            ),
            enable_for_cross_rollup: true,
            default_tip_strategy: TipStrategy::Fixed { lamports: 50_000 },
            default_landing_target: LandingTarget::BySlots { slots: 5 },
            default_fallback_policy: FallbackPolicy::FallbackToSequential,
            min_tip_lamports: 1_000,
            max_tip_lamports: 10_000_000,
            poll_interval_ms: 5_000,
            http_timeout_ms: 100, // fast-fail
            evm_priority_fee_threshold_wei: None,
            ..Default::default()
        };
        // Expect Err: `getTipAccounts` priming call against unreachable URL.
        let res = JitoBundleClient::new(&cfg).await;
        assert!(res.is_err(), "expected unreachable URL to surface as Err");
    }

    /// Build a [`ProxyBundler`] for predicate-level tests.
    ///
    /// `JitoBundleClient::new` makes one HTTP call (`getTipAccounts`) to
    /// prime the tip-account cache. We back it with a `wiremock` server
    /// that immediately returns a valid `getTipAccounts` response so the
    /// constructor succeeds without hitting any real Block Engine. The
    /// resulting [`ProxyBundler`] is used purely as a `Some(_)` placeholder
    /// for `should_auto_promote`; no further HTTP traffic flows during
    /// these tests.
    ///
    /// JB7 will exercise the full submit/await cycle against a real
    /// Block Engine (a dedicated test Block Engine). For JB6.2,
    /// wiremock is sufficient.
    async fn build_wiremock_backed_bundler(threshold_wei: Option<u128>) -> ProxyBundler {
        use rome_jito_bundler::{Cluster, FallbackPolicy, LandingTarget, TipStrategy};
        // Spin up a tiny wiremock server. Mounting `getTipAccounts` is
        // sufficient — `JitoBundleClient::new` only primes the tip-account
        // cache during construction; the predicate tests above don't make
        // any further HTTP calls.
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::body_partial_json(serde_json::json!({
                "method": "getTipAccounts"
            })))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": [
                        // Solana System Program — placeholder; tests don't actually submit bundles.
                        "11111111111111111111111111111111"
                    ],
                }),
            ))
            .mount(&server)
            .await;

        let cfg = BundlerConfig {
            cluster: Cluster::Devnet,
            block_engine_url: Some(format!("{}/api/v1/bundles", server.uri())),
            enable_for_cross_rollup: true,
            default_tip_strategy: TipStrategy::Fixed { lamports: 50_000 },
            default_landing_target: LandingTarget::BySlots { slots: 5 },
            default_fallback_policy: FallbackPolicy::FallbackToSequential,
            min_tip_lamports: 1_000,
            max_tip_lamports: 10_000_000,
            poll_interval_ms: 5_000,
            http_timeout_ms: 1_000,
            evm_priority_fee_threshold_wei: threshold_wei,
            ..Default::default()
        };
        let client = JitoBundleClient::new(&cfg)
            .await
            .expect("wiremock-backed JitoBundleClient::new should succeed");
        ProxyBundler { client, config: cfg }
    }

    #[test]
    fn derive_max_connections_floors_at_100() {
        // Never below the historical floor, even on a tiny fd limit.
        assert_eq!(derive_max_connections(0), 100);
        assert_eq!(derive_max_connections(50), 100);
        assert_eq!(derive_max_connections(200), 100);
    }

    #[test]
    fn derive_max_connections_scales_with_fd_limit() {
        // Half the fd limit (leaving headroom for outbound RPC/DB), derived —
        // not a machine-specific magic number.
        assert_eq!(derive_max_connections(1024), 512);
        assert_eq!(derive_max_connections(65536), 32768);
    }

    #[test]
    fn derive_max_connections_is_not_a_fixed_number() {
        assert!(derive_max_connections(100_000) > derive_max_connections(10_000));
    }

    // ── Filter-registry bounds (DoS: unbounded eth_newFilter map) ───────────
    // Instant math uses ADDITION only (base + age) so it never underflows the
    // monotonic clock on a freshly-booted CI host.

    fn dummy_state() -> FilterState {
        FilterState::NewBlocks { last_block: 0 }
    }

    #[test]
    fn evict_expired_filters_drops_only_stale_entries() {
        let base = Instant::now();
        let ttl = Duration::from_secs(300);
        let mut map: HashMap<u64, Filter> = HashMap::new();
        map.insert(1, Filter { state: dummy_state(), last_touched: base }); // will be 400s old
        map.insert(2, Filter { state: dummy_state(), last_touched: base + Duration::from_secs(390) }); // 10s old

        // Sweep "now" = base + 400s: entry 1 is 400s idle (>= ttl) → gone; entry 2 is 10s → kept.
        evict_expired_filters(&mut map, base + Duration::from_secs(400), ttl);
        assert!(!map.contains_key(&1), "stale filter (400s idle) must be evicted");
        assert!(map.contains_key(&2), "fresh filter (10s idle) must survive");
    }

    #[test]
    fn insert_bounded_filter_refuses_at_capacity_then_ttl_makes_room() {
        let counter = AtomicU64::new(0);
        let ttl = Duration::from_secs(300);
        let max = 2usize;
        let now = Instant::now();
        let mut map: HashMap<u64, Filter> = HashMap::new();

        // Fill to capacity with fresh entries.
        assert!(insert_bounded_filter(&mut map, &counter, dummy_state(), now, max, ttl).is_some());
        assert!(insert_bounded_filter(&mut map, &counter, dummy_state(), now, max, ttl).is_some());
        assert_eq!(map.len(), max);

        // Full of FRESH entries → next insert is refused (fail-closed), map unchanged.
        assert!(
            insert_bounded_filter(&mut map, &counter, dummy_state(), now, max, ttl).is_none(),
            "a full registry of fresh filters must refuse new filters"
        );
        assert_eq!(map.len(), max, "a refused insert must not grow the map");

        // Advance past the TTL: the fresh entries are now stale → eviction frees
        // room and the insert succeeds, but the map still never exceeds `max`.
        let later = now + ttl + Duration::from_secs(1);
        assert!(
            insert_bounded_filter(&mut map, &counter, dummy_state(), later, max, ttl).is_some(),
            "expired entries must be evicted to admit a new filter"
        );
        assert!(map.len() <= max, "registry must never exceed max after insert");
    }
}
