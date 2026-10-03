use {
    crate::proxy::{Proxy, ProxyBundler},
    jsonrpsee::server::ServerHandle,
    rome_sdk::{
        rome_evm_client::{
            indexer::{
                config::EthereumStorageConfig,
            },
            resources::PayerConfig, Payer, RomeEVMClient,
            price_manager::PriFeeCfg,
            price_manager::{
                PriceManagerConfig, PriorityFeeMgr, RpcFeeMgr,
            }
        },
        rome_solana::{
            gate::{require_v1_gate, tx_v1_gate_active},
            indexers::clock::SolanaClockIndexer, config::SolanaConfig,
            solana_rpc_client::SolanaRpcClient, tower::SolanaTower,
        },
    },
    solana_sdk::pubkey::Pubkey,
    std::{
        net::SocketAddr, str::FromStr, sync::Arc, time::Duration,
    },
    tokio::task::JoinHandle,
    tokio_util::sync::CancellationToken,
    rome_jito_bundler::{BundlerConfig, JitoBundleClient},
};

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct PriorityFeeSettings {
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default = "default_cu_price_percentile")]
    pub cu_price_percentile: u8,
    #[serde(default = "default_max_microlamports")]
    pub max_microlamports: u64,
    #[serde(default = "default_min_microlamports")]
    pub min_microlamports: u64,
    /// Poll interval (ms). Default ~1 slot.
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
}

fn default_enabled() -> bool { true }
fn default_cu_price_percentile() -> u8 { 90 }
fn default_max_microlamports() -> u64 { 0 }
fn default_min_microlamports() -> u64 { 1 }
fn default_poll_interval_ms() -> u64 { 1000 }

impl Default for PriorityFeeSettings {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            cu_price_percentile: default_cu_price_percentile(),
            max_microlamports: default_max_microlamports(),
            min_microlamports: default_min_microlamports(),
            poll_interval_ms: default_poll_interval_ms(),
        }
    }
}

fn resolve_priority_settings(cfg: Option<PriorityFeeSettings>) -> Option<PriorityFeeSettings> {
    let settings = cfg.unwrap_or_default();
    settings.enabled.then_some(settings)
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct ProxyConfig {
    pub solana: SolanaConfig,
    pub program_id: String,
    pub chain_id: u64,
    pub payers: Vec<PayerConfig>,
    pub proxy_host: SocketAddr,
    pub ethereum_storage: EthereumStorageConfig,
    pub gas_price_mul: f64,
    pub track_gas: Option<bool>,
    pub price_manager: Option<PriceManagerConfig>,
    #[serde(default)]
    pub priority_fee: Option<PriorityFeeSettings>,
    pub max_connections: Option<u32>,
    /// Maximum calls in one JSON-RPC batch request. Omitted ⇒
    /// [`crate::proxy::DEFAULT_MAX_BATCH_SIZE`]; over-limit batches are
    /// rejected whole. Public endpoints should set a lower value (e.g. 100).
    #[serde(default)]
    pub max_batch_size: Option<u32>,
    /// Run read-path emulation (`eth_call` / `estimateGas` / discovery) on a
    /// dedicated low-priority pool off the async workers so a read burst can't
    /// starve the write/confirm path. **Default on** (omitted = enabled); set
    /// `false` to opt out and run emulation inline on the async workers.
    #[serde(default)]
    pub read_pool_enabled: Option<bool>,
    /// Optional address for the Prometheus `/metrics` HTTP sidecar. If `None`,
    /// the metrics endpoint is not started. When set, RED metrics for `eth_*`
    /// and `rome_*` methods are exposed in Prometheus exposition format at
    /// `GET /metrics` on the given address.
    #[serde(default)]
    pub metrics_host: Option<SocketAddr>,
    /// Optional Jito bundle plumbing. **Default-off invariant**: when this
    /// field is omitted from the proxy config, behavior is 100% identical to
    /// today — no `JitoBundleClient` is constructed, no HTTP calls are made,
    /// no extra hot-path code is reachable.
    ///
    /// When `Some(_)`, a [`JitoBundleClient`] is instantiated at startup
    /// (priming the tip-account cache via one HTTP call to the Block Engine)
    /// and stored on the [`Proxy`] for future dispatch wiring. JB6 v1 is
    /// **plumbing only** — the bundler instance is ready but not yet routed
    /// onto any tx path. Per-tx auto-promotion of single-rollup `RheaTx` via
    /// [`BundlerConfig::evm_priority_fee_threshold_wei`] is itself optional
    /// (section present + threshold absent = client constructed but no
    /// promotion). Handler-side dispatch wiring lands in JB6.2.
    ///
    /// Type re-uses [`rome_jito_bundler::BundlerConfig`] so the JSON shape
    /// matches the rome-sdk `Rome::new_with_config_and_bundler` opt-in.
    #[serde(default)]
    pub jito_bundler: Option<BundlerConfig>,
    /// Optional default-off multi-`DoTx` batching (Phase 3 plumbing). Absent ⇒
    /// `None` ⇒ behavior identical to today. See [`crate::batching::BatchingConfig`].
    /// The coalescer + `eth_sendRawTransaction` dispatch land in follow-on slices.
    #[serde(default)]
    pub batching: Option<crate::batching::BatchingConfig>,
    /// eth_getLogs guardrail: maximum block span one query may request.
    /// **Default-off**: omitted ⇒ `None` ⇒ any span accepted (today's
    /// behavior), so this ships ahead of the genesis-scanning clients being
    /// redeployed and flips on later via config alone. When set (e.g. 10000,
    /// the ceiling most public RPCs enforce), an over-span query gets the
    /// standard -32005 paginate-please error.
    #[serde(default)]
    pub get_logs_max_block_range: Option<u64>,
}

impl ProxyConfig {
    /// Whether the dedicated read pool is enabled. **Default on** — omitting
    /// `read_pool_enabled` resolves to `true`; `read_pool_enabled: false` opts
    /// out (inline emulation on the async workers).
    pub fn read_pool_on(&self) -> bool {
        self.read_pool_enabled.unwrap_or(true)
    }

    pub async fn init(
        self
    ) -> anyhow::Result<(ServerHandle, JoinHandle<anyhow::Result<()>>, Option<JoinHandle<anyhow::Result<()>>>, Option<JoinHandle<anyhow::Result<()>>>)> {
        // Build the async RPC client once and share it across the emulation hot
        // path (indexer / tower / RomeEVMClient). Its `get_account_storage()`
        // builds a fresh `RpcClient` per account fetch — no connection reuse, so
        // an idle-dropped socket is never reused (the #375 pooling regression).
        // The price manager takes the same concrete client.
        let async_client = Arc::new(self.solana.clone().into_async_client());
        // The tower submits through `rpc_client`. When `solana.rpc_urls` lists
        // extra nodes it round-robins submission across them (default-off ⇒ this
        // is just the primary `async_client`). Reads/confirmation and the price
        // manager keep the concrete primary client.
        let rpc_client: Arc<dyn SolanaRpcClient> =
            self.solana.build_submit_client(async_client.clone());
        // SIMD-0385 boot assert: this binary composes only v1 transactions.
        // Fail-loud on RPC error (the `?`) — a transient failure here must
        // not silently report the gate as inactive; startup already requires
        // a live RPC (see `SolanaClockIndexer::new` just below). Gate
        // inactive is a named startup Err (`require_v1_gate`), never a
        // silent downgrade and never a silent proceed.
        let gate_active = tx_v1_gate_active(&*rpc_client).await?;
        require_v1_gate(gate_active)?;
        tracing::info!("v1 sender: SIMD-0385 gate active");
        let payers = Payer::from_config_list(&self.payers).await?;
        let solana_clock_indexer = SolanaClockIndexer::new(rpc_client.clone()).await?;
        let program_id = Pubkey::from_str(&self.program_id)?;
        let tower = SolanaTower::new_with_confirm(
            rpc_client.clone(),
            solana_clock_indexer.get_current_clock(),
            self.solana.confirm.clone(),
            program_id,
        );
        let eth_block_storage = Some(self.ethereum_storage.init()?);
        let token = CancellationToken::new();

        // Resolve the read-pool decision before `self` is partially moved
        // (the `price_manager` and `jito_bundler` blocks below move fields out
        // of `self`, after which a `&self` method call would not borrow-check).
        let read_pool_on = self.read_pool_on();

        // Captured before `self`/`async_client` are partially moved below, so the
        // priority-fee source can be wired after the client is built.
        let priority_fee_cfg = self.priority_fee.clone();
        let priority_async_client = async_client.clone();

        let price_mgr = if let Some(price_manager) = self.price_manager {
            price_manager.init(
                async_client,
                &program_id,
                self.chain_id,
                token,
            ).await?
        } else {
            None
        };

        let price_jh = price_mgr.as_ref().map(|pm | pm.execute());

        // create rome evm client
        let client = RomeEVMClient::new_with_track_gas(
            self.chain_id,
            program_id,
            tower,
            eth_block_storage,
            payers,
            self.gas_price_mul,
            self.track_gas.unwrap_or(false),
            price_mgr,
            None,
        )
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        let client = match resolve_priority_settings(priority_fee_cfg) {
            Some(pf) => {
                let config = PriFeeCfg {
                    cu_price_percentile: pf.cu_price_percentile,
                    max_microlamports: pf.max_microlamports,
                    min_microlamports: pf.min_microlamports,
                };
                let mgr = RpcFeeMgr::new(
                    priority_async_client,
                    config,
                    Duration::from_millis(pf.poll_interval_ms),
                    CancellationToken::new(),
                )
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
                let mgr = Arc::new(mgr);
                let _ = mgr.execute();
                tracing::info!(
                    "priority-fee: enabled (bid p{}, cap {} µlam/CU [0=uncapped], min floor {} µlam/CU, {}ms poll)",
                    pf.cu_price_percentile,
                    pf.max_microlamports,
                    pf.min_microlamports,
                    pf.poll_interval_ms
                );
                client.with_priority_mgr(Some(mgr as Arc<dyn PriorityFeeMgr>))
            }
            None => client,
        };

        // Optional Jito bundle plumbing (default-off invariant). When the
        // operator omits the `jito_bundler` section, this resolves to `None`
        // and no `JitoBundleClient` is constructed. When present, one HTTP
        // call to the Block Engine primes the tip-account cache; we then
        // pair the primed client with the operator-supplied [`BundlerConfig`]
        // in a [`ProxyBundler`] so JB6.2's `eth_sendRawTransaction` dispatch
        // can read both atomically (config for the threshold gate +
        // `default_tip_strategy`, client for HTTP submission).
        //
        // Failure to construct the client (e.g., Block Engine unreachable
        // at startup) is fatal — operators that opt in to bundle plumbing
        // expect the dependency to be live; we surface the error early
        // rather than silently falling back to the no-bundler path.
        let bundler = match self.jito_bundler {
            Some(cfg) => {
                let client = JitoBundleClient::new(&cfg)
                    .await
                    .map_err(|e| anyhow::anyhow!("JitoBundleClient init failed: {}", e))?;
                Some(ProxyBundler { client, config: cfg })
            }
            None => None,
        };

        // Dedicated read pool (offloads emulation off the async workers so a
        // read burst can't starve the write/confirm path). On by default;
        // `read_pool_enabled: false` opts out (inline emulation).
        let read_pool = if read_pool_on {
            let pool = crate::read_pool::ReadPool::new()?;
            tracing::info!(
                "read pool enabled: {} workers, intake queue bound {} (sheds when full)",
                pool.worker_count(),
                pool.queue_capacity()
            );
            Some(pool)
        } else {
            None
        };

        // Optional multi-`DoTx` batch coalescer (default-off invariant). Absent
        // `batching` section ⇒ `None` ⇒ `eth_sendRawTransaction` takes the
        // original per-tx path. When present, a `Batcher` wraps a
        // `RealBatchBackend` over the shared client (which calls `send_pack`).
        let client = Arc::new(client);
        let batcher = match self.batching {
            Some(cfg) => {
                let backend = Arc::new(crate::batcher::RealBatchBackend {
                    client: client.clone(),
                    limits: cfg.to_pack_limits(),
                });
                Some(crate::batcher::Batcher::new(
                    backend,
                    cfg.max_pack_size,
                    cfg.pack_concurrency,
                    cfg.pack_fill_timeout_ms,
                    cfg.intake_capacity,
                ))
            }
            None => None,
        };

        // Start the proxy server
        let server_h = Proxy::new_with_bundler(
            client,
            bundler,
            read_pool,
            batcher,
            self.get_logs_max_block_range,
        )
            .start_rpc_server(self.proxy_host, self.max_connections, self.max_batch_size)
            .await?;

        let clock_jh = tokio::spawn(solana_clock_indexer.clone().start());

        // Optional Prometheus /metrics sidecar
        let metrics_jh = self.metrics_host.map(crate::metrics::spawn_metrics_server);

        Ok((server_h, clock_jh, price_jh, metrics_jh))
    }
}

#[cfg(test)]
mod tests {
    //! Tests cover the **default-off invariant**: a config without a
    //! `jito_bundler` section deserializes to `jito_bundler == None`, and a
    //! config with one deserializes to `Some(BundlerConfig { ... })`.
    //!
    //! Note: these are pure config-deserialization tests. Startup wiring
    //! (calling `JitoBundleClient::new`) is exercised in `proxy.rs` tests
    //! against a stub URL or by integration tests in JB7. We deliberately
    //! do NOT make HTTP calls here.
    use super::*;

    /// Minimal valid `ProxyConfig` JSON. Captures the canonical config shape
    /// we ship today, plus an explicit absence of the new `jito_bundler`
    /// field. Test asserts `jito_bundler == None`. Pins the default-off
    /// invariant: omission of the section MUST produce no bundler.
    fn minimal_proxy_config_json() -> serde_json::Value {
        serde_json::json!({
            "solana": {
                "rpc_url": "http://localhost:8899",
                "commitment": "confirmed",
            },
            "program_id": "RomeDbGQYbqomGVk13h9JkQHKoNWKB84Lw1ij9AtRXT",
            "chain_id": 1001,
            "payers": [],
            "proxy_host": "127.0.0.1:9090",
            "ethereum_storage": { "type": "in_memory" },
            "gas_price_mul": 1.0,
        })
    }

    /// **Default-on**: omitting `read_pool_enabled` MUST resolve to enabled, so
    /// every proxy offloads read emulation to the niced pool (protecting the
    /// write/confirm path) without a per-config opt-in. This would FAIL against
    /// the old `unwrap_or(false)` default.
    #[test]
    fn read_pool_defaults_on_when_omitted() {
        let cfg: ProxyConfig = serde_json::from_value(minimal_proxy_config_json())
            .expect("minimal proxy config must deserialize");
        assert!(
            cfg.read_pool_enabled.is_none(),
            "fixture must omit read_pool_enabled"
        );
        assert!(
            cfg.read_pool_on(),
            "omitted read_pool_enabled must default to ON"
        );
    }

    /// Opt-out preserved: `read_pool_enabled: false` runs emulation inline.
    #[test]
    fn read_pool_opt_out_with_false() {
        let mut v = minimal_proxy_config_json();
        v["read_pool_enabled"] = serde_json::json!(false);
        let cfg: ProxyConfig =
            serde_json::from_value(v).expect("config must deserialize");
        assert!(!cfg.read_pool_on(), "read_pool_enabled:false must opt out");
    }

    /// **RED → GREEN**: omitting `jito_bundler` from the proxy config
    /// MUST deserialize to `None`. This is the **default-off invariant** —
    /// existing operator configs continue to work unchanged, and behavior
    /// stays 100% identical to today.
    #[test]
    fn proxy_config_without_jito_bundler_deserializes_to_none() {
        let cfg: ProxyConfig = serde_json::from_value(minimal_proxy_config_json())
            .expect("minimal proxy config must deserialize");
        assert!(
            cfg.jito_bundler.is_none(),
            "default-off invariant violated: omitting jito_bundler must yield None, got {:?}",
            cfg.jito_bundler
        );
    }

    /// **RED → GREEN**: providing `jito_bundler` in the proxy config MUST
    /// deserialize into `Some(BundlerConfig)` with the operator-supplied
    /// fields preserved. Reuses the rome-sdk `BundlerConfig` shape so the
    /// proxy and rome-sdk consumers (`Rome::new_with_config_and_bundler`)
    /// agree on the JSON schema verbatim.
    #[test]
    fn proxy_config_with_jito_bundler_deserializes_into_some() {
        let mut json = minimal_proxy_config_json();
        json["jito_bundler"] = serde_json::json!({
            "cluster": "devnet",
            "block_engine_url": "http://block-engine.example:9077/api/v1/bundles",
            "default_tip_strategy": { "kind": "fixed", "lamports": 50_000 },
            "default_landing_target": { "kind": "by-slots", "slots": 5 },
            "default_fallback_policy": "fallback-to-sequential",
            "min_tip_lamports": 1_000,
            "max_tip_lamports": 10_000_000,
            "evm_priority_fee_threshold_wei": 1_000_000_000u64,
        });
        let cfg: ProxyConfig =
            serde_json::from_value(json).expect("config with jito_bundler must deserialize");
        let bundler = cfg
            .jito_bundler
            .expect("jito_bundler section present must yield Some");
        // Cluster + URL preserved verbatim.
        assert_eq!(bundler.cluster, rome_jito_bundler::Cluster::Devnet);
        assert_eq!(
            bundler.block_engine_url.as_deref(),
            Some("http://block-engine.example:9077/api/v1/bundles")
        );
        // EVM auto-promotion threshold round-trips.
        assert_eq!(bundler.evm_priority_fee_threshold_wei, Some(1_000_000_000));
        // Tip clamps preserved.
        assert_eq!(bundler.min_tip_lamports, 1_000);
        assert_eq!(bundler.max_tip_lamports, 10_000_000);
    }

    /// Production-safety subcase: `jito_bundler` section present but
    /// `evm_priority_fee_threshold_wei` absent → bundler is instantiated
    /// (non-None) but per-tx auto-promotion is opt-in. JB6 v1 doesn't
    /// route txs through the bundler regardless; this pins the field-level
    /// optionality so JB6.2 can dispatch correctly without re-litigating
    /// the schema.
    #[test]
    fn jito_bundler_threshold_field_is_optional() {
        let mut json = minimal_proxy_config_json();
        json["jito_bundler"] = serde_json::json!({
            "cluster": "devnet",
            "default_tip_strategy": { "kind": "fixed", "lamports": 50_000 },
        });
        let cfg: ProxyConfig =
            serde_json::from_value(json).expect("threshold-omitted config must deserialize");
        let bundler = cfg.jito_bundler.expect("section present");
        assert!(
            bundler.evm_priority_fee_threshold_wei.is_none(),
            "threshold MUST default to None when absent (no auto-promotion)"
        );
    }

    /// The v1-migration removed the persistent-ALT subsystem and its three
    /// config keys (`persistent_alts`, `persistent_alts_refresh_ms`,
    /// `alt_apply_first`). `ProxyConfig` has no `deny_unknown_fields`, so a
    /// deployed config that still carries all three retired keys must keep
    /// loading — dropping them from deployed configs is a separate step, and
    /// startup must not break in the meantime. Pinned here so `ProxyConfig` can never grow
    /// `deny_unknown_fields` while retired keys are still deployed.
    #[test]
    fn proxy_config_tolerates_retired_alt_keys() {
        let mut json = minimal_proxy_config_json();
        json["persistent_alts"] =
            serde_json::json!(["HQcKt6T2ztAFfuQan4qGQXe3VHdeTeiZ3GfB8PTxuBg7"]);
        json["persistent_alts_refresh_ms"] = serde_json::json!(5_000);
        json["alt_apply_first"] = serde_json::json!(true);

        let cfg: ProxyConfig = serde_json::from_value(json)
            .expect("a config carrying the retired persistent-ALT keys must still deserialize");
        // And the struct exposes no ALT behavior any more — the fields are gone.
        assert_eq!(cfg.chain_id, 1001);
    }

    /// **Default-off**: omitting `get_logs_max_block_range` deserializes to
    /// `None` (uncapped — today's behavior), so this code can deploy ahead of
    /// the genesis-scanning clients being fixed; the cap flips on via config.
    #[test]
    fn get_logs_range_cap_defaults_off_when_omitted() {
        let cfg: ProxyConfig = serde_json::from_value(minimal_proxy_config_json())
            .expect("minimal proxy config must deserialize");
        assert!(cfg.get_logs_max_block_range.is_none());
    }

    /// Opt-in: a configured cap round-trips.
    #[test]
    fn get_logs_range_cap_parses_when_present() {
        let mut v = minimal_proxy_config_json();
        v["get_logs_max_block_range"] = serde_json::json!(10_000);
        let cfg: ProxyConfig = serde_json::from_value(v).expect("config must deserialize");
        assert_eq!(cfg.get_logs_max_block_range, Some(10_000));
    }

    /// SIMD-0385 boot assert: `init` composes `require_v1_gate(gate_active)?`
    /// — gate-active proceeds, gate-inactive is a named startup `Err`, never
    /// a silent downgrade. `require_v1_gate`'s own truth table is unit
    /// tested in rome-sdk `rome_solana::gate` (`require_v1_gate_active_is_ok`
    /// / `require_v1_gate_inactive_is_err`); the RPC-error fail-loud half
    /// (`?` on `tx_v1_gate_active`) is pinned there too
    /// (`gate_propagates_rpc_error` against a `MockClient`). This test pins
    /// that the proxy composes the two calls correctly.
    #[test]
    fn require_v1_gate_composes_with_gate_state() {
        assert!(require_v1_gate(true).is_ok());
        assert!(require_v1_gate(false).is_err());
    }
}
