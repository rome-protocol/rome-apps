//! Binary entrypoint for `cardo-service`.
//!
//! Branches on `CARDO_MCP_MODE` (default `both`) to decide which surfaces to
//! serve:
//!
//! | Mode    | REST | MCP HTTP | MCP stdio | Poll loop |
//! |---------|------|----------|-----------|-----------|
//! | `both`  | on   | on       | off       | on        |
//! | `http`  | off  | on       | off       | on        |
//! | `stdio` | off  | off      | on        | off       |
//! | `none`  | on   | off      | off       | on        |
//!
//! - `both` is the default — one process, one listener, REST + MCP HTTP
//!   merged on the same Axum router, poll loop in the background.
//! - `http` is the agent-only variant (`mcp.cardo.rome.builders`).
//! - `stdio` is the local subprocess flavour (desktop MCP clients). Skips
//!   Postgres poll + REST: the DB is expected to already be populated by
//!   another running `cardo-service` or by CI data.
//! - `none` is the pre-M3C legacy — keeps backwards compat for anyone
//!   still running the M3B image without reading `CARDO_MCP_MODE`.
//!
//! # Security invariant
//!
//! The `RomeEVMClient` this binary constructs is **read-only**. See
//! [`build_rome_evm_client`] for the full story — short version: an ephemeral
//! keypair is generated at process start purely to satisfy the client's
//! emulator plumbing (which asks for a payer pubkey when estimating gas
//! against the in-process Rust emulator). That key is never persisted, never
//! sent to Solana, and never signs any user transaction. `/execute` (REST)
//! and the `execute` MCP tool both return unsigned tx material; signing is
//! always client-side.

use anyhow::{Context, Result};
use cardo_service::{
    config::Config,
    manifest::{Ingester, SchemaValidator, Verifier},
    mcp,
    rest::router::{build_mcp_only_router, build_router, build_router_with_mcp, AppState, AppStateInner},
    store::PostgresStore,
    tx_builder::TxBuilder,
};
use rome_sdk::{
    rome_evm_client::{resources::ResourceType, Payer, RomeEVMClient},
    rome_solana::{
        config::SolanaConfig, indexers::clock::SolanaClockIndexer, tower::SolanaTower,
    },
};
use solana_sdk::{pubkey::Pubkey, signature::Keypair};
use std::{path::PathBuf, str::FromStr, sync::Arc, time::Duration};

/// Serving mode selected at process start via `CARDO_MCP_MODE`.
///
/// Parsed lazily in `main`; the `stdio` variant short-circuits before we even
/// build the REST state, so stdio deployments don't need `CARDO_BIND_ADDR`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ServingMode {
    /// REST + MCP HTTP merged on the same listener, poll loop in background.
    Both,
    /// MCP HTTP only — no REST endpoints. Still runs the poll loop.
    Http,
    /// MCP over stdin/stdout. Skips REST + poll loop.
    Stdio,
    /// REST only, no MCP (pre-M3C behaviour). Still runs the poll loop.
    None,
}

impl ServingMode {
    fn from_env() -> Self {
        match std::env::var("CARDO_MCP_MODE")
            .unwrap_or_else(|_| "both".to_string())
            .to_ascii_lowercase()
            .as_str()
        {
            "both" | "" => Self::Both,
            "http" => Self::Http,
            "stdio" => Self::Stdio,
            "none" => Self::None,
            other => {
                tracing::warn!(
                    value = %other,
                    "unknown CARDO_MCP_MODE; falling back to `both`"
                );
                Self::Both
            }
        }
    }

    /// Whether the REST layer should be mounted.
    fn rest_on(self) -> bool {
        matches!(self, Self::Both | Self::None)
    }

    /// Whether the MCP HTTP layer should be mounted on the Axum router.
    fn mcp_http_on(self) -> bool {
        matches!(self, Self::Both | Self::Http)
    }

    /// Whether the poll loop should run. Only off for stdio (where the
    /// binary is a short-lived subprocess client and the DB is populated
    /// elsewhere).
    fn poll_loop_on(self) -> bool {
        !matches!(self, Self::Stdio)
    }

    /// Whether the binary should connect MCP to stdin/stdout.
    fn stdio_on(self) -> bool {
        matches!(self, Self::Stdio)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    tracing::info!("cardo-service starting");

    let mode = ServingMode::from_env();
    tracing::info!(?mode, "MCP serving mode selected");

    let cfg = Config::from_env()?;
    tracing::info!(?cfg, "config loaded");

    // --- Postgres (same as M3A) ------------------------------------------------
    let store = PostgresStore::connect(&cfg.postgres_url).await?;
    store.migrate().await?;
    tracing::info!("postgres connected + migrated");

    // --- Manifest schema + verifier (same as M3A) ------------------------------
    let schema_path = std::env::var("CARDO_SCHEMA_PATH")
        .unwrap_or_else(|_| "/etc/cardo/catalog.schema.json".to_string());
    let schema = SchemaValidator::from_json_str(
        &std::fs::read_to_string(&schema_path)
            .with_context(|| format!("reading schema at {}", schema_path))?,
    )?;
    let keys_dir = std::env::var("CARDO_KEYS_DIR")
        .unwrap_or_else(|_| "/etc/cardo/keys".to_string());
    let verifier = Verifier::from_keys_dir(&PathBuf::from(&keys_dir))?;
    let ingester = Arc::new(Ingester::new(cfg.cdn_url.clone(), schema, verifier));

    // --- Rome EVM client + TxBuilder (M3B) -------------------------------------
    let rome_evm_rpc_url = std::env::var("ROME_EVM_RPC_URL")
        .context("ROME_EVM_RPC_URL not set (e.g. https://<chain>.<env>.romeprotocol.xyz/)")?;
    let chain_id: u64 = std::env::var("ROME_CHAIN_ID")
        .context("ROME_CHAIN_ID not set (numeric EVM chain id of the target Rome chain)")?
        .parse()
        .context("ROME_CHAIN_ID must be a u64")?;
    let program_id_str = std::env::var("ROME_PROGRAM_ID")
        .context("ROME_PROGRAM_ID not set (Solana pubkey of the rome-evm program)")?;
    let program_id = Pubkey::from_str(&program_id_str)
        .with_context(|| format!("parsing ROME_PROGRAM_ID={}", program_id_str))?;

    let rome_evm_client =
        Arc::new(build_rome_evm_client(&rome_evm_rpc_url, chain_id, program_id).await?);
    let tx_builder = TxBuilder::new(rome_evm_client, chain_id);
    tracing::info!(chain_id, program_id = %program_id, "rome-evm-client + tx_builder ready");

    let state: AppState = Arc::new(AppStateInner {
        pool: store.pool().clone(),
        tx_builder,
        cdn_url: cfg.cdn_url.clone(),
    });

    let mcp_handler = Arc::new(mcp::McpHandler::new(state.clone()));

    // --- stdio short-circuit: no REST, no poll loop ----------------------------
    if mode.stdio_on() {
        tracing::info!("serving MCP over stdio; REST + poll loop disabled");
        mcp::transport::stdio::serve(mcp_handler).await?;
        return Ok(());
    }

    // --- Background poll loop --------------------------------------------------
    // Spawn onto a dedicated task so the REST/MCP server can accept traffic
    // while ingest runs on its own cadence. Uses an independent Postgres
    // connection so the REST pool is unaffected by the loop's workload.
    if mode.poll_loop_on() {
        let ingester_bg = ingester.clone();
        let store_bg = PostgresStore::connect(&cfg.postgres_url).await?;
        let poll_interval = cfg.poll_interval_secs;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(poll_interval));
            loop {
                ticker.tick().await;
                match ingester_bg.fetch_all().await {
                    Ok(items) => {
                        let count = items.len();
                        for item in &items {
                            if let Err(e) =
                                store_bg.upsert_manifest(&item.manifest, &item.raw).await
                            {
                                tracing::error!(id = %item.manifest.id, error = ?e, "upsert failed");
                            }
                        }
                        tracing::info!(count, "ingest cycle ok");
                    }
                    Err(e) => tracing::error!(error = ?e, "ingest cycle failed"),
                }
            }
        });
    } else {
        tracing::info!("poll loop disabled by CARDO_MCP_MODE");
    }

    // --- HTTP server (REST, MCP, or merged) -----------------------------------
    let bind_addr = std::env::var("CARDO_BIND_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    let listener = tokio::net::TcpListener::bind(&bind_addr)
        .await
        .with_context(|| format!("bind {}", bind_addr))?;

    // DNS-rebinding guard: rmcp's default is loopback only, which is wrong for
    // a cloud service behind a public hostname. `CARDO_MCP_ALLOWED_HOSTS` is
    // a comma-separated list; default covers loopback so local dev "just works".
    let allowed_hosts: Vec<String> = std::env::var("CARDO_MCP_ALLOWED_HOSTS")
        .ok()
        .map(|raw| {
            raw.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_else(|| {
            vec![
                "localhost".into(),
                "127.0.0.1".into(),
                "::1".into(),
                "0.0.0.0".into(),
            ]
        });

    let router = match (mode.rest_on(), mode.mcp_http_on()) {
        (true, true) => {
            tracing::info!(%bind_addr, "serving REST + MCP streamable HTTP");
            build_router_with_mcp(state, mcp_handler, allowed_hosts)
        }
        (true, false) => {
            tracing::info!(%bind_addr, "serving REST only");
            build_router(state)
        }
        (false, true) => {
            tracing::info!(%bind_addr, "serving MCP streamable HTTP only");
            build_mcp_only_router(mcp_handler, allowed_hosts)
        }
        (false, false) => {
            // Defensive: stdio mode returned early above, so this can only
            // happen if someone adds a new mode. Serve nothing and log.
            unreachable!("serving mode with neither REST nor MCP HTTP")
        }
    };

    axum::serve(listener, router).await?;
    Ok(())
}

/// Construct a read-only [`RomeEVMClient`] for use by the explorer's
/// `/quote` + `/execute` endpoints.
///
/// # Why we still need a keypair even though we never sign
///
/// `RomeEVMClient::estimate_gas` internally asks a `ResourceFactory` for a
/// `Payer`, then reads **only** that payer's public key (`payer().pubkey()`)
/// and hands it to the in-process Rust emulator as the "sender" when
/// estimating gas. No transaction is ever submitted to Solana from this
/// binary — the `/execute` endpoint returns unsigned tx material; the user
/// signs client-side.
///
/// Therefore we generate a fresh ephemeral [`Keypair`] at startup. It lives
/// only in this process's memory, is never written to disk, never shipped to
/// Solana, and has no funds. Its sole role is to provide a pubkey for the
/// emulator's signer slot. If a future change to `rome-evm-client` removes
/// that requirement, this keypair can be deleted.
///
/// # `ethereum_block_storage = None`
///
/// We pass `None` because the REST endpoints this binary serves never call
/// `block_number()` / `fee_history()` / `get_block()` — only `call`,
/// `estimate_gas`, `transaction_count`, and `gas_price`, none of which
/// consult block storage.
async fn build_rome_evm_client(
    rpc_url: &str,
    chain_id: u64,
    program_id: Pubkey,
) -> Result<RomeEVMClient> {
    use rome_sdk::rome_solana::solana_rpc_client::SolanaRpcClient;

    // Build a `SolanaConfig` from the URL (same default `Confirmed` commitment
    // proxy uses) and convert it into a non-blocking RPC client — matches the
    // pattern in `rome-apps/proxy/src/config.rs::ProxyConfig::init`.
    let parsed_url = url::Url::parse(rpc_url)
        .with_context(|| format!("parsing ROME_EVM_RPC_URL={}", rpc_url))?;
    let solana_cfg = SolanaConfig {
        rpc_url: parsed_url,
        commitment: solana_commitment_config::CommitmentLevel::Confirmed,
        ..Default::default()
    };
    let rpc_client: Arc<dyn SolanaRpcClient> = Arc::new(solana_cfg.into_async_client());

    let clock_indexer = SolanaClockIndexer::new(rpc_client.clone())
        .await
        .map_err(|e| anyhow::anyhow!("SolanaClockIndexer::new failed: {:?}", e))?;
    let tower = SolanaTower::new(rpc_client, clock_indexer.get_current_clock());

    // SECURITY: ephemeral keypair for emulator-side pubkey plumbing only.
    // See the function-level doc comment for the full rationale.
    let ephemeral_keypair = Arc::new(Keypair::new());
    let ephemeral_payer = Payer {
        payer_keypair: ephemeral_keypair,
        // `Holders(1)` is the simplest valid `ResourceType`. The factory only
        // needs a populated resource; we never actually consume a holder
        // because we never submit a Solana tx.
        resource_type: ResourceType::Holders(1),
    };

    let client = RomeEVMClient::new(
        chain_id,
        program_id,
        tower,
        /* ethereum_block_storage */ None,
        /* payers */ vec![ephemeral_payer],
        /* gas_price_mul */ 1.0,
        /* price_manager */ None,
        None,
    )
    .map_err(|e| anyhow::anyhow!("RomeEVMClient::new failed: {:?}", e))?;

    Ok(client)
}

#[cfg(test)]
mod tests {
    //! Unit-level coverage of `ServingMode::from_env` + the `rest_on` /
    //! `mcp_http_on` / `stdio_on` predicates. Running `main` itself needs a
    //! live Postgres + RPC, so we only test the mode routing here.
    //!
    //! Tests touch a process-wide env var (`CARDO_MCP_MODE`), which isn't
    //! safe when cargo runs tests in parallel. We serialize through a shared
    //! `Mutex` so only one test at a time manipulates the variable. `std::sync::Mutex`
    //! is fine because we never panic while holding it (tests just do
    //! comparison asserts after releasing).
    use super::ServingMode;
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn with_env<T>(key: &str, val: Option<&str>, f: impl FnOnce() -> T) -> T {
        let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        let old = std::env::var(key).ok();
        match val {
            Some(v) => std::env::set_var(key, v),
            None => std::env::remove_var(key),
        }
        let out = f();
        match old {
            Some(v) => std::env::set_var(key, v),
            None => std::env::remove_var(key),
        }
        out
    }

    #[test]
    fn defaults_to_both_when_unset() {
        let mode = with_env("CARDO_MCP_MODE", None, ServingMode::from_env);
        assert_eq!(mode, ServingMode::Both);
        assert!(mode.rest_on());
        assert!(mode.mcp_http_on());
        assert!(mode.poll_loop_on());
        assert!(!mode.stdio_on());
    }

    #[test]
    fn stdio_disables_rest_and_poll_loop() {
        let mode = with_env("CARDO_MCP_MODE", Some("stdio"), ServingMode::from_env);
        assert_eq!(mode, ServingMode::Stdio);
        assert!(!mode.rest_on());
        assert!(!mode.mcp_http_on());
        assert!(!mode.poll_loop_on());
        assert!(mode.stdio_on());
    }

    #[test]
    fn http_mode_drops_rest_keeps_poll_loop() {
        let mode = with_env("CARDO_MCP_MODE", Some("http"), ServingMode::from_env);
        assert_eq!(mode, ServingMode::Http);
        assert!(!mode.rest_on());
        assert!(mode.mcp_http_on());
        assert!(mode.poll_loop_on());
    }

    #[test]
    fn none_mode_is_rest_only_legacy() {
        let mode = with_env("CARDO_MCP_MODE", Some("none"), ServingMode::from_env);
        assert_eq!(mode, ServingMode::None);
        assert!(mode.rest_on());
        assert!(!mode.mcp_http_on());
        assert!(mode.poll_loop_on());
    }

    #[test]
    fn unknown_value_falls_back_to_both() {
        let mode = with_env("CARDO_MCP_MODE", Some("quacking"), ServingMode::from_env);
        assert_eq!(mode, ServingMode::Both);
    }

    #[test]
    fn parsing_is_case_insensitive() {
        let mode = with_env("CARDO_MCP_MODE", Some("STDIO"), ServingMode::from_env);
        assert_eq!(mode, ServingMode::Stdio);
    }
}
