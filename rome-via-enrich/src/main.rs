use anyhow::anyhow;
use clap::Parser;
use rome_obs::Otel;
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;
use tokio::signal::unix::{signal, SignalKind};
use tokio_util::sync::CancellationToken;

use rome_via_enrich::cli::Cli;
use rome_via_enrich::config::ViaEnrichConfig;
use rome_via_enrich::server::start_health_server;
use rome_via_enrich::supervisor::supervise;
use rome_via_enrich::workers::{address_stats, batch_trace, contract_creation, contract_labels, cross_chain, cross_vm_seams, factory_tokens, gate_events, holders, hook_executions, hook_metadata, hooks_registry, meta_hook_indexer, metadata, method_decoder, search_indexer, throughput_record, verified_labels};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    Otel::init_from_env("rome-via-enrich").map_err(|e| anyhow!(e.to_string()))?;

    let cli = Cli::parse();
    let config_path = cli.get_config_path()?;
    let cfg = ViaEnrichConfig::load(&config_path).await?;

    // ── One-shot maintenance subcommand ────────────────────────────────────────
    //
    // `rome-via-enrich maintenance <op>` runs a bounded offline op then exits.
    // It must short-circuit here — BEFORE migrations, the health server, or any
    // worker spawn below — so it never behaves like (or interferes with) the
    // daemon. It opens its own small pool rather than reusing the daemon's,
    // since it needs no supervision and exits as soon as the op completes. The
    // no-subcommand path (`cli.command == None`) falls through unchanged.
    if let Some(rome_via_enrich::cli::Command::Maintenance { op }) = &cli.command {
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .acquire_timeout(Duration::from_secs(10))
            .connect(&cfg.db_url)
            .await
            .map_err(|e| anyhow!("failed to connect to DB: {e}"))?;
        match op {
            rome_via_enrich::cli::MaintenanceOp::ReextractTransfers { apply } => {
                rome_via_enrich::maintenance::reextract_transfers(
                    &pool,
                    cfg.chain_id as i64,
                    *apply,
                )
                .await?
            }
            rome_via_enrich::cli::MaintenanceOp::BackfillBalances {
                apply,
                token,
                concurrency,
            } => {
                rome_via_enrich::maintenance::backfill_balances(
                    &pool,
                    cfg.chain_id as i64,
                    &cfg.proxy_url,
                    *apply,
                    token.clone(),
                    *concurrency,
                )
                .await?
            }
            rome_via_enrich::cli::MaintenanceOp::BackfillOracleFlag { apply } => {
                rome_via_enrich::maintenance::backfill_oracle_flag(
                    &pool,
                    cfg.chain_id as i64,
                    *apply,
                )
                .await?
            }
        }
        return Ok(());
    }

    tracing::info!(
        chain_id = cfg.chain_id,
        health_addr = %cfg.health_addr,
        "starting rome-via-enrich"
    );

    // ── Database pool ──────────────────────────────────────────────────────────
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&cfg.db_url)
        .await
        .map_err(|e| anyhow!("failed to connect to DB: {e}"))?;

    // Run migrations (enrich-specific tables on top of rome_via schema from sync).
    // ignore_missing=true because the shared _sqlx_migrations table tracks
    // rome-via-sync's versions (1..=10) which aren't present in enrich's
    // migrations folder. Same rationale as sync-side.
    let mut migrator = sqlx::migrate!("./migrations");
    migrator.set_ignore_missing(true);
    migrator
        .run(&pool)
        .await
        .map_err(|e| anyhow!("migration failed: {e}"))?;

    tracing::info!("enrich migrations applied");

    // ── Cancellation token ─────────────────────────────────────────────────────
    let cancel = CancellationToken::new();

    // ── Health server ──────────────────────────────────────────────────────────
    let health_addr_str = cfg.health_addr.to_string();
    let (addr, shutdown_tx) = start_health_server(&health_addr_str).await?;
    tracing::info!(?addr, "rome-via-enrich health endpoint listening");

    let chain_id = cfg.chain_id as i64;
    let poll_interval = Duration::from_secs(cfg.poll_interval_seconds);
    let batch_size = cfg.batch_size;

    // ── Seed static method signatures ─────────────────────────────────────────
    method_decoder::seed_static(&pool).await?;

    // ── Spawn supervised worker tasks ─────────────────────────────────────────
    //
    // Each worker is wrapped by `supervise(name, factory)` so a transient
    // failure (DB blip, RPC timeout, panic) triggers an exponential-backoff
    // restart (1s → 60s, reset after a 60s healthy run) instead of silently
    // halting the enrichment job. See `supervisor.rs` for the policy.
    let handles = vec![
        {
            let pool = pool.clone();
            let fourbyte = cfg.fourbyte_enabled;
            supervise("method_decoder", move || {
                let pool = pool.clone();
                async move {
                    method_decoder::run(pool, chain_id, fourbyte, poll_interval, batch_size).await
                }
            })
        },
        {
            let pool = pool.clone();
            supervise("holders", move || {
                let pool = pool.clone();
                async move { holders::run(pool, chain_id, poll_interval, batch_size).await }
            })
        },
        {
            // F7 — factory-token discovery. Sibling to holders: tails the same
            // evm_tx_result.tx_result logs but matches the ERC20SPLFactory
            // `TokenCreated` topic0 and inserts the new wrapper address (from
            // topics[3]) into token_metadata with kind NULL, so the metadata
            // worker enriches name/symbol/decimals/kind. Catches zero-Transfer
            // factory tokens that Transfer-log-only discovery never sees.
            let pool = pool.clone();
            supervise("factory_tokens", move || {
                let pool = pool.clone();
                async move {
                    factory_tokens::run(pool, chain_id, poll_interval, batch_size).await
                }
            })
        },
        {
            // Gate-events — freshness + history for gated (Arc-style) tokens.
            // Tails the same evm_tx_result logs for SpecificRestrictionModuleSet
            // (TRANSFER_RESTRICTION) + TransfersRestrictionToggled, records them in
            // token_gate_events, and NULLs the affected token's `gated` so the
            // metadata worker re-derives it — fixing the metadata worker's
            // read-once staleness for gates wired/toggled after first enrichment.
            let pool = pool.clone();
            supervise("gate_events", move || {
                let pool = pool.clone();
                async move { gate_events::run(pool, chain_id, poll_interval, batch_size).await }
            })
        },
        {
            let pool = pool.clone();
            let proxy_url = cfg.proxy_url.clone();
            supervise("contract_creation", move || {
                let pool = pool.clone();
                let proxy_url = proxy_url.clone();
                async move {
                    contract_creation::run(pool, chain_id, proxy_url, poll_interval, batch_size)
                        .await
                }
            })
        },
        {
            let pool = pool.clone();
            let proxy_url = cfg.proxy_url.clone();
            let solana_rpc = cfg.solana_rpc_url.clone();
            supervise("metadata", move || {
                let pool = pool.clone();
                let proxy_url = proxy_url.clone();
                let solana_rpc = solana_rpc.clone();
                async move {
                    metadata::run(pool, chain_id, proxy_url, solana_rpc, poll_interval, batch_size)
                        .await
                }
            })
        },
        {
            let pool = pool.clone();
            let proxy_url = cfg.proxy_url.clone();
            let labels = cfg.contract_labels.clone();
            supervise("contract_labels", move || {
                let pool = pool.clone();
                let proxy_url = proxy_url.clone();
                let labels = labels.clone();
                async move {
                    contract_labels::run(
                        pool,
                        chain_id,
                        proxy_url,
                        labels,
                        poll_interval,
                        batch_size,
                    )
                    .await
                }
            })
        },
        {
            // Sourcify-verified label promotion (disabled unless
            // `verifier_url` is configured — see config.rs / verified_labels.rs).
            let pool = pool.clone();
            let verifier_url = cfg.verifier_url.clone();
            supervise("verified_labels", move || {
                let pool = pool.clone();
                let verifier_url = verifier_url.clone();
                async move {
                    verified_labels::run(pool, chain_id, verifier_url, poll_interval, batch_size)
                        .await
                }
            })
        },
        {
            let pool = pool.clone();
            supervise("address_stats", move || {
                let pool = pool.clone();
                async move {
                    address_stats::run(pool, chain_id, poll_interval, batch_size).await
                }
            })
        },
        {
            let pool = pool.clone();
            supervise("search_indexer", move || {
                let pool = pool.clone();
                async move {
                    search_indexer::run(pool, chain_id, poll_interval, batch_size).await
                }
            })
        },
        // Phase 4 workers
        {
            let pool = pool.clone();
            let solana_rpc = cfg.solana_rpc_url.clone();
            let solana_cluster = cfg.solana_cluster.clone();
            let rome_evm_id = cfg.rome_evm_program_id.clone();
            let mh_id = cfg.meta_hook_program_id.clone();
            let extra_infra = cfg.extra_infra_programs.clone();
            let program_labels: Vec<(String, String)> = cfg
                .program_labels
                .iter()
                .map(|p| (p.program_id.clone(), p.label.clone()))
                .collect();
            let cpi_plumbing_programs = cfg.cpi_plumbing_programs.clone();
            supervise("cross_chain", move || {
                let pool = pool.clone();
                let solana_rpc = solana_rpc.clone();
                let solana_cluster = solana_cluster.clone();
                let rome_evm_id = rome_evm_id.clone();
                let mh_id = mh_id.clone();
                let extra_infra = extra_infra.clone();
                let program_labels = program_labels.clone();
                let cpi_plumbing_programs = cpi_plumbing_programs.clone();
                async move {
                    cross_chain::run(
                        pool,
                        chain_id,
                        solana_rpc,
                        solana_cluster,
                        rome_evm_id,
                        mh_id,
                        extra_infra,
                        program_labels,
                        cpi_plumbing_programs,
                        poll_interval,
                        batch_size,
                    )
                    .await
                }
            })
        },
        {
            let pool = pool.clone();
            supervise("hooks_registry", move || {
                let pool = pool.clone();
                async move { hooks_registry::run(pool, chain_id).await }
            })
        },
        {
            let pool = pool.clone();
            let solana_rpc = cfg.solana_rpc_url.clone();
            supervise("batch_trace", move || {
                let pool = pool.clone();
                let solana_rpc = solana_rpc.clone();
                async move {
                    batch_trace::run(pool, chain_id, solana_rpc, poll_interval, batch_size).await
                }
            })
        },
        {
            let pool = pool.clone();
            let solana_rpc = cfg.solana_rpc_url.clone();
            let mh_id = cfg.meta_hook_program_id.clone();
            supervise("hook_executions", move || {
                let pool = pool.clone();
                let solana_rpc = solana_rpc.clone();
                let mh_id = mh_id.clone();
                async move {
                    hook_executions::run(
                        pool,
                        chain_id,
                        solana_rpc,
                        mh_id,
                        poll_interval,
                        batch_size,
                    )
                    .await
                }
            })
        },
        {
            let pool = pool.clone();
            let solana_rpc = cfg.solana_rpc_url.clone();
            let mh_id = cfg.meta_hook_program_id.clone();
            supervise("meta_hook_indexer", move || {
                let pool = pool.clone();
                let solana_rpc = solana_rpc.clone();
                let mh_id = mh_id.clone();
                async move {
                    meta_hook_indexer::run(
                        pool,
                        chain_id,
                        solana_rpc,
                        mh_id,
                        poll_interval,
                        batch_size,
                    )
                    .await
                }
            })
        },
        {
            let pool = pool.clone();
            let proxy_url = cfg.proxy_url.clone();
            supervise("hook_metadata", move || {
                let pool = pool.clone();
                let proxy_url = proxy_url.clone();
                async move {
                    hook_metadata::run(pool, chain_id, proxy_url, poll_interval, batch_size).await
                }
            })
        },
        {
            let pool = pool.clone();
            let poll = cfg.throughput_record_poll_interval();
            supervise("throughput_record", move || {
                let pool = pool.clone();
                async move {
                    throughput_record::run(pool, chain_id, poll).await
                }
            })
        },
        {
            let pool = pool.clone();
            let poll = cfg.cross_vm_seams_poll_interval();
            supervise("cross_vm_seams", move || {
                let pool = pool.clone();
                async move {
                    cross_vm_seams::run(pool, chain_id, poll, batch_size).await
                }
            })
        },
    ];

    // ── Signal handling ────────────────────────────────────────────────────────
    let mut sigterm = signal(SignalKind::terminate())
        .map_err(|e| anyhow!("failed to install SIGTERM handler: {e}"))?;
    let mut sigint = signal(SignalKind::interrupt())
        .map_err(|e| anyhow!("failed to install SIGINT handler: {e}"))?;

    tokio::select! {
        _ = sigterm.recv() => {
            tracing::info!("SIGTERM received, shutting down rome-via-enrich...");
        }
        _ = sigint.recv() => {
            tracing::info!("SIGINT received, shutting down rome-via-enrich...");
        }
    }

    // Signal cancellation and stop health server.
    cancel.cancel();
    let _ = shutdown_tx.send(());

    // Abort worker tasks (they poll in infinite loops — no graceful drain needed).
    for handle in handles {
        handle.abort();
    }

    Ok(())
}
