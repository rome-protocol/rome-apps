//! The polling loop — mirrors `rome-via-sync::sync::run`'s shape (two
//! pools + config + `CancellationToken`, looping on an interval), now
//! wrapping a testable [`AuditWorker`] (P3c a.5) instead of calling
//! `ingest::run_ingest_once` directly.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use crate::backfill::{run_backfill_once, BackfillOutcome};
use crate::config::AuditConfig;
use crate::ingest::{
    block_frontier_through_slot, run_ingest_once, IngestConfig, IngestOutcome, SourceSpec,
    WatermarkTracker,
};
use crate::registry::AbiRegistry;
use crate::resolve::{discover_authoritative_tokens, run_resolve_pass, PassError, RegistrySource, ResolvedSet, ResolverRpc};

/// How [`AuditWorker`] gets its `(address -> SourceSpec)` ingest map (a.5).
pub enum SourceMode {
    /// P1-style: a caller-supplied, never-changing map. Byte-for-byte the
    /// pre-P3c behavior — an empty map is valid (the worker idles, watermark
    /// still advances, nothing decodes).
    Static(BTreeMap<String, SourceSpec>),
    /// P3c: `resolve(token)` for every configured token, re-run
    /// periodically. Starts with an EMPTY map — ingest is held (never ticks
    /// `run_ingest_once`) until the first resolve pass succeeds.
    Resolved {
        registry: Arc<dyn RegistrySource>,
        rpc: Arc<dyn ResolverRpc>,
        tokens: Vec<[u8; 20]>,
        /// Re-resolution cadence. `Duration::ZERO` = boot-only (resolve
        /// once, never again).
        reresolve_interval: Duration,
        /// S4 — when `true`, every due resolve pass first enumerates the
        /// registry-pinned factory's admission events
        /// (`resolve::discover_authoritative_tokens`, gate-filtered) and
        /// unions the result into `tokens` before calling
        /// `run_resolve_pass`. `false` (byte-identical to pre-S4): `tokens`
        /// is used verbatim, discovery is never invoked.
        discovery_enabled: bool,
    },
}

/// One tick's outcome (a.5) — every case a test can assert on directly,
/// never by scraping log text.
#[derive(Debug)]
pub enum TickOutcome {
    /// Ingest ran this tick (whether or not any slot actually advanced).
    Ingested(IngestOutcome),
    /// `SourceMode::Resolved` and no resolve pass has EVER succeeded yet —
    /// `run_ingest_once` was skipped entirely this tick (a.5 step 2: never
    /// advance the watermark under an empty map when capture is intended).
    AwaitingFirstResolution,
    /// A re-resolution attempt (i.e. one occurring AFTER at least one prior
    /// success) failed this tick. The last-good source map is kept and
    /// ingest still ran on it — `ingest` carries that tick's real outcome,
    /// so a failed re-resolution never silently stalls capture.
    ReResolveFailed { error: PassError, ingest: IngestOutcome },
}

/// The EVM-block-number frontier `run_resolve_pass` needs (P3c H1) — reads
/// `target`'s persisted Solana-slot watermark, then queries `source`
/// (Hercules) for the real scan frontier through that slot. No watermark
/// row yet ⇒ `-1` (nothing scanned; the fresh-chain gap-detection fixed
/// point). Any DB failure along the way PROPAGATES (P3c M2) rather than
/// defaulting to `-1`, which would otherwise mark every address
/// previously-known and permanently suppress gap detection.
async fn compute_resolve_frontier(
    source: &PgPool,
    target: &PgPool,
    chain_id: i64,
) -> Result<i64, PassError> {
    let verified_slot = crate::ingest::pipeline::load_watermark(target, chain_id).await?;
    if verified_slot < 0 {
        return Ok(-1);
    }
    let frontier = block_frontier_through_slot(source, verified_slot).await?;
    Ok(frontier.unwrap_or(-1))
}

/// S4 — computes the resolve-pass frontier, optionally unions `tokens` with
/// every factory-admitted, gate-passed discovery candidate, then runs the
/// pass. `discovery_enabled = false` is BYTE-IDENTICAL to pre-S4:
/// `resolve::discover_authoritative_tokens` is never called, `tokens` is
/// passed through verbatim.
#[allow(clippy::too_many_arguments)]
async fn resolve_with_discovery(
    source: &PgPool,
    target: &PgPool,
    chain_id: i64,
    tokens: &[[u8; 20]],
    discovery_enabled: bool,
    registry: &dyn RegistrySource,
    rpc: &dyn ResolverRpc,
    abi: &AbiRegistry,
) -> Result<ResolvedSet, PassError> {
    let watermark = compute_resolve_frontier(source, target, chain_id).await?;

    let effective_tokens: Vec<[u8; 20]> = if discovery_enabled {
        let discovered = discover_authoritative_tokens(rpc, registry, abi, registry.factory()).await?;
        let mut set: std::collections::BTreeSet<[u8; 20]> = discovered.into_iter().collect();
        set.extend(tokens.iter().copied());
        set.into_iter().collect()
    } else {
        tokens.to_vec()
    };

    run_resolve_pass(target, chain_id, &effective_tokens, registry, rpc, abi, watermark).await
}

/// The testable unit `run()` wraps (a.5) — one `tick()` per wake, factored
/// out so DB-backed tests can drive it directly without a real
/// `CancellationToken`/sleep loop.
pub struct AuditWorker {
    source: PgPool,
    target: PgPool,
    registry_abi: AbiRegistry,
    ingest_config: IngestConfig,
    tracker: WatermarkTracker,
    mode: SourceMode,
    max_slots_per_tick: usize,
    resolved_once: bool,
    last_resolve: Option<Instant>,
}

impl AuditWorker {
    pub fn new(
        source: PgPool,
        target: PgPool,
        chain_id: i64,
        confirmation_lag: i64,
        max_slots_per_tick: usize,
        mode: SourceMode,
    ) -> Self {
        let sources = match &mode {
            SourceMode::Static(map) => map.clone(),
            SourceMode::Resolved { .. } => BTreeMap::new(),
        };
        Self {
            source,
            target,
            registry_abi: crate::build_registry(),
            ingest_config: IngestConfig {
                chain_id,
                confirmation_lag,
                sources,
            },
            tracker: WatermarkTracker::new(),
            mode,
            max_slots_per_tick,
            resolved_once: false,
            last_resolve: None,
        }
    }

    /// One wake's worth of work: the resolution gate (if in `Resolved`
    /// mode and due), then either the hold (never resolved yet) or a real
    /// ingest tick.
    pub async fn tick(&mut self) -> TickOutcome {
        // B1 — first-boot watermark init at the forward tip, BEFORE the
        // resolve gate, for BOTH SourceModes. Holding the WHOLE tick when the
        // source has no tip yet also defends B2: never run the resolve gate
        // against a not-yet-real frontier (`-1`), which would falsely mark
        // every source previously-known and permanently suppress gap
        // detection. See `ingest::pipeline::ensure_watermark_initialized`.
        match crate::ingest::pipeline::ensure_watermark_initialized(
            &self.source,
            &self.target,
            self.ingest_config.chain_id,
            self.ingest_config.confirmation_lag,
        )
        .await
        {
            Ok(true) => {}
            Ok(false) => {
                // Empty source — nothing to anchor the forward tip to yet.
                // Hold this whole tick (retry next tick); report an empty
                // ingest outcome (nothing captured, nothing resolved).
                return TickOutcome::Ingested(IngestOutcome::default());
            }
            Err(e) => {
                tracing::error!(?e, "audit watermark init failed — will retry; holding this tick");
                return TickOutcome::Ingested(IngestOutcome::default());
            }
        }

        let mut reresolution_failure: Option<PassError> = None;

        if let SourceMode::Resolved {
            registry,
            rpc,
            tokens,
            reresolve_interval,
            discovery_enabled,
        } = &self.mode
        {
            let due = !self.resolved_once
                || (!reresolve_interval.is_zero()
                    && self
                        .last_resolve
                        .is_none_or(|t| t.elapsed() >= *reresolve_interval));

            if due {
                // The EVM-block-number frontier (P3c H1) — computed from
                // BOTH pools this worker owns: `target`'s persisted
                // Solana-slot watermark, then `source` (Hercules) for the
                // real EVM-block-number scan frontier through that slot.
                // A failure here (M2) must propagate — never swallowed into
                // the "fresh chain" `-1` sentinel, which would falsely mark
                // every address previously-known and permanently suppress
                // gap detection.
                let resolve_result = resolve_with_discovery(
                    &self.source,
                    &self.target,
                    self.ingest_config.chain_id,
                    tokens,
                    *discovery_enabled,
                    registry.as_ref(),
                    rpc.as_ref(),
                    &self.registry_abi,
                )
                .await;

                match resolve_result {
                    Ok(resolved) => {
                        self.ingest_config.sources = resolved.sources;
                        self.resolved_once = true;
                        self.last_resolve = Some(Instant::now());
                    }
                    Err(e) => {
                        tracing::error!(?e, "P3c resolve pass failed — keeping last-good source map");
                        self.last_resolve = Some(Instant::now());
                        if self.resolved_once {
                            // A RE-resolution failure — last-good map keeps
                            // working; surfaced on the returned outcome
                            // below, never only via the log line above.
                            reresolution_failure = Some(e);
                        } else {
                            // Never resolved even once — fall through to
                            // the hold below.
                            return TickOutcome::AwaitingFirstResolution;
                        }
                    }
                }
            }
        }

        // Hold-until-first-resolution (a.5 step 2): never advance the
        // watermark under an empty map when capture is intended.
        if matches!(self.mode, SourceMode::Resolved { .. }) && !self.resolved_once {
            return TickOutcome::AwaitingFirstResolution;
        }

        let ingest_outcome = match run_ingest_once(
            &self.source,
            &self.target,
            &self.registry_abi,
            &mut self.tracker,
            &self.ingest_config,
            self.max_slots_per_tick,
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(e) => {
                // Propagate-don't-swallow (rome-apps convention): log
                // loudly and retry at the base cadence, exactly as the
                // pre-P3c loop did.
                tracing::error!(?e, "audit ingest tick failed — will retry");
                IngestOutcome::default()
            }
        };

        // P4a — one backfill-gap remediation per tick, AFTER live ingest has
        // already run above. Log-and-continue on error (same
        // propagate-don't-swallow-but-never-stall convention as the ingest
        // error arm just above): a backfill failure must never affect the
        // ingest outcome already computed this tick.
        match run_backfill_once(
            &self.source,
            &self.target,
            &self.registry_abi,
            &self.ingest_config,
            crate::backfill::DEFAULT_MAX_SLOTS_PER_BACKFILL_CALL,
        )
        .await
        {
            Ok(BackfillOutcome::Remediated {
                source_contract,
                from_block,
                events_inserted,
            }) => {
                tracing::info!(
                    chain_id = self.ingest_config.chain_id,
                    address = %format!("0x{}", hex::encode(source_contract)),
                    from_block,
                    events_inserted,
                    "P4a: backfill gap remediated"
                );
            }
            Ok(BackfillOutcome::Progressed {
                source_contract,
                from_block,
                resumed_through_slot,
                events_inserted,
            }) => {
                tracing::info!(
                    chain_id = self.ingest_config.chain_id,
                    address = %format!("0x{}", hex::encode(source_contract)),
                    from_block,
                    resumed_through_slot,
                    events_inserted,
                    "P4a: backfill gap chunk complete — resuming next call"
                );
            }
            Ok(BackfillOutcome::SourcePruned {
                source_contract,
                from_block,
                retained_floor,
            }) => {
                tracing::error!(
                    chain_id = self.ingest_config.chain_id,
                    address = %format!("0x{}", hex::encode(source_contract)),
                    from_block,
                    retained_floor,
                    "P4a/H1: backfill gap flagged source_pruned — its low end was pruned from the \
                     source and can never be captured (visibly unrecoverable, NOT falsely clean)"
                );
            }
            Ok(_) => {}
            Err(e) => {
                tracing::error!(?e, "P4a backfill tick failed — will retry; live ingest unaffected");
            }
        }

        match reresolution_failure {
            Some(error) => TickOutcome::ReResolveFailed {
                error,
                ingest: ingest_outcome,
            },
            None => TickOutcome::Ingested(ingest_outcome),
        }
    }
}

/// Run the polling loop until cancellation.
///
/// `mode` resolves `evm_log` addresses to `SourceKind` — either a static,
/// caller-supplied map (P1) or a live `resolve(token)` pipeline (P3c, see
/// [`SourceMode::Resolved`]).
pub async fn run(
    source: PgPool,
    target: PgPool,
    cfg: AuditConfig,
    mode: SourceMode,
    token: CancellationToken,
) -> anyhow::Result<()> {
    let mut worker = AuditWorker::new(
        source,
        target,
        cfg.chain_id as i64,
        cfg.confirmation_lag,
        cfg.max_slots_per_tick,
        mode,
    );
    let interval = Duration::from_millis(cfg.poll_interval_ms);
    let mut was_awaiting = false;

    loop {
        if token.is_cancelled() {
            return Ok(());
        }

        match worker.tick().await {
            TickOutcome::Ingested(outcome) | TickOutcome::ReResolveFailed { ingest: outcome, .. } => {
                was_awaiting = false;
                if !outcome.slots_passed.is_empty() {
                    tracing::info!(
                        chain_id = cfg.chain_id,
                        slots = outcome.slots_passed.len(),
                        events = outcome.events_inserted,
                        "audit ingest tick advanced"
                    );
                }
            }
            TickOutcome::AwaitingFirstResolution => {
                // P3c L2: log the transition ONCE, not once per poll (an
                // unresolved chain would otherwise log at the poll cadence
                // forever — every ~`poll_interval_ms`).
                if awaiting_transition_should_log(was_awaiting, true) {
                    tracing::info!(chain_id = cfg.chain_id, "awaiting first successful resolve pass");
                }
                was_awaiting = true;
            }
        }

        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = token.cancelled() => return Ok(()),
        }
    }
}

/// Whether the "awaiting first resolve pass" transition should be logged
/// this tick — only on the false→true edge. Pure so it's unit-testable
/// without driving the real polling loop.
fn awaiting_transition_should_log(was_awaiting: bool, is_awaiting: bool) -> bool {
    is_awaiting && !was_awaiting
}

#[cfg(test)]
mod run_tests {
    use super::*;

    #[test]
    fn awaiting_transition_logs_only_on_the_false_to_true_edge() {
        assert!(awaiting_transition_should_log(false, true), "first entry must log");
        assert!(
            !awaiting_transition_should_log(true, true),
            "staying in the awaiting state must NOT log again"
        );
        assert!(
            !awaiting_transition_should_log(false, false),
            "never entering the awaiting state must not log"
        );
        assert!(
            !awaiting_transition_should_log(true, false),
            "leaving the awaiting state is handled by the Ingested/ReResolveFailed arm, not this fn"
        );
    }
}
