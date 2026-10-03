//! The resolve→ingest bridge (P3c a.3/a.4): turns N tokens' resolved graphs
//! into the persisted `capture_manifest`/`asset_event` artifacts AND the
//! single merged `(address -> SourceKind)` map the ingest pipeline consumes.
//!
//! **Gap-detection unit (P3c verify-item #2 — read before touching a.6):**
//! [`SourceInterval::from_block`]/[`ResolvedGraph`] intervals are **EVM
//! block numbers** — every `from_block` in this crate comes from either a
//! real `eth_getLogs` response's `log.block_number` ([`super::live_rpc`]) or
//! `audit.chain_event.block_number` (itself `receipt_params.block_number`,
//! see `ingest::hercules_reads::TxReceiptInfo`). The ingest worker's OWN
//! watermark (`audit.ingest_watermark.verified_through_slot`) is a
//! **Solana slot number** — a DIFFERENT axis (one EVM block ≠ one Solana
//! slot outside the opt-in `single_state_slot_aligned` mode — slot-aligned
//! block numbering is opt-in, not the default). Comparing
//! `from_block <= verified_through_slot` directly would compare two
//! different units. [`run_resolve_pass`]'s `current_watermark` parameter is
//! therefore documented and used as an **EVM-block-number watermark**.
//!
//! **Where that watermark actually comes from (P3c H1 fix).** An earlier
//! version of this module computed it as `MAX(audit.chain_event.block_number)`
//! — wrong, because `audit.chain_event` only records blocks that had a
//! MATCHED log under whatever source map was active at the time; it
//! under-reports how far ingest has actually SCANNED, so a chain-wide-visible
//! new source registered between the last captured event and the true scan
//! frontier is silently missed. The correct frontier is Hercules-derived:
//! `run::AuditWorker::tick` (which owns BOTH pools) reads
//! `audit.ingest_watermark.verified_through_slot` from `target`, then calls
//! `ingest::block_frontier_through_slot(source, that_slot)` against `source`
//! (Hercules) to get the real EVM-block-number frontier, and passes THAT
//! into `run_resolve_pass`. No watermark row / fresh chain ⇒ `-1`, which is
//! also exactly the "fresh DB ⇒ nothing fires" fixed point (`from_block <=
//! -1` never holds for any realistic, non-negative `from_block`).

use std::collections::{BTreeMap, BTreeSet};

use sqlx::PgPool;

use crate::ingest::{IngestFilter, SourceSpec};
use crate::registry::AbiRegistry;
use crate::types::SourceKind;

use super::asset_event::{rebuild_asset_event_tx, AssetManifest};
use super::discovery::DiscoveryError;
use super::graph::{ResolvedGraph, ScopeFilter};
use super::manifest::CaptureManifest;
use super::registry_source::RegistrySource;
use super::resolver::{resolve, resolve_pinned_sanctions_floor, ResolveError};
use super::rpc::ResolverRpc;
use super::store::{
    insert_backfill_gap, insert_capture_manifest, previously_known_ingest_scope,
    upsert_ingest_scope,
};

/// The output of one resolve pass: the merged ingest source map (a.4 — each
/// entry now a [`SourceSpec`], P4a §"Layer 2") plus every asset's
/// freshly-persisted manifest (so the caller — the ingest worker — can
/// log/trace which manifest backs each asset without a separate read).
#[derive(Debug, Clone)]
pub struct ResolvedSet {
    pub sources: BTreeMap<String, SourceSpec>,
    pub assets: Vec<AssetManifest>,
}

#[derive(Debug, thiserror::Error)]
pub enum PassError {
    #[error("resolve(0x{token}) failed: {source}")]
    Resolve {
        token: String,
        #[source]
        source: ResolveError,
    },
    #[error(transparent)]
    Db(#[from] sqlx::Error),
    /// P3c M2 — the caller (`run::AuditWorker::tick`) failed to compute the
    /// EVM-block-number frontier `run_resolve_pass` needs (a Hercules read
    /// via `ingest::block_frontier_through_slot`, or the `audit.ingest_watermark`
    /// read that feeds it) BEFORE ever calling `run_resolve_pass` — the pass
    /// itself never ran, nothing was persisted, and the failure must
    /// propagate (never be swallowed into the "fresh chain" `-1` sentinel,
    /// which would falsely mark every address previously-known and
    /// permanently suppress gap detection).
    #[error("failed to compute the resolve-pass frontier: {0}")]
    Frontier(#[from] anyhow::Error),
    /// S4 — a discovery-enumeration or discovery-filtering failure (a
    /// `logs_for`/RPC error, or a malformed admission-event arg) that
    /// occurred BEFORE `resolve()` ever ran for the union'd token set.
    /// Same "never swallowed" discipline as `Frontier`: the caller
    /// (`run::AuditWorker::tick`) surfaces this on `TickOutcome`, never
    /// only via a log line.
    #[error("S4 token discovery failed: {0}")]
    Discovery(#[from] DiscoveryError),
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Resolves every token in `tokens`, persists the resulting manifests +
/// rebuilds `asset_event`, records any backfill gap a newly-discovered
/// source's history opens up, and returns the merged ingest source map.
///
/// **LOAD-BEARING ordering (a.3), all-or-nothing:** every token is resolved
/// FIRST, collecting every graph, BEFORE any database write — a single
/// token's resolution failure aborts the whole pass with NOTHING persisted
/// (never a partial manifest set for the tokens that happened to resolve
/// first).
///
/// `current_watermark` is an **EVM-block-number** watermark — see the
/// module doc's unit note. Callers pass `-1` on a fresh chain, or the
/// result of `ingest::block_frontier_through_slot` otherwise (`run::AuditWorker::tick`
/// is the real caller and computes this correctly — see the module doc).
pub async fn run_resolve_pass(
    target: &PgPool,
    chain_id: i64,
    tokens: &[[u8; 20]],
    registry: &dyn RegistrySource,
    rpc: &dyn ResolverRpc,
    abi: &AbiRegistry,
    current_watermark: i64,
) -> Result<ResolvedSet, PassError> {
    // (1) Resolve EVERY token before any write.
    let mut graphs: Vec<ResolvedGraph> = Vec::with_capacity(tokens.len());
    for &token in tokens {
        let graph = resolve(token, registry, rpc, abi)
            .await
            .map_err(|source| PassError::Resolve {
                token: format!("0x{}", hex::encode(token)),
                source,
            })?;
        graphs.push(graph);
    }

    // Pinned sanctions floor (additive, chain-global). Resolved like any
    // token graph — pushed into the SAME `graphs` vec, BEFORE
    // graph_refs/merge_ingest_sources and before the write transaction opens
    // — so it flows through manifest + asset_event + merge + gap-detection
    // completely unchanged: its module's history backfills (a real graph
    // interval with `from_block == 0` is what gives `detect_backfill_gaps`
    // something to anchor a gap row on), and its future events are captured
    // live. Absent config ⇒ `registry.global_sanctions_router()` is `None`
    // ⇒ this block never runs ⇒ byte-identical to before this feature.
    if let Some(router) = registry.global_sanctions_router() {
        let from_block = registry.global_sanctions_router_from_block().unwrap_or(0);
        let pinned = resolve_pinned_sanctions_floor(router, from_block, rpc, abi)
            .await
            .map_err(|source| PassError::Resolve {
                token: format!("0x{}", hex::encode(router)),
                source,
            })?;
        graphs.push(pinned);
    }

    // The POST-retain EFFECTIVE ingest map this pass produces (P4a §3) —
    // moved EARLY (was computed only after commit, pre-P4a) because
    // gap-detection re-keying and the `ingest_scope` upsert below both need
    // it, and it's pure (no DB) so computing it before the transaction opens
    // costs nothing. See `merge_ingest_sources`'s doc for the merge/filter-
    // union rules and the module doc above C1's comment for why
    // descriptor-less kinds are retained OUT of this map.
    let graph_refs: Vec<&ResolvedGraph> = graphs.iter().collect();
    let mut sources = merge_ingest_sources(&graph_refs);
    sources.retain(|_, spec| abi.has_events_for(spec.kind));

    // The pre-this-pass (address, filter_fingerprint) universe (P4a §3,
    // superseding a.6's manifest-known-address check — see
    // `detect_backfill_gaps`'s doc) — read BEFORE this pass's own
    // `ingest_scope` rows land. A plain pool read (no concurrent resolve
    // pass for the same chain exists — one `AuditWorker` per chain), so it
    // doesn't need to share the write transaction below.
    let previously_known_scope = previously_known_ingest_scope(target, chain_id).await?;

    // (2)-(6) run as ONE transaction (P3c H2): a failure partway through
    // (e.g. after some manifests land but before the asset_event rebuild, a
    // gap insert, or the ingest_scope upsert) must leave NOTHING persisted —
    // otherwise the NEXT pass's `previously_known_ingest_scope` read would
    // already include this pass's half-landed rows and permanently suppress
    // their gap detection.
    let mut tx = target.begin().await?;

    let registry_commit_sha = registry.commit_sha();
    let generated_at = now_millis();
    let mut assets: Vec<AssetManifest> = Vec::with_capacity(graphs.len());
    for graph in &graphs {
        let asset_id = format!("{chain_id}:0x{}", hex::encode(graph.token));
        let manifest =
            CaptureManifest::from_graph(asset_id.clone(), registry_commit_sha.clone(), graph);
        let manifest_hash =
            insert_capture_manifest(&mut *tx, chain_id, &manifest, generated_at).await?;
        assets.push(AssetManifest {
            asset_id,
            manifest_hash,
            graph: graph.clone(),
        });
    }

    // (4) Rebuild asset_event with the FULL asset list — never partial (it
    // deletes and recomputes this chain's rows only, P3c M3).
    rebuild_asset_event_tx(&mut tx, chain_id, &assets).await?;

    // (5) Gap detection (P4a §3 re-key — see `detect_backfill_gaps`'s doc).
    // Note this now iterates `sources` (the POST-retain effective ingest
    // map, computed above) rather than every graph interval — a
    // descriptor-less kind (never in `sources`, e.g. `Morpho`'s
    // non-`CreateMarket` surface) is correctly never a gap candidate:
    // nothing will ever backfill it either.
    detect_backfill_gaps(
        &mut tx,
        chain_id,
        &assets,
        &sources,
        &previously_known_scope,
        current_watermark,
        generated_at,
    )
    .await?;

    // (6) `audit.ingest_scope` upsert — records exactly what THIS pass made
    // ingestable, so the NEXT pass's `previously_known_ingest_scope` read
    // reflects it. Inside the same transaction as (2)-(5): a rollback must
    // never leave a phantom fingerprint that would permanently suppress a
    // real gap.
    for (key, spec) in &sources {
        let Some(address) = parse_address_key(key) else {
            // `sources`' keys are always produced by `merge_ingest_sources`
            // as `format!("0x{}", hex::encode(source.address))` — a
            // malformed key here would be an authoring bug, not a runtime
            // condition; skip rather than panic in a daemon, but this
            // should never actually trigger.
            tracing::error!(chain_id, key = %key, "ingest_scope upsert: malformed address key — skipped");
            continue;
        };
        upsert_ingest_scope(
            &mut *tx,
            chain_id,
            address,
            spec.kind.as_db_str(),
            &spec.ingest_filter.fingerprint(),
            generated_at,
        )
        .await?;
    }

    tx.commit().await?;

    Ok(ResolvedSet { sources, assets })
}

/// Parses a `merge_ingest_sources`-produced map key (`"0x" + 40 lowercase
/// hex chars`) back into raw bytes.
fn parse_address_key(key: &str) -> Option<[u8; 20]> {
    let bytes = hex::decode(key.trim_start_matches("0x")).ok()?;
    bytes.try_into().ok()
}

fn source_kind_for_address(graph: &ResolvedGraph, address: [u8; 20]) -> Option<SourceKind> {
    graph
        .sources
        .iter()
        .find(|s| s.address == address)
        .map(|s| s.source_kind)
}

/// **P4a §3 re-key (the silent-gap fix).** The PRE-P4a version of this
/// function fired only for an address NEVER seen in ANY prior manifest
/// (`previously_known_addresses` — every graph source ever recorded,
/// including descriptor-less ones C1's `retain` drops from the live ingest
/// map). That silently missed two real cases once P4a registered real
/// descriptors for previously descriptor-less kinds:
///
/// 1. A kind that was manifest-known-but-descriptor-less in every earlier
///    pass and only becomes INGESTABLE this pass (its address was already
///    "previously known" from those earlier manifests, so the old check
///    would wrongly treat it as not-new and skip it — even though NOTHING
///    ever actually ingested its pre-existing history).
/// 2. Filter-WIDENING: a 2nd asset onboarding a chain-wide token already
///    known under a NARROWER `IngestFilter` broadens the union'd filter —
///    the address itself isn't new, but the broader capture set's earlier
///    history (specifically, the newly-added party's own transfers before
///    this pass) was never captured under the old, narrower filter.
///
/// The fix: iterate `sources` (P4a — the POST-retain EFFECTIVE ingest map
/// this pass computed, not every graph interval) and key "was this already
/// ingestable" on the exact `(address, filter_fingerprint)` pair against
/// `audit.ingest_scope`, rather than on the address alone against
/// `capture_manifest`. A pair that was NEVER in `ingest_scope` before AND
/// whose earliest interval starts AT OR before `current_watermark` (EVM-
/// block-number units — see module doc; P3c M1: `from_block ==
/// current_watermark` means that block was ALREADY scanned under the old
/// map, so `<=`, not `<`) records a `audit.backfill_gap` row: ingest has
/// already passed that point without ever knowing to capture this
/// (address, filter) combination, so events between `from_block` and the
/// watermark are a genuine, honestly-recorded gap. A descriptor-less kind
/// (never in `sources` at all) is correctly never a candidate here —
/// nothing would ever backfill it either.
async fn detect_backfill_gaps(
    tx: &mut sqlx::PgConnection,
    chain_id: i64,
    assets: &[AssetManifest],
    sources: &BTreeMap<String, SourceSpec>,
    previously_known_scope: &BTreeSet<(String, String)>,
    current_watermark: i64,
    detected_at: i64,
) -> Result<(), sqlx::Error> {
    // The earliest interval this PASS resolved for each address, across
    // every asset — a source's history may be attributed to more than one
    // asset's graph; the earliest `from_block` is what the gap row anchors
    // on (deliberately the MIN across every contributing asset, not just
    // the newest one — see the module doc's case 2: a widened filter's gap
    // must cover the FULL union'd history, not just the newly-added
    // party's slice, so a backfill re-scan from the true earliest bound
    // never misses anything; already-captured events in that range are
    // simply idempotent no-ops on re-ingest).
    let mut earliest: BTreeMap<[u8; 20], (i64, [u8; 32])> = BTreeMap::new();
    for asset in assets {
        for interval in &asset.graph.intervals {
            if source_kind_for_address(&asset.graph, interval.address).is_none() {
                continue; // shouldn't happen — every interval's address has a matching source
            }
            earliest
                .entry(interval.address)
                .and_modify(|(from_block, manifest_hash)| {
                    if interval.from_block < *from_block {
                        *from_block = interval.from_block;
                        *manifest_hash = asset.manifest_hash;
                    }
                })
                .or_insert((interval.from_block, asset.manifest_hash));
        }
    }

    for (key, spec) in sources {
        let fingerprint = spec.ingest_filter.fingerprint();
        if previously_known_scope.contains(&(key.clone(), fingerprint.clone())) {
            continue; // this EXACT (address, filter) pair was already ingestable before this pass
        }
        let Some(address) = parse_address_key(key) else {
            continue; // malformed key — an authoring bug elsewhere, never a runtime panic here
        };
        let Some(&(from_block, manifest_hash)) = earliest.get(&address) else {
            continue; // shouldn't happen — every `sources` entry comes from some graph's interval
        };
        if from_block <= current_watermark {
            insert_backfill_gap(
                &mut *tx,
                chain_id,
                address,
                spec.kind.as_db_str(),
                from_block,
                current_watermark,
                manifest_hash,
                detected_at,
            )
            .await?;
            tracing::error!(
                chain_id,
                address = %key,
                source_kind = spec.kind.as_db_str(),
                filter_fingerprint = %fingerprint,
                from_block,
                current_watermark,
                "P4a: newly-ingestable (address, filter) pair's history starts BEHIND the ingest \
                 watermark — recorded a backfill gap (audit.backfill_gap); events between \
                 from_block and the watermark were never captured for this source"
            );
        }
    }

    Ok(())
}

/// Fixed precedence when the SAME address resolves under a different
/// `SourceKind` across two tokens' graphs (a.4) — richer registered event
/// surface wins. Lower number = higher precedence. Order-independent by
/// construction: [`merge_ingest_sources`] always compares ranks, never
/// "whichever graph came first".
///
/// **INVARIANT (MED-1, delta re-review) — every DESCRIPTOR-FUL kind
/// (has a registered ABI descriptor, `AbiRegistry::has_events_for` is true)
/// must rank strictly above every DESCRIPTOR-LESS kind.** C1 (`run_resolve_pass`)
/// filters the merged map down to descriptor-ful kinds only — if a
/// descriptor-less kind ever won a merge conflict against a descriptor-ful
/// one, that address would be silently dropped from the live ingest map
/// entirely (a regression from the pre-C1 behavior, which at least
/// quarantined the collision loudly). Ranks 0-7 are descriptor-ful; 8-10
/// are descriptor-less. `every_descriptor_ful_kind_outranks_every_descriptor_less_kind`
/// (below) guards this — a future kind added at the wrong rank fails there.
fn source_kind_precedence(kind: SourceKind) -> u8 {
    match kind {
        SourceKind::ArcToken => 0,
        SourceKind::Storefront => 1,
        SourceKind::Factory => 2,
        SourceKind::Router => 3,
        SourceKind::Morpho => 4,
        SourceKind::Uv2Pair => 5,
        SourceKind::Axis1Module => 6,
        SourceKind::GlobalSanctions => 7,
        // ---- descriptor-less below this line (P4 capture, not yet ingestable) ----
        SourceKind::YieldBlacklist => 8,
        SourceKind::YieldToken => 9,
        SourceKind::PurchaseToken => 10,
    }
}

/// The ingest-time filter this graph contributes for `address` (P4a §"Layer
/// 2") — folded (unioned) across EVERY one of this graph's OWN intervals
/// that covers that address, never just the first match.
///
/// **P4a CRITICAL-1 fix:** a single graph can carry TWO SEPARATE intervals
/// for the SAME address under DIFFERENT `ScopeFilter`s — the default Bloom
/// shape hits this for real: one asset's own chain-wide token (wUSDC) is
/// BOTH its `YieldToken` leg (`ScopeFilter::TransferFrom{from: token}`,
/// pushed by the yield-token-history walk) AND its `PurchaseToken` leg
/// (`ScopeFilter::TransferTouches{party: storefront}`, pushed by the
/// purchase-token-history walk) — `resolver::resolve` pushes these as two
/// independent `SourceInterval` entries (intervals carry no `SourceKind` tag
/// to distinguish them by). The OLD `.find()` (first match only) silently
/// dropped whichever leg's interval came second in the Vec — buyer→storefront
/// payments on that wUSDC would never even reach the cross-graph merge
/// logic in [`merge_ingest_sources`] to be unioned there, because this
/// function had already thrown the second filter away. `Morpho`'s
/// `MorphoMarkets` filter is deliberately NOT translated to an
/// `IngestFilter` (Morpho is join-time-scoped only, never ingest-filtered —
/// see `IngestFilter`'s doc); a `MorphoMarkets` interval unions in as
/// `IngestFilter::None`, which `union()` absorbs into whatever real filter(s)
/// exist for the same address, never overriding them.
fn ingest_filter_for(graph: &ResolvedGraph, address: [u8; 20]) -> IngestFilter {
    // P4a NEW-CRITICAL fix: `IngestFilter::None` is now UNION-ABSORBING
    // (pass-all wins — see `IngestFilter::union`'s doc), so it can no
    // longer double as this fold's "nothing contributed yet" SEED — a
    // `.fold(IngestFilter::None, union)` would now absorb every REAL
    // filter into `None` unconditionally, silently WIDENING every address
    // to pass-all regardless of its actual intervals. The seed lives in
    // `Option<IngestFilter>` space instead (`None`-the-Option = "nothing
    // seen yet", entirely distinct from `IngestFilter::None` = "a real
    // pass-all interval") — only `union()` ever sees two REAL filters. An
    // address with NO matching interval at all (every real
    // resolver-produced source pairs one via `push_source`/
    // `fold_walked_sources`; a synthetic test fixture that doesn't is not
    // a safety violation either way) falls through to the `unwrap_or`
    // below — pass-all, never a silent drop.
    graph
        .intervals
        .iter()
        .filter(|i| i.address == address)
        .map(|i| match i.scope_filter.as_ref() {
            Some(ScopeFilter::TransferFrom { from }) => {
                IngestFilter::TransferFrom(std::collections::BTreeSet::from([*from]))
            }
            Some(ScopeFilter::TransferTouches { party }) => {
                IngestFilter::TransferTouches(std::collections::BTreeSet::from([*party]))
            }
            Some(ScopeFilter::MorphoMarkets { .. }) | None => IngestFilter::None,
        })
        .fold(None::<IngestFilter>, |acc, f| match acc {
            None => Some(f),
            Some(existing) => Some(existing.union(f)),
        })
        // No matching interval at all: pass-all is the safe fallback —
        // worst case is MORE capture (Layer 1 still slices each asset
        // correctly), never a silent drop.
        .unwrap_or(IngestFilter::None)
}

/// Unions every graph's `(address -> SourceSpec)` ingest map (pure,
/// unit-testable). When two graphs disagree on the SAME address's
/// `SourceKind` (a real scenario: one asset's own token IS another asset's
/// yield/purchase-token leg), [`source_kind_precedence`] decides —
/// deterministically, regardless of which graph is merged first — and a
/// `warn!` names both kinds so the conflict is visible, never silently
/// resolved. When two graphs AGREE on the kind (e.g. two assets sharing the
/// SAME chain-wide yield token), their `IngestFilter`s are UNIONED (P4a §3)
/// rather than either one silently winning — otherwise a second asset
/// sharing the source would have its own transfers dropped at ingest.
pub fn merge_ingest_sources(graphs: &[&ResolvedGraph]) -> BTreeMap<String, SourceSpec> {
    let mut merged: BTreeMap<String, SourceSpec> = BTreeMap::new();

    for graph in graphs {
        for source in &graph.sources {
            let key = format!("0x{}", hex::encode(source.address));
            let filter = ingest_filter_for(graph, source.address);
            match merged.remove(&key) {
                None => {
                    merged.insert(
                        key,
                        SourceSpec {
                            kind: source.source_kind,
                            ingest_filter: filter,
                        },
                    );
                }
                Some(existing) if existing.kind == source.source_kind => {
                    // Agreement on kind — union the filters (P4a §3), never
                    // just keep the first.
                    merged.insert(
                        key,
                        SourceSpec {
                            kind: existing.kind,
                            ingest_filter: existing.ingest_filter.union(filter),
                        },
                    );
                }
                Some(existing) => {
                    // P4a CRITICAL-1 fix: precedence decides ONLY which
                    // `SourceKind` to DECODE this address under (Transfer
                    // decodes byte-identically regardless of which kind
                    // wins — see `source_kind_precedence`'s doc) — it must
                    // NEVER decide which filter(s) survive. The default
                    // Bloom shape hits this for real: one asset's own
                    // chain-wide token is BOTH its `YieldToken` leg
                    // (`TransferFrom`) AND (via a DIFFERENT asset sharing
                    // the SAME token as ITS purchase token) a
                    // `PurchaseToken` leg (`TransferTouches`) — dropping
                    // either leg's filter here would silently un-capture
                    // that leg's real events at Layer 2, no error, no
                    // quarantine, no gap. `union()` never panics (P4a
                    // CRITICAL-1) — it folds incompatible variants into
                    // `IngestFilter::Any`, which is the CORRECT semantics,
                    // not an error condition.
                    let winner_kind = if source_kind_precedence(source.source_kind)
                        < source_kind_precedence(existing.kind)
                    {
                        source.source_kind
                    } else {
                        existing.kind
                    };
                    tracing::warn!(
                        address = %key,
                        existing = ?existing.kind,
                        incoming = ?source.source_kind,
                        winner = ?winner_kind,
                        "merge_ingest_sources: source-kind conflict — decode kind picked by precedence, \
                         BOTH kinds' filters kept (unioned)"
                    );
                    merged.insert(
                        key,
                        SourceSpec {
                            kind: winner_kind,
                            ingest_filter: existing.ingest_filter.union(filter),
                        },
                    );
                }
            }
        }
    }

    merged
}

#[cfg(test)]
mod ingest_filter_for_tests {
    //! P4a NEW-CRITICAL fix — the union-matrix rows that live at THIS
    //! layer (the pure per-graph fold), complementing
    //! `ingest::pipeline::ingest_filter_union_semantics_tests`'s
    //! variant-vs-variant matrix.

    use super::*;
    use crate::resolve::graph::SourceInterval;

    fn addr(b: u8) -> [u8; 20] {
        [b; 20]
    }

    fn graph_with_intervals(intervals: Vec<SourceInterval>) -> ResolvedGraph {
        ResolvedGraph {
            token: addr(0x01),
            sources: vec![],
            intervals,
        }
    }

    fn interval(address: [u8; 20], scope_filter: Option<ScopeFilter>) -> SourceInterval {
        SourceInterval {
            address,
            from_block: 0,
            to_block: None,
            scope_filter,
        }
    }

    #[test]
    fn single_transfer_from_interval_yields_that_filter_unchanged() {
        let a = addr(0xA0);
        let target = addr(0xE0);
        let graph = graph_with_intervals(vec![interval(target, Some(ScopeFilter::TransferFrom { from: a }))]);
        assert_eq!(
            ingest_filter_for(&graph, target),
            IngestFilter::TransferFrom(std::collections::BTreeSet::from([a]))
        );
    }

    #[test]
    fn transfer_from_and_transfer_touches_intervals_union_into_any() {
        let a = addr(0xA0);
        let c = addr(0xC0);
        let target = addr(0xE0);
        let graph = graph_with_intervals(vec![
            interval(target, Some(ScopeFilter::TransferFrom { from: a })),
            interval(target, Some(ScopeFilter::TransferTouches { party: c })),
        ]);
        match ingest_filter_for(&graph, target) {
            IngestFilter::Any(members) => {
                assert_eq!(members.len(), 2, "got {members:?}");
            }
            other => panic!("expected Any, got {other:?}"),
        }
    }

    #[test]
    fn a_none_interval_alongside_a_real_filter_wins_as_pass_all() {
        // THE regression this whole fix closes: an asset's OWN token
        // (None-scoped, pass-all) sharing an address with a filtered leg
        // (e.g. another asset's YieldToken leg) must end up PASS-ALL, not
        // silently narrowed to the filtered leg's own filter.
        let a = addr(0xA0);
        let target = addr(0xE0);
        let graph = graph_with_intervals(vec![
            interval(target, Some(ScopeFilter::TransferFrom { from: a })),
            interval(target, None),
        ]);
        assert_eq!(ingest_filter_for(&graph, target), IngestFilter::None);
    }

    #[test]
    fn a_single_none_interval_yields_none_pass_all() {
        let target = addr(0xE0);
        let graph = graph_with_intervals(vec![interval(target, None)]);
        assert_eq!(ingest_filter_for(&graph, target), IngestFilter::None);
    }

    #[test]
    fn no_matching_interval_falls_back_to_none_pass_all() {
        let target = addr(0xE0);
        let graph = graph_with_intervals(vec![interval(addr(0xFF), None)]); // different address
        assert_eq!(ingest_filter_for(&graph, target), IngestFilter::None);
    }
}

#[cfg(test)]
mod merge_tests {
    use super::*;
    use crate::resolve::graph::{ResolvedFrom, ResolvedSource, SourceInterval};

    fn addr(b: u8) -> [u8; 20] {
        [b; 20]
    }

    fn graph_with(token: [u8; 20], sources: Vec<(SourceKind, [u8; 20])>) -> ResolvedGraph {
        ResolvedGraph {
            token,
            sources: sources
                .into_iter()
                .map(|(source_kind, address)| ResolvedSource {
                    source_kind,
                    address,
                    resolved_from: ResolvedFrom::TokenRead,
                    abi_ref: "test",
                })
                .collect(),
            intervals: vec![SourceInterval {
                address: token,
                from_block: 0,
                to_block: None,
                scope_filter: None,
            }],
        }
    }

    #[test]
    fn merge_unions_disjoint_graphs() {
        let token_a = addr(0xA0);
        let token_b = addr(0xB0);
        let router_a = addr(0xA1);
        let router_b = addr(0xB1);

        let graph_a = graph_with(
            token_a,
            vec![
                (SourceKind::ArcToken, token_a),
                (SourceKind::Router, router_a),
            ],
        );
        let graph_b = graph_with(
            token_b,
            vec![
                (SourceKind::ArcToken, token_b),
                (SourceKind::Router, router_b),
            ],
        );

        let merged = merge_ingest_sources(&[&graph_a, &graph_b]);

        assert_eq!(merged.len(), 4, "every distinct address must appear once");
        let key = |a: [u8; 20]| format!("0x{}", hex::encode(a));
        assert_eq!(merged.get(&key(token_a)).map(|s| s.kind), Some(SourceKind::ArcToken));
        assert_eq!(merged.get(&key(token_b)).map(|s| s.kind), Some(SourceKind::ArcToken));
        assert_eq!(merged.get(&key(router_a)).map(|s| s.kind), Some(SourceKind::Router));
        assert_eq!(merged.get(&key(router_b)).map(|s| s.kind), Some(SourceKind::Router));
    }

    #[test]
    fn merge_conflict_arc_token_beats_yield_token() {
        // The SAME address resolved as ArcToken in one graph and YieldToken
        // in another (a real scenario: token X is itself an asset AND
        // another asset's yield-distribution leg) — ArcToken's richer
        // registered event surface must win, REGARDLESS of graph order.
        let shared_addr = addr(0x77);
        let token_a = addr(0xA0);
        let token_b = addr(0xB0);

        let graph_arc_first = graph_with(token_a, vec![(SourceKind::ArcToken, shared_addr)]);
        let graph_yield_first = graph_with(token_b, vec![(SourceKind::YieldToken, shared_addr)]);

        let key = format!("0x{}", hex::encode(shared_addr));

        let merged_arc_then_yield =
            merge_ingest_sources(&[&graph_arc_first, &graph_yield_first]);
        assert_eq!(
            merged_arc_then_yield.get(&key).map(|s| s.kind),
            Some(SourceKind::ArcToken),
            "ArcToken must win when it's merged FIRST"
        );

        let merged_yield_then_arc =
            merge_ingest_sources(&[&graph_yield_first, &graph_arc_first]);
        assert_eq!(
            merged_yield_then_arc.get(&key).map(|s| s.kind),
            Some(SourceKind::ArcToken),
            "ArcToken must win when it's merged SECOND too — order-independent by construction"
        );
    }

    #[test]
    fn merge_conflict_global_sanctions_beats_yield_blacklist() {
        // MED-1 (delta re-review): a dual-role address — resolved as
        // GlobalSanctions in one graph, YieldBlacklist in another (plausible
        // under BYO-sanctions: a token's yield-blacklist slot pointing at
        // the chain's own GlobalSanctions contract). Both kinds are
        // resolved, but only GlobalSanctions has a registered descriptor —
        // if YieldBlacklist won the merge, C1's `retain` would drop the
        // address ENTIRELY, silently un-ingesting real Sanctioned/
        // Unsanctioned events (a regression from the pre-C1 behavior, which
        // at least quarantined the collision loudly).
        let shared_addr = addr(0x88);
        let token_a = addr(0xA0);
        let token_b = addr(0xB0);

        let graph_sanctions_first =
            graph_with(token_a, vec![(SourceKind::GlobalSanctions, shared_addr)]);
        let graph_blacklist_first =
            graph_with(token_b, vec![(SourceKind::YieldBlacklist, shared_addr)]);

        let key = format!("0x{}", hex::encode(shared_addr));

        let merged_sanctions_then_blacklist =
            merge_ingest_sources(&[&graph_sanctions_first, &graph_blacklist_first]);
        assert_eq!(
            merged_sanctions_then_blacklist.get(&key).map(|s| s.kind),
            Some(SourceKind::GlobalSanctions),
            "GlobalSanctions must win when it's merged FIRST"
        );

        let merged_blacklist_then_sanctions =
            merge_ingest_sources(&[&graph_blacklist_first, &graph_sanctions_first]);
        assert_eq!(
            merged_blacklist_then_sanctions.get(&key).map(|s| s.kind),
            Some(SourceKind::GlobalSanctions),
            "GlobalSanctions must win when it's merged SECOND too — order-independent by construction"
        );
    }

    /// P4a: two DIFFERENT assets sharing the SAME chain-wide yield token —
    /// the merge must UNION both assets' `TransferFrom` filters, never let
    /// one silently overwrite the other (which would drop the second
    /// asset's own distributions from the live ingest map's effective
    /// filter).
    #[test]
    fn merge_unions_ingest_filters_for_two_assets_sharing_the_same_yield_token() {
        let yield_token = addr(0xE0);
        let arc_token_a = addr(0xA0);
        let arc_token_b = addr(0xB0);

        let graph_a = ResolvedGraph {
            token: arc_token_a,
            sources: vec![ResolvedSource {
                source_kind: SourceKind::YieldToken,
                address: yield_token,
                resolved_from: ResolvedFrom::TokenEvent,
                abi_ref: "test",
            }],
            intervals: vec![SourceInterval {
                address: yield_token,
                from_block: 0,
                to_block: None,
                scope_filter: Some(super::super::graph::ScopeFilter::TransferFrom {
                    from: arc_token_a,
                }),
            }],
        };
        let graph_b = ResolvedGraph {
            token: arc_token_b,
            sources: vec![ResolvedSource {
                source_kind: SourceKind::YieldToken,
                address: yield_token,
                resolved_from: ResolvedFrom::TokenEvent,
                abi_ref: "test",
            }],
            intervals: vec![SourceInterval {
                address: yield_token,
                from_block: 0,
                to_block: None,
                scope_filter: Some(super::super::graph::ScopeFilter::TransferFrom {
                    from: arc_token_b,
                }),
            }],
        };

        let merged = merge_ingest_sources(&[&graph_a, &graph_b]);
        let key = format!("0x{}", hex::encode(yield_token));
        let spec = merged.get(&key).expect("yield_token must be in the map");
        assert_eq!(spec.kind, SourceKind::YieldToken);
        match &spec.ingest_filter {
            crate::ingest::IngestFilter::TransferFrom(set) => {
                assert!(set.contains(&arc_token_a), "asset A's from-party must survive the union");
                assert!(set.contains(&arc_token_b), "asset B's from-party must survive the union");
            }
            other => panic!("expected a unioned TransferFrom filter, got {other:?}"),
        }
    }

    #[test]
    fn every_descriptor_ful_kind_outranks_every_descriptor_less_kind() {
        // The GENERAL invariant MED-1 establishes (not just the one pair):
        // `merge_ingest_sources` + C1's `retain(has_events_for)` compose
        // safely ONLY IF every kind with a real ABI registration outranks
        // every kind without one — otherwise a dual-role collision can pick
        // the descriptor-less kind and get silently dropped downstream.
        // Also closes the earlier nit: this is the assertion that every
        // "should-ingest" kind (has a descriptor) is actually verified
        // against the REAL crate-wide registry, not just the two named
        // descriptor-less kinds `registry::tests` checked.
        let registry = crate::abi::build_registry();
        let all_kinds = [
            SourceKind::ArcToken,
            SourceKind::Axis1Module,
            SourceKind::YieldBlacklist,
            SourceKind::Router,
            SourceKind::GlobalSanctions,
            SourceKind::Storefront,
            SourceKind::Factory,
            SourceKind::Uv2Pair,
            SourceKind::Morpho,
            SourceKind::YieldToken,
            SourceKind::PurchaseToken,
        ];

        let (descriptor_ful, descriptor_less): (Vec<_>, Vec<_>) = all_kinds
            .into_iter()
            .partition(|k| registry.has_events_for(*k));

        assert!(!descriptor_ful.is_empty());
        // P4a CLOSED THE GAP this test originally guarded against: as of
        // P4a, YieldToken/PurchaseToken/YieldBlacklist all gained real ABI
        // registrations (Transfer-under-each-kind for the first two,
        // YieldBlacklistUpdated for the third), so every kind in the crate
        // is legitimately descriptor-ful today — `descriptor_less` is
        // EMPTY, not a bug. Restated as a CONDITIONAL rather than deleted
        // (P4a review): the invariant below still holds — vacuously,
        // since there is nothing on the descriptor-less side left to
        // violate it — and a FUTURE descriptor-less kind re-populates this
        // loop with real coverage automatically, rather than this test
        // having quietly disappeared.
        for &ful in &descriptor_ful {
            for &less in &descriptor_less {
                assert!(
                    source_kind_precedence(ful) < source_kind_precedence(less),
                    "{ful:?} (descriptor-ful, rank {}) must outrank {less:?} \
                     (descriptor-less, rank {}) — a dual-role address resolving as \
                     BOTH must never let the descriptor-less kind win the merge and then \
                     get silently dropped by C1's retain",
                    source_kind_precedence(ful),
                    source_kind_precedence(less)
                );
            }
        }
    }
}
