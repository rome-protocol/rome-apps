//! The P1 ingest pipeline: watermark evaluation → indexed `evm_log` read →
//! P0 decode → append-only write into `audit.chain_event` → watermark
//! advance. IMPL-PLAN §3 P1 / §5.4.
//!
//! Two plain `PgPool`s, exactly like `rome-via-sync`'s `source`/`target`
//! (`rome-via-sync/src/sync.rs::run`) — `source` = Hercules (read-only
//! usage here), `target` = the database the `audit` schema lives in. No
//! bespoke access abstraction: reads go straight through
//! `super::hercules_reads`'s plain functions against `source`.
//!
//! Source resolution (which addresses map to which `SourceKind`) is a
//! CALLER input (`IngestConfig::sources`) — P1 does not hardcode any chain's
//! addresses; that resolution work lands in a later phase (capture §1.2).
//!
//! **Throughput (review, C1 fix):** a candidate slot's FIRST
//! observation always returns `Wait` from `WatermarkTracker::evaluate`
//! (digest-stability needs two reads) — so a naive "evaluate one candidate,
//! stop at the first non-Pass" loop can only ever advance ONE slot per
//! tick, regardless of `max_slots_per_tick`. The fix: evaluate the WHOLE
//! candidate window every tick (recording first-seen for all of it), then
//! separately walk the CONTIGUOUS Pass-prefix to decide how far to actually
//! advance — §5.2's "never skip a non-Pass slot" guarantee is unchanged
//! (the advance walk still stops dead at the first non-Pass), but a chain
//! that's been stable across two ticks now advances its whole window at
//! once instead of one slot at a time. `finalized_tip`/`max_produced_slot`
//! are also hoisted to ONCE PER TICK (they're chain-global, not per-slot).
//!
//! **Errors (review, H2 fix):** mirrors the repo's `rpc_verdict`
//! Transient/Terminal doctrine (`rome-via-enrich/src/workers/rpc_verdict.rs`).
//! Transient (a real DB/transport error, or `MissingReceiptInfo` — no way to
//! prove from here whether a not-yet-visible receipt is a race or a
//! permanent gap, so it defaults to the safer "hold and retry, stall
//! visibly" side) propagates and HOLDS the whole tick's advance at that
//! slot — safe to retry indefinitely because every write is
//! `ON CONFLICT DO NOTHING`-idempotent (already-inserted rows from a
//! partially-completed slot are simply skipped on retry). Terminal
//! (`Decode`, `MissingSigner`, `MalformedHex` — deterministic content
//! problems that will NEVER resolve on retry) writes the offending log to
//! `audit.quarantine` + a loud `tracing::error!`, then the slot's advance
//! continues past it — never silently dropped (the quarantine row IS the
//! "saw it, couldn't process it" record) and never wedged forever.

use std::collections::{BTreeMap, BTreeSet};

use sqlx::PgPool;

use crate::registry::AbiRegistry;
use crate::types::{DecodedEvent, RawLog, SourceKind};

use super::hercules_reads::{self, MatchedLog, TxReceiptInfo};
use super::watermark::{WatermarkTracker, WatermarkVerdict};

/// P4a Layer 2 — the ingest-time half of `resolve::graph::ScopeFilter`
/// enforcement (Layer 1 is the `asset_event` join predicate). Applied
/// AFTER `matched_logs_at_slot`'s address+topic0 match, using the log's own
/// indexed topics (topic1=`from`, topic2=`to` for a `Transfer`-shaped
/// event) — never a DB round-trip. **Only `YieldToken`/`PurchaseToken` use
/// this** (chain-wide ERC20s where an unfiltered ingest would fetch every
/// holder's transfer, THE landmine — see `graph::ScopeFilter`'s doc).
/// `Morpho`/`Uv2Pair` are deliberately NOT ingest-filtered (bounded volume;
/// join-time scope only, so a late-discovered market/pair never creates an
/// ingest gap for events already captured chain-globally).
///
/// A `BTreeSet` (not a single address) because TWO assets can share the
/// SAME chain-wide token address under DIFFERENT filter parties (asset A's
/// ArcToken and asset B's ArcToken both distributing the same wUSDC as
/// yield) — [`crate::resolve::pass::merge_ingest_sources`] unions every
/// asset's contribution into one set per address, never picks just one.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum IngestFilter {
    /// No filter — every matched log for this address passes through.
    None,
    /// Keep only `Transfer` logs whose indexed `from` (topic1) is in this
    /// set (yield-token leg).
    TransferFrom(BTreeSet<[u8; 20]>),
    /// Keep only `Transfer` logs whose indexed `from` OR `to`
    /// (topic1/topic2) is in this set (purchase-token leg).
    TransferTouches(BTreeSet<[u8; 20]>),
    /// Passes if ANY member filter passes (P4a CRITICAL-1 fix). Built ONLY
    /// by [`IngestFilter::union`] when two INCOMPATIBLE variants meet for
    /// the same address — the default Bloom shape hits this for real: one
    /// asset's chain-wide token (e.g. wUSDC) can be BOTH its own
    /// `YieldToken` leg (`TransferFrom{arcToken}`) AND its `PurchaseToken`
    /// leg (`TransferTouches{storefront}`) — the two legs' filters must
    /// BOTH stay enforced, never one silently dropped by a decode-kind
    /// precedence pick (`resolve::pass::merge_ingest_sources`'s kind
    /// conflict only decides which `SourceKind` to DECODE under — Transfer
    /// decodes identically regardless — never which filter(s) to keep).
    /// `BTreeSet<IngestFilter>` (not `Vec`) for the same canonical,
    /// order-independent-fingerprint reason every other variant here uses
    /// a `BTreeSet`.
    Any(BTreeSet<IngestFilter>),
}

impl IngestFilter {
    /// Unions two filters for the SAME address (P4a §3: "UNION'd across
    /// assets sharing the source"; P4a CRITICAL-1: also across DIFFERENT
    /// filter-bearing `SourceKind`s sharing an address). **Never panics** —
    /// a resolve-worker panic kills the daemon; two incompatible variants
    /// fold into [`IngestFilter::Any`] instead of failing loud, since
    /// "passes if either leg would" is the CORRECT semantics here, not an
    /// error condition.
    ///
    /// **`None` is ABSORBING, not identity (P4a NEW-CRITICAL fix).** A real
    /// `None` here means an actual interval whose `scope_filter` is `None`
    /// — "capture EVERYTHING for this address" (an asset's own token, or a
    /// Morpho interval) — never "nothing contributed yet" (that's a
    /// SEPARATE concern `resolve::pass::ingest_filter_for`'s fold seed
    /// must handle via `Option<IngestFilter>`, never by passing a bare
    /// `IngestFilter::None` into `union` as a placeholder). Treating `None`
    /// as identity here let a genuinely pass-all interval (asset A's own
    /// token) get silently NARROWED down to whatever filter another
    /// asset's leg happened to contribute at the same address — the exact
    /// same silent-uncapture class CRITICAL-1 fixed, just with the
    /// pass-all side losing instead of a filtered side. Capturing MORE is
    /// always the safe direction here: Layer 1's per-asset join
    /// (`resolve::asset_event`) still slices each asset's own events
    /// correctly regardless of how wide Layer 2's capture is.
    pub fn union(self, other: IngestFilter) -> IngestFilter {
        match (self, other) {
            (IngestFilter::None, _) | (_, IngestFilter::None) => IngestFilter::None,
            (IngestFilter::TransferFrom(mut a), IngestFilter::TransferFrom(b)) => {
                a.extend(b);
                IngestFilter::TransferFrom(a)
            }
            (IngestFilter::TransferTouches(mut a), IngestFilter::TransferTouches(b)) => {
                a.extend(b);
                IngestFilter::TransferTouches(a)
            }
            (IngestFilter::Any(mut a), IngestFilter::Any(b)) => {
                a.extend(b);
                IngestFilter::Any(a)
            }
            (IngestFilter::Any(mut members), other) | (other, IngestFilter::Any(mut members)) => {
                members.insert(other);
                IngestFilter::Any(members)
            }
            (a, b) => IngestFilter::Any(BTreeSet::from([a, b])),
        }
    }

    /// Canonical, order-independent string form (P4a §3) — used as
    /// `audit.ingest_scope.filter_fingerprint`, the gap-detection re-key's
    /// other half. `BTreeSet` iteration is already sorted, so this is
    /// canonical by construction: unioning in either order produces the
    /// SAME fingerprint (see `ingest_filter_fingerprint_tests` below).
    pub fn fingerprint(&self) -> String {
        match self {
            IngestFilter::None => "none".to_string(),
            IngestFilter::TransferFrom(set) => {
                let parts: Vec<String> =
                    set.iter().map(|a| format!("0x{}", hex::encode(a))).collect();
                format!("transfer_from:{}", parts.join(","))
            }
            IngestFilter::TransferTouches(set) => {
                let parts: Vec<String> =
                    set.iter().map(|a| format!("0x{}", hex::encode(a))).collect();
                format!("transfer_touches:{}", parts.join(","))
            }
            IngestFilter::Any(members) => {
                let parts: Vec<String> = members.iter().map(IngestFilter::fingerprint).collect();
                format!("any:{}", parts.join("|"))
            }
        }
    }

    /// Whether `log` (a `Transfer`-shaped matched log) passes this filter.
    /// A missing/malformed topic1/topic2 fails closed (never passes) — a
    /// filtered kind's log that can't even be checked is safer dropped than
    /// silently admitted.
    fn passes(&self, log: &MatchedLog) -> bool {
        match self {
            IngestFilter::None => true,
            IngestFilter::TransferFrom(set) => log
                .topic1
                .as_deref()
                .and_then(topic_to_address)
                .is_some_and(|from| set.contains(&from)),
            IngestFilter::TransferTouches(set) => {
                let from_in = log
                    .topic1
                    .as_deref()
                    .and_then(topic_to_address)
                    .is_some_and(|a| set.contains(&a));
                let to_in = log
                    .topic2
                    .as_deref()
                    .and_then(topic_to_address)
                    .is_some_and(|a| set.contains(&a));
                from_in || to_in
            }
            IngestFilter::Any(members) => members.iter().any(|m| m.passes(log)),
        }
    }
}

fn topic_to_address(topic_hex: &str) -> Option<[u8; 20]> {
    let bytes = hex::decode(topic_hex.trim_start_matches("0x")).ok()?;
    if bytes.len() != 32 {
        return None;
    }
    bytes[12..32].try_into().ok()
}

/// One resolved ingest source: its `SourceKind` (which decoder to use) plus
/// its `IngestFilter` (P4a Layer 2 — `IngestFilter::None` for every kind
/// that isn't chain-wide-scoped).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceSpec {
    pub kind: SourceKind,
    pub ingest_filter: IngestFilter,
}

impl SourceSpec {
    pub fn new(kind: SourceKind) -> Self {
        Self {
            kind,
            ingest_filter: IngestFilter::None,
        }
    }
}

/// Caller-supplied ingest configuration. `sources` resolves an `evm_log`
/// address (lowercase 0x-hex, as Hercules stores it) to the [`SourceSpec`]
/// (kind + ingest filter) the P0 decoder / P4a filter need — this is
/// deliberately NOT hardcoded to any real chain's addresses here; tests
/// pass their own fixture addresses. In production, `run::AuditWorker`
/// populates this either from a static, caller-supplied map
/// (`SourceMode::Static`) or from the capture-spec resolver's live output
/// (`SourceMode::Resolved` — `resolve::run_resolve_pass`, P3c), swapping it
/// in between ticks; `run_ingest_once` itself never knows which.
pub struct IngestConfig {
    pub chain_id: i64,
    pub confirmation_lag: i64,
    pub sources: BTreeMap<String, SourceSpec>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct IngestOutcome {
    pub slots_passed: Vec<i64>,
    pub events_inserted: usize,
    /// How many candidate slots were actually EVALUATED this tick (B3) — the
    /// tip-clamp shrinks this to `tip - last_verified` near the head instead
    /// of a flat `max_slots_per_tick`, so a steady-state tick no longer
    /// probes ~`max_slots_per_tick` slots beyond the tip that can only ever
    /// return `Wait`. Purely observational (tests assert the clamp holds);
    /// the set of slots that actually PASS is unchanged.
    pub slots_evaluated: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error(transparent)]
    Source(#[from] anyhow::Error),
    #[error(transparent)]
    Db(#[from] sqlx::Error),
    #[error("decode failed for tx {tx_hash} log_ordinal {log_ordinal}: {source}")]
    Decode {
        tx_hash: String,
        log_ordinal: i32,
        #[source]
        source: crate::decode::DecodeError,
    },
    #[error("tx {tx_hash} at slot {slot} matched a log but has no receipt info yet — refusing to advance past it")]
    MissingReceiptInfo { slot: i64, tx_hash: String },
    #[error("tx {tx_hash} at slot {slot} has no signer (evm_tx.from_address is NULL) — refusing to write an incomplete row")]
    MissingSigner { slot: i64, tx_hash: String },
    #[error("malformed hex from Hercules for {field}: {value:?}")]
    MalformedHex { field: &'static str, value: String },
    #[error("evm_log address {address} at slot {slot} tx {tx_hash} isn't in the resolved sources map (matched_logs_at_slot should have filtered it)")]
    UnresolvedAddress {
        slot: i64,
        tx_hash: String,
        address: String,
    },
}

/// Transient/Terminal classification, mirroring `rpc_verdict`'s
/// `HoldState`/`ValveOutcome` doctrine: Transient ⇒ propagate (hold this
/// slot, retry next tick — safe because writes are idempotent); Terminal(reason)
/// ⇒ the caller quarantines the offending log(s) and keeps going.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ErrorClass {
    Transient,
    Terminal(&'static str),
}

impl IngestError {
    fn classify(&self) -> ErrorClass {
        match self {
            // Deterministic content problems — will NEVER succeed on retry.
            IngestError::Decode { .. } => ErrorClass::Terminal("undecodable log"),
            IngestError::MissingSigner { .. } => {
                ErrorClass::Terminal("missing signer (evm_tx.from_address NULL)")
            }
            IngestError::MalformedHex { .. } => ErrorClass::Terminal("malformed hex from Hercules"),
            IngestError::UnresolvedAddress { .. } => {
                ErrorClass::Terminal("address not in resolved sources map")
            }
            // No deterministic signal available here to prove "will never
            // resolve" vs. "hasn't caught up yet" — default to the safer
            // side (transient DB/transport errors go the same way).
            IngestError::MissingReceiptInfo { .. } => ErrorClass::Transient,
            IngestError::Db(_) | IngestError::Source(_) => ErrorClass::Transient,
        }
    }
}

/// Maps a `SourceKind` to the exact `source_kind` string the capture spec's
/// catalog uses (IMPL-PLAN §1.2 `chain-event.json#/properties/source_kind`)
/// — NOT the Rust `Debug` name, so `audit.chain_event.source_kind` matches
/// the spec's enum verbatim.
fn source_kind_db_name(kind: SourceKind) -> &'static str {
    match kind {
        SourceKind::ArcToken => "ARC_TOKEN",
        SourceKind::Axis1Module => "AXIS1_MODULE",
        SourceKind::YieldBlacklist => "YIELD_BLACKLIST",
        SourceKind::Router => "ROUTER",
        SourceKind::GlobalSanctions => "GLOBAL_SANCTIONS",
        SourceKind::Storefront => "STOREFRONT",
        SourceKind::Factory => "FACTORY",
        SourceKind::Uv2Pair => "UV2_PAIR",
        SourceKind::Morpho => "MORPHO",
        SourceKind::YieldToken => "YIELD_TOKEN",
        SourceKind::PurchaseToken => "PURCHASE_TOKEN",
    }
}

fn hex_to_bytes(field: &'static str, s: &str) -> Result<Vec<u8>, IngestError> {
    hex::decode(s.trim_start_matches("0x")).map_err(|_| IngestError::MalformedHex {
        field,
        value: s.to_string(),
    })
}

/// One ingest tick. Evaluates the WHOLE candidate window (never stops
/// early — see module doc's C1 fix), then advances the watermark through
/// the contiguous Pass-prefix, decoding + writing every matched log along
/// the way. Returns which slots passed and how many NEW rows landed
/// (idempotent re-runs report 0 new rows for slots already ingested).
///
/// `source` = Hercules; `target` = where `audit.chain_event` /
/// `audit.ingest_watermark` live (same two-pool shape as `rome-via-sync::sync::run`).
pub async fn run_ingest_once(
    source: &PgPool,
    target: &PgPool,
    registry: &AbiRegistry,
    tracker: &mut WatermarkTracker,
    config: &IngestConfig,
    max_slots_per_tick: usize,
) -> Result<IngestOutcome, IngestError> {
    let last_verified = load_watermark(target, config.chain_id).await?;
    let addresses: BTreeSet<String> = config.sources.keys().cloned().collect();
    let topic0_set: BTreeSet<String> = registry
        .all_topic0s()
        .iter()
        .map(|t| format!("0x{}", hex::encode(t)))
        .collect();

    // Hoisted ONCE per tick — chain-global reads (§5.2-i/ii), not per-slot.
    let finalized_tip = hercules_reads::finalized_tip(source).await?;
    let max_produced = hercules_reads::max_produced_slot(source).await?;

    // B3: clamp the candidate window to the tip. `last_verified +
    // max_slots_per_tick` alone probes ~`max_slots_per_tick` slots BEYOND
    // the tip every tick forever at steady state (each of those can only
    // return `Wait` — a slot past the finalized/produced tip is never
    // audit-final), i.e. ~`max_slots_per_tick` wasted per-slot point queries
    // against prod Hercules per tick. The tip is `min(finalized_tip,
    // max_produced)` — either being `None` (empty source) makes the window
    // empty this tick (`tip_cap = last_verified` ⇒ the range below is
    // `(last_verified+1)..=last_verified`, empty). Semantics are unchanged:
    // every slot that could PASS is `≤ tip - lag < tip = tip_cap`, so the
    // clamp only ever drops beyond-tip slots that would `Wait` anyway.
    let tip_cap = match (finalized_tip, max_produced) {
        (Some(f), Some(m)) => f.min(m),
        _ => last_verified,
    };
    let window_end = (last_verified + max_slots_per_tick as i64).min(tip_cap);

    // Evaluate EVERY candidate in the window — never stop at the first
    // non-Pass — so first-seen digests get recorded for the whole window.
    // This is what lets a later, stable tick advance many slots at once.
    let mut verdicts = Vec::with_capacity(max_slots_per_tick);
    for candidate in (last_verified + 1)..=window_end {
        let observation = hercules_reads::slot_observation(source, candidate).await?;
        let verdict = tracker.evaluate(
            candidate,
            observation.status,
            finalized_tip,
            max_produced,
            observation.digest,
            config.confirmation_lag,
        );
        verdicts.push((candidate, verdict));
    }

    // Advance through the CONTIGUOUS Pass-prefix only — stop at (never
    // skip past) the first non-Pass slot, so §5.2's no-gaps guarantee holds
    // no matter how wide the window is.
    let mut outcome = IngestOutcome {
        slots_evaluated: verdicts.len(),
        ..IngestOutcome::default()
    };
    for (candidate, verdict) in verdicts {
        if verdict != WatermarkVerdict::Pass {
            break;
        }

        let inserted = ingest_slot(
            source,
            target,
            registry,
            config,
            &addresses,
            &topic0_set,
            candidate,
        )
        .await?;
        outcome.events_inserted += inserted;
        outcome.slots_passed.push(candidate);

        persist_watermark(target, config.chain_id, candidate).await?;
    }

    Ok(outcome)
}

/// `pub(crate)` (not just `run_ingest_once`'s private helper) — P3c's
/// `run::AuditWorker::tick` also needs the raw persisted watermark (in
/// Solana-slot units) to derive the EVM-block-number resolve-pass frontier
/// from `source` (see `ingest::hercules_reads::block_frontier_through_slot`).
pub(crate) async fn load_watermark(pool: &PgPool, chain_id: i64) -> Result<i64, sqlx::Error> {
    let row: Option<(i64,)> = sqlx::query_as(
        "SELECT verified_through_slot FROM audit.ingest_watermark WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_optional(pool)
    .await?;
    // No row yet ⇒ "verified through slot -1" (nothing verified), so the
    // first candidate evaluated is slot 0.
    Ok(row.map(|(s,)| s).unwrap_or(-1))
}

/// First-boot watermark initialization at the FORWARD TIP (B1).
///
/// The live watermark is a PURE FORWARD TIP — historical capture is the job
/// of the existing P4a backfill lane (`resolve::pass::detect_backfill_gaps` +
/// `backfill.rs`). First boot is the degenerate "source whose history starts
/// behind the watermark" case that the gap/backfill path already handles.
/// This is the start-semantics authority: AUDIT-TRAIL-IMPL-PLAN line ~552
/// ("from the earliest manifest interval, NOT from genesis") + CAPTURE-SPEC
/// §7 ("backfill each in-scope source over its resolved interval").
///
/// Called at the TOP of `run::AuditWorker::tick` (before the resolve gate)
/// for BOTH `SourceMode`s. Behavior:
/// - A watermark row already exists ⇒ NO-OP (restart-safe — a persisted tip
///   is respected exactly, never re-initialized; the `-1` "no row" sentinel
///   in [`load_watermark`] is deliberately left intact, init happens a layer
///   up here so `run_ingest_once`/`load_watermark` keep their P1 contract).
/// - No row + `finalized_tip` or `max_produced_slot` is `None` (empty source
///   — Hercules hasn't indexed this chain yet) ⇒ HOLD (`Ok(false)`): do NOT
///   initialize, retry next tick. The caller holds the WHOLE tick, which also
///   defends B2 — never run the resolve gate against a not-yet-real frontier
///   (frontier `-1` would falsely mark every source previously-known and
///   permanently suppress gap detection).
/// - No row + both tips present ⇒ persist `S = min(finalized_tip,
///   max_produced_slot) - lag` as the initial `verified_through_slot` and
///   return `Ok(true)`. After this, `run::compute_resolve_frontier` sees a
///   real watermark, so the FIRST resolve pass records honest backfill gaps
///   for any source whose history predates `S`.
///
/// **Residual (accepted):** ranges backfilled at `≤ S` skip the
/// digest-stability double-read the live watermark applies — but every such
/// row is already inside the audit-final, append-only window (`backfill.rs`'s
/// module doc accepts this class under the audit-final lag bound: the live
/// watermark has long since passed it, so it cannot race live ingest).
pub async fn ensure_watermark_initialized(
    source: &PgPool,
    target: &PgPool,
    chain_id: i64,
    lag: i64,
) -> Result<bool, IngestError> {
    // Restart-safe: any persisted row (>= 0) is respected untouched.
    if load_watermark(target, chain_id).await? >= 0 {
        return Ok(true);
    }

    let finalized_tip = hercules_reads::finalized_tip(source).await?;
    let max_produced = hercules_reads::max_produced_slot(source).await?;
    let (Some(tip), Some(max)) = (finalized_tip, max_produced) else {
        // Empty source — nothing to anchor the forward tip to yet. HOLD.
        return Ok(false);
    };

    // Clamp to the `-1` floor so a pathologically tiny source (tip < lag)
    // can never persist a watermark BELOW the "no row" sentinel — that would
    // read back as "uninitialized" and loop.
    let initial = (tip.min(max) - lag).max(-1);
    persist_watermark(target, chain_id, initial).await?;
    tracing::info!(
        chain_id,
        finalized_tip = tip,
        max_produced = max,
        lag,
        initial_verified_through_slot = initial,
        "audit watermark initialized at the forward tip (first boot)"
    );
    Ok(true)
}

async fn persist_watermark(pool: &PgPool, chain_id: i64, slot: i64) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO audit.ingest_watermark (chain_id, verified_through_slot, updated_at)
        VALUES ($1, $2, $3)
        ON CONFLICT (chain_id) DO UPDATE SET verified_through_slot = excluded.verified_through_slot,
                                              updated_at = excluded.updated_at
        "#,
    )
    .bind(chain_id)
    .bind(slot)
    .bind(now_unix())
    .execute(pool)
    .await?;
    Ok(())
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Writes one quarantined log (H2): the append-only "saw it, couldn't
/// process it" record — never a silent skip. `raw_data` is best-effort
/// (empty if the error happened before `data` was fetched).
#[allow(clippy::too_many_arguments)]
async fn quarantine_log(
    target: &PgPool,
    chain_id: i64,
    slot: i64,
    tx_hash_hex: &str,
    log_ordinal: i32,
    topic0_hex: Option<&str>,
    address_hex: Option<&str>,
    raw_topics: &serde_json::Value,
    raw_data: &[u8],
    reason: &str,
) -> Result<(), sqlx::Error> {
    let tx_hash_bytes = hex::decode(tx_hash_hex.trim_start_matches("0x")).unwrap_or_default();
    let topic0_bytes = topic0_hex.and_then(|s| hex::decode(s.trim_start_matches("0x")).ok());
    let address_bytes = address_hex.and_then(|s| hex::decode(s.trim_start_matches("0x")).ok());

    sqlx::query(
        r#"
        INSERT INTO audit.quarantine
            (chain_id, slot_number, tx_hash, log_ordinal, topic0, source_contract, raw_topics, raw_data, reason, first_seen_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
        ON CONFLICT (chain_id, slot_number, tx_hash, log_ordinal) DO NOTHING
        "#,
    )
    .bind(chain_id)
    .bind(slot)
    .bind(&tx_hash_bytes)
    .bind(log_ordinal)
    .bind(&topic0_bytes)
    .bind(&address_bytes)
    .bind(raw_topics)
    .bind(raw_data)
    .bind(reason)
    .bind(now_unix())
    .execute(target)
    .await?;
    Ok(())
}

fn raw_topics_json(log: &MatchedLog) -> serde_json::Value {
    serde_json::json!([&log.topic0, &log.topic1, &log.topic2, &log.topic3])
}

/// Decodes and writes every matched log at `slot`. Returns the count of
/// NEW rows actually inserted (idempotent: a re-run over an already-ingested
/// slot inserts 0 more rows via `ON CONFLICT DO NOTHING`).
///
/// `pub(crate)` (P4a deliverable 2) — `backfill::run_backfill_once` reuses
/// this SAME decode+insert path (restricted to one gap's address, with that
/// address's real `IngestFilter` still applied) rather than forking a
/// second one.
pub(crate) async fn ingest_slot(
    source: &PgPool,
    target: &PgPool,
    registry: &AbiRegistry,
    config: &IngestConfig,
    addresses: &BTreeSet<String>,
    topic0_set: &BTreeSet<String>,
    slot: i64,
) -> Result<usize, IngestError> {
    let matched = hercules_reads::matched_logs_at_slot(source, slot, addresses, topic0_set).await?;
    if matched.is_empty() {
        return Ok(0); // genuinely empty/skipped slot — store nothing (no placeholder rows)
    }

    let mut inserted = 0usize;

    // Grouped by tx so `tx_receipt_info` is fetched once per tx, not once
    // per log (M4: BTreeMap iterates in tx_hash byte order, not tx_index —
    // harmless, since `event_id` is a DB surrogate never used as a natural
    // key and every row's real ordering key is `(block_number, tx_index,
    // log_index)`, read back from `receipt`/`log`, not from iteration order).
    let mut by_tx: BTreeMap<String, Vec<MatchedLog>> = BTreeMap::new();
    for log in matched {
        by_tx.entry(log.tx_hash.clone()).or_default().push(log);
    }

    for (tx_hash, logs) in by_tx {
        let (receipt, signer_bytes, block_hash_bytes, tx_hash_bytes) =
            match resolve_tx_receipt(source, slot, &tx_hash).await {
                Ok(v) => v,
                Err(e) => match e.classify() {
                    ErrorClass::Terminal(reason) => {
                        for log in &logs {
                            quarantine_log(
                                target,
                                config.chain_id,
                                slot,
                                &tx_hash,
                                log.log_ordinal,
                                Some(&log.topic0),
                                Some(&log.address),
                                &raw_topics_json(log),
                                &[],
                                reason,
                            )
                            .await?;
                        }
                        tracing::error!(
                            chain_id = config.chain_id,
                            slot,
                            tx_hash = %tx_hash,
                            reason,
                            logs = logs.len(),
                            "quarantined tx — terminal error resolving receipt"
                        );
                        continue; // next tx-group; this tx contributes nothing
                    }
                    ErrorClass::Transient => return Err(e), // hold this slot, retry next tick
                },
            };

        for log in logs {
            let spec = match config.sources.get(&log.address) {
                Some(s) => s,
                None => {
                    // matched_logs_at_slot filters by `addresses`, so this
                    // shouldn't happen — but a daemon never panics on a
                    // should-not-happen; quarantine it and move on.
                    quarantine_log(
                        target,
                        config.chain_id,
                        slot,
                        &tx_hash,
                        log.log_ordinal,
                        Some(&log.topic0),
                        Some(&log.address),
                        &raw_topics_json(&log),
                        &[],
                        "address not in resolved sources map",
                    )
                    .await?;
                    tracing::error!(chain_id = config.chain_id, slot, tx_hash = %tx_hash, address = %log.address, "quarantined log — unresolved address");
                    continue;
                }
            };
            let source_kind = spec.kind;

            // P4a Layer 2: a chain-wide-scoped kind's foreign log is
            // dropped HERE, before decode/write — neither an error nor a
            // quarantine row (it was correctly matched by address+topic0;
            // it just isn't in THIS asset's scope). See `IngestFilter`'s doc
            // for why this exists at all (THE landmine).
            if !spec.ingest_filter.passes(&log) {
                continue;
            }

            match try_ingest_log(
                source,
                target,
                registry,
                config,
                slot,
                source_kind,
                &log,
                &tx_hash,
                &tx_hash_bytes,
                &receipt,
                &block_hash_bytes,
                &signer_bytes,
            )
            .await
            {
                Ok(true) => inserted += 1,
                Ok(false) => {} // already landed (ON CONFLICT DO NOTHING) — not new, not an error
                Err(e) => match e.classify() {
                    ErrorClass::Terminal(reason) => {
                        quarantine_log(
                            target,
                            config.chain_id,
                            slot,
                            &tx_hash,
                            log.log_ordinal,
                            Some(&log.topic0),
                            Some(&log.address),
                            &raw_topics_json(&log),
                            &[],
                            reason,
                        )
                        .await?;
                        tracing::error!(
                            chain_id = config.chain_id,
                            slot,
                            tx_hash = %tx_hash,
                            log_ordinal = log.log_ordinal,
                            reason,
                            error = %e,
                            "quarantined log — terminal decode/write error"
                        );
                    }
                    ErrorClass::Transient => return Err(e), // hold this slot, retry next tick
                },
            }
        }
    }

    Ok(inserted)
}

/// Fetches + validates the per-tx receipt facts every log in this tx needs.
/// `MissingReceiptInfo` (Transient) and `MissingSigner`/`MalformedHex`
/// (Terminal) are surfaced as typed errors for the caller to classify —
/// this function itself never quarantines or logs.
async fn resolve_tx_receipt(
    source: &PgPool,
    slot: i64,
    tx_hash: &str,
) -> Result<(TxReceiptInfo, Vec<u8>, Vec<u8>, Vec<u8>), IngestError> {
    let receipt = hercules_reads::tx_receipt_info(source, slot, tx_hash)
        .await?
        .ok_or_else(|| IngestError::MissingReceiptInfo {
            slot,
            tx_hash: tx_hash.to_string(),
        })?;
    let signer = receipt
        .signer
        .as_deref()
        .ok_or_else(|| IngestError::MissingSigner {
            slot,
            tx_hash: tx_hash.to_string(),
        })?;
    let signer_bytes = hex_to_bytes("tx_signer", signer)?;
    let block_hash_bytes = hex_to_bytes("block_hash", &receipt.block_hash)?;
    let tx_hash_bytes = hex_to_bytes("tx_hash", tx_hash)?;
    Ok((receipt, signer_bytes, block_hash_bytes, tx_hash_bytes))
}

/// Decodes one log and writes it to `audit.chain_event`. Returns `true` if
/// a NEW row landed, `false` if it already existed (idempotent re-ingest).
#[allow(clippy::too_many_arguments)]
async fn try_ingest_log(
    source: &PgPool,
    target: &PgPool,
    registry: &AbiRegistry,
    config: &IngestConfig,
    slot: i64,
    source_kind: SourceKind,
    log: &MatchedLog,
    tx_hash: &str,
    tx_hash_bytes: &[u8],
    receipt: &TxReceiptInfo,
    block_hash_bytes: &[u8],
    signer_bytes: &[u8],
) -> Result<bool, IngestError> {
    let address_bytes = hex_to_bytes("source_contract", &log.address)?;
    let topic0_bytes = hex_to_bytes("topic0", &log.topic0)?;

    let mut topics = vec![parse_topic(&log.topic0)?];
    for t in [&log.topic1, &log.topic2, &log.topic3]
        .into_iter()
        .flatten()
    {
        topics.push(parse_topic(t)?);
    }

    // Only fetch `data` if the registered descriptor actually has a
    // non-indexed arg — the targeted-JSONB-read half of §5.4.
    let needs_data = registry
        .lookup(source_kind, &topics[0])
        .map(|d| d.args.iter().any(|a| !a.indexed))
        .unwrap_or(false);
    let data = if needs_data {
        match hercules_reads::log_data(source, slot, tx_hash, log.log_ordinal).await? {
            Some(hex_str) => hex_to_bytes("log_data", &hex_str)?,
            None => Vec::new(),
        }
    } else {
        Vec::new()
    };

    let raw_log = RawLog {
        address: to_array20(&address_bytes)?,
        topics,
        data,
    };

    let decoded: DecodedEvent = crate::decode::decode_log(registry, source_kind, &raw_log)
        .map_err(|source| IngestError::Decode {
            tx_hash: tx_hash.to_string(),
            log_ordinal: log.log_ordinal,
            source,
        })?;

    let log_index = (log.log_ordinal as i64 + receipt.first_log_index) as i32;
    let args_json =
        serde_json::to_value(&decoded.args).expect("ArgValue is infallibly serializable");

    let result = sqlx::query(
        r#"
        INSERT INTO audit.chain_event
            (chain_id, source_contract, source_kind, event_name, projection_tag, topic0,
             block_number, block_hash, block_timestamp, tx_hash, tx_index, log_index, tx_signer, args)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
        ON CONFLICT (chain_id, tx_hash, log_index, block_hash) DO NOTHING
        "#,
    )
    .bind(config.chain_id)
    .bind(&address_bytes)
    .bind(source_kind_db_name(source_kind))
    .bind(&decoded.event_name)
    .bind(match decoded.projection_tag {
        crate::types::ProjectionTag::Primary => "primary",
        crate::types::ProjectionTag::Supporting => "supporting",
    })
    .bind(&topic0_bytes)
    .bind(receipt.block_number)
    .bind(block_hash_bytes)
    .bind(receipt.block_timestamp)
    .bind(tx_hash_bytes)
    .bind(receipt.tx_index)
    .bind(log_index)
    .bind(signer_bytes)
    .bind(&args_json)
    .execute(target)
    .await?;

    Ok(result.rows_affected() > 0)
}

#[cfg(test)]
mod ingest_filter_any_tests {
    //! P4a CRITICAL-1 fix: a shared address resolved under two DIFFERENT
    //! filter-bearing variants (e.g. one asset's wUSDC is BOTH its own
    //! `TransferFrom` yield leg AND `TransferTouches` purchase leg) must
    //! union into an `Any` that passes a log if EITHER leg would — union
    //! must NEVER panic (a panic in the resolve worker kills the daemon).

    use super::*;

    fn addr(b: u8) -> [u8; 20] {
        [b; 20]
    }

    fn transfer_log(from: [u8; 20], to: [u8; 20]) -> MatchedLog {
        fn word(a: [u8; 20]) -> String {
            let mut w = [0u8; 32];
            w[12..].copy_from_slice(&a);
            format!("0x{}", hex::encode(w))
        }
        MatchedLog {
            tx_hash: "0xdead".to_string(),
            log_ordinal: 0,
            address: "0xaddr".to_string(),
            topic0: "0xtopic0".to_string(),
            topic1: Some(word(from)),
            topic2: Some(word(to)),
            topic3: None,
        }
    }

    #[test]
    fn union_of_mismatched_variants_never_panics_and_produces_any() {
        let from = IngestFilter::TransferFrom(BTreeSet::from([addr(0xA0)]));
        let touches = IngestFilter::TransferTouches(BTreeSet::from([addr(0xB0)]));
        let combined = from.union(touches);
        assert!(matches!(combined, IngestFilter::Any(_)));
    }

    #[test]
    fn any_passes_if_either_member_passes() {
        let arc_token = addr(0xA0);
        let storefront = addr(0xB0);
        let combined = IngestFilter::TransferFrom(BTreeSet::from([arc_token]))
            .union(IngestFilter::TransferTouches(BTreeSet::from([storefront])));

        // Yield leg: from == arc_token.
        assert!(combined.passes(&transfer_log(arc_token, addr(0x99))));
        // Purchase leg: touches storefront (as `to`).
        assert!(combined.passes(&transfer_log(addr(0x11), storefront)));
        // Neither leg — a genuine stranger.
        assert!(!combined.passes(&transfer_log(addr(0x11), addr(0x12))));
    }

    #[test]
    fn any_fingerprint_is_stable_and_order_independent() {
        let a = IngestFilter::TransferFrom(BTreeSet::from([addr(0xA0)]));
        let b = IngestFilter::TransferTouches(BTreeSet::from([addr(0xB0)]));
        let ab = a.clone().union(b.clone()).fingerprint();
        let ba = b.union(a).fingerprint();
        assert_eq!(ab, ba, "Any's fingerprint must not depend on union order");
    }

    #[test]
    fn union_of_two_any_merges_members_without_panicking() {
        let any1 = IngestFilter::TransferFrom(BTreeSet::from([addr(0xA0)]))
            .union(IngestFilter::TransferTouches(BTreeSet::from([addr(0xB0)])));
        let any2 = IngestFilter::TransferFrom(BTreeSet::from([addr(0xA0)]))
            .union(IngestFilter::TransferTouches(BTreeSet::from([addr(0xC0)])));
        let merged = any1.union(any2);
        assert!(merged.passes(&transfer_log(addr(0x11), addr(0xC0))));
        assert!(merged.passes(&transfer_log(addr(0xA0), addr(0x99))));
    }
}

#[cfg(test)]
mod ingest_filter_union_semantics_tests {
    //! P4a NEW-CRITICAL fix — `union`'s complete semantics, settled as an
    //! invariant matrix (this logic has now caused a real silent-uncapture
    //! bug TWICE; a whack-a-mole fix without a full matrix would just leave
    //! the next combination unguarded).
    //!
    //! **The core distinction this matrix protects:** `IngestFilter::None`
    //! has TWO meanings that must NEVER be conflated — (1) the fold's
    //! "nothing contributed yet" SEED, and (2) a REAL interval whose
    //! `scope_filter` is `None`, i.e. "capture EVERYTHING for this
    //! address" (pass-all — an asset's own token, or a Morpho interval).
    //! `union` only ever sees case (2) — case (1) is `ingest_filter_for`'s
    //! own problem, solved by seeding its fold with `Option<IngestFilter>`,
    //! never a bare `IngestFilter::None` (see `resolve::pass`). Given that,
    //! `None` inside `union` MUST be ABSORBING (pass-all wins — capturing
    //! MORE is always the safe direction; Layer 1's per-asset join still
    //! slices each asset correctly regardless of how wide Layer 2's capture
    //! is), never identity.

    use super::*;

    fn addr(b: u8) -> [u8; 20] {
        [b; 20]
    }

    fn from(a: [u8; 20]) -> IngestFilter {
        IngestFilter::TransferFrom(BTreeSet::from([a]))
    }

    fn touches(a: [u8; 20]) -> IngestFilter {
        IngestFilter::TransferTouches(BTreeSet::from([a]))
    }

    fn any_of(members: Vec<IngestFilter>) -> IngestFilter {
        IngestFilter::Any(members.into_iter().collect())
    }

    fn all_sample_filters() -> Vec<IngestFilter> {
        vec![
            IngestFilter::None,
            from(addr(0xA0)),
            touches(addr(0xB0)),
            any_of(vec![from(addr(0xC0)), touches(addr(0xD0))]),
        ]
    }

    #[test]
    fn union_is_commutative_for_every_pair_of_variants() {
        let samples = all_sample_filters();
        for a in &samples {
            for b in &samples {
                assert_eq!(
                    a.clone().union(b.clone()),
                    b.clone().union(a.clone()),
                    "union must be commutative for {a:?} vs {b:?}"
                );
            }
        }
    }

    #[test]
    fn none_absorbs_every_variant_both_orders() {
        for x in all_sample_filters() {
            assert_eq!(
                IngestFilter::None.union(x.clone()),
                IngestFilter::None,
                "None.union({x:?}) must stay None (pass-all wins)"
            );
            assert_eq!(
                x.union(IngestFilter::None),
                IngestFilter::None,
                "x.union(None) must become None (pass-all wins) regardless of order"
            );
        }
    }

    #[test]
    fn union_of_any_and_a_plain_filter_flattens_never_nests() {
        let combined = any_of(vec![from(addr(0xA0)), touches(addr(0xB0))]).union(touches(addr(0xC0)));
        match combined {
            IngestFilter::Any(members) => {
                assert_eq!(members.len(), 3, "expected 3 flat members, got {members:?}");
                assert!(
                    members.iter().all(|m| !matches!(m, IngestFilter::Any(_))),
                    "no member may itself be an Any — never nest Any-in-Any, got {members:?}"
                );
            }
            other => panic!("expected Any, got {other:?}"),
        }
    }

    #[test]
    fn union_of_two_anys_merges_members_flat() {
        let any1 = any_of(vec![from(addr(0xA0)), touches(addr(0xB0))]);
        let any2 = any_of(vec![from(addr(0xC0))]);
        match any1.union(any2) {
            IngestFilter::Any(members) => {
                assert_eq!(members.len(), 3, "got {members:?}");
                assert!(members.iter().all(|m| !matches!(m, IngestFilter::Any(_))));
            }
            other => panic!("expected Any, got {other:?}"),
        }
    }

    #[test]
    fn none_is_never_a_member_inside_an_any() {
        // Any real union sequence that ever touches `None` must collapse
        // the WHOLE result to `None` (absorbing) — `None` must never end
        // up sitting quietly inside an `Any`'s member set.
        let combined = any_of(vec![from(addr(0xA0)), touches(addr(0xB0))]).union(IngestFilter::None);
        assert_eq!(combined, IngestFilter::None);

        let combined2 = IngestFilter::None.union(any_of(vec![from(addr(0xA0))]));
        assert_eq!(combined2, IngestFilter::None);
    }

    #[test]
    fn fingerprint_prefixes_are_disjoint_across_all_variants() {
        let none_fp = IngestFilter::None.fingerprint();
        let from_fp = from(addr(0xA0)).fingerprint();
        let touches_fp = touches(addr(0xA0)).fingerprint();
        let any_fp = any_of(vec![from(addr(0xA0)), touches(addr(0xB0))]).fingerprint();

        let all = [&none_fp, &from_fp, &touches_fp, &any_fp];
        for (i, a) in all.iter().enumerate() {
            for (j, b) in all.iter().enumerate() {
                if i != j {
                    assert_ne!(a, b, "fingerprints must be pairwise distinct: {a} vs {b}");
                }
            }
        }
        assert_eq!(none_fp, "none");
        assert!(from_fp.starts_with("transfer_from:"));
        assert!(touches_fp.starts_with("transfer_touches:"));
        assert!(any_fp.starts_with("any:"));
    }
}

#[cfg(test)]
mod ingest_filter_fingerprint_tests {
    //! P4a §3: `IngestFilter::fingerprint` is `audit.ingest_scope`'s
    //! canonical key half — it MUST be stable across repeated calls and
    //! independent of the union's construction order (two assets sharing a
    //! chain-wide token can union in either order depending on resolve-pass
    //! iteration, and the fingerprint must come out identical either way).

    use super::*;

    fn addr(b: u8) -> [u8; 20] {
        [b; 20]
    }

    #[test]
    fn none_fingerprints_to_the_literal_string() {
        assert_eq!(IngestFilter::None.fingerprint(), "none");
    }

    #[test]
    fn transfer_from_fingerprint_is_order_independent_across_union_order() {
        let a = IngestFilter::TransferFrom(BTreeSet::from([addr(0xA0)]));
        let b = IngestFilter::TransferFrom(BTreeSet::from([addr(0xB0)]));

        let ab = a.clone().union(b.clone()).fingerprint();
        let ba = b.union(a).fingerprint();

        assert_eq!(ab, ba, "union order must not change the fingerprint");
        assert!(ab.contains(&format!("0x{}", hex::encode(addr(0xA0)))));
        assert!(ab.contains(&format!("0x{}", hex::encode(addr(0xB0)))));
    }

    #[test]
    fn transfer_touches_fingerprint_is_order_independent() {
        let a = IngestFilter::TransferTouches(BTreeSet::from([addr(0x11), addr(0x33)]));
        let b = IngestFilter::TransferTouches(BTreeSet::from([addr(0x22)]));

        let ab = a.clone().union(b.clone()).fingerprint();
        let ba = b.union(a).fingerprint();
        assert_eq!(ab, ba);
    }

    #[test]
    fn widening_the_filter_changes_the_fingerprint() {
        // THE property gap-detection re-keying depends on: a 2nd party
        // joining the union must produce a DIFFERENT fingerprint from the
        // narrower, single-party filter — otherwise filter-widening could
        // never be detected as "a new (address, fingerprint) pair".
        let narrow = IngestFilter::TransferFrom(BTreeSet::from([addr(0xA0)]));
        let widened = narrow
            .clone()
            .union(IngestFilter::TransferFrom(BTreeSet::from([addr(0xB0)])));
        assert_ne!(narrow.fingerprint(), widened.fingerprint());
    }

    #[test]
    fn different_variants_never_collide() {
        let from = IngestFilter::TransferFrom(BTreeSet::from([addr(0xA0)]));
        let touches = IngestFilter::TransferTouches(BTreeSet::from([addr(0xA0)]));
        assert_ne!(from.fingerprint(), touches.fingerprint());
        assert_ne!(from.fingerprint(), IngestFilter::None.fingerprint());
    }
}

fn parse_topic(hex_str: &str) -> Result<[u8; 32], IngestError> {
    let bytes = hex_to_bytes("topic", hex_str)?;
    to_array32(&bytes)
}

fn to_array20(bytes: &[u8]) -> Result<[u8; 20], IngestError> {
    bytes.try_into().map_err(|_| IngestError::MalformedHex {
        field: "address",
        value: hex::encode(bytes),
    })
}

fn to_array32(bytes: &[u8]) -> Result<[u8; 32], IngestError> {
    bytes.try_into().map_err(|_| IngestError::MalformedHex {
        field: "topic32",
        value: hex::encode(bytes),
    })
}
