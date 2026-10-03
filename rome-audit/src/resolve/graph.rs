//! The resolution output shape (capture §1.4 / IMPL-PLAN §1.4
//! `capture_manifest`): a resolved source set, plus the per-source block
//! interval each source was in scope (capture §1.2's "block intervals,
//! never current membership").

use crate::types::SourceKind;

/// How one source was discovered (IMPL-PLAN §1.4 `resolved_from` enum).
/// `SpineProbe`/`MorphoCreateMarket` are carried here for shape-completeness
/// with the plan's schema even though P3 doesn't populate them yet (UV2
/// spine discovery + Morpho loan/collateral filtering are DEFERRED — see the
/// P3 report).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ResolvedFrom {
    TokenRead,
    TokenEvent,
    Registry,
    SpineProbe,
    MorphoCreateMarket,
}

impl ResolvedFrom {
    pub fn as_db_str(&self) -> &'static str {
        match self {
            ResolvedFrom::TokenRead => "TOKEN_READ",
            ResolvedFrom::TokenEvent => "TOKEN_EVENT",
            ResolvedFrom::Registry => "REGISTRY",
            ResolvedFrom::SpineProbe => "SPINE_PROBE",
            ResolvedFrom::MorphoCreateMarket => "MORPHO_CREATE_MARKET",
        }
    }
}

/// One resolved source contract (IMPL-PLAN §1.4 `resolved_sources[]`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSource {
    pub source_kind: SourceKind,
    pub address: [u8; 20],
    pub resolved_from: ResolvedFrom,
    /// Static ABI-registry name (e.g. `"ArcToken"`) — never a content hash;
    /// P3 doesn't build a content-addressed ABI store (that's P0's
    /// `contract_abi` table, a separate later concern).
    pub abi_ref: &'static str,
}

/// A structured, ENFORCED join predicate on top of an interval's plain
/// `(address, block range)` scope (P4a — previously this was a documentary
/// `Option<&'static str>` comment, never applied as a real filter; see this
/// module's history and `resolve::asset_event`'s Layer 1 / `ingest`'s Layer
/// 2, both of which now branch on this enum). Every variant maps to a
/// `chain_event.args` JSONB predicate — see `resolve::asset_event::scope_sql`.
///
/// **THE LANDMINE this exists to defuse:** `YieldToken`/`PurchaseToken` reuse
/// `ArcToken`'s own `Transfer` topic0 CHAIN-WIDE (capture §1.2's documented
/// gap) — the moment either kind gains a registered ABI descriptor,
/// `resolve::pass::run_resolve_pass`'s C1 `retain(has_events_for)` stops
/// dropping it from the live ingest map, and ingest would ask Hercules for
/// EVERY transfer of that chain-wide ERC20 (e.g. wUSDC) across the WHOLE
/// chain — the M2 firehose. `TransferFrom`/`TransferTouches` are what keep
/// that registration safe: enforced at BOTH the `asset_event` join (Layer 1,
/// this crate's own scoping) AND at ingest capture time (Layer 2,
/// `ingest::pipeline::IngestFilter` — so the firehose never even lands in
/// `chain_event` for a chain-wide kind).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopeFilter {
    /// Yield-token leg (capture §1.2): only `Transfer` events where `from`
    /// is this ArcToken — i.e. distributions FROM the token, never the
    /// yield-token's unrelated chain-wide transfer history.
    TransferFrom { from: [u8; 20] },
    /// Purchase-token leg (capture §1.2): only `Transfer` events touching
    /// the storefront (either leg — the buyer→storefront payment or a
    /// storefront-initiated leg), never the purchase-token's unrelated
    /// chain-wide transfer history.
    TransferTouches { party: [u8; 20] },
    /// Morpho market leg (capture §2.9): market-keyed events (`args ? 'id'`)
    /// scoped to exactly this asset's matched market ids; market-less
    /// governance events (`SetOwner`/`EnableIrm`/…) have no `id` arg at all
    /// and stay in scope unconditionally — they're chain-global supporting
    /// facts, not market-specific.
    MorphoMarkets { market_ids: Vec<[u8; 32]> },
}

impl ScopeFilter {
    /// A canonical, sorted-where-order-matters string form — used both as
    /// the manifest's serialized `scope_filter` field (so a filter change
    /// changes `manifest_hash`, P4a verify-item #2) AND as
    /// `audit.ingest_scope.filter_fingerprint` (P4a §3) — the SAME string
    /// both places, so "did the effective filter change" is one comparison.
    pub fn fingerprint(&self) -> String {
        match self {
            ScopeFilter::TransferFrom { from } => {
                format!("transfer_from:0x{}", hex::encode(from))
            }
            ScopeFilter::TransferTouches { party } => {
                format!("transfer_touches:0x{}", hex::encode(party))
            }
            ScopeFilter::MorphoMarkets { market_ids } => {
                let mut ids: Vec<String> = market_ids
                    .iter()
                    .map(|id| format!("0x{}", hex::encode(id)))
                    .collect();
                ids.sort();
                format!("morpho_markets:{}", ids.join(","))
            }
        }
    }
}

/// One source's historical scope, as a block interval (capture §1.2: "union
/// over history", `to_block: None` = still in scope). IMPL-PLAN §1.4
/// `source_intervals[]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceInterval {
    pub address: [u8; 20],
    pub from_block: i64,
    /// Exclusive upper bound (`None` = open-ended, still in scope).
    pub to_block: Option<i64>,
    pub scope_filter: Option<ScopeFilter>,
}

/// The full resolved source graph for one token (capture §1.2's fixed
/// point). This is what [`super::manifest::CaptureManifest`] hashes (minus
/// `generated_at`) and what [`super::asset_event`] materializes into the
/// `asset_event` junction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedGraph {
    pub token: [u8; 20],
    pub sources: Vec<ResolvedSource>,
    pub intervals: Vec<SourceInterval>,
}
