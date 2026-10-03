//! Envelope types for one decoded compliance event.
//!
//! Shapes follow the audit-trail implementation plan §1.2 (event
//! envelope) and its stated conventions: uint256 is ALWAYS a base-10 decimal
//! string (a JSON/Rust native int can't hold 2^256-1 losslessly), and
//! addresses/hashes are lowercase, 0x-prefixed, fixed-width hex.

use std::collections::BTreeMap;

/// Compliance-relevant EVM contract kinds this crate decodes events from.
/// Mirrors `chain-event.json#/properties/source_kind` (IMPL-PLAN §1.2).
///
/// P0 registers events for `ArcToken`, `Axis1Module` (the resolved
/// transfer-restriction module — `WhitelistRestrictions` today), and
/// `GlobalSanctions` only (IMPL-PLAN §3 P0 scope). The remaining variants
/// are carried here so `SourceKind` doesn't need a breaking change when P1+
/// registers their events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SourceKind {
    ArcToken,
    Axis1Module,
    YieldBlacklist,
    Router,
    GlobalSanctions,
    Storefront,
    Factory,
    Uv2Pair,
    Morpho,
    YieldToken,
    PurchaseToken,
}

impl SourceKind {
    /// The exact `source_kind` string capture spec's catalog uses
    /// (`chain-event.json#/properties/source_kind`) — NOT the Rust `Debug`
    /// name. `ingest::pipeline` has its own private copy of this mapping
    /// (predates this method); P3's `resolve::manifest` uses this one so a
    /// new consumer doesn't grow a third copy.
    pub fn as_db_str(&self) -> &'static str {
        match self {
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
}

/// Whether this event is the primary compliance signal for its axis, or a
/// captured-but-secondary event. Both are always decoded — dropping a
/// known-but-non-primary event silently would defeat the phantom-vocabulary
/// guard just as much as failing to register it at all (capture spec
/// §"phantom guard"; IMPL-PLAN §13.6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionTag {
    Primary,
    Supporting,
}

/// A decoded event argument value.
///
/// `Uint256` is ALWAYS a base-10 decimal string matching `^(0|[1-9][0-9]*)$`
/// — uint256 does not fit into any native Rust integer type losslessly
/// (IMPL-PLAN §"conventions": "uint256 as decimal strings"). `Address` and
/// `Bytes32` are lowercase, 0x-prefixed hex of fixed width (20 / 32 bytes).
///
/// `#[serde(untagged)]` (added for P1's `audit.chain_event.args` JSONB
/// write): each single-field variant serializes as its bare inner value —
/// `Address("0x..")` → the JSON string `"0x.."`, `Bool(true)` → the JSON
/// bool `true` — never a `{"Address": "0x.."}` wrapper. This matches the
/// IMPL-PLAN §1.3 `args` shapes, which are plain typed values, not
/// enum-tagged ones.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(untagged)]
pub enum ArgValue {
    /// Lowercase 0x-prefixed hex, 40 hex chars (20 bytes).
    Address(String),
    /// Base-10 decimal string matching `^(0|[1-9][0-9]*)$`.
    Uint256(String),
    Bool(bool),
    /// Lowercase 0x-prefixed hex, 64 hex chars (32 bytes).
    Bytes32(String),
    String(String),
}

/// One decoded EVM log, typed against the registered ABI for its
/// `(source_kind, topic0)` pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedEvent {
    pub source_kind: SourceKind,
    pub event_name: String,
    pub projection_tag: ProjectionTag,
    pub args: BTreeMap<String, ArgValue>,
}

/// A raw EVM log as read off-chain — the decoder's input. Shape matches the
/// Hercules source `evm_tx_result.tx_result->'logs'` entries (IMPL-PLAN §5):
/// contract address, the full topic list (`topics[0]` = topic0, `topics[1..]`
/// = indexed args in declaration order), and the non-indexed arg data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawLog {
    pub address: [u8; 20],
    pub topics: Vec<[u8; 32]>,
    pub data: Vec<u8>,
}
