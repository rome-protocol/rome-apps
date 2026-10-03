//! The ABI registry — maps `(source_kind, topic0)` to a decode shape.
//!
//! This IS the phantom-vocabulary guard (capture spec §"phantom guard";
//! IMPL-PLAN §13.6): a log whose `(source_kind, topic0)` isn't registered
//! here decodes to a loud `DecodeError::UnknownTopic`, never a silent skip
//! or a coerced best-guess.

use std::collections::BTreeMap;

use crate::types::{ProjectionTag, SourceKind};

/// The ABI shape of one event argument (IMPL-PLAN §1.3 `args` shapes).
///
/// P0's registered events only use the fixed-32-byte-word types (`Address`,
/// `Uint256`, `Bool`, `Bytes32`); `String` is included so `ArgValue::String`
/// isn't dead — the decoder supports it via standard ABI dynamic (offset +
/// length) encoding for non-indexed positions. Solidity indexes a dynamic
/// type by its keccak256 hash, so the original value is unrecoverable from
/// an indexed topic; an `ArgSpec` with `ty: String, indexed: true` is a
/// registration error the decoder rejects loudly rather than silently
/// returning the hash.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArgType {
    Address,
    Uint256,
    Bool,
    Bytes32,
    String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArgSpec {
    pub name: &'static str,
    pub ty: ArgType,
    pub indexed: bool,
}

impl ArgSpec {
    pub const fn new(name: &'static str, ty: ArgType, indexed: bool) -> Self {
        Self { name, ty, indexed }
    }
}

/// One registered event's decode shape (IMPL-PLAN §1.2/§1.3 + capture §2
/// catalog). `args` must be in Solidity declaration order — that order is
/// what indexed args are matched against `topics[1..]` and non-indexed args
/// against `data`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventDescriptor {
    pub event_name: &'static str,
    pub projection_tag: ProjectionTag,
    pub args: Vec<ArgSpec>,
}

/// Maps `(source_kind, topic0)` to an [`EventDescriptor`]. See module docs
/// for the phantom-guard contract.
#[derive(Debug, Clone, Default)]
pub struct AbiRegistry {
    entries: BTreeMap<(SourceKind, [u8; 32]), EventDescriptor>,
}

impl AbiRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers one event under `(source_kind, topic0)`.
    ///
    /// # Panics
    /// Panics on a duplicate `(source_kind, topic0)` registration — a
    /// collision is an authoring bug caught at registry-construction time
    /// (called once, at process start), not a runtime condition to handle
    /// gracefully.
    pub fn register(
        &mut self,
        source_kind: SourceKind,
        topic0: [u8; 32],
        descriptor: EventDescriptor,
    ) {
        let key = (source_kind, topic0);
        assert!(
            self.entries.insert(key, descriptor).is_none(),
            "rome-audit: duplicate ABI registration for {source_kind:?}/0x{}",
            hex::encode(topic0)
        );
    }

    pub fn lookup(&self, source_kind: SourceKind, topic0: &[u8; 32]) -> Option<&EventDescriptor> {
        self.entries.get(&(source_kind, *topic0))
    }

    /// Every registered topic0, across all source kinds. Added for P1's
    /// ingest pipeline (IMPL-PLAN §5.4): the Hercules `evm_log` filter needs
    /// the full registered topic0 SET up front — an unregistered event is
    /// then simply never fetched (the efficiency half of the phantom
    /// guard), while `lookup`'s `(source_kind, topic0)` pairing stays the
    /// decode-time guard against a registered-but-wrong-shape topic. NOT a
    /// decode-time API — decoding still goes through `lookup` only.
    pub fn all_topic0s(&self) -> std::collections::BTreeSet<[u8; 32]> {
        self.entries.keys().map(|(_, topic0)| *topic0).collect()
    }

    /// Whether ANY event is registered for `source_kind` at all (P3c C1: the
    /// live-ingest map must exclude a resolved source whose `SourceKind` has
    /// no registered descriptor — e.g. `YieldToken`/`PurchaseToken`/
    /// `YieldBlacklist`, which reuse another kind's topic0 chain-wide.
    /// Without this check, `matched_logs_at_slot`'s address+topic0 filter
    /// matches every one of that address's transfers against a registry
    /// lookup that always misses, quarantining the entire chain-wide event
    /// stream — the exact firehose the yield-leg `scope_filter` comment in
    /// `resolver.rs` exists to prevent (capture is real P4 work, WITH the
    /// scope filter enforced, never a bare address→kind map). `capture_manifest`/
    /// `asset_event` still record these kinds — this only gates the LIVE
    /// ingest map.)
    pub fn has_events_for(&self, source_kind: SourceKind) -> bool {
        self.entries.keys().any(|(kind, _)| *kind == source_kind)
    }

    /// A copy of this registry with every entry for `kind` removed — e.g.
    /// to reconstruct "the registry as it looked before `kind` gained a
    /// live-ingest descriptor," for testing the newly-ingestable
    /// gap-detection transition (`resolve::pass::detect_backfill_gaps`,
    /// P4a §3) without hand-duplicating every OTHER kind's decode shape
    /// (which `resolve::resolver::resolve` itself needs to decode the
    /// events it walks — this registry isn't just a live-ingest filter).
    pub fn without_source_kind(&self, kind: SourceKind) -> Self {
        Self {
            entries: self
                .entries
                .iter()
                .filter(|((k, _), _)| *k != kind)
                .map(|(k, v)| (*k, v.clone()))
                .collect(),
        }
    }

    /// Every registered event NAME, across all source kinds. Lets a
    /// registered-vs-fixture coverage check use set equality, so a newly
    /// registered event with no fixture coverage fails instead of silently
    /// passing a hand-maintained list.
    pub fn event_names(&self) -> std::collections::BTreeSet<&'static str> {
        self.entries.values().map(|d| d.event_name).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn has_events_for_is_true_only_for_a_kind_with_a_real_registration() {
        let mut registry = AbiRegistry::new();
        registry.register(
            SourceKind::ArcToken,
            [0x11u8; 32],
            EventDescriptor {
                event_name: "Test",
                projection_tag: crate::types::ProjectionTag::Supporting,
                args: vec![],
            },
        );

        assert!(registry.has_events_for(SourceKind::ArcToken));
        // This FRESH, empty-except-for-ArcToken registry proves the METHOD
        // itself — YieldToken has no registration HERE regardless of what
        // the real `build_registry()` happens to contain (which, as of
        // P4a, DOES register it — see
        // `p4a_closed_the_gap_all_three_kinds_now_have_real_registrations`
        // below).
        assert!(!registry.has_events_for(SourceKind::YieldToken));
    }

    #[test]
    fn p4a_closed_the_gap_all_three_kinds_now_have_real_registrations() {
        // Historical note (this test's ORIGINAL premise, now honestly
        // corrected rather than silently deleted — P4a review): C1
        // (P3c) fixed the case where these three kinds were resolved
        // (capture-graph members) but had NO registered descriptor of their
        // own — their address's events were only reachable via ANOTHER
        // kind's registered topic0 (ArcToken's/Uv2Pair's Transfer,
        // chain-wide), so `has_events_for` had to read false for all three
        // and C1's `retain` dropped them from the live ingest map entirely.
        //
        // P4a (this task) REGISTERED real descriptors for all three
        // (`abi::erc20`'s YieldToken/PurchaseToken `Transfer` reuse,
        // `abi::yield_blacklist`'s `YieldBlacklistUpdated`) — WITH the
        // enforced `ScopeFilter` (join + ingest layers) built first, so the
        // landmine this registration risked (an unfiltered chain-wide
        // ERC20 firehose) never opened. `has_events_for` now reads TRUE for
        // all three, by design — the mechanism this test's SIBLING
        // (`has_events_for_is_true_only_for_a_kind_with_a_real_registration`,
        // above) proves against a synthetic registry is what actually
        // matters; this test is now the "did the real crate-wide registry
        // apply that mechanism as intended" check, inverted from its
        // original assertions.
        let registry = crate::abi::build_registry();
        assert!(registry.has_events_for(SourceKind::YieldToken));
        assert!(registry.has_events_for(SourceKind::PurchaseToken));
        assert!(registry.has_events_for(SourceKind::YieldBlacklist));
        // Sanity: a real, independently-decodable kind must still read true.
        assert!(registry.has_events_for(SourceKind::ArcToken));
    }

    #[test]
    fn without_source_kind_removes_only_that_kind() {
        let full = crate::abi::build_registry();
        let reduced = full.without_source_kind(SourceKind::YieldToken);

        assert!(!reduced.has_events_for(SourceKind::YieldToken));
        // Every OTHER kind's registration must survive untouched —
        // including ArcToken's OWN entries (e.g. its YieldTokenUpdated
        // decode shape), which live under a DIFFERENT key
        // (`SourceKind::ArcToken`, not `SourceKind::YieldToken`).
        assert!(reduced.has_events_for(SourceKind::ArcToken));
        assert!(reduced.has_events_for(SourceKind::PurchaseToken));
        assert!(reduced.has_events_for(SourceKind::Router));
        assert!(
            !reduced.all_topic0s().is_empty(),
            "removing one kind must not empty the whole registry"
        );
    }
}
