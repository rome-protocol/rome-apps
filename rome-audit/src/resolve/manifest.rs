//! `capture_manifest` (capture §1.4 / IMPL-PLAN §1.4): the frozen,
//! self-recording resolution input. `manifest_hash = H(JCS(manifest minus
//! generated_at))` — `generated_at` is excluded so two resolutions of an
//! identical graph at different wall-clock times produce the same hash
//! (capture §1.2 point 4 / IMPL-PLAN §1.4's note).
//!
//! **Canonicalization — simplified, not full RFC 8785 (honesty note for the
//! P3 report).** The full JCS canonicalizer is IMPL-PLAN §"conventions"'
//! job, layered in at P6 when `ReportContent` itself needs hashing. This
//! module gets the two properties that actually matter for P3's
//! determinism test without it: (1) `serde_json`'s `preserve_order` feature
//! (already a workspace-wide dependency feature) makes object-key order the
//! STRUCT FIELD DECLARATION order — stable across two calls, since it's the
//! same Rust type both times; (2) every array field here is explicitly
//! SORTED before serialization (`sources` by `(source_kind, address)`,
//! `intervals` by `(address, from_block)`) — so array order never depends on
//! RPC-response order or Rust `HashMap`/walk-order variance. What's NOT
//! implemented: RFC 8785's exact number/string escaping edge cases (moot
//! here — every field is either an integer, a lowercase hex string, or an
//! enum tag, none of which have a JCS-vs-`serde_json::to_vec` divergence).
//!
//! **Hash function: keccak256, not SHA-256** (IMPL-PLAN §6 open Q8 — flagged
//! there as a P6 decision). This module uses `ethers::utils::keccak256` —
//! already a workspace dependency, no new crate — leaning toward the
//! EVM-native choice the plan's own gate flag favors; P6 can override this
//! without changing anything upstream of the hash call.

use super::graph::{ResolvedGraph, ScopeFilter};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub(crate) struct ManifestSource {
    // Fields ALPHABETICAL (F1, P3 review): serde_json emits
    // struct-declaration order; RFC-8785/JCS (P6's canonicalizer) sorts keys
    // alphabetically. Keeping declaration order == alphabetical means this
    // all-ASCII/int/null shape is ALREADY byte-identical to JCS, so P6 needs
    // no hash migration for already-persisted manifests.
    abi_ref: String,
    address: String,
    resolved_from: String,
    source_kind: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub(crate) struct ManifestInterval {
    // Fields ALPHABETICAL (F1) — see ManifestSource.
    address: String,
    from_block: i64,
    scope_filter: Option<String>,
    to_block: Option<i64>,
}

/// The hashed content (`generated_at` deliberately excluded — see module
/// doc). Field declaration order here IS the canonical key order.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct CaptureManifest {
    pub asset_id: String,
    pub registry_commit_sha: String,
    // `pub(crate)` (not private): `resolve::store` builds the persisted
    // JSONB straight from these two fields, so the DB payload is
    // byte-for-byte the same shape the hash was computed over.
    pub(crate) resolved_sources: Vec<ManifestSource>,
    pub(crate) source_intervals: Vec<ManifestInterval>,
}

impl CaptureManifest {
    /// Builds a manifest from a resolved graph, sorting every array field
    /// (see module doc) so the byte-serialization is independent of
    /// resolution walk order.
    pub fn from_graph(
        asset_id: String,
        registry_commit_sha: String,
        graph: &ResolvedGraph,
    ) -> Self {
        let mut sources: Vec<ManifestSource> = graph
            .sources
            .iter()
            .map(|s| ManifestSource {
                source_kind: s.source_kind.as_db_str().to_string(),
                address: format!("0x{}", hex::encode(s.address)),
                resolved_from: s.resolved_from.as_db_str().to_string(),
                abi_ref: s.abi_ref.to_string(),
            })
            .collect();
        sources.sort_by(|a, b| (&a.source_kind, &a.address).cmp(&(&b.source_kind, &b.address)));

        let mut intervals: Vec<ManifestInterval> = graph
            .intervals
            .iter()
            .map(|i| ManifestInterval {
                address: format!("0x{}", hex::encode(i.address)),
                from_block: i.from_block,
                to_block: i.to_block,
                scope_filter: i.scope_filter.as_ref().map(ScopeFilter::fingerprint),
            })
            .collect();
        intervals.sort_by(|a, b| (&a.address, a.from_block).cmp(&(&b.address, b.from_block)));

        Self {
            asset_id,
            registry_commit_sha,
            resolved_sources: sources,
            source_intervals: intervals,
        }
    }

    /// `H(JCS(manifest minus generated_at))` — see module doc for the
    /// canonicalization + hash-function caveats.
    pub fn manifest_hash(&self) -> [u8; 32] {
        let bytes = serde_json::to_vec(self).expect("CaptureManifest is infallibly serializable");
        ethers::utils::keccak256(bytes)
    }
}

#[cfg(test)]
mod tests {
    //! `generated_at` isn't even a field of `CaptureManifest` (it's tracked
    //! only in `resolve::store`'s DB call) — so THIS module's determinism
    //! property is really "sort order doesn't leak into the hash". The
    //! meaningful "two resolutions at different wall-clock times produce
    //! the same stored `manifest_hash`" test is a DB integration test
    //! (`tests/resolve_db.rs`), since that's where `generated_at` actually
    //! enters the picture at all.

    use super::*;
    use crate::resolve::graph::{ResolvedFrom, ResolvedSource, SourceInterval};
    use crate::types::SourceKind;

    fn addr(b: u8) -> [u8; 20] {
        [b; 20]
    }

    fn sample_graph() -> ResolvedGraph {
        ResolvedGraph {
            token: addr(0xA0),
            sources: vec![
                ResolvedSource {
                    source_kind: SourceKind::Router,
                    address: addr(0xB0),
                    resolved_from: ResolvedFrom::TokenRead,
                    abi_ref: "RestrictionsRouter",
                },
                ResolvedSource {
                    source_kind: SourceKind::ArcToken,
                    address: addr(0xA0),
                    resolved_from: ResolvedFrom::TokenRead,
                    abi_ref: "ArcToken",
                },
            ],
            intervals: vec![
                SourceInterval {
                    address: addr(0xB0),
                    from_block: 100,
                    to_block: None,
                    scope_filter: None,
                },
                SourceInterval {
                    address: addr(0xA0),
                    from_block: 100,
                    to_block: None,
                    scope_filter: None,
                },
            ],
        }
    }

    #[test]
    fn hash_is_stable_across_repeated_calls_on_the_same_graph() {
        let graph = sample_graph();
        let m1 =
            CaptureManifest::from_graph("200010:0xa0".to_string(), "deadbeef".to_string(), &graph);
        let m2 =
            CaptureManifest::from_graph("200010:0xa0".to_string(), "deadbeef".to_string(), &graph);
        assert_eq!(m1.manifest_hash(), m2.manifest_hash());
    }

    #[test]
    fn hash_is_independent_of_input_source_and_interval_order() {
        let mut shuffled = sample_graph();
        shuffled.sources.reverse();
        shuffled.intervals.reverse();

        let ordered = CaptureManifest::from_graph(
            "200010:0xa0".to_string(),
            "deadbeef".to_string(),
            &sample_graph(),
        );
        let reversed = CaptureManifest::from_graph(
            "200010:0xa0".to_string(),
            "deadbeef".to_string(),
            &shuffled,
        );

        assert_eq!(ordered.manifest_hash(), reversed.manifest_hash());
    }

    #[test]
    fn different_registry_commit_sha_changes_the_hash() {
        let graph = sample_graph();
        let m1 =
            CaptureManifest::from_graph("200010:0xa0".to_string(), "aaaaaaaa".to_string(), &graph);
        let m2 =
            CaptureManifest::from_graph("200010:0xa0".to_string(), "bbbbbbbb".to_string(), &graph);
        assert_ne!(m1.manifest_hash(), m2.manifest_hash());
    }

    /// P4a verify-item #2: `ScopeFilter` entering `source_intervals` (via
    /// its `fingerprint()`) MUST change `manifest_hash` — that's the whole
    /// point of hashing the interval's `scope_filter` field at all. This is
    /// the "last free window" check named in the P4a task: since no
    /// `report_hash`/attested-report machinery exists yet (P6, not built —
    /// confirmed by grep, zero hits for `report_hash`/`ReportContent`/
    /// `attested` anywhere in this crate), a manifest_hash shape change
    /// today has NO already-persisted attested report to invalidate.
    #[test]
    fn manifest_hash_changes_when_scope_filter_changes() {
        use crate::resolve::graph::ScopeFilter;

        let mut with_filter = sample_graph();
        with_filter.intervals[0].scope_filter = Some(ScopeFilter::TransferFrom {
            from: with_filter.token,
        });
        let mut without_filter = sample_graph();
        without_filter.intervals[0].scope_filter = None;

        let m_with = CaptureManifest::from_graph(
            "200010:0xa0".to_string(),
            "deadbeef".to_string(),
            &with_filter,
        );
        let m_without = CaptureManifest::from_graph(
            "200010:0xa0".to_string(),
            "deadbeef".to_string(),
            &without_filter,
        );
        assert_ne!(
            m_with.manifest_hash(),
            m_without.manifest_hash(),
            "adding a scope_filter to an interval must change manifest_hash"
        );

        // A DIFFERENT filter party must also change the hash — not just
        // presence-vs-absence of a filter at all.
        let mut different_party = sample_graph();
        different_party.intervals[0].scope_filter = Some(ScopeFilter::TransferFrom {
            from: [0x77u8; 20], // a party DIFFERENT from with_filter's `token`
        });
        let m_different_party = CaptureManifest::from_graph(
            "200010:0xa0".to_string(),
            "deadbeef".to_string(),
            &different_party,
        );
        assert_ne!(
            m_with.manifest_hash(),
            m_different_party.manifest_hash(),
            "a different filter party must also change manifest_hash, not just filter presence"
        );
    }
}
