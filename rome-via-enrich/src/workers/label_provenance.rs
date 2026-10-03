//! Provenance ranking for `rome_via.contract_labels.display_label` writers.
//!
//! Three tiers, ranked so a higher-confidence writer's label survives a
//! later lower-confidence pass:
//!   * `registry` (3) — curated, projected from `rome-protocol/registry`.
//!   * `verified`  (2) — the Sourcify-verified compilation name (a contract
//!     verified on the Rome Sourcify instance).
//!   * `onchain`   (1) — best-effort `name()`/`symbol()`/bytecode heuristic —
//!     the pre-existing single tier, kept as the default/floor.
//!
//! **Every writer of `display_label` must route its upsert's
//! `ON CONFLICT ... DO UPDATE ... WHERE` clause through [`GUARD_WHERE_CLAUSE`]**
//! (or an equivalent rank comparison), or a higher-tier label can be silently
//! clobbered by a lower-tier re-resolution. See `contract_labels.rs`
//! (`upsert_label`) and `verified_labels.rs` (`upsert_verified`) for the two
//! current call sites.

/// Registry-curated label — the highest tier.
pub const PROVENANCE_REGISTRY: &str = "registry";
/// Sourcify-verified compilation name — the middle tier.
pub const PROVENANCE_VERIFIED: &str = "verified";
/// On-chain name()/symbol()/bytecode heuristic — the floor tier (also the
/// column's DEFAULT, so every pre-existing row ranks lowest).
pub const PROVENANCE_ONCHAIN: &str = "onchain";

/// Numeric rank for a provenance tag. Higher = more authoritative. An
/// unrecognized value ranks as the floor (`onchain`) rather than erroring, so
/// a typo'd or future provenance value degrades safely (loses ties) instead
/// of always winning.
///
/// Pure function — fully unit-tested. Mirrors [`GUARD_WHERE_CLAUSE`]'s
/// `CASE` expression; keep the two in lockstep if a tier is ever added.
pub fn rank(provenance: &str) -> i32 {
    match provenance {
        PROVENANCE_REGISTRY => 3,
        PROVENANCE_VERIFIED => 2,
        _ => 1,
    }
}

/// `WHERE` clause fragment for a rank-guarded `contract_labels` upsert,
/// intended for an `INSERT ... ON CONFLICT (chain_id, address) DO UPDATE ...
/// WHERE <this>` statement. The incoming (`EXCLUDED.provenance`) rank must be
/// `>=` the existing row's rank — `>=`, not `>`, so a same-tier re-resolution
/// (e.g. an on-chain worker re-probing an already-onchain row) still
/// refreshes, matching the unconditional-upsert behavior every writer had
/// before this guard existed. Only a *lower*-tier write is rejected.
pub const GUARD_WHERE_CLAUSE: &str = "(CASE EXCLUDED.provenance WHEN 'registry' THEN 3 WHEN 'verified' THEN 2 ELSE 1 END) \
     >= (CASE rome_via.contract_labels.provenance WHEN 'registry' THEN 3 WHEN 'verified' THEN 2 ELSE 1 END)";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rank_orders_registry_over_verified_over_onchain() {
        assert!(rank(PROVENANCE_REGISTRY) > rank(PROVENANCE_VERIFIED));
        assert!(rank(PROVENANCE_VERIFIED) > rank(PROVENANCE_ONCHAIN));
    }

    #[test]
    fn rank_unknown_provenance_floors_to_onchain() {
        assert_eq!(rank("bogus"), rank(PROVENANCE_ONCHAIN));
    }

    #[test]
    fn guard_where_clause_references_excluded_and_existing_row() {
        let clause = GUARD_WHERE_CLAUSE;
        assert!(clause.contains("EXCLUDED.provenance"), "{clause}");
        assert!(clause.contains("rome_via.contract_labels.provenance"), "{clause}");
        assert!(clause.contains(">="), "must be >= (same-tier still refreshes): {clause}");
    }
}
