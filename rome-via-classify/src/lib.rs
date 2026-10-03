//! Shared transaction classification for the Rome Via stack.
//!
//! Both rome-via-api (read path) and rome-via-enrich (the worker that persists the
//! classification) must agree exactly on what a transaction *is*. Keeping the logic
//! in one dependency-light crate makes that agreement structural rather than a
//! convention two crates are trusted to uphold.
//!
//! - [`classify`] — action tags (what the tx *did*).
//! - [`seams`] — which EVM<->Solana seams the tx sits on (cross-VM classification).
pub mod classify;

/// Oracle-keeper refresh selectors. Keeper traffic is infrastructure, not user activity,
/// and it dwarfs everything it sits next to — so several surfaces (the cross-VM feed's
/// default hide, the Ledger's `include_oracle` de-noise, and the application-vs-oracle TPS
/// split) must identify it. ONE definition so those screens can't disagree about what
/// "oracle" means; every consumer routes through [`is_oracle_method`] or this slice.
///
/// - `0xf8ac93e8` — legacy `refresh()`: the retired per-asset keeper, one call per asset.
/// - `0xae27861b` — PriceBook `refreshAll(bytes32[])`: the current keeper, every asset in
///   one atomic tx per tick (~1-2 min). Authored from Solana (a `sol_to_evm` crossing),
///   so before it was recognised it flooded the cross-VM feed and the TPS split.
pub const ORACLE_SELECTORS: &[&str] = &["0xf8ac93e8", "0xae27861b"];

/// True when this method id is any oracle-keeper refresh (legacy per-asset or PriceBook).
pub fn is_oracle_method(method_id: Option<&str>) -> bool {
    method_id.is_some_and(|m| ORACLE_SELECTORS.contains(&m))
}
pub mod seams;
pub mod status;

pub use classify::{classify, ClassifyInput};
pub use seams::{seams, Seam, SeamInput};
pub use status::{derive_status, revert_reason};

#[cfg(test)]
mod oracle_tests {
    use super::*;

    #[test]
    fn is_oracle_matches_every_keeper_selector() {
        // Legacy per-asset keeper — one refresh() call per asset.
        assert!(is_oracle_method(Some("0xf8ac93e8")), "legacy refresh() is oracle");
        // Current PriceBook keeper — refreshAll(bytes32[]) reads every asset in one tx per
        // tick. Missing this selector let ~880 keeper crossings flood /cross-vm's default
        // (oracle-hidden) view and inflate the application-vs-oracle TPS split.
        assert!(is_oracle_method(Some("0xae27861b")), "PriceBook refreshAll(bytes32[]) is oracle");
        // User activity stays user activity.
        assert!(!is_oracle_method(Some("0xa9059cbb")), "erc20 transfer is not oracle");
        assert!(!is_oracle_method(None), "absent method is not oracle");
    }
}
