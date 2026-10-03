//! Per-contract ABI registrations for P0's event set (IMPL-PLAN §3 P0):
//! GlobalSanctions, ArcToken (Transfer + RoleGranted), WhitelistRestrictions,
//! plus Router (`ModuleTypeRegistered`), whose registration tx is part of
//! the on-chain event set this crate decodes.
//!
//! Every `topic0` here is a hardcoded constant (same pattern as
//! rome-via-enrich's `token_gate_events` worker: "hardcoded topic0 +
//! decoder"), computed once via `cast keccak "<signature>"` against the real
//! vendored Solidity and cross-checked in `decode::tests` against an
//! independently-computed keccak256 — never invented, never derived at
//! runtime from a string the decoder trusts blindly.

pub mod arc_token;
pub mod erc20;
pub mod factory;
pub mod global_sanctions;
pub mod morpho;
pub mod router;
pub mod storefront;
pub mod uv2_pair;
pub mod whitelist_restrictions;
pub mod yield_blacklist;

use crate::registry::AbiRegistry;

/// Builds the full registry: P0's GlobalSanctions + ArcToken +
/// WhitelistRestrictions + Router, P3's Factory (`TokenRegistered`) and
/// Storefront (`PurchaseTokenUpdated`), P3b's Uv2Pair (`Transfer`) and
/// Morpho (`CreateMarket`), plus P4a's full ABI-registration surface
/// (`erc20` — YieldToken/PurchaseToken `Transfer` reuse; `yield_blacklist`
/// — `YieldBlacklistUpdated`; the expanded per-module OZ surface on
/// `arc_token`/`whitelist_restrictions`/`storefront`/`factory`/`uv2_pair`/
/// `morpho` themselves).
pub fn build_registry() -> AbiRegistry {
    let mut registry = AbiRegistry::new();
    global_sanctions::register(&mut registry);
    arc_token::register(&mut registry);
    whitelist_restrictions::register(&mut registry);
    router::register(&mut registry);
    factory::register(&mut registry);
    storefront::register(&mut registry);
    uv2_pair::register(&mut registry);
    morpho::register(&mut registry);
    erc20::register(&mut registry);
    yield_blacklist::register(&mut registry);
    registry
}

/// Parses a bare (no `0x` prefix), 64-hex-char topic0 literal into bytes at
/// COMPILE TIME. A malformed literal (wrong length, non-hex digit) is a
/// compile error, not a runtime panic — the earliest possible catch for a
/// hardcoded-constant authoring mistake. Each ABI submodule's `_TOPIC0`
/// consts are the single source of truth: `register()` uses them directly,
/// so there is exactly one copy of each literal (no drift between a
/// registration call and a test fixture).
pub(crate) const fn topic0_from_hex(s: &str) -> [u8; 32] {
    let bytes = s.as_bytes();
    assert!(
        bytes.len() == 64,
        "topic0 hex literal must be exactly 64 hex chars (32 bytes)"
    );
    let mut out = [0u8; 32];
    let mut i = 0;
    while i < 32 {
        out[i] = (hex_nibble(bytes[i * 2]) << 4) | hex_nibble(bytes[i * 2 + 1]);
        i += 1;
    }
    out
}

const fn hex_nibble(c: u8) -> u8 {
    match c {
        b'0'..=b'9' => c - b'0',
        b'a'..=b'f' => c - b'a' + 10,
        b'A'..=b'F' => c - b'A' + 10,
        _ => panic!("invalid hex digit in topic0 literal"),
    }
}
