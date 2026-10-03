//! Server-side address-kind classification for `GET /addresses/:address`.
//!
//! Mirrors the client resolver in rome-via `src/lib/address-kind.ts` —
//! precedence is address-intrinsic facts (burn sentinel, precompiles) over
//! indexer-derived facts (token, contract) over behavioral facts
//! (Solana-controlled), with EOA as the fallthrough. `factory` is deliberately
//! NOT a server kind: canonical-factory membership is deploy-time config
//! (`/config.json#canonicalFactories`), so the client refines `contract` →
//! `factory` itself. When you change the precedence here, mirror it in the
//! client resolver or the two will drift.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum AddressKind {
    Burn,
    PrecompileRome,
    PrecompileEth,
    Token,
    Contract,
    SolAccount,
    Eoa,
}

const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

/// The four Rome precompiles (lowercased). Same set the tx classifier in
/// `classify.rs` detects by address.
const ROME_PRECOMPILES: [&str; 4] = [
    "0xff00000000000000000000000000000000000007", // System
    "0xff00000000000000000000000000000000000008", // Solana CPI
    "0xff00000000000000000000000000000000000009", // Helper
    "0x4200000000000000000000000000000000000016", // Withdraw
];

/// Standard Ethereum precompiles 0x01–0x0a, all implemented by rome-evm.
fn is_eth_precompile(addr_lower: &str) -> bool {
    let Some(hex) = addr_lower.strip_prefix("0x") else {
        return false;
    };
    if hex.len() != 40 || !hex[..38].bytes().all(|b| b == b'0') {
        return false;
    }
    matches!(
        u8::from_str_radix(&hex[38..], 16),
        Ok(n) if (1..=0x0a).contains(&n)
    )
}

/// First match wins, top to bottom.
pub fn classify_address_kind(
    address: &str,
    is_token: bool,
    is_contract: bool,
    controlled_by_solana: bool,
) -> AddressKind {
    let addr = address.to_lowercase();
    if addr == ZERO_ADDRESS {
        return AddressKind::Burn;
    }
    if ROME_PRECOMPILES.contains(&addr.as_str()) {
        return AddressKind::PrecompileRome;
    }
    if is_eth_precompile(&addr) {
        return AddressKind::PrecompileEth;
    }
    if is_token {
        return AddressKind::Token;
    }
    if is_contract {
        return AddressKind::Contract;
    }
    if controlled_by_solana {
        return AddressKind::SolAccount;
    }
    AddressKind::Eoa
}

#[cfg(test)]
mod tests {
    use super::*;

    const ZERO: &str = "0x0000000000000000000000000000000000000000";
    const CPI: &str = "0xff00000000000000000000000000000000000008";
    const ECRECOVER: &str = "0x0000000000000000000000000000000000000001";
    const KZG: &str = "0x000000000000000000000000000000000000000a";
    const EOA: &str = "0x1111111111111111111111111111111111111111";

    #[test]
    fn burn_wins_over_everything() {
        assert_eq!(classify_address_kind(ZERO, true, true, true), AddressKind::Burn);
    }

    #[test]
    fn rome_precompiles_win_over_data_kinds() {
        for addr in [
            "0xff00000000000000000000000000000000000007",
            CPI,
            "0xff00000000000000000000000000000000000009",
            "0x4200000000000000000000000000000000000016",
        ] {
            assert_eq!(
                classify_address_kind(addr, true, true, true),
                AddressKind::PrecompileRome
            );
        }
    }

    #[test]
    fn eth_precompile_range_is_01_to_0a() {
        assert_eq!(classify_address_kind(ECRECOVER, false, false, false), AddressKind::PrecompileEth);
        assert_eq!(classify_address_kind(KZG, false, false, false), AddressKind::PrecompileEth);
        assert_eq!(
            classify_address_kind("0x000000000000000000000000000000000000000b", false, false, false),
            AddressKind::Eoa
        );
    }

    #[test]
    fn token_beats_contract_beats_sol_account_beats_eoa() {
        assert_eq!(classify_address_kind(EOA, true, true, true), AddressKind::Token);
        assert_eq!(classify_address_kind(EOA, false, true, true), AddressKind::Contract);
        assert_eq!(classify_address_kind(EOA, false, false, true), AddressKind::SolAccount);
        assert_eq!(classify_address_kind(EOA, false, false, false), AddressKind::Eoa);
    }

    #[test]
    fn case_insensitive_on_address() {
        let upper = CPI.to_uppercase().replace("0X", "0x");
        assert_eq!(classify_address_kind(&upper, false, false, false), AddressKind::PrecompileRome);
    }

    #[test]
    fn serializes_snake_case() {
        assert_eq!(
            serde_json::to_string(&AddressKind::PrecompileRome).unwrap(),
            "\"precompile_rome\""
        );
        assert_eq!(serde_json::to_string(&AddressKind::SolAccount).unwrap(), "\"sol_account\"");
        assert_eq!(serde_json::to_string(&AddressKind::Eoa).unwrap(), "\"eoa\"");
    }
}
