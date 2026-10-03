//! ERC-7201 namespaced storage slot for `ArcTokenStorage` (capture §1.2,
//! corrected 2026-08-17): `restrictionsRouter` is a **private** field with
//! **no getter** and no event that carries it — so it is read directly from
//! token STORAGE (`eth_getStorageAt`) rather than via an `eth_call`. The slot
//! is DERIVED IN CODE (repo Hard Rule 6 — never a pasted literal) per the
//! standard ERC-7201 formula:
//!
//! ```text
//! slot = keccak256(abi.encode(keccak256("asset.token.storage") - 1)) & ~0xff
//! ```
//!
//! `abi.encode(uint256 n)` for a single static value is just `n`'s 32-byte
//! big-endian word (no head/tail indirection — the same shape
//! `ethers::types::U256::to_big_endian` produces), so the derivation needs no
//! ABI-encoding machinery beyond that.
//!
//! `ArcTokenStorage`'s FIRST field is `restrictionsRouter` (`ArcToken.sol:62`),
//! so the storage word AT this slot carries the router address in its low 20
//! bytes (standard Solidity right-aligned address packing) — verified live:
//! `eth_getStorageAt(SANC 0x9cd690069f279c3912506e8b642fdcb13da7f847, slot)`
//! on Hadrian returned a word whose low 20 bytes are
//! `0x9657d932fbb434362122f034b76aa0981380d507`, the fresh sanctions router.

use ethers::types::U256;
use ethers::utils::keccak256;

/// The ERC-7201 namespaced slot of `ArcTokenStorage`
/// (`erc7201:asset.token.storage`). Self-checked in tests against the
/// vendored `ArcToken.sol:62` constant
/// `0xf52c08b2e4132efdd78c079b339999bf65bd68aae758ed08b1bb84dc8f47c000` — this
/// function computes that value from the namespace STRING, never the other
/// way around.
pub fn restrictions_router_slot() -> [u8; 32] {
    let namespace_hash = keccak256(b"asset.token.storage");
    let n = U256::from_big_endian(&namespace_hash) - U256::one();
    let mut encoded = [0u8; 32];
    n.to_big_endian(&mut encoded);
    let h2 = keccak256(encoded);
    let masked = U256::from_big_endian(&h2) & !U256::from(0xffu64);
    let mut slot = [0u8; 32];
    masked.to_big_endian(&mut slot);
    slot
}

/// Decodes a 32-byte storage word into the right-aligned 20-byte address it
/// carries (standard Solidity address packing — the low 20 bytes; whatever
/// occupies the upper 12 bytes, if `ArcTokenStorage` ever packs a second
/// field into the same slot, is not this crate's concern here).
pub fn decode_address_from_storage_word(word: [u8; 32]) -> [u8; 20] {
    let mut addr = [0u8; 20];
    addr.copy_from_slice(&word[12..32]);
    addr
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Task self-check: the CODE-DERIVED slot must equal the vendored
    /// `ArcToken.sol:62` constant `0xf52c08b2…c000`.
    #[test]
    fn derived_slot_matches_the_vendored_arc_token_constant() {
        let slot = restrictions_router_slot();
        assert_eq!(
            hex::encode(slot),
            "f52c08b2e4132efdd78c079b339999bf65bd68aae758ed08b1bb84dc8f47c000"
        );
    }

    /// Deriving twice must agree (pure function, no hidden state).
    #[test]
    fn derivation_is_deterministic() {
        assert_eq!(restrictions_router_slot(), restrictions_router_slot());
    }

    /// Decodes a storage word carrying the live-verified fresh-router
    /// address in its low 20 bytes (capture §1.2's confirmed Hadrian read) —
    /// the upper 12 bytes are non-zero filler to prove only the low 20 are
    /// read, not the whole word compared byte-for-byte.
    #[test]
    fn decodes_the_router_address_from_a_storage_word() {
        let router: [u8; 20] = hex::decode("9657d932fbb434362122f034b76aa0981380d507")
            .unwrap()
            .try_into()
            .unwrap();
        let mut word = [0xAAu8; 32]; // non-zero filler in the upper 12 bytes
        word[12..].copy_from_slice(&router);

        assert_eq!(decode_address_from_storage_word(word), router);
    }

    #[test]
    fn decodes_the_zero_address_from_an_all_zero_word() {
        assert_eq!(decode_address_from_storage_word([0u8; 32]), [0u8; 20]);
    }
}
