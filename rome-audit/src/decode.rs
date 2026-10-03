//! The decoder: `RawLog` + `(AbiRegistry, SourceKind)` → `DecodedEvent`.
//!
//! P0 CONTRACT (restated, IMPL-PLAN §3 P0): given a raw EVM log and the
//! caller-resolved `SourceKind` it came from, decode it into a typed
//! `DecodedEvent` using ONLY the registered ABI shape for that
//! `(source_kind, topic0)` — no DB, no Hercules connection, no ingest
//! cursor, no finality. An unregistered `(source_kind, topic0)` is a loud,
//! typed `DecodeError`, never a silent skip or a coerced guess.

use std::collections::BTreeMap;

use ethers::types::U256;

use crate::registry::{AbiRegistry, ArgSpec, ArgType};
use crate::types::{ArgValue, DecodedEvent, RawLog, SourceKind};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DecodeError {
    #[error("log has no topics (missing topic0)")]
    MissingTopic0,

    #[error("no registered ABI for source_kind={source_kind:?} topic0=0x{topic0_hex}")]
    UnknownTopic {
        source_kind: SourceKind,
        topic0_hex: String,
    },

    #[error("event {event_name}: expected {expected} indexed topic(s), got {actual}")]
    TopicCountMismatch {
        event_name: &'static str,
        expected: usize,
        actual: usize,
    },

    #[error("event {event_name} arg `{arg_name}`: data too short (need at least {expected_at_least} bytes, got {actual})")]
    DataTooShort {
        event_name: &'static str,
        arg_name: &'static str,
        expected_at_least: usize,
        actual: usize,
    },

    #[error("event {event_name} arg `{arg_name}`: malformed bool word 0x{word_hex} (ABI bool must be right-aligned 0 or 1)")]
    MalformedBool {
        event_name: &'static str,
        arg_name: &'static str,
        word_hex: String,
    },

    #[error("event {event_name} arg `{arg_name}`: {reason} (indexed dynamic types are unrecoverable — Solidity indexes them by keccak256 hash)")]
    IndexedDynamicType {
        event_name: &'static str,
        arg_name: &'static str,
        reason: &'static str,
    },

    #[error("event {event_name} arg `{arg_name}`: malformed dynamic-string encoding: {reason}")]
    MalformedDynamicArg {
        event_name: &'static str,
        arg_name: &'static str,
        reason: String,
    },

    #[error("event {event_name} arg `{arg_name}`: dynamic-string bytes are not valid UTF-8")]
    InvalidUtf8 {
        event_name: &'static str,
        arg_name: &'static str,
    },

    #[error("event {event_name}: {actual} bytes of data, expected exactly {expected} (all-static args — trailing bytes are never silently tolerated)")]
    TrailingData {
        event_name: &'static str,
        expected: usize,
        actual: usize,
    },
}

/// Decodes one raw log against the registered ABI for `(source_kind,
/// topics[0])`. See module docs for the P0 contract.
pub fn decode_log(
    registry: &AbiRegistry,
    source_kind: SourceKind,
    log: &RawLog,
) -> Result<DecodedEvent, DecodeError> {
    let topic0 = log.topics.first().ok_or(DecodeError::MissingTopic0)?;

    let descriptor =
        registry
            .lookup(source_kind, topic0)
            .ok_or_else(|| DecodeError::UnknownTopic {
                source_kind,
                topic0_hex: hex::encode(topic0),
            })?;

    let indexed_specs: Vec<&ArgSpec> = descriptor.args.iter().filter(|a| a.indexed).collect();
    let non_indexed_specs: Vec<&ArgSpec> = descriptor.args.iter().filter(|a| !a.indexed).collect();

    let indexed_topics = &log.topics[1..];
    if indexed_topics.len() != indexed_specs.len() {
        return Err(DecodeError::TopicCountMismatch {
            event_name: descriptor.event_name,
            expected: indexed_specs.len(),
            actual: indexed_topics.len(),
        });
    }

    let mut args = BTreeMap::new();

    for (spec, word) in indexed_specs.iter().zip(indexed_topics.iter()) {
        let value = decode_static_word(descriptor.event_name, spec, word)?;
        args.insert(spec.name.to_string(), value);
    }

    decode_data_args(
        descriptor.event_name,
        &non_indexed_specs,
        &log.data,
        &mut args,
    )?;

    Ok(DecodedEvent {
        source_kind,
        event_name: descriptor.event_name.to_string(),
        projection_tag: descriptor.projection_tag,
        args,
    })
}

/// Decodes non-indexed args from `data`. All P0 non-indexed types are
/// fixed-width (32-byte words), decoded sequentially; `ArgType::String` uses
/// standard ABI head/tail dynamic encoding (a 32-byte offset in the head,
/// then a 32-byte length + UTF-8 bytes at the offset).
///
/// No upfront aggregate length check: each arg's head word is bounds-checked
/// individually by `word_at`, so a short-data error attributes to the
/// SPECIFIC arg whose word was actually missing, not always the first spec
/// (a log with N args and room for N-1 words correctly names the Nth arg).
fn decode_data_args(
    event_name: &'static str,
    specs: &[&ArgSpec],
    data: &[u8],
    out: &mut BTreeMap<String, ArgValue>,
) -> Result<(), DecodeError> {
    let head_len = specs.len() * 32;
    let has_dynamic_arg = specs.iter().any(|s| s.ty == ArgType::String);

    for (i, spec) in specs.iter().enumerate() {
        let head_word = word_at(event_name, spec.name, data, i * 32)?;

        let value = if spec.ty == ArgType::String {
            decode_dynamic_string(event_name, spec.name, data, &head_word)?
        } else {
            decode_static_word(event_name, spec, &head_word)?
        };
        out.insert(spec.name.to_string(), value);
    }

    // Trailing-data policy (never coerce): when every non-indexed arg is a
    // fixed-width static type, the head IS the entire data payload — any
    // extra bytes are anomalous and rejected loudly rather than silently
    // ignored. An event with a dynamic (String) arg legitimately has more
    // data than `head_len` (the tail holds the offset-addressed content), so
    // it's exempt from the exact-length check.
    if !has_dynamic_arg && data.len() != head_len {
        return Err(DecodeError::TrailingData {
            event_name,
            expected: head_len,
            actual: data.len(),
        });
    }

    Ok(())
}

fn decode_dynamic_string(
    event_name: &'static str,
    arg_name: &'static str,
    data: &[u8],
    offset_word: &[u8; 32],
) -> Result<ArgValue, DecodeError> {
    let offset = u256_to_usize(event_name, arg_name, U256::from_big_endian(offset_word))?;
    let len_word = word_at(event_name, arg_name, data, offset)?;
    let len = u256_to_usize(event_name, arg_name, U256::from_big_endian(&len_word))?;

    let start = offset + 32;
    let end = start
        .checked_add(len)
        .ok_or_else(|| DecodeError::MalformedDynamicArg {
            event_name,
            arg_name,
            reason: format!("length {len} overflows at offset {start}"),
        })?;

    let bytes = data
        .get(start..end)
        .ok_or_else(|| DecodeError::MalformedDynamicArg {
            event_name,
            arg_name,
            reason: format!(
                "declared length {len} at offset {offset} exceeds data (data.len()={})",
                data.len()
            ),
        })?;

    let s = String::from_utf8(bytes.to_vec()).map_err(|_| DecodeError::InvalidUtf8 {
        event_name,
        arg_name,
    })?;

    Ok(ArgValue::String(s))
}

fn u256_to_usize(
    event_name: &'static str,
    arg_name: &'static str,
    v: U256,
) -> Result<usize, DecodeError> {
    if v > U256::from(usize::MAX) {
        return Err(DecodeError::MalformedDynamicArg {
            event_name,
            arg_name,
            reason: format!("offset/length {v} does not fit in usize"),
        });
    }
    Ok(v.as_usize())
}

fn word_at(
    event_name: &'static str,
    arg_name: &'static str,
    data: &[u8],
    offset: usize,
) -> Result<[u8; 32], DecodeError> {
    let end = offset.checked_add(32).ok_or(DecodeError::DataTooShort {
        event_name,
        arg_name,
        expected_at_least: usize::MAX,
        actual: data.len(),
    })?;
    let slice = data.get(offset..end).ok_or(DecodeError::DataTooShort {
        event_name,
        arg_name,
        expected_at_least: end,
        actual: data.len(),
    })?;
    let mut word = [0u8; 32];
    word.copy_from_slice(slice);
    Ok(word)
}

/// Decodes a single 32-byte ABI word into the fixed-width `ArgType`s
/// (`Address`, `Uint256`, `Bool`, `Bytes32`). Used for both indexed args
/// (word = a topic) and non-indexed static args (word = a `data` chunk).
/// `ArgType::String` is dynamic and never reaches this function through
/// `decode_data_args`'s non-indexed path (it special-cases String first);
/// if it's requested for an INDEXED position, that's a registration error —
/// rejected loudly rather than silently returning the (unrecoverable) hash.
fn decode_static_word(
    event_name: &'static str,
    spec: &ArgSpec,
    word: &[u8; 32],
) -> Result<ArgValue, DecodeError> {
    match spec.ty {
        ArgType::Address => {
            let addr = &word[12..32];
            Ok(ArgValue::Address(format!("0x{}", hex::encode(addr))))
        }
        ArgType::Uint256 => {
            let value = U256::from_big_endian(word);
            Ok(ArgValue::Uint256(value.to_string()))
        }
        ArgType::Bool => {
            let canonical = word[..31].iter().all(|&b| b == 0) && word[31] <= 1;
            if !canonical {
                return Err(DecodeError::MalformedBool {
                    event_name,
                    arg_name: spec.name,
                    word_hex: hex::encode(word),
                });
            }
            Ok(ArgValue::Bool(word[31] == 1))
        }
        ArgType::Bytes32 => Ok(ArgValue::Bytes32(format!("0x{}", hex::encode(word)))),
        ArgType::String => Err(DecodeError::IndexedDynamicType {
            event_name,
            arg_name: spec.name,
            reason: "String arg registered as indexed",
        }),
    }
}

#[cfg(test)]
mod tests {
    //! P0 CONTRACT UNDER TEST (restated): a raw EVM log decodes into a typed
    //! `DecodedEvent` using ONLY the registered ABI for its
    //! `(source_kind, topic0)`; an unregistered pair fails loudly
    //! (phantom-vocabulary guard); every registered topic0 is verified
    //! against an independently-computed keccak256 of its real Solidity
    //! event signature — never invented.

    use sha3::{Digest, Keccak256};

    use crate::abi::{build_registry, global_sanctions, whitelist_restrictions};
    use crate::types::{ProjectionTag, SourceKind};

    use super::*;

    fn keccak_topic0(signature: &str) -> [u8; 32] {
        let mut out = [0u8; 32];
        out.copy_from_slice(&Keccak256::digest(signature.as_bytes()));
        out
    }

    fn address_topic(addr: &[u8; 20]) -> [u8; 32] {
        let mut word = [0u8; 32];
        word[12..].copy_from_slice(addr);
        word
    }

    fn uint256_word(v: u64) -> [u8; 32] {
        let mut word = [0u8; 32];
        word[24..].copy_from_slice(&v.to_be_bytes());
        word
    }

    fn bool_word(v: bool) -> [u8; 32] {
        let mut word = [0u8; 32];
        word[31] = v as u8;
        word
    }

    const SANCTIONS_MODULE: [u8; 20] = [0x11; 20];
    const ARC_TOKEN: [u8; 20] = [0x22; 20];
    const WHITELIST_MODULE: [u8; 20] = [0x33; 20];
    const ALICE: [u8; 20] = [0xAA; 20];
    const BOB: [u8; 20] = [0xBB; 20];

    // ---- Topic0 correctness against real chain constants (IMPL-PLAN §1) ----
    //
    // These three are the LIVE on-chain values (the audit-trail spec
    // §4.2, GlobalSanctions deployed & verified on Hadrian 2026-08-16). If a
    // computed keccak of the real event signature doesn't match, the
    // registered signature/indexedness is wrong.

    #[test]
    fn sanctioned_topic0_matches_known_chain_constant() {
        let computed = keccak_topic0("Sanctioned(address)");
        let known = hex::decode("63d72e072b8020c07d946ff140e00fdb10195e4cd045c83d7db998ad1239d101")
            .unwrap();
        assert_eq!(computed.as_slice(), known.as_slice());
        assert_eq!(computed, global_sanctions::SANCTIONED_TOPIC0);
    }

    #[test]
    fn unsanctioned_topic0_matches_known_chain_constant() {
        let computed = keccak_topic0("Unsanctioned(address)");
        let known = hex::decode("6341cf58a5c9c850948623b3033047d31cfb1e741434141a9b4cafabe9cc9572")
            .unwrap();
        assert_eq!(computed.as_slice(), known.as_slice());
        assert_eq!(computed, global_sanctions::UNSANCTIONED_TOPIC0);
    }

    #[test]
    fn transfer_screened_topic0_matches_known_chain_constant() {
        let computed = keccak_topic0("TransferScreened(address,address,uint256)");
        let known = hex::decode("6722dfcb37c25fad39b85791cf1db49a3f82299501ed35c38a1736963f9c23a1")
            .unwrap();
        assert_eq!(computed.as_slice(), known.as_slice());
        assert_eq!(computed, global_sanctions::TRANSFER_SCREENED_TOPIC0);
    }

    // ---- GlobalSanctions V2 surface (P4b, 2026-08-20) — topic0 correctness
    // against the event signatures, independently re-verified via `cast
    // keccak` against the deployed V2 module. ----

    #[test]
    fn sanctioned_set_topic0_matches_known_chain_constant() {
        let computed = keccak_topic0("SanctionedSet(address,bool)");
        let known = hex::decode("e120f1c2a7416d3ef8776f69c89583cdc9ab7f2c3e525f55df52c5b5ce453608")
            .unwrap();
        assert_eq!(computed.as_slice(), known.as_slice());
        assert_eq!(computed, global_sanctions::SANCTIONED_SET_TOPIC0);
    }

    #[test]
    fn asset_frozen_set_topic0_matches_known_chain_constant() {
        let computed = keccak_topic0("AssetFrozenSet(address,bool)");
        let known = hex::decode("68e2297c215eb4de4ed9a376854b97df7af5350d4b0d2e489620509ce1147575")
            .unwrap();
        assert_eq!(computed.as_slice(), known.as_slice());
        assert_eq!(computed, global_sanctions::ASSET_FROZEN_SET_TOPIC0);
    }

    #[test]
    fn transfer_screened_v2_topic0_matches_known_chain_constant() {
        let computed = keccak_topic0("TransferScreened(address,address,address,uint256)");
        let known = hex::decode("edcf706d8177eb31edf03ec879920a41a678ebc47c659dcdd5ed35d660f8d11f")
            .unwrap();
        assert_eq!(computed.as_slice(), known.as_slice());
        assert_eq!(computed, global_sanctions::TRANSFER_SCREENED_V2_TOPIC0);
    }

    // ---- Topic0 vs keccak-of-signature only (M1, review 2026-08-16) ----
    //
    // UNLIKE the three GlobalSanctions tests above, these four do NOT check
    // against an independently-known on-chain constant — `keccak_topic0`
    // hashes a signature string I ALSO wrote (from reading the vendored
    // Solidity), so a self-consistent typo in BOTH the registered const and
    // this string (or a wrong `indexed` flag, which topic0 can't encode —
    // Transfer(address,address,uint256) hashes identically whether `from`
    // is indexed or not) would pass every one of these four tests. They
    // guard against a literal-transcription slip between this file and
    // `abi/*.rs` only. Catching a wrong `indexed` flag needs a replay of
    // real on-chain receipts that checks the resulting arg SHAPE (which
    // topics carried which values), not just the topic0 hash; that replay
    // runs against internal testnet data and is not part of this repo.

    #[test]
    fn transfer_topic0_matches_keccak_of_signature() {
        use crate::abi::arc_token;
        let computed = keccak_topic0("Transfer(address,address,uint256)");
        assert_eq!(computed, arc_token::TRANSFER_TOPIC0);
    }

    #[test]
    fn role_granted_topic0_matches_keccak_of_signature() {
        use crate::abi::arc_token;
        let computed = keccak_topic0("RoleGranted(bytes32,address,address)");
        assert_eq!(computed, arc_token::ROLE_GRANTED_TOPIC0);
    }

    #[test]
    fn whitelist_status_changed_topic0_matches_keccak_of_signature() {
        let computed = keccak_topic0("WhitelistStatusChanged(address,bool)");
        assert_eq!(
            computed,
            whitelist_restrictions::WHITELIST_STATUS_CHANGED_TOPIC0
        );
    }

    #[test]
    fn transfers_restriction_toggled_topic0_matches_keccak_of_signature() {
        let computed = keccak_topic0("TransfersRestrictionToggled(bool)");
        assert_eq!(
            computed,
            whitelist_restrictions::TRANSFERS_RESTRICTION_TOGGLED_TOPIC0
        );
    }

    // ---- P3 resolution-history events (Factory/Storefront/ArcToken) ----

    #[test]
    fn specific_restriction_module_set_topic0_matches_keccak_of_signature() {
        use crate::abi::arc_token;
        let computed = keccak_topic0("SpecificRestrictionModuleSet(bytes32,address)");
        assert_eq!(computed, arc_token::SPECIFIC_RESTRICTION_MODULE_SET_TOPIC0);
    }

    #[test]
    fn yield_token_updated_topic0_matches_keccak_of_signature() {
        use crate::abi::arc_token;
        let computed = keccak_topic0("YieldTokenUpdated(address)");
        assert_eq!(computed, arc_token::YIELD_TOKEN_UPDATED_TOPIC0);
    }

    #[test]
    fn token_registered_topic0_matches_keccak_of_signature() {
        use crate::abi::factory;
        let computed = keccak_topic0("TokenRegistered(address,address)");
        assert_eq!(computed, factory::TOKEN_REGISTERED_TOPIC0);
    }

    #[test]
    fn purchase_token_updated_topic0_matches_keccak_of_signature() {
        use crate::abi::storefront;
        let computed = keccak_topic0("PurchaseTokenUpdated(address)");
        assert_eq!(computed, storefront::PURCHASE_TOKEN_UPDATED_TOPIC0);
    }

    // ---- P4a: every new constant, independently cross-checked (M1-style —
    // same caveat as the four above: this catches a literal-transcription
    // slip between this table and `abi/*.rs`, never a wrong `indexed` flag,
    // which topic0 can't encode at all). Table-driven rather than one
    // function per constant (34 of them) — same guarantee, less repetition. ----

    #[test]
    fn every_p4a_registered_constant_matches_keccak_of_its_vendored_signature() {
        use crate::abi::{arc_token, factory, morpho, storefront, uv2_pair, whitelist_restrictions, yield_blacklist};

        let cases: &[(&str, [u8; 32])] = &[
            // arc_token
            ("YieldDistributed(uint256,address)", arc_token::YIELD_DISTRIBUTED_TOPIC0),
            ("Upgraded(address)", arc_token::UPGRADED_TOPIC0),
            ("RoleRevoked(bytes32,address,address)", arc_token::ROLE_REVOKED_TOPIC0),
            ("RoleAdminChanged(bytes32,bytes32,bytes32)", arc_token::ROLE_ADMIN_CHANGED_TOPIC0),
            ("Approval(address,address,uint256)", arc_token::APPROVAL_TOPIC0),
            ("Initialized(uint64)", arc_token::INITIALIZED_TOPIC0),
            ("TokenNameUpdated(string,string)", arc_token::TOKEN_NAME_UPDATED_TOPIC0),
            ("SymbolUpdated(string,string)", arc_token::SYMBOL_UPDATED_TOPIC0),
            ("TokenURIUpdated(string)", arc_token::TOKEN_URI_UPDATED_TOPIC0),
            // yield_blacklist
            ("YieldBlacklistUpdated(address,bool)", yield_blacklist::YIELD_BLACKLIST_UPDATED_TOPIC0),
            // whitelist_restrictions
            ("AddedToWhitelist(address)", whitelist_restrictions::ADDED_TO_WHITELIST_TOPIC0),
            ("RemovedFromWhitelist(address)", whitelist_restrictions::REMOVED_FROM_WHITELIST_TOPIC0),
            // storefront
            ("PurchaseMade(address,address,uint256,uint256)", storefront::PURCHASE_MADE_TOPIC0),
            ("TokenSaleEnabled(address,uint256,uint256)", storefront::TOKEN_SALE_ENABLED_TOPIC0),
            ("TokenSaleDisabled(address)", storefront::TOKEN_SALE_DISABLED_TOPIC0),
            ("StorefrontConfigSet(address,string)", storefront::STOREFRONT_CONFIG_SET_TOPIC0),
            ("TokenFactoryUpdated(address)", storefront::TOKEN_FACTORY_UPDATED_TOPIC0),
            // factory
            ("TokenCreated(address,address,address,string,string,string,uint8)", factory::TOKEN_CREATED_TOPIC0),
            ("ModuleLinked(address,address,bytes32)", factory::MODULE_LINKED_TOPIC0),
            ("ImplementationWhitelisted(address)", factory::IMPLEMENTATION_WHITELISTED_TOPIC0),
            ("ImplementationRemoved(address)", factory::IMPLEMENTATION_REMOVED_TOPIC0),
            ("TokenUpgraded(address,address)", factory::TOKEN_UPGRADED_TOPIC0),
            // uv2_pair
            ("Swap(address,uint256,uint256,uint256,uint256,address)", uv2_pair::SWAP_TOPIC0),
            ("Mint(address,uint256,uint256)", uv2_pair::MINT_TOPIC0),
            ("Burn(address,uint256,uint256,address)", uv2_pair::BURN_TOPIC0),
            ("Sync(uint112,uint112)", uv2_pair::SYNC_TOPIC0),
            // morpho
            ("Supply(bytes32,address,address,uint256,uint256)", morpho::SUPPLY_TOPIC0),
            ("Withdraw(bytes32,address,address,address,uint256,uint256)", morpho::WITHDRAW_TOPIC0),
            ("Borrow(bytes32,address,address,address,uint256,uint256)", morpho::BORROW_TOPIC0),
            ("Repay(bytes32,address,address,uint256,uint256)", morpho::REPAY_TOPIC0),
            ("SupplyCollateral(bytes32,address,address,uint256)", morpho::SUPPLY_COLLATERAL_TOPIC0),
            ("WithdrawCollateral(bytes32,address,address,address,uint256)", morpho::WITHDRAW_COLLATERAL_TOPIC0),
            ("Liquidate(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)", morpho::LIQUIDATE_TOPIC0),
            ("SetOwner(address)", morpho::SET_OWNER_TOPIC0),
            ("SetFee(bytes32,uint256)", morpho::SET_FEE_TOPIC0),
            ("SetFeeRecipient(address)", morpho::SET_FEE_RECIPIENT_TOPIC0),
            ("EnableIrm(address)", morpho::ENABLE_IRM_TOPIC0),
            ("EnableLltv(uint256)", morpho::ENABLE_LLTV_TOPIC0),
            ("FlashLoan(address,address,uint256)", morpho::FLASH_LOAN_TOPIC0),
            ("SetAuthorization(address,address,address,bool)", morpho::SET_AUTHORIZATION_TOPIC0),
            ("IncrementNonce(address,address,uint256)", morpho::INCREMENT_NONCE_TOPIC0),
            ("AccrueInterest(bytes32,uint256,uint256,uint256)", morpho::ACCRUE_INTEREST_TOPIC0),
        ];

        for (signature, registered) in cases {
            let computed = keccak_topic0(signature);
            assert_eq!(
                computed, *registered,
                "topic0 for `{signature}` doesn't match its registered constant"
            );
        }
    }

    // ---- Decode round-trip ----

    #[test]
    fn decodes_indexed_address_arg_sanctioned() {
        let registry = build_registry();
        let log = RawLog {
            address: SANCTIONS_MODULE,
            topics: vec![global_sanctions::SANCTIONED_TOPIC0, address_topic(&ALICE)],
            data: vec![],
        };

        let decoded = decode_log(&registry, SourceKind::GlobalSanctions, &log).unwrap();

        assert_eq!(decoded.source_kind, SourceKind::GlobalSanctions);
        assert_eq!(decoded.event_name, "Sanctioned");
        assert_eq!(decoded.projection_tag, ProjectionTag::Primary);
        assert_eq!(
            decoded.args.get("account"),
            Some(&ArgValue::Address(format!("0x{}", hex::encode(ALICE))))
        );
    }

    #[test]
    fn decodes_mixed_indexed_and_non_indexed_transfer_screened() {
        let registry = build_registry();
        let log = RawLog {
            address: SANCTIONS_MODULE,
            topics: vec![
                global_sanctions::TRANSFER_SCREENED_TOPIC0,
                address_topic(&ALICE),
                address_topic(&BOB),
            ],
            data: uint256_word(1_000_000).to_vec(),
        };

        let decoded = decode_log(&registry, SourceKind::GlobalSanctions, &log).unwrap();

        assert_eq!(decoded.event_name, "TransferScreened");
        assert_eq!(decoded.projection_tag, ProjectionTag::Primary);
        assert_eq!(
            decoded.args.get("from"),
            Some(&ArgValue::Address(format!("0x{}", hex::encode(ALICE))))
        );
        assert_eq!(
            decoded.args.get("to"),
            Some(&ArgValue::Address(format!("0x{}", hex::encode(BOB))))
        );
        assert_eq!(
            decoded.args.get("amount"),
            Some(&ArgValue::Uint256("1000000".to_string()))
        );
    }

    // ---- GlobalSanctions V2 surface (P4b, 2026-08-20) ----

    /// Decodes a `SanctionedSet` log with the account in an indexed topic
    /// and the flag in the data word, using synthetic values.
    #[test]
    fn decodes_indexed_address_and_data_bool_sanctioned_set() {
        const SANCTIONED_ACCOUNT: [u8; 20] = [0x5A; 20];
        let registry = build_registry();
        let log = RawLog {
            address: SANCTIONS_MODULE,
            topics: vec![
                global_sanctions::SANCTIONED_SET_TOPIC0,
                address_topic(&SANCTIONED_ACCOUNT),
            ],
            data: bool_word(true).to_vec(),
        };

        let decoded = decode_log(&registry, SourceKind::GlobalSanctions, &log).unwrap();

        assert_eq!(decoded.source_kind, SourceKind::GlobalSanctions);
        assert_eq!(decoded.event_name, "SanctionedSet");
        assert_eq!(decoded.projection_tag, ProjectionTag::Primary);
        assert_eq!(
            decoded.args.get("account"),
            Some(&ArgValue::Address(format!(
                "0x{}",
                hex::encode(SANCTIONED_ACCOUNT)
            )))
        );
        assert_eq!(decoded.args.get("sanctioned"), Some(&ArgValue::Bool(true)));
    }

    #[test]
    fn decodes_transfer_screened_v2_four_topics_asset_indexed() {
        let registry = build_registry();
        let log = RawLog {
            address: SANCTIONS_MODULE,
            topics: vec![
                global_sanctions::TRANSFER_SCREENED_V2_TOPIC0,
                address_topic(&ARC_TOKEN), // asset
                address_topic(&ALICE),     // from
                address_topic(&BOB),       // to
            ],
            data: uint256_word(1_000_000).to_vec(),
        };

        let decoded = decode_log(&registry, SourceKind::GlobalSanctions, &log).unwrap();

        assert_eq!(decoded.event_name, "TransferScreened");
        assert_eq!(decoded.projection_tag, ProjectionTag::Primary);
        assert_eq!(decoded.args.len(), 4, "asset/from/to/amount");
        assert_eq!(
            decoded.args.get("asset"),
            Some(&ArgValue::Address(format!("0x{}", hex::encode(ARC_TOKEN))))
        );
        assert_eq!(
            decoded.args.get("from"),
            Some(&ArgValue::Address(format!("0x{}", hex::encode(ALICE))))
        );
        assert_eq!(
            decoded.args.get("to"),
            Some(&ArgValue::Address(format!("0x{}", hex::encode(BOB))))
        );
        assert_eq!(
            decoded.args.get("amount"),
            Some(&ArgValue::Uint256("1000000".to_string()))
        );
    }

    #[test]
    fn decodes_role_granted_under_global_sanctions() {
        use crate::abi::arc_token;

        let role = [0x77u8; 32];
        let registry = build_registry();
        let log = RawLog {
            address: SANCTIONS_MODULE,
            topics: vec![
                arc_token::ROLE_GRANTED_TOPIC0,
                role,
                address_topic(&ALICE), // account
                address_topic(&BOB),   // sender
            ],
            data: vec![],
        };

        let decoded = decode_log(&registry, SourceKind::GlobalSanctions, &log).unwrap();

        assert_eq!(decoded.event_name, "RoleGranted");
        assert_eq!(decoded.projection_tag, ProjectionTag::Supporting);
        assert_eq!(
            decoded.args.get("role"),
            Some(&ArgValue::Bytes32(format!("0x{}", hex::encode(role))))
        );
        assert_eq!(
            decoded.args.get("account"),
            Some(&ArgValue::Address(format!("0x{}", hex::encode(ALICE))))
        );
        assert_eq!(
            decoded.args.get("sender"),
            Some(&ArgValue::Address(format!("0x{}", hex::encode(BOB))))
        );
    }

    /// Regression: registering the V2 surface (SanctionedSet, a new
    /// TransferScreened topic0, AssetFrozenSet, AccessControl) must not
    /// disturb V1's `Sanctioned` — a distinct topic0 under the same
    /// `SourceKind::GlobalSanctions` key.
    #[test]
    fn v1_sanctioned_still_decodes_after_v2_registration() {
        let registry = build_registry();
        let log = RawLog {
            address: SANCTIONS_MODULE,
            topics: vec![global_sanctions::SANCTIONED_TOPIC0, address_topic(&ALICE)],
            data: vec![],
        };

        let decoded = decode_log(&registry, SourceKind::GlobalSanctions, &log).unwrap();

        assert_eq!(decoded.event_name, "Sanctioned");
        assert_eq!(decoded.projection_tag, ProjectionTag::Primary);
        assert_eq!(
            decoded.args.get("account"),
            Some(&ArgValue::Address(format!("0x{}", hex::encode(ALICE))))
        );
    }

    #[test]
    fn decodes_bool_in_data_transfers_restriction_toggled() {
        let registry = build_registry();
        let log = RawLog {
            address: WHITELIST_MODULE,
            topics: vec![whitelist_restrictions::TRANSFERS_RESTRICTION_TOGGLED_TOPIC0],
            data: bool_word(true).to_vec(),
        };

        let decoded = decode_log(&registry, SourceKind::Axis1Module, &log).unwrap();

        assert_eq!(decoded.event_name, "TransfersRestrictionToggled");
        assert_eq!(
            decoded.args.get("transfersAllowed"),
            Some(&ArgValue::Bool(true))
        );
    }

    #[test]
    fn decodes_whitelist_status_changed_indexed_address_plus_data_bool() {
        let registry = build_registry();
        let log = RawLog {
            address: WHITELIST_MODULE,
            topics: vec![
                whitelist_restrictions::WHITELIST_STATUS_CHANGED_TOPIC0,
                address_topic(&ALICE),
            ],
            data: bool_word(false).to_vec(),
        };

        let decoded = decode_log(&registry, SourceKind::Axis1Module, &log).unwrap();

        assert_eq!(decoded.event_name, "WhitelistStatusChanged");
        assert_eq!(
            decoded.args.get("account"),
            Some(&ArgValue::Address(format!("0x{}", hex::encode(ALICE))))
        );
        assert_eq!(
            decoded.args.get("isWhitelisted"),
            Some(&ArgValue::Bool(false))
        );
    }

    #[test]
    fn decodes_transfer_two_indexed_one_data() {
        use crate::abi::arc_token;
        let registry = build_registry();
        let log = RawLog {
            address: ARC_TOKEN,
            topics: vec![
                arc_token::TRANSFER_TOPIC0,
                address_topic(&ALICE),
                address_topic(&BOB),
            ],
            data: uint256_word(42).to_vec(),
        };

        let decoded = decode_log(&registry, SourceKind::ArcToken, &log).unwrap();

        assert_eq!(decoded.event_name, "Transfer");
        assert_eq!(decoded.projection_tag, ProjectionTag::Primary);
        assert_eq!(
            decoded.args.get("from"),
            Some(&ArgValue::Address(format!("0x{}", hex::encode(ALICE))))
        );
        assert_eq!(
            decoded.args.get("to"),
            Some(&ArgValue::Address(format!("0x{}", hex::encode(BOB))))
        );
        assert_eq!(
            decoded.args.get("value"),
            Some(&ArgValue::Uint256("42".to_string()))
        );
    }

    // ---- Projection tag: RoleGranted lands as Supporting, not dropped ----

    #[test]
    fn role_granted_decodes_as_supporting_all_three_args_indexed() {
        use crate::abi::arc_token;
        let registry = build_registry();
        let role = [0x77u8; 32];
        let log = RawLog {
            address: ARC_TOKEN,
            topics: vec![
                arc_token::ROLE_GRANTED_TOPIC0,
                role,
                address_topic(&ALICE),
                address_topic(&BOB),
            ],
            data: vec![],
        };

        let decoded = decode_log(&registry, SourceKind::ArcToken, &log).unwrap();

        assert_eq!(decoded.event_name, "RoleGranted");
        assert_eq!(decoded.projection_tag, ProjectionTag::Supporting);
        assert_eq!(
            decoded.args.get("role"),
            Some(&ArgValue::Bytes32(format!("0x{}", hex::encode(role))))
        );
        assert_eq!(
            decoded.args.get("account"),
            Some(&ArgValue::Address(format!("0x{}", hex::encode(ALICE))))
        );
        assert_eq!(
            decoded.args.get("sender"),
            Some(&ArgValue::Address(format!("0x{}", hex::encode(BOB))))
        );
    }

    #[test]
    fn transfer_decodes_as_primary() {
        use crate::abi::arc_token;
        let registry = build_registry();
        let log = RawLog {
            address: ARC_TOKEN,
            topics: vec![
                arc_token::TRANSFER_TOPIC0,
                address_topic(&ALICE),
                address_topic(&BOB),
            ],
            data: uint256_word(1).to_vec(),
        };
        let decoded = decode_log(&registry, SourceKind::ArcToken, &log).unwrap();
        assert_eq!(decoded.projection_tag, ProjectionTag::Primary);
    }

    // ---- Phantom guard ----

    #[test]
    fn unregistered_topic0_on_known_contract_is_a_loud_error() {
        let registry = build_registry();
        let fabricated_topic0 = [0xDE; 32]; // not registered for any source_kind
        let log = RawLog {
            address: SANCTIONS_MODULE,
            topics: vec![fabricated_topic0, address_topic(&ALICE)],
            data: vec![],
        };

        let err = decode_log(&registry, SourceKind::GlobalSanctions, &log).unwrap_err();

        match err {
            DecodeError::UnknownTopic {
                source_kind,
                topic0_hex,
            } => {
                assert_eq!(source_kind, SourceKind::GlobalSanctions);
                assert_eq!(topic0_hex, hex::encode(fabricated_topic0));
            }
            other => panic!("expected UnknownTopic, got {other:?}"),
        }
    }

    #[test]
    fn a_topic0_registered_under_a_different_source_kind_is_also_unknown() {
        // Sanctioned's topic0 IS registered — but only for GlobalSanctions,
        // never for ArcToken. The registry key is the PAIR, not the topic0
        // alone: a real event replayed against the wrong contract kind must
        // still be rejected, not coerced.
        let registry = build_registry();
        let log = RawLog {
            address: ARC_TOKEN,
            topics: vec![global_sanctions::SANCTIONED_TOPIC0, address_topic(&ALICE)],
            data: vec![],
        };

        let err = decode_log(&registry, SourceKind::ArcToken, &log).unwrap_err();
        assert!(matches!(err, DecodeError::UnknownTopic { .. }));
    }

    #[test]
    fn missing_topic0_is_a_loud_error_not_a_panic() {
        let registry = build_registry();
        let log = RawLog {
            address: SANCTIONS_MODULE,
            topics: vec![],
            data: vec![],
        };
        let err = decode_log(&registry, SourceKind::GlobalSanctions, &log).unwrap_err();
        assert_eq!(err, DecodeError::MissingTopic0);
    }

    // ---- Arg-count / malformed: loud error, never a panic or silent truncation ----

    #[test]
    fn wrong_indexed_topic_count_is_a_loud_error() {
        let registry = build_registry();
        // TransferScreened expects 2 indexed topics (from, to) + topic0 = 3
        // total; give it only topic0 + one indexed topic.
        let log = RawLog {
            address: SANCTIONS_MODULE,
            topics: vec![
                global_sanctions::TRANSFER_SCREENED_TOPIC0,
                address_topic(&ALICE),
            ],
            data: uint256_word(1).to_vec(),
        };

        let err = decode_log(&registry, SourceKind::GlobalSanctions, &log).unwrap_err();

        match err {
            DecodeError::TopicCountMismatch {
                event_name,
                expected,
                actual,
            } => {
                assert_eq!(event_name, "TransferScreened");
                assert_eq!(expected, 2);
                assert_eq!(actual, 1);
            }
            other => panic!("expected TopicCountMismatch, got {other:?}"),
        }
    }

    #[test]
    fn extra_indexed_topics_is_also_a_loud_error() {
        let registry = build_registry();
        let log = RawLog {
            address: SANCTIONS_MODULE,
            topics: vec![
                global_sanctions::SANCTIONED_TOPIC0,
                address_topic(&ALICE),
                address_topic(&BOB), // extra, unexpected
            ],
            data: vec![],
        };

        let err = decode_log(&registry, SourceKind::GlobalSanctions, &log).unwrap_err();
        assert!(matches!(
            err,
            DecodeError::TopicCountMismatch {
                expected: 1,
                actual: 2,
                ..
            }
        ));
    }

    #[test]
    fn truncated_data_is_a_loud_error_not_silent_truncation() {
        let registry = build_registry();
        // TransfersRestrictionToggled needs a full 32-byte word for its bool;
        // give it 4 bytes.
        let log = RawLog {
            address: WHITELIST_MODULE,
            topics: vec![whitelist_restrictions::TRANSFERS_RESTRICTION_TOGGLED_TOPIC0],
            data: vec![0u8; 4],
        };

        let err = decode_log(&registry, SourceKind::Axis1Module, &log).unwrap_err();

        match err {
            DecodeError::DataTooShort {
                event_name,
                expected_at_least,
                actual,
                ..
            } => {
                assert_eq!(event_name, "TransfersRestrictionToggled");
                assert_eq!(expected_at_least, 32);
                assert_eq!(actual, 4);
            }
            other => panic!("expected DataTooShort, got {other:?}"),
        }
    }

    #[test]
    fn non_canonical_bool_word_is_a_loud_error() {
        let registry = build_registry();
        let mut malformed = [0u8; 32];
        malformed[0] = 1; // non-zero outside the last byte — not ABI-canonical
        let log = RawLog {
            address: WHITELIST_MODULE,
            topics: vec![whitelist_restrictions::TRANSFERS_RESTRICTION_TOGGLED_TOPIC0],
            data: malformed.to_vec(),
        };

        let err = decode_log(&registry, SourceKind::Axis1Module, &log).unwrap_err();
        assert!(matches!(err, DecodeError::MalformedBool { .. }));
    }

    #[test]
    fn data_too_short_names_the_specific_arg_whose_word_is_missing_not_always_the_first() {
        // M2 (review 2026-08-16): a two-non-indexed-arg event with room
        // for the FIRST arg's word but not the second's must name the SECOND
        // arg, not unconditionally report the first spec.
        use crate::registry::{ArgSpec, ArgType, EventDescriptor};
        use crate::types::ProjectionTag;

        let mut registry = AbiRegistry::new();
        let topic0 = [0x97u8; 32];
        registry.register(
            SourceKind::ArcToken,
            topic0,
            EventDescriptor {
                event_name: "TestTwoUintEvent",
                projection_tag: ProjectionTag::Supporting,
                args: vec![
                    ArgSpec::new("first", ArgType::Uint256, false),
                    ArgSpec::new("second", ArgType::Uint256, false),
                ],
            },
        );
        let log = RawLog {
            address: ARC_TOKEN,
            topics: vec![topic0],
            data: uint256_word(1).to_vec(), // only 32 bytes — room for "first", not "second"
        };

        let err = decode_log(&registry, SourceKind::ArcToken, &log).unwrap_err();

        match err {
            DecodeError::DataTooShort {
                event_name,
                arg_name,
                expected_at_least,
                actual,
            } => {
                assert_eq!(event_name, "TestTwoUintEvent");
                assert_eq!(arg_name, "second");
                assert_eq!(expected_at_least, 64);
                assert_eq!(actual, 32);
            }
            other => panic!("expected DataTooShort naming `second`, got {other:?}"),
        }
    }

    #[test]
    fn trailing_data_on_an_all_static_event_is_a_loud_error() {
        // M3 (review 2026-08-16, "decide, don't default" on trailing
        // bytes): TransfersRestrictionToggled has exactly one non-indexed
        // bool (32 bytes). Extra trailing bytes on an all-static event must
        // never be silently tolerated on an audit decoder.
        let registry = build_registry();
        let mut data = bool_word(true).to_vec();
        data.extend_from_slice(&[0xFFu8; 32]); // anomalous trailing word
        let log = RawLog {
            address: WHITELIST_MODULE,
            topics: vec![whitelist_restrictions::TRANSFERS_RESTRICTION_TOGGLED_TOPIC0],
            data,
        };

        let err = decode_log(&registry, SourceKind::Axis1Module, &log).unwrap_err();

        match err {
            DecodeError::TrailingData {
                event_name,
                expected,
                actual,
            } => {
                assert_eq!(event_name, "TransfersRestrictionToggled");
                assert_eq!(expected, 32);
                assert_eq!(actual, 64);
            }
            other => panic!("expected TrailingData, got {other:?}"),
        }
    }

    #[test]
    fn trailing_data_is_tolerated_when_the_event_has_a_dynamic_arg() {
        // The exact-length check is exempt for events with a String arg —
        // the tail legitimately extends past `head_len` (offset + length +
        // padded UTF-8 bytes), so this must NOT raise TrailingData.
        use crate::registry::{ArgSpec, ArgType, EventDescriptor};
        use crate::types::ProjectionTag;

        let mut registry = AbiRegistry::new();
        let topic0 = [0x96u8; 32];
        registry.register(
            SourceKind::ArcToken,
            topic0,
            EventDescriptor {
                event_name: "TestStringEvent2",
                projection_tag: ProjectionTag::Supporting,
                args: vec![ArgSpec::new("label", ArgType::String, false)],
            },
        );
        let mut data = Vec::new();
        data.extend_from_slice(&uint256_word(32));
        data.extend_from_slice(&uint256_word(5));
        let mut payload = b"hello".to_vec();
        payload.resize(32, 0);
        data.extend_from_slice(&payload);

        let log = RawLog {
            address: ARC_TOKEN,
            topics: vec![topic0],
            data,
        };

        let decoded = decode_log(&registry, SourceKind::ArcToken, &log).unwrap();
        assert_eq!(
            decoded.args.get("label"),
            Some(&ArgValue::String("hello".to_string()))
        );
    }

    // ---- ArgValue::String: not dead code — dynamic ABI decode is exercised
    // directly (no P0-scoped event uses it; this proves the arm works). ----

    #[test]
    fn decodes_non_indexed_dynamic_string() {
        use crate::registry::{ArgSpec, ArgType, EventDescriptor};
        use crate::types::ProjectionTag;

        let mut registry = AbiRegistry::new();
        let topic0 = [0x99u8; 32];
        registry.register(
            SourceKind::ArcToken,
            topic0,
            EventDescriptor {
                event_name: "TestStringEvent",
                projection_tag: ProjectionTag::Supporting,
                args: vec![ArgSpec::new("label", ArgType::String, false)],
            },
        );

        // Standard ABI dynamic encoding: one head word (offset = 32), then
        // at that offset: a length word, then the UTF-8 bytes padded to 32.
        let mut data = Vec::new();
        data.extend_from_slice(&uint256_word(32)); // offset
        data.extend_from_slice(&uint256_word(5)); // length
        let mut payload = b"hello".to_vec();
        payload.resize(32, 0); // right-pad to a 32-byte boundary
        data.extend_from_slice(&payload);

        let log = RawLog {
            address: ARC_TOKEN,
            topics: vec![topic0],
            data,
        };

        let decoded = decode_log(&registry, SourceKind::ArcToken, &log).unwrap();
        assert_eq!(
            decoded.args.get("label"),
            Some(&ArgValue::String("hello".to_string()))
        );
    }

    #[test]
    fn indexed_string_arg_is_a_registration_error_not_a_hash_coercion() {
        use crate::registry::{ArgSpec, ArgType, EventDescriptor};
        use crate::types::ProjectionTag;

        let mut registry = AbiRegistry::new();
        let topic0 = [0x98u8; 32];
        registry.register(
            SourceKind::ArcToken,
            topic0,
            EventDescriptor {
                event_name: "TestBadIndexedString",
                projection_tag: ProjectionTag::Supporting,
                args: vec![ArgSpec::new("label", ArgType::String, true)],
            },
        );
        let log = RawLog {
            address: ARC_TOKEN,
            topics: vec![topic0, [0x55u8; 32]],
            data: vec![],
        };

        let err = decode_log(&registry, SourceKind::ArcToken, &log).unwrap_err();
        assert!(matches!(err, DecodeError::IndexedDynamicType { .. }));
    }
}
