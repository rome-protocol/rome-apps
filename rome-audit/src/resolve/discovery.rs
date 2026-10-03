//! S4 — on-chain token DISCOVERY (design doc §3b point 5): replaces the
//! requirement of a static `[resolve].tokens` list with enumeration of the
//! registry-pinned factory's admission events, so a newly-registered Arc
//! token is captured the moment its registration event lands ("the first
//! event IS the discovery").
//!
//! **Enumeration = `TokenRegistered` ∪ `TokenCreated`.** Verified in
//! the Arc contracts: Rome issuance is piecewise `registerToken` → `TokenRegistered`,
//! NOT `createToken` — `TokenCreated`-only discovery would find ZERO
//! Rome-issued tokens. Both topics are already registered + decoded under
//! `SourceKind::Factory` (`abi::factory`); this module adds no new decoding.
//!
//! **Reuses the existing historical-read seam.** Enumeration reads via
//! `rpc.logs_for(factory, topic0)` — the SAME call
//! [`super::resolver::token_registered_block`] already makes. In
//! production `rpc` is [`super::hercules_rpc::HerculesResolverRpc`], so this
//! is a Hercules-DB read; no new RPC surface, no new query.
//!
//! **Fail-loud, not quarantine.** An undecodable/mis-shaped factory admission
//! log wedges the resolve pass loudly-and-retries (like the rest of the resolve
//! pass's hold-and-stall-visibly doctrine), rather than quarantine-and-continue
//! the way *ingest* handles an undecodable event. This is deliberate: an
//! address+topic0-matched factory log IS a genuine admission emission, so a
//! decode failure means the ABI no longer matches the chain (a factory upgrade
//! changed an event shape) — a condition an operator must SEE, not skip past.
//!
//! **Discovery is a hint, never the admission gate (capture §3b point 3).**
//! Every candidate must independently pass the UNCHANGED §1.1
//! authoritativeness gate ([`super::resolver::passes_authoritativeness_gate`]
//! — the same check [`super::resolver::resolve`] itself performs) before it
//! ever enters the resolve set. A deployed-but-never-registered proxy fails
//! the gate by design — that's correct, not a miss.

use std::collections::BTreeSet;

use crate::decode::{decode_log, DecodeError};
use crate::registry::AbiRegistry;
use crate::types::{ArgValue, RawLog, SourceKind};

use super::registry_source::RegistrySource;
use super::resolver::passes_authoritativeness_gate;
use super::rpc::{LogEntry, ResolverRpc, RpcError};

#[derive(Debug, thiserror::Error)]
pub enum DiscoveryError {
    #[error(transparent)]
    Rpc(#[from] RpcError),
    #[error(transparent)]
    Decode(#[from] DecodeError),
    #[error("factory admission event arg `{arg}` is not a well-formed 20-byte address: {reason}")]
    MalformedAddress { arg: &'static str, reason: String },
}

fn to_raw_log(log: &LogEntry) -> RawLog {
    RawLog {
        address: log.address,
        topics: log.topics.clone(),
        data: log.data.clone(),
    }
}

fn parse_hex20(arg: &'static str, s: &str) -> Result<[u8; 20], DiscoveryError> {
    let bytes = hex::decode(s.trim_start_matches("0x"))
        .map_err(|e| DiscoveryError::MalformedAddress { arg, reason: e.to_string() })?;
    bytes
        .try_into()
        .map_err(|v: Vec<u8>| DiscoveryError::MalformedAddress {
            arg,
            reason: format!("must be 20 bytes, got {}", v.len()),
        })
}

/// Enumerates every distinct token address the registry-pinned `factory`
/// has admitted — the UNION of `TokenRegistered.token` and
/// `TokenCreated.tokenAddress` histories. A token appearing under both
/// events (or emitting either event more than once) is de-duplicated by the
/// `BTreeSet`.
pub async fn discover_factory_tokens(
    rpc: &dyn ResolverRpc,
    abi: &AbiRegistry,
    factory: [u8; 20],
) -> Result<BTreeSet<[u8; 20]>, DiscoveryError> {
    let mut tokens = BTreeSet::new();
    for (topic0, arg_name) in [
        (crate::abi::factory::TOKEN_REGISTERED_TOPIC0, "token"),
        (crate::abi::factory::TOKEN_CREATED_TOPIC0, "tokenAddress"),
    ] {
        let logs = rpc.logs_for(factory, topic0).await?;
        for log in &logs {
            // `decode_log`'s `?` above is the fail-loud gate: a mis-shaped
            // factory admission log (ABI drift from a factory upgrade) errors
            // here as DiscoveryError::Decode and wedges the pass loudly — it is
            // never silently skipped. Reaching this line means decode succeeded
            // against the fixed `abi::factory` shape, so the token address arg
            // is present by construction; the `if let` is the extract, not a gate.
            let decoded = decode_log(abi, SourceKind::Factory, &to_raw_log(log))?;
            if let Some(ArgValue::Address(s)) = decoded.args.get(arg_name) {
                tokens.insert(parse_hex20(arg_name, s)?);
            }
        }
    }
    Ok(tokens)
}

/// [`discover_factory_tokens`], filtered through the UNCHANGED §1.1
/// authoritativeness gate — a discovered candidate that isn't ALSO
/// factory-admitted on-chain (or registry-listed) never enters the resolve
/// set. Order is caller-irrelevant (the caller unions this into a set), so
/// this returns in whatever order `discover_factory_tokens`' `BTreeSet`
/// iterates (address-sorted).
pub async fn discover_authoritative_tokens(
    rpc: &dyn ResolverRpc,
    registry: &dyn RegistrySource,
    abi: &AbiRegistry,
    factory: [u8; 20],
) -> Result<Vec<[u8; 20]>, DiscoveryError> {
    let candidates = discover_factory_tokens(rpc, abi, factory).await?;
    let mut authoritative = Vec::with_capacity(candidates.len());
    for token in candidates {
        if passes_authoritativeness_gate(token, factory, registry, rpc).await? {
            authoritative.push(token);
        }
    }
    Ok(authoritative)
}

#[cfg(test)]
mod tests {
    //! Pure, fixture-backed tests (zero DB, zero network) — same doubles
    //! `resolver.rs`'s own unit tests use. `discover_factory_tokens`/
    //! `discover_authoritative_tokens` go through the injected
    //! `ResolverRpc`/`RegistrySource` seams exactly like `resolve()` does,
    //! so the fake is a faithful stand-in for `HerculesResolverRpc` here.

    use super::*;
    use crate::abi::build_registry;
    use crate::resolve::fixture::{FakeRegistry, FakeRpc};

    fn addr(b: u8) -> [u8; 20] {
        [b; 20]
    }

    fn address_word(a: [u8; 20]) -> [u8; 32] {
        let mut w = [0u8; 32];
        w[12..].copy_from_slice(&a);
        w
    }

    /// A valid ABI encoding for `TokenCreated`'s four non-indexed args
    /// (`name`, `symbol`, `tokenUri` — dynamic strings — then `decimals` —
    /// a static uint256 word), standard Solidity head/tail dynamic layout.
    fn encode_token_created_data(name: &str, symbol: &str, token_uri: &str, decimals: u64) -> Vec<u8> {
        fn word_usize(n: usize) -> [u8; 32] {
            let mut w = [0u8; 32];
            w[24..].copy_from_slice(&(n as u64).to_be_bytes());
            w
        }
        fn pad32(bytes: &[u8]) -> Vec<u8> {
            let mut v = bytes.to_vec();
            let rem = v.len() % 32;
            if rem != 0 {
                v.extend(std::iter::repeat_n(0u8, 32 - rem));
            }
            v
        }
        fn string_tail(s: &str) -> Vec<u8> {
            let mut out = word_usize(s.len()).to_vec();
            out.extend(pad32(s.as_bytes()));
            out
        }

        let head_len = 4 * 32; // name, symbol, tokenUri (offsets) + decimals (inline value)
        let name_tail = string_tail(name);
        let symbol_tail = string_tail(symbol);
        let uri_tail = string_tail(token_uri);

        let name_offset = head_len;
        let symbol_offset = name_offset + name_tail.len();
        let uri_offset = symbol_offset + symbol_tail.len();

        let mut out = Vec::new();
        out.extend(word_usize(name_offset));
        out.extend(word_usize(symbol_offset));
        out.extend(word_usize(uri_offset));
        out.extend(word_usize(decimals as usize));
        out.extend(name_tail);
        out.extend(symbol_tail);
        out.extend(uri_tail);
        out
    }

    fn token_registered_log(block_number: i64, factory: [u8; 20], token: [u8; 20], implementation: [u8; 20]) -> LogEntry {
        LogEntry {
            block_number,
            tx_index: 0,
            log_index: 0,
            address: factory,
            topics: vec![
                crate::abi::factory::TOKEN_REGISTERED_TOPIC0,
                address_word(token),
                address_word(implementation),
            ],
            data: vec![],
        }
    }

    fn token_created_log(
        block_number: i64,
        factory: [u8; 20],
        token: [u8; 20],
        owner: [u8; 20],
        implementation: [u8; 20],
    ) -> LogEntry {
        LogEntry {
            block_number,
            tx_index: 0,
            log_index: 0,
            address: factory,
            topics: vec![
                crate::abi::factory::TOKEN_CREATED_TOPIC0,
                address_word(token),
                address_word(owner),
                address_word(implementation),
            ],
            data: encode_token_created_data("Name", "SYM", "ipfs://x", 18),
        }
    }

    #[tokio::test]
    async fn no_admission_events_yields_an_empty_set() {
        let rpc = FakeRpc::new();
        let abi = build_registry();
        let factory = addr(0xF0);

        let tokens = discover_factory_tokens(&rpc, &abi, factory).await.unwrap();
        assert!(tokens.is_empty(), "empty-set pre-issuance must not panic and must yield nothing");
    }

    #[tokio::test]
    async fn token_registered_event_is_discovered() {
        let mut rpc = FakeRpc::new();
        let abi = build_registry();
        let factory = addr(0xF1);
        let token = addr(0xA1);
        let implementation = addr(0xD1);

        rpc.add_log(
            factory,
            crate::abi::factory::TOKEN_REGISTERED_TOPIC0,
            token_registered_log(10, factory, token, implementation),
        );

        let tokens = discover_factory_tokens(&rpc, &abi, factory).await.unwrap();
        assert_eq!(tokens, BTreeSet::from([token]));
    }

    #[tokio::test]
    async fn token_created_event_is_also_discovered() {
        // Verified never emitted by Bloom on Rome (piecewise registerToken
        // is the real issuance path) — decoded here for completeness so a
        // TokenCreated-only admission path (outside this crate's Bloom
        // scope) is not silently missed either.
        let mut rpc = FakeRpc::new();
        let abi = build_registry();
        let factory = addr(0xF2);
        let token = addr(0xA2);
        let owner = addr(0x99);
        let implementation = addr(0xD2);

        rpc.add_log(
            factory,
            crate::abi::factory::TOKEN_CREATED_TOPIC0,
            token_created_log(11, factory, token, owner, implementation),
        );

        let tokens = discover_factory_tokens(&rpc, &abi, factory).await.unwrap();
        assert_eq!(tokens, BTreeSet::from([token]));
    }

    #[tokio::test]
    async fn union_of_both_event_kinds_with_no_duplicates() {
        let mut rpc = FakeRpc::new();
        let abi = build_registry();
        let factory = addr(0xF3);
        let registered_token = addr(0xA3);
        let created_token = addr(0xA4);
        let shared_token = addr(0xA5); // admitted via BOTH events — must dedupe to one

        rpc.add_log(
            factory,
            crate::abi::factory::TOKEN_REGISTERED_TOPIC0,
            token_registered_log(10, factory, registered_token, addr(0xD3)),
        );
        rpc.add_log(
            factory,
            crate::abi::factory::TOKEN_REGISTERED_TOPIC0,
            token_registered_log(11, factory, shared_token, addr(0xD4)),
        );
        rpc.add_log(
            factory,
            crate::abi::factory::TOKEN_CREATED_TOPIC0,
            token_created_log(12, factory, created_token, addr(0x99), addr(0xD5)),
        );
        rpc.add_log(
            factory,
            crate::abi::factory::TOKEN_CREATED_TOPIC0,
            token_created_log(13, factory, shared_token, addr(0x99), addr(0xD4)),
        );

        let tokens = discover_factory_tokens(&rpc, &abi, factory).await.unwrap();
        assert_eq!(
            tokens,
            BTreeSet::from([registered_token, created_token, shared_token]),
            "a token admitted via BOTH events must appear exactly once"
        );
    }

    #[tokio::test]
    async fn a_mis_shaped_factory_log_fails_loud_at_decode_not_silently_skipped() {
        // A factory log matched by address+topic0 but whose indexed topics are
        // missing (ABI drift from a factory upgrade) must ERROR at decode_log,
        // never silently drop a factory-admitted token. This proves the
        // fail-loud gate is decode_log's `?`, upstream of the address extract.
        let mut rpc = FakeRpc::new();
        let abi = build_registry();
        let factory = addr(0xF9);

        let mut bad = token_registered_log(10, factory, addr(0xA9), addr(0xD9));
        bad.topics.truncate(1); // keep topic0, drop the indexed token+impl topics
        rpc.add_log(factory, crate::abi::factory::TOKEN_REGISTERED_TOPIC0, bad);

        let err = discover_factory_tokens(&rpc, &abi, factory).await;
        assert!(
            matches!(err, Err(DiscoveryError::Decode(_))),
            "a mis-shaped factory admission log must fail loud at decode, got {err:?}"
        );
    }

    #[tokio::test]
    async fn a_candidate_that_passes_the_gate_is_included() {
        let mut rpc = FakeRpc::new();
        let abi = build_registry();
        let factory = addr(0xF6);
        let token = addr(0xA6);
        let implementation = addr(0xD6);

        rpc.add_log(
            factory,
            crate::abi::factory::TOKEN_REGISTERED_TOPIC0,
            token_registered_log(10, factory, token, implementation),
        );
        // §1.1 gate: getTokenImplementation(factory, token) != 0.
        rpc.set_token_implementation(factory, token, implementation);

        let registry = FakeRegistry::new("d".repeat(40).as_str(), factory, addr(0xC6));

        let authoritative = discover_authoritative_tokens(&rpc, &registry, &abi, factory)
            .await
            .unwrap();
        assert_eq!(authoritative, vec![token]);
    }

    #[tokio::test]
    async fn a_candidate_that_fails_the_gate_is_excluded() {
        // Discovered (a TokenRegistered log exists) but NEITHER
        // registry-listed NOR factory-admitted per getTokenImplementation
        // (deliberately never wired via `set_token_implementation`) — the
        // §1.1 gate must exclude it. Enumeration is a hint, never the gate.
        let mut rpc = FakeRpc::new();
        let abi = build_registry();
        let factory = addr(0xF7);
        let token = addr(0xA7);

        rpc.add_log(
            factory,
            crate::abi::factory::TOKEN_REGISTERED_TOPIC0,
            token_registered_log(10, factory, token, addr(0xD7)),
        );

        let registry = FakeRegistry::new("e".repeat(40).as_str(), factory, addr(0xC7));

        let authoritative = discover_authoritative_tokens(&rpc, &registry, &abi, factory)
            .await
            .unwrap();
        assert!(
            authoritative.is_empty(),
            "a candidate failing the §1.1 gate must never enter the resolve set, got {authoritative:?}"
        );
    }

    #[tokio::test]
    async fn a_registry_listed_candidate_passes_the_gate_even_with_zero_implementation() {
        let mut rpc = FakeRpc::new();
        let abi = build_registry();
        let factory = addr(0xF8);
        let token = addr(0xA8);

        rpc.add_log(
            factory,
            crate::abi::factory::TOKEN_REGISTERED_TOPIC0,
            token_registered_log(10, factory, token, addr(0xD8)),
        );

        let mut registry = FakeRegistry::new("f".repeat(40).as_str(), factory, addr(0xC8));
        registry.list_asset(token); // listed, but getTokenImplementation stays unconfigured (zero)

        let authoritative = discover_authoritative_tokens(&rpc, &registry, &abi, factory)
            .await
            .unwrap();
        assert_eq!(authoritative, vec![token]);
    }
}
