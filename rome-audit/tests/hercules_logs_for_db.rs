//! The log-history-from-Hercules read (`logs_for_address_topic0`) + the
//! `HerculesResolverRpc` seam that feeds it into `resolve()`. Against a REAL
//! local Postgres holding the Hercules-shaped fixture schema (the `source`
//! side) — exactly what production reads, since the proxy's `eth_getLogs` is
//! itself backed by the same `evm_log` table.
//!
//! This is the RED case the FakeRpc fixtures never had: proving that a log
//! walk resolved from the indexed DB (not a `[Earliest, Latest]`
//! `eth_getLogs`, which the production proxy caps at 12,000 blocks →
//! `-32005 block range too wide`) produces a correct `LogEntry` set — the
//! same `(block_number, tx_index, log_index)` ordering + `data`/`topics`
//! shape the fixed-point walk depends on.

use std::sync::Arc;

use ethers::providers::Provider;
use ethers::types::{Bytes, H256};

use rome_audit::ingest::hercules_reads::logs_for_address_topic0;
use rome_audit::resolve::{resolve, EthersResolverRpc, HerculesResolverRpc};
use rome_audit::types::SourceKind;

mod common;
use common::{
    address_topic, fresh_hercules_audit_db, hex_addr, hex_topic, seed_tx_with_logs,
    seed_tx_with_logs_indexed, FakeRegistry,
};

fn topic(b: u8) -> [u8; 32] {
    [b; 32]
}

/// An address-uniform 32-byte word (`0x00..00<b×20>`) — the mock's
/// single-`address`-return / storage-word shape.
fn word20(b: u8) -> [u8; 32] {
    let mut w = [0u8; 32];
    w[12..].copy_from_slice(&[b; 20]);
    w
}

fn zero_word() -> [u8; 32] {
    [0u8; 32]
}

// ---- (a) logs_for correctness DB test --------------------------------------

#[tokio::test]
async fn logs_for_address_topic0_is_offset_correct_ordered_and_filtered() {
    let pool = fresh_hercules_audit_db().await;

    let target = 0xA0u8; // the (address) we walk
    let other_addr = 0xB0u8; // a different contract — must be filtered OUT
    let t0 = topic(0x11); // the topic0 we walk
    let other_t0 = topic(0x22); // a different event at `target` — filtered OUT

    let t0_hex = hex_topic(&t0);
    let other_t0_hex = hex_topic(&other_t0);
    let from = hex_addr(0x55);

    // Seeded OUT OF ORDER (block 200 before block 100) on purpose, so the
    // ascending (block_number, tx_index, log_index) ordering is a real
    // assertion, not an artefact of insertion order.

    // tx2: block 200, tx_index 3, first_log_index 10 — one matching log at
    // ordinal 0 ⇒ log_index = 0 + 10 = 10.
    seed_tx_with_logs_indexed(
        &pool,
        8,
        &format!("0x{:064x}", 0x2222u64),
        &format!("0x{:064x}", 200),
        200,
        3,
        10,
        &from,
        &[(
            hex_addr(target),
            t0_hex.clone(),
            Some(hex_topic(&topic(0x33))),
            None,
            None,
            None,
        )],
    )
    .await;

    // tx1: block 100, tx_index 0, first_log_index 5 — TWO matching logs in one
    // tx ⇒ ordinal 0 → log_index 5, ordinal 1 → log_index 6 (exercises the
    // per-tx offset math on a multi-log tx).
    let data_a = format!("0x{}", hex::encode([0xCDu8; 32]));
    seed_tx_with_logs_indexed(
        &pool,
        5,
        &format!("0x{:064x}", 0x1111u64),
        &format!("0x{:064x}", 100),
        100,
        0,
        5,
        &from,
        &[
            (
                hex_addr(target),
                t0_hex.clone(),
                Some(hex_topic(&topic(0xAA))),
                Some(hex_topic(&topic(0xBB))),
                None,
                Some(data_a.clone()),
            ),
            (
                hex_addr(target),
                t0_hex.clone(),
                Some(hex_topic(&topic(0xDD))),
                None,
                None,
                None,
            ),
        ],
    )
    .await;

    // Noise: right topic0 wrong address, and right address wrong topic0 —
    // both MUST be excluded.
    seed_tx_with_logs_indexed(
        &pool,
        9,
        &format!("0x{:064x}", 0x3333u64),
        &format!("0x{:064x}", 250),
        250,
        0,
        0,
        &from,
        &[
            (
                hex_addr(other_addr),
                t0_hex.clone(),
                Some(hex_topic(&topic(0x01))),
                None,
                None,
                None,
            ),
            (
                hex_addr(target),
                other_t0_hex,
                Some(hex_topic(&topic(0x02))),
                None,
                None,
                None,
            ),
        ],
    )
    .await;

    let logs = logs_for_address_topic0(&pool, [target; 20], t0)
        .await
        .unwrap();

    // Exactly the three matching logs, ascending by (block, tx_index, log_index).
    assert_eq!(logs.len(), 3, "only the (target, t0) logs may be returned — got {logs:?}");
    let keys: Vec<(i64, i32, i32)> = logs
        .iter()
        .map(|l| (l.block_number, l.tx_index, l.log_index))
        .collect();
    assert_eq!(
        keys,
        vec![(100, 0, 5), (100, 0, 6), (200, 3, 10)],
        "ordering + log_index-offset (log_ordinal + first_log_index) math is wrong"
    );

    // Every returned entry is the right address + topic0 (filter correctness).
    assert!(
        logs.iter().all(|l| l.address == [target; 20] && l.topics[0] == t0),
        "a returned log wasn't filtered to (target, t0): {logs:?}"
    );

    // Shape of the first entry: topic0 + topic1 + topic2, plus its data.
    assert_eq!(logs[0].topics, vec![t0, topic(0xAA), topic(0xBB)]);
    assert_eq!(logs[0].data, vec![0xCDu8; 32]);
    // The second log in the same tx: topic0 + topic1 only, no data.
    assert_eq!(logs[1].topics, vec![t0, topic(0xDD)]);
    assert!(logs[1].data.is_empty());
}

// ---- (b) end-to-end resolve() fed by the Hercules log read -----------------

#[tokio::test]
async fn resolve_reads_its_log_walks_from_hercules_via_hercules_resolver_rpc() {
    let pool = fresh_hercules_audit_db().await;
    let abi = rome_audit::build_registry();

    let token = 0xA0u8;
    let factory = 0xF0u8;
    let router = 0xB0u8;
    let storefront = 0xC0u8;
    let yield_token = 0xE0u8;
    let impl_addr = 0xD0u8;
    let registered_block = 5i64;

    // ---- LOG history, seeded into Hercules (served by logs_for) ----
    // The factory's TokenRegistered(token, implementation) — anchors the
    // router's from_block.
    seed_tx_with_logs(
        &pool,
        5,
        &format!("0x{:064x}", 5),
        registered_block,
        &hex_addr(0x55),
        &[(
            hex_addr(factory),
            hex_topic(&rome_audit::abi::factory::TOKEN_REGISTERED_TOPIC0),
            Some(address_topic(token)),
            Some(address_topic(impl_addr)),
            None,
            None,
        )],
    )
    .await;
    // The token's YieldTokenUpdated(newYieldToken) — the source we assert
    // came from a Hercules log walk, not a hardcode.
    seed_tx_with_logs(
        &pool,
        6,
        &format!("0x{:064x}", 6),
        6,
        &hex_addr(0x55),
        &[(
            hex_addr(token),
            hex_topic(&rome_audit::abi::arc_token::YIELD_TOKEN_UPDATED_TOPIC0),
            Some(address_topic(yield_token)),
            None,
            None,
            None,
        )],
    )
    .await;

    // ---- STATE reads, scripted on the inner EthersResolverRpc's mock
    // transport (LIFO: last push is popped first). resolve() issues, in
    // order: get_token_implementation, restrictions_router (getStorageAt),
    // get_restriction_module×2, get_global_module_address. No UV2 probes
    // (no Transfer logs seeded), no Morpho (registry.morpho()==0). ----
    let (provider, mock) = Provider::mocked();
    mock.push::<Bytes, _>(Bytes::from(zero_word().to_vec())).unwrap(); // #5 get_global_module_address
    mock.push::<Bytes, _>(Bytes::from(zero_word().to_vec())).unwrap(); // #4 get_restriction_module(YIELD)
    mock.push::<Bytes, _>(Bytes::from(zero_word().to_vec())).unwrap(); // #3 get_restriction_module(TRANSFER)
    mock.push(H256::from(word20(router))).unwrap(); // #2 restrictions_router
    mock.push::<Bytes, _>(Bytes::from(word20(impl_addr).to_vec())).unwrap(); // #1 get_token_implementation

    let inner = EthersResolverRpc::new(Arc::new(provider));
    let rpc = HerculesResolverRpc::new(pool.clone(), inner);
    let registry = FakeRegistry::new(
        "abcabcabcabcabcabcabcabcabcabcabcabcabca",
        [factory; 20],
        [storefront; 20],
    );

    let graph = resolve([token; 20], &registry, &rpc, &abi)
        .await
        .expect("resolve() must SUCCEED with its log walks served from Hercules");

    // The headline: the YieldToken source came from a Hercules log walk.
    assert!(
        graph
            .sources
            .iter()
            .any(|s| s.address == [yield_token; 20] && s.source_kind == SourceKind::YieldToken),
        "YieldToken must be discovered from the seeded Hercules YieldTokenUpdated log — got {:?}",
        graph.sources
    );
    // The seed sources are all present.
    for (addr_byte, kind) in [
        (token, SourceKind::ArcToken),
        (router, SourceKind::Router),
        (factory, SourceKind::Factory),
        (storefront, SourceKind::Storefront),
    ] {
        assert!(
            graph
                .sources
                .iter()
                .any(|s| s.address == [addr_byte; 20] && s.source_kind == kind),
            "missing {kind:?} at 0x{addr_byte:02x}.. — got {:?}",
            graph.sources
        );
    }
    // The router's interval is anchored at the token's registration block —
    // proving the block_number came off the Hercules TokenRegistered log,
    // not a default.
    assert!(
        graph
            .intervals
            .iter()
            .any(|i| i.address == [router; 20] && i.from_block == registered_block),
        "router interval must anchor at the Hercules-derived registered block {registered_block}"
    );
}
