//! P4a deliverable 1 — `audit.ingest_scope` + the gap-detection re-key,
//! against a REAL local Postgres (same disposable `rome-audit-test-pg`
//! container every other `tests/*_db.rs` file uses).
//!
//! **What this proves that `tests/resolve_db.rs`'s pre-existing
//! `new_source_behind_the_watermark_records_a_backfill_gap` doesn't:** that
//! test only ever runs ONE resolve pass, so the OLD (manifest-address-keyed)
//! and NEW (ingest_scope, `(address, filter_fingerprint)`-keyed) mechanisms
//! agree — a brand-new address is "not previously known" under either
//! definition. These tests specifically construct the case where the two
//! definitions DIVERGE: an address that WAS already manifest-known (an
//! earlier pass recorded it, under a kind with no live-ingest descriptor)
//! but only becomes INGESTABLE later, and the filter-widening case where the
//! address was already ingestable but under a narrower filter. Both are the
//! silent-gap trap `resolve::pass::detect_backfill_gaps`'s doc names.

use sqlx::PgPool;

use rome_audit::registry::AbiRegistry;
use rome_audit::resolve::pass::run_resolve_pass;
use rome_audit::resolve::rpc::LogEntry;
use rome_audit::types::SourceKind;

mod common;
use common::{FakeRegistry, FakeRpc};

fn addr(b: u8) -> [u8; 20] {
    [b; 20]
}

fn address_word(a: [u8; 20]) -> [u8; 32] {
    let mut w = [0u8; 32];
    w[12..].copy_from_slice(&a);
    w
}

/// The real production registry, optionally with `YieldToken`'s own
/// live-ingest descriptor stripped (`AbiRegistry::without_source_kind`) —
/// "the registry as it looked before P4a registered YieldToken." Using the
/// REAL `build_registry()` (rather than a hand-built partial one) matters
/// because `resolve::resolver::resolve` itself calls `decode::decode_log`
/// against THIS registry while walking (e.g. to decode `ArcToken`'s own
/// `YieldTokenUpdated` event and extract the new yield-token address) — a
/// registry missing those OTHER kinds' decode shapes would fail resolution
/// itself, not just the live-ingest gate under test.
fn registry_with(include_yield_token: bool) -> AbiRegistry {
    let full = rome_audit::build_registry();
    if include_yield_token {
        full
    } else {
        full.without_source_kind(SourceKind::YieldToken)
    }
}

async fn setup_minimal_token(
    rpc: &FakeRpc,
    factory: [u8; 20],
    token: [u8; 20],
    router: [u8; 20],
    registered_block: i64,
) {
    let impl_addr = addr(0xD0);
    rpc.set_token_implementation(factory, token, impl_addr);
    rpc.set_router(token, router);
    rpc.add_log(
        factory,
        rome_audit::abi::factory::TOKEN_REGISTERED_TOPIC0,
        LogEntry {
            block_number: registered_block,
            tx_index: 0,
            log_index: 0,
            address: factory,
            topics: vec![
                rome_audit::abi::factory::TOKEN_REGISTERED_TOPIC0,
                address_word(token),
                address_word(impl_addr),
            ],
            data: vec![],
        },
    );
}

async fn ingest_scope_rows(pool: &PgPool, chain_id: i64) -> Vec<(Vec<u8>, String)> {
    sqlx::query_as(
        "SELECT address, filter_fingerprint FROM audit.ingest_scope WHERE chain_id = $1 ORDER BY address, filter_fingerprint",
    )
    .bind(chain_id)
    .fetch_all(pool)
    .await
    .unwrap()
}

async fn backfill_gap_rows(pool: &PgPool, chain_id: i64) -> Vec<(Vec<u8>, i64, i64)> {
    sqlx::query_as(
        "SELECT source_contract, from_block, watermark_at_detection FROM audit.backfill_gap \
         WHERE chain_id = $1 ORDER BY source_contract",
    )
    .bind(chain_id)
    .fetch_all(pool)
    .await
    .unwrap()
}

// ---- (a): a kind that gains a descriptor only in a LATER pass ----

#[tokio::test]
async fn newly_ingestable_kind_fires_backfill_gap() {
    let pool = common::fresh_audit_db().await;
    let chain_id = 400001i64;

    let factory = addr(0xF0);
    let storefront = addr(0xC0);
    let token = addr(0xA0);
    let router = addr(0xB0);
    let yield_token = addr(0xE0);

    let registry = FakeRegistry::new(
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        factory,
        storefront,
    );
    let rpc = FakeRpc::new();
    setup_minimal_token(&rpc, factory, token, router, 5).await;
    // The YieldTokenUpdated event is seen in BOTH passes (block 6) — what
    // changes between passes is only whether the ABI registry has a
    // descriptor for YieldToken, not whether the graph discovers it.
    rpc.add_log(
        token,
        rome_audit::abi::arc_token::YIELD_TOKEN_UPDATED_TOPIC0,
        LogEntry {
            block_number: 6,
            tx_index: 0,
            log_index: 0,
            address: token,
            topics: vec![
                rome_audit::abi::arc_token::YIELD_TOKEN_UPDATED_TOPIC0,
                address_word(yield_token),
            ],
            data: vec![],
        },
    );

    // Pass 1: registry LACKS the YieldToken descriptor — fresh chain
    // (watermark -1) so nothing fires yet regardless of mechanism.
    let no_yield_registry = registry_with(false);
    let resolved1 = run_resolve_pass(&pool, chain_id, &[token], &registry, &rpc, &no_yield_registry, -1)
        .await
        .unwrap();
    assert!(
        !resolved1.sources.contains_key(&format!("0x{}", hex::encode(yield_token))),
        "YieldToken must NOT be in the live ingest map while its descriptor is absent"
    );
    let key = format!("0x{}", hex::encode(token));
    assert!(
        resolved1.sources.contains_key(&key),
        "the token's own ArcToken source must be ingestable in pass 1"
    );

    let scope_after_pass1 = ingest_scope_rows(&pool, chain_id).await;
    assert!(
        !scope_after_pass1
            .iter()
            .any(|(a, _)| a.as_slice() == yield_token.as_slice()),
        "yield_token must not have entered ingest_scope while descriptor-less — got {scope_after_pass1:?}"
    );
    assert!(
        scope_after_pass1
            .iter()
            .any(|(a, fp)| a.as_slice() == token.as_slice() && fp == "none"),
        "the token's own ArcToken (address, \"none\") pair must be in ingest_scope after pass 1"
    );

    // Pass 2: the FULL registry (YieldToken now HAS a descriptor) + a real
    // watermark ahead of the YieldToken's own anchor (block 6) — the newly
    // ingestable (yield_token, filter) pair must fire a gap.
    let full_registry = registry_with(true);
    let watermark = 1_000i64;
    let resolved2 = run_resolve_pass(&pool, chain_id, &[token], &registry, &rpc, &full_registry, watermark)
        .await
        .unwrap();
    assert!(
        resolved2
            .sources
            .contains_key(&format!("0x{}", hex::encode(yield_token))),
        "YieldToken must now be in the live ingest map"
    );

    let gaps = backfill_gap_rows(&pool, chain_id).await;
    assert!(
        gaps.iter().any(|(a, from_block, wm)| {
            a.as_slice() == yield_token.as_slice() && *from_block == 6 && *wm == watermark
        }),
        "expected a NEW backfill_gap for yield_token (block 6, watermark {watermark}) — got {gaps:?}"
    );
    // The token's own ArcToken pair was ALREADY ingestable before pass 2 —
    // must NOT fire a second, spurious gap for it.
    assert!(
        !gaps.iter().any(|(a, ..)| a.as_slice() == token.as_slice()),
        "the token's own (already-ingestable) source must not re-fire a gap on pass 2 — got {gaps:?}"
    );
}

// ---- (b): filter-widening — a 2nd asset sharing the same chain-wide token
// broadens the union'd filter, exposing history the narrower filter never
// captured. ----

#[tokio::test]
async fn filter_widening_fires_backfill_gap() {
    let pool = common::fresh_audit_db().await;
    let chain_id = 400002i64;
    let full_registry = registry_with(true);

    let factory = addr(0xF1);
    let storefront = addr(0xC1);
    let token_a = addr(0xA1);
    let router_a = addr(0xB1);
    let token_b = addr(0xA2);
    let router_b = addr(0xB2);
    let yield_token = addr(0xE1); // the shared chain-wide token (wUSDC-shaped)

    let registry = FakeRegistry::new(
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        factory,
        storefront,
    );
    let rpc = FakeRpc::new();
    setup_minimal_token(&rpc, factory, token_a, router_a, 5).await;
    setup_minimal_token(&rpc, factory, token_b, router_b, 40).await;
    rpc.add_log(
        token_a,
        rome_audit::abi::arc_token::YIELD_TOKEN_UPDATED_TOPIC0,
        LogEntry {
            block_number: 6,
            tx_index: 0,
            log_index: 0,
            address: token_a,
            topics: vec![
                rome_audit::abi::arc_token::YIELD_TOKEN_UPDATED_TOPIC0,
                address_word(yield_token),
            ],
            data: vec![],
        },
    );

    // Pass 1: token_a ALONE onboards the shared yield_token — filter is
    // narrow (TransferFrom{token_a}). Fresh chain, nothing fires.
    run_resolve_pass(&pool, chain_id, &[token_a], &registry, &rpc, &full_registry, -1)
        .await
        .unwrap();
    let narrow_fp = rome_audit::ingest::IngestFilter::TransferFrom(std::collections::BTreeSet::from([token_a])).fingerprint();
    let scope_after_pass1 = ingest_scope_rows(&pool, chain_id).await;
    assert!(
        scope_after_pass1
            .iter()
            .any(|(a, fp)| a.as_slice() == yield_token.as_slice() && *fp == narrow_fp),
        "expected the narrow (yield_token, {narrow_fp}) pair after pass 1 — got {scope_after_pass1:?}"
    );

    // token_b's own YieldTokenUpdated event lands at block 50 — BEHIND the
    // watermark pass 2 will use (100).
    rpc.add_log(
        token_b,
        rome_audit::abi::arc_token::YIELD_TOKEN_UPDATED_TOPIC0,
        LogEntry {
            block_number: 50,
            tx_index: 0,
            log_index: 0,
            address: token_b,
            topics: vec![
                rome_audit::abi::arc_token::YIELD_TOKEN_UPDATED_TOPIC0,
                address_word(yield_token),
            ],
            data: vec![],
        },
    );

    // Pass 2: BOTH tokens resolved together — the union'd filter WIDENS to
    // TransferFrom{token_a, token_b}, a fingerprint NEVER seen before, even
    // though `yield_token` the ADDRESS was already known.
    let watermark = 100i64;
    run_resolve_pass(
        &pool,
        chain_id,
        &[token_a, token_b],
        &registry,
        &rpc,
        &full_registry,
        watermark,
    )
    .await
    .unwrap();

    let widened_fp = rome_audit::ingest::IngestFilter::TransferFrom(std::collections::BTreeSet::from([
        token_a, token_b,
    ]))
    .fingerprint();
    assert_ne!(narrow_fp, widened_fp);

    let gaps = backfill_gap_rows(&pool, chain_id).await;
    assert!(
        gaps.iter().any(|(a, from_block, wm)| {
            a.as_slice() == yield_token.as_slice() && *from_block == 6 && *wm == watermark
        }),
        "filter-widening must fire a NEW gap for yield_token, anchored at the EARLIEST \
         contributing interval (block 6) — got {gaps:?}"
    );

    let scope_after_pass2 = ingest_scope_rows(&pool, chain_id).await;
    assert!(
        scope_after_pass2
            .iter()
            .any(|(a, fp)| a.as_slice() == yield_token.as_slice() && *fp == widened_fp),
        "the widened fingerprint must now be recorded in ingest_scope too — got {scope_after_pass2:?}"
    );
}

// ---- ingest_scope upsert is idempotent + the fingerprint is canonical ----

#[tokio::test]
async fn ingest_scope_upsert_is_idempotent_and_canonical() {
    let pool = common::fresh_audit_db().await;
    let chain_id = 400003i64;
    let full_registry = registry_with(true);

    let factory = addr(0xF3);
    let storefront = addr(0xC3);
    let token = addr(0xA3);
    let router = addr(0xB3);

    let registry = FakeRegistry::new(
        "cccccccccccccccccccccccccccccccccccccccc",
        factory,
        storefront,
    );
    let rpc = FakeRpc::new();
    setup_minimal_token(&rpc, factory, token, router, 5).await;

    // Same graph, resolved TWICE (the natural re-resolution cadence) — no
    // new source, no filter change.
    run_resolve_pass(&pool, chain_id, &[token], &registry, &rpc, &full_registry, -1)
        .await
        .unwrap();
    let rows_after_first = ingest_scope_rows(&pool, chain_id).await;

    run_resolve_pass(&pool, chain_id, &[token], &registry, &rpc, &full_registry, -1)
        .await
        .unwrap();
    let rows_after_second = ingest_scope_rows(&pool, chain_id).await;

    assert_eq!(
        rows_after_first, rows_after_second,
        "re-resolving an UNCHANGED graph must not add any new ingest_scope row"
    );

    // No spurious gap on the 2nd pass either (nothing actually changed).
    let gaps = backfill_gap_rows(&pool, chain_id).await;
    assert!(
        gaps.is_empty(),
        "a fresh-chain watermark (-1) must never fire a gap regardless of pass count — got {gaps:?}"
    );
}
