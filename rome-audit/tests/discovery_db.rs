//! S4 — on-chain token DISCOVERY, end-to-end through `AuditWorker`
//! (`SourceMode::Resolved { discovery_enabled: true, .. }`) against a REAL
//! local Postgres (Hercules-shaped fixture schema + the real `audit`
//! schema), mirroring `tests/resolve_ingest_db.rs`'s own AuditWorker-level
//! test style.
//!
//! Discovery reads via the injected `ResolverRpc.logs_for` seam — the SAME
//! call `resolver::token_registered_block` already makes — so these tests
//! script `FakeRpc.add_log(factory, TOKEN_REGISTERED_TOPIC0, ..)` exactly
//! like the existing resolve-pass tests do; no real Hercules `evm_log` rows
//! are needed to exercise discovery itself (only the Hercules-shaped
//! `sol_slot`/`eth_block` rows the watermark/frontier machinery reads).

use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;

use rome_audit::resolve::rpc::LogEntry;
use rome_audit::run::{AuditWorker, SourceMode, TickOutcome};

mod common;
use common::{fresh_hercules_audit_db, seed_contiguous_finalized_chain, FakeRegistry, FakeRpc};

fn addr(b: u8) -> [u8; 20] {
    [b; 20]
}

fn address_word(a: [u8; 20]) -> [u8; 32] {
    let mut w = [0u8; 32];
    w[12..].copy_from_slice(&a);
    w
}

fn token_registered_log(block_number: i64, factory: [u8; 20], token: [u8; 20], implementation: [u8; 20]) -> LogEntry {
    LogEntry {
        block_number,
        tx_index: 0,
        log_index: 0,
        address: factory,
        topics: vec![
            rome_audit::abi::factory::TOKEN_REGISTERED_TOPIC0,
            address_word(token),
            address_word(implementation),
        ],
        data: vec![],
    }
}

/// Wires a minimal, resolvable token into `rpc`/`registry`: registered on
/// `factory` at `registered_block`, with an immutable `router` — enough for
/// `resolve()` to succeed. Also adds the `TokenRegistered` log discovery
/// enumerates.
async fn setup_discoverable_token(
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
        token_registered_log(registered_block, factory, token, impl_addr),
    );
}

async fn capture_manifest_count(pool: &PgPool, chain_id: i64) -> i64 {
    let (n,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM audit.capture_manifest WHERE chain_id = $1")
            .bind(chain_id)
            .fetch_one(pool)
            .await
            .unwrap();
    n
}

async fn capture_manifest_count_for_asset(pool: &PgPool, chain_id: i64, asset_id: &str) -> i64 {
    let (n,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM audit.capture_manifest WHERE chain_id = $1 AND asset_id = $2",
    )
    .bind(chain_id)
    .bind(asset_id)
    .fetch_one(pool)
    .await
    .unwrap();
    n
}

async fn chain_event_count(pool: &PgPool, chain_id: i64) -> i64 {
    let (n,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM audit.chain_event WHERE chain_id = $1")
        .bind(chain_id)
        .fetch_one(pool)
        .await
        .unwrap();
    n
}

async fn watermark(pool: &PgPool, chain_id: i64) -> Option<i64> {
    let row: Option<(i64,)> = sqlx::query_as(
        "SELECT verified_through_slot FROM audit.ingest_watermark WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_optional(pool)
    .await
    .unwrap();
    row.map(|(s,)| s)
}

async fn set_ingest_watermark(pool: &PgPool, chain_id: i64, slot: i64) {
    sqlx::query(
        "INSERT INTO audit.ingest_watermark (chain_id, verified_through_slot, updated_at) \
         VALUES ($1, $2, 1700000000) \
         ON CONFLICT (chain_id) DO UPDATE SET verified_through_slot = excluded.verified_through_slot",
    )
    .bind(chain_id)
    .bind(slot)
    .execute(pool)
    .await
    .unwrap();
}

async fn backfill_gaps(pool: &PgPool, chain_id: i64) -> Vec<(Vec<u8>, i64, i64)> {
    sqlx::query_as(
        "SELECT source_contract, from_block, watermark_at_detection FROM audit.backfill_gap \
         WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_all(pool)
    .await
    .unwrap()
}

// ---- T3: discovery ON + empty static tokens + two TokenRegistered events
// -> both tokens enter the resolve set (union), each having passed the §1.1
// gate. ----

#[tokio::test]
async fn discovery_admits_every_gate_passed_factory_registered_token() {
    let pool = fresh_hercules_audit_db().await;
    let chain_id = 400010i64;

    let factory = addr(0xF0);
    let storefront = addr(0xC0);
    let token1 = addr(0xA1);
    let router1 = addr(0xB1);
    let token2 = addr(0xA2);
    let router2 = addr(0xB2);

    let registry = Arc::new(FakeRegistry::new(
        "4000104000104000104000104000104000104000",
        factory,
        storefront,
    ));
    let rpc = Arc::new(FakeRpc::new());
    setup_discoverable_token(&rpc, factory, token1, router1, 5).await;
    setup_discoverable_token(&rpc, factory, token2, router2, 6).await;

    seed_contiguous_finalized_chain(&pool, 0, 10).await;

    let mut worker = AuditWorker::new(
        pool.clone(),
        pool.clone(),
        chain_id,
        2,
        30,
        SourceMode::Resolved {
            registry,
            rpc,
            tokens: vec![], // nothing statically configured — discovery must supply both
            reresolve_interval: Duration::from_secs(3600),
            discovery_enabled: true,
        },
    );

    worker.tick().await;

    assert_eq!(
        capture_manifest_count(&pool, chain_id).await,
        2,
        "both factory-registered, gate-passed tokens must have entered the resolve set"
    );
}

// ---- T4: discovery ON + a candidate that FAILS the §1.1 gate (no
// implementation, not registry-listed) -> excluded, never reaching
// resolve(). ----

#[tokio::test]
async fn discovery_excludes_a_candidate_that_fails_the_authoritativeness_gate() {
    let pool = fresh_hercules_audit_db().await;
    let chain_id = 400011i64;

    let factory = addr(0xF1);
    let storefront = addr(0xC1);
    let token_good = addr(0xA3);
    let router_good = addr(0xB3);
    let token_bad = addr(0xA4); // discovered (a TokenRegistered log exists) but never admitted

    let registry = Arc::new(FakeRegistry::new(
        "4000114000114000114000114000114000114000",
        factory,
        storefront,
    ));
    let rpc = Arc::new(FakeRpc::new());
    setup_discoverable_token(&rpc, factory, token_good, router_good, 5).await;
    // token_bad: a TokenRegistered log exists (so discovery enumerates it),
    // but `set_token_implementation` is deliberately never called AND it is
    // never registry-listed — the §1.1 gate must exclude it before
    // `resolve()` is ever invoked for it (no router is configured for it
    // either — if `resolve()` were wrongly called, this would fail loudly
    // instead of silently passing).
    rpc.add_log(
        factory,
        rome_audit::abi::factory::TOKEN_REGISTERED_TOPIC0,
        token_registered_log(6, factory, token_bad, addr(0xD9)),
    );

    seed_contiguous_finalized_chain(&pool, 0, 10).await;

    let mut worker = AuditWorker::new(
        pool.clone(),
        pool.clone(),
        chain_id,
        2,
        30,
        SourceMode::Resolved {
            registry,
            rpc,
            tokens: vec![],
            reresolve_interval: Duration::from_secs(3600),
            discovery_enabled: true,
        },
    );

    let outcome = worker.tick().await;
    assert!(
        matches!(outcome, TickOutcome::Ingested(_)),
        "the excluded candidate must never reach resolve() and abort the pass, got {outcome:?}"
    );

    assert_eq!(
        capture_manifest_count(&pool, chain_id).await,
        1,
        "only the gate-passed token must have a manifest"
    );
    let good_asset_id = format!("{chain_id}:0x{}", hex::encode(token_good));
    assert_eq!(capture_manifest_count_for_asset(&pool, chain_id, &good_asset_id).await, 1);
}

// ---- T5: discovery ON + union with a configured static token -> both
// present, no dupes (including the SAME address configured statically AND
// discovered). ----

#[tokio::test]
async fn discovery_unions_with_configured_tokens_without_duplicating_a_shared_address() {
    let pool = fresh_hercules_audit_db().await;
    let chain_id = 400012i64;

    let factory = addr(0xF2);
    let storefront = addr(0xC2);
    let token_static_only = addr(0xA5);
    let router_static = addr(0xB5);
    let token_shared = addr(0xA6); // BOTH statically configured AND discovered
    let router_shared = addr(0xB6);
    let token_discovered_only = addr(0xA7);
    let router_discovered = addr(0xB7);

    let registry = Arc::new(FakeRegistry::new(
        "4000124000124000124000124000124000124000",
        factory,
        storefront,
    ));
    let rpc = Arc::new(FakeRpc::new());
    // token_static_only: resolvable, but NOT registered on the factory (no
    // TokenRegistered log) — discovery must never be required for a
    // statically-configured token to resolve.
    let impl_static = addr(0xD0);
    rpc.set_token_implementation(factory, token_static_only, impl_static);
    rpc.set_router(token_static_only, router_static);

    setup_discoverable_token(&rpc, factory, token_shared, router_shared, 5).await;
    setup_discoverable_token(&rpc, factory, token_discovered_only, router_discovered, 6).await;

    seed_contiguous_finalized_chain(&pool, 0, 10).await;

    let mut worker = AuditWorker::new(
        pool.clone(),
        pool.clone(),
        chain_id,
        2,
        30,
        SourceMode::Resolved {
            registry,
            rpc,
            tokens: vec![token_static_only, token_shared], // token_shared ALSO discoverable
            reresolve_interval: Duration::from_secs(3600),
            discovery_enabled: true,
        },
    );

    worker.tick().await;

    assert_eq!(
        capture_manifest_count(&pool, chain_id).await,
        3,
        "static-only + shared + discovered-only = exactly 3 assets, no dupes"
    );
    let shared_asset_id = format!("{chain_id}:0x{}", hex::encode(token_shared));
    assert_eq!(
        capture_manifest_count_for_asset(&pool, chain_id, &shared_asset_id).await,
        1,
        "a token that is BOTH statically configured AND discovered must resolve exactly once per pass"
    );
}

// ---- T6: late discovery -> the new token's pre-discovery history is
// gap-backfilled from ITS OWN from_block (a real block, never genesis). ----

#[tokio::test]
async fn late_discovery_records_a_backfill_gap_from_the_tokens_own_from_block_not_genesis() {
    let pool = fresh_hercules_audit_db().await;
    let chain_id = 400013i64;

    let factory = addr(0xF3);
    let storefront = addr(0xC3);
    let token = addr(0xA8);
    let router = addr(0xB8);
    let registered_block = 50i64; // a real, non-zero anchor

    let registry = Arc::new(FakeRegistry::new(
        "4000134000134000134000134000134000134000",
        factory,
        storefront,
    ));
    let rpc = Arc::new(FakeRpc::new());
    // The token's life began at block 50 — LONG before discovery ever runs
    // (the scan frontier below is pinned far ahead, at 200), modeling "a
    // token discovered later than its life began".
    setup_discoverable_token(&rpc, factory, token, router, registered_block).await;

    seed_contiguous_finalized_chain(&pool, 0, 200).await;
    set_ingest_watermark(&pool, chain_id, 200).await;

    let mut worker = AuditWorker::new(
        pool.clone(),
        pool.clone(),
        chain_id,
        2,
        30,
        SourceMode::Resolved {
            registry,
            rpc,
            tokens: vec![], // nothing statically configured — discovery is the ONLY path in
            reresolve_interval: Duration::from_secs(3600),
            discovery_enabled: true,
        },
    );

    worker.tick().await; // the resolve pass; may also run an ingest tick

    let gaps = backfill_gaps(&pool, chain_id).await;
    let token_gap = gaps
        .iter()
        .find(|(addr_bytes, _, _)| addr_bytes.as_slice() == token.as_slice());
    assert!(
        token_gap.is_some_and(|(_, from_block, wm)| *from_block == registered_block && *wm == 200),
        "expected the token's own gap anchored at its registration block ({registered_block}), \
         never at genesis (0) — got {gaps:?}"
    );
    // Note: `Factory`/`Storefront` are chain-global sources deliberately
    // anchored at from_block=0 (they span from genesis by design — see
    // `resolver::resolve`'s doc) and legitimately gap on ANY first-ever
    // resolve pass, discovery or not — that is pre-existing P4a behavior,
    // not something S4 changes. The property under test here is specific to
    // the DISCOVERED TOKEN's own anchor, asserted above.
}

// ---- T7: empty discovered set + empty static tokens on a pre-issuance
// chain -> no panic, watermark advances, nothing decoded. ----

#[tokio::test]
async fn empty_discovery_and_empty_static_tokens_advances_without_panic_or_decoding() {
    let pool = fresh_hercules_audit_db().await;
    let chain_id = 400014i64;

    let factory = addr(0xF4);
    let storefront = addr(0xC4);

    // No `setup_discoverable_token` call at all — the factory has ZERO
    // admission events (a genuinely pre-issuance chain).
    let registry = Arc::new(FakeRegistry::new(
        "4000144000144000144000144000144000144000",
        factory,
        storefront,
    ));
    let rpc = Arc::new(FakeRpc::new());

    seed_contiguous_finalized_chain(&pool, 0, 20).await;

    let mut worker = AuditWorker::new(
        pool.clone(),
        pool.clone(),
        chain_id,
        2,
        30,
        SourceMode::Resolved {
            registry,
            rpc,
            tokens: vec![],
            reresolve_interval: Duration::from_secs(3600),
            discovery_enabled: true,
        },
    );

    for _ in 0..5 {
        let outcome = worker.tick().await;
        assert!(
            matches!(outcome, TickOutcome::Ingested(_)),
            "an empty discovered set must resolve trivially (0 tokens) and never hold on \
             AwaitingFirstResolution, got {outcome:?}"
        );
    }

    assert!(
        watermark(&pool, chain_id).await.unwrap_or(-1) >= 0,
        "the watermark must have initialized and advanced despite zero discovered/configured tokens"
    );
    assert_eq!(chain_event_count(&pool, chain_id).await, 0, "nothing to decode pre-issuance");
    assert_eq!(capture_manifest_count(&pool, chain_id).await, 0, "no tokens resolved, no manifests");
}
