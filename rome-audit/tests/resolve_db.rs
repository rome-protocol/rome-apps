//! P3 DB-facing tests — against a REAL local Postgres (same disposable
//! `rome-audit-test-pg` container `tests/ingest_db.rs`/`tests/tier2_db.rs`
//! use; one fresh database per test).
//!
//! - `capture_manifest` determinism: two inserts of the SAME resolved graph
//!   at different wall-clock `generated_at` values must land under the
//!   SAME `manifest_hash` — `ON CONFLICT DO NOTHING` keeps the row from the
//!   first insert (capture §1.2 point 4's determinism guarantee).
//! - `asset_event` C1: a shared-source event (router) maps into BOTH
//!   assets' `asset_event` rows when two tokens share that router.
//! - `asset_event` rebuild determinism (§13.1-style, Tier-2 doctrine):
//!   dropping and rebuilding the junction from the same `chain_event` +
//!   manifest inputs reproduces byte-identical rows.

use std::sync::atomic::{AtomicI64, Ordering};

use sqlx::PgPool;

use rome_audit::resolve::asset_event::{rebuild_asset_event, AssetManifest};
use rome_audit::resolve::graph::{ResolvedFrom, ResolvedGraph, ResolvedSource, SourceInterval};
use rome_audit::resolve::pass::{run_resolve_pass, PassError};

mod common;
use common::{fresh_audit_db as fresh_test_db, FakeRegistry, FakeRpc};
use rome_audit::resolve::manifest::CaptureManifest;
use rome_audit::resolve::store::{generated_at_for, insert_backfill_gap, insert_capture_manifest};
use rome_audit::types::SourceKind;

// Test-DB provisioning itself lives in `tests/common/mod.rs` (this file used
// to carry its own duplicate `CREATE DATABASE` — no matching `DROP DATABASE`
// anywhere — one of three leak sites fixed together; see `common::TestDb`).

fn addr(b: u8) -> [u8; 20] {
    [b; 20]
}

// ---- chain_event seeding (direct INSERT, same pattern as tests/tier2_db.rs) ----

static EVENT_COUNTER: AtomicI64 = AtomicI64::new(1);

#[allow(clippy::too_many_arguments)]
async fn seed_event(
    pool: &PgPool,
    chain_id: i64,
    source_contract: [u8; 20],
    event_name: &str,
    block_number: i64,
) -> i64 {
    let n = EVENT_COUNTER.fetch_add(1, Ordering::SeqCst);
    let tx_hash = format!("0x{:064x}", n);
    let block_hash = format!("0x{:064x}", block_number * 1_000_000 + n);

    let row: (i64,) = sqlx::query_as(
        r#"
        INSERT INTO audit.chain_event
            (chain_id, source_contract, source_kind, event_name, projection_tag, topic0,
             block_number, block_hash, block_timestamp, tx_hash, tx_index, log_index, tx_signer, args)
        VALUES ($1,$2,'TEST_SOURCE',$3,'primary',$4,$5,$6,$7,$8,0,0,$9,'{}')
        RETURNING event_id
        "#,
    )
    .bind(chain_id)
    .bind(source_contract.as_slice())
    .bind(event_name)
    .bind(vec![0u8; 32])
    .bind(block_number)
    .bind(hex::decode(block_hash.trim_start_matches("0x")).unwrap())
    .bind(1_700_000_000i64 + block_number)
    .bind(hex::decode(tx_hash.trim_start_matches("0x")).unwrap())
    .bind(addr(0x99).as_slice())
    .fetch_one(pool)
    .await
    .unwrap();
    row.0
}

/// Like [`seed_event`], but with a caller-supplied `args` JSONB payload —
/// what the P4a `ScopeFilter` join predicates (`args->>'from'`,
/// `args->>'to'`, `args->>'id'`) actually read.
#[allow(clippy::too_many_arguments)]
async fn seed_event_with_args(
    pool: &PgPool,
    chain_id: i64,
    source_contract: [u8; 20],
    event_name: &str,
    block_number: i64,
    args: serde_json::Value,
) -> i64 {
    let n = EVENT_COUNTER.fetch_add(1, Ordering::SeqCst);
    let tx_hash = format!("0x{:064x}", n);
    let block_hash = format!("0x{:064x}", block_number * 1_000_000 + n);

    let row: (i64,) = sqlx::query_as(
        r#"
        INSERT INTO audit.chain_event
            (chain_id, source_contract, source_kind, event_name, projection_tag, topic0,
             block_number, block_hash, block_timestamp, tx_hash, tx_index, log_index, tx_signer, args)
        VALUES ($1,$2,'TEST_SOURCE',$3,'primary',$4,$5,$6,$7,$8,0,0,$9,$10)
        RETURNING event_id
        "#,
    )
    .bind(chain_id)
    .bind(source_contract.as_slice())
    .bind(event_name)
    .bind(vec![0u8; 32])
    .bind(block_number)
    .bind(hex::decode(block_hash.trim_start_matches("0x")).unwrap())
    .bind(1_700_000_000i64 + block_number)
    .bind(hex::decode(tx_hash.trim_start_matches("0x")).unwrap())
    .bind(addr(0x99).as_slice())
    .bind(args)
    .fetch_one(pool)
    .await
    .unwrap();
    row.0
}

fn hexaddr(a: [u8; 20]) -> String {
    format!("0x{}", hex::encode(a))
}

fn hexid(id: [u8; 32]) -> String {
    format!("0x{}", hex::encode(id))
}

/// A minimal, single-source, single-interval graph carrying exactly one
/// `ScopeFilter` — the shape the P4a scope-filter tests below build against
/// (never the full multi-source `sample_graph`, which has no filter at all).
fn scope_filtered_graph(
    token: [u8; 20],
    source_kind: rome_audit::types::SourceKind,
    address: [u8; 20],
    from_block: i64,
    scope_filter: rome_audit::resolve::graph::ScopeFilter,
) -> ResolvedGraph {
    ResolvedGraph {
        token,
        sources: vec![ResolvedSource {
            source_kind,
            address,
            resolved_from: ResolvedFrom::TokenEvent,
            abi_ref: "test",
        }],
        intervals: vec![SourceInterval {
            address,
            from_block,
            to_block: None,
            scope_filter: Some(scope_filter),
        }],
    }
}

async fn asset_event_addresses_for(pool: &PgPool, event_id: i64) -> Vec<String> {
    sqlx::query_scalar("SELECT asset_id FROM audit.asset_event WHERE event_id = $1 ORDER BY asset_id")
        .bind(event_id)
        .fetch_all(pool)
        .await
        .unwrap()
}

// ---- P4a: ScopeFilter enforced as a REAL asset_event join predicate ----

#[tokio::test]
async fn scope_filter_yield_token_excludes_foreign_transfers() {
    let pool = fresh_test_db().await;
    let chain_id = 600001i64;

    let yield_token = addr(0xE0); // the chain-wide ERC20 (e.g. wUSDC)
    let arc_token_a = addr(0xA0);
    let stranger = addr(0xAA);
    let arc_token_b = addr(0xB0);

    // Three Transfers on the SAME chain-wide yield_token contract: one FROM
    // asset A's ArcToken, one from a totally unrelated stranger, one FROM
    // asset B's ArcToken.
    let event_a = seed_event_with_args(
        &pool,
        chain_id,
        yield_token,
        "Transfer",
        100,
        serde_json::json!({"from": hexaddr(arc_token_a), "to": hexaddr(addr(0x01)), "value": "1"}),
    )
    .await;
    let event_stranger = seed_event_with_args(
        &pool,
        chain_id,
        yield_token,
        "Transfer",
        101,
        serde_json::json!({"from": hexaddr(stranger), "to": hexaddr(addr(0x02)), "value": "1"}),
    )
    .await;
    let event_b = seed_event_with_args(
        &pool,
        chain_id,
        yield_token,
        "Transfer",
        102,
        serde_json::json!({"from": hexaddr(arc_token_b), "to": hexaddr(addr(0x03)), "value": "1"}),
    )
    .await;

    let graph_a = scope_filtered_graph(
        arc_token_a,
        SourceKind::YieldToken,
        yield_token,
        90,
        rome_audit::resolve::graph::ScopeFilter::TransferFrom { from: arc_token_a },
    );
    let graph_b = scope_filtered_graph(
        arc_token_b,
        SourceKind::YieldToken,
        yield_token,
        90,
        rome_audit::resolve::graph::ScopeFilter::TransferFrom { from: arc_token_b },
    );

    let asset_a = format!("{chain_id}:0x{}", hex::encode(arc_token_a));
    let asset_b = format!("{chain_id}:0x{}", hex::encode(arc_token_b));
    let manifest_a = CaptureManifest::from_graph(asset_a.clone(), "deadbeef".to_string(), &graph_a);
    let manifest_b = CaptureManifest::from_graph(asset_b.clone(), "deadbeef".to_string(), &graph_b);
    let hash_a = insert_capture_manifest(&pool, chain_id, &manifest_a, 1_000)
        .await
        .unwrap();
    let hash_b = insert_capture_manifest(&pool, chain_id, &manifest_b, 1_000)
        .await
        .unwrap();

    rebuild_asset_event(
        &pool,
        chain_id,
        &[
            AssetManifest { asset_id: asset_a.clone(), manifest_hash: hash_a, graph: graph_a },
            AssetManifest { asset_id: asset_b.clone(), manifest_hash: hash_b, graph: graph_b },
        ],
    )
    .await
    .unwrap();

    assert_eq!(
        asset_event_addresses_for(&pool, event_a).await,
        vec![asset_a],
        "A's own from=A transfer must land in A's junction only"
    );
    assert_eq!(
        asset_event_addresses_for(&pool, event_stranger).await,
        Vec::<String>::new(),
        "a foreign from=stranger transfer must join NEITHER asset — the whole point of the filter"
    );
    assert_eq!(
        asset_event_addresses_for(&pool, event_b).await,
        vec![asset_b],
        "B's own from=B transfer must land in B's junction only"
    );
}

#[tokio::test]
async fn scope_filter_purchase_token_touches_storefront_only() {
    let pool = fresh_test_db().await;
    let chain_id = 600002i64;

    let purchase_token = addr(0xE1); // the chain-wide ERC20 (e.g. wUSDC)
    let storefront = addr(0xC0);
    let token = addr(0xA0);
    let buyer = addr(0x50);
    let stranger_1 = addr(0x51);
    let stranger_2 = addr(0x52);

    let event_buyer_to_storefront = seed_event_with_args(
        &pool,
        chain_id,
        purchase_token,
        "Transfer",
        100,
        serde_json::json!({"from": hexaddr(buyer), "to": hexaddr(storefront), "value": "1"}),
    )
    .await;
    let event_storefront_to_someone = seed_event_with_args(
        &pool,
        chain_id,
        purchase_token,
        "Transfer",
        101,
        serde_json::json!({"from": hexaddr(storefront), "to": hexaddr(stranger_1), "value": "1"}),
    )
    .await;
    let event_unrelated = seed_event_with_args(
        &pool,
        chain_id,
        purchase_token,
        "Transfer",
        102,
        serde_json::json!({"from": hexaddr(stranger_1), "to": hexaddr(stranger_2), "value": "1"}),
    )
    .await;

    let graph = scope_filtered_graph(
        token,
        SourceKind::PurchaseToken,
        purchase_token,
        90,
        rome_audit::resolve::graph::ScopeFilter::TransferTouches { party: storefront },
    );
    let asset_id = format!("{chain_id}:0x{}", hex::encode(token));
    let manifest = CaptureManifest::from_graph(asset_id.clone(), "deadbeef".to_string(), &graph);
    let hash = insert_capture_manifest(&pool, chain_id, &manifest, 1_000)
        .await
        .unwrap();

    rebuild_asset_event(
        &pool,
        chain_id,
        &[AssetManifest { asset_id: asset_id.clone(), manifest_hash: hash, graph }],
    )
    .await
    .unwrap();

    assert_eq!(asset_event_addresses_for(&pool, event_buyer_to_storefront).await, vec![asset_id.clone()]);
    assert_eq!(asset_event_addresses_for(&pool, event_storefront_to_someone).await, vec![asset_id.clone()]);
    assert_eq!(
        asset_event_addresses_for(&pool, event_unrelated).await,
        Vec::<String>::new(),
        "a transfer touching NEITHER leg of the storefront must join no asset"
    );
}

#[tokio::test]
async fn scope_filter_morpho_market_ids() {
    let pool = fresh_test_db().await;
    let chain_id = 600003i64;

    let morpho = addr(0xE2);
    let token = addr(0xA0);
    let matched_id = [0x01u8; 32];
    let foreign_id = [0x02u8; 32];

    let event_matched_supply = seed_event_with_args(
        &pool,
        chain_id,
        morpho,
        "Supply",
        100,
        serde_json::json!({"id": hexid(matched_id), "caller": hexaddr(addr(0x10)), "onBehalf": hexaddr(addr(0x11)), "assets": "1", "shares": "1"}),
    )
    .await;
    let event_foreign_supply = seed_event_with_args(
        &pool,
        chain_id,
        morpho,
        "Supply",
        101,
        serde_json::json!({"id": hexid(foreign_id), "caller": hexaddr(addr(0x10)), "onBehalf": hexaddr(addr(0x11)), "assets": "1", "shares": "1"}),
    )
    .await;
    // Governance event with NO `id` arg at all — must stay in scope
    // unconditionally (chain-global supporting fact).
    let event_enable_irm = seed_event_with_args(
        &pool,
        chain_id,
        morpho,
        "EnableIrm",
        102,
        serde_json::json!({"irm": hexaddr(addr(0x12))}),
    )
    .await;

    let graph = scope_filtered_graph(
        token,
        SourceKind::Morpho,
        morpho,
        90,
        rome_audit::resolve::graph::ScopeFilter::MorphoMarkets {
            market_ids: vec![matched_id],
        },
    );
    let asset_id = format!("{chain_id}:0x{}", hex::encode(token));
    let manifest = CaptureManifest::from_graph(asset_id.clone(), "deadbeef".to_string(), &graph);
    let hash = insert_capture_manifest(&pool, chain_id, &manifest, 1_000)
        .await
        .unwrap();

    rebuild_asset_event(
        &pool,
        chain_id,
        &[AssetManifest { asset_id: asset_id.clone(), manifest_hash: hash, graph }],
    )
    .await
    .unwrap();

    assert_eq!(asset_event_addresses_for(&pool, event_matched_supply).await, vec![asset_id.clone()]);
    assert_eq!(
        asset_event_addresses_for(&pool, event_foreign_supply).await,
        Vec::<String>::new(),
        "a Supply on a market this asset never matched must not join"
    );
    assert_eq!(
        asset_event_addresses_for(&pool, event_enable_irm).await,
        vec![asset_id],
        "a market-less governance event (no `id` arg) stays in scope unconditionally"
    );
}

fn sample_graph(token: [u8; 20], router: [u8; 20], from_block: i64) -> ResolvedGraph {
    ResolvedGraph {
        token,
        sources: vec![
            ResolvedSource {
                source_kind: SourceKind::ArcToken,
                address: token,
                resolved_from: ResolvedFrom::TokenRead,
                abi_ref: "ArcToken",
            },
            ResolvedSource {
                source_kind: SourceKind::Router,
                address: router,
                resolved_from: ResolvedFrom::TokenRead,
                abi_ref: "RestrictionsRouter",
            },
        ],
        intervals: vec![
            SourceInterval {
                address: token,
                from_block,
                to_block: None,
                scope_filter: None,
            },
            SourceInterval {
                address: router,
                from_block,
                to_block: None,
                scope_filter: None,
            },
        ],
    }
}

// ---- capture_manifest determinism ----

#[tokio::test]
async fn capture_manifest_hash_is_identical_across_different_generated_at() {
    let pool = fresh_test_db().await;
    let chain_id = 200010i64;
    let token = addr(0xA0);
    let router = addr(0xB0);
    let graph = sample_graph(token, router, 100);

    let asset_id = format!("{chain_id}:0x{}", hex::encode(token));
    let manifest =
        CaptureManifest::from_graph(asset_id, "deadbeef".repeat(5)[..40].to_string(), &graph);

    let hash1 = insert_capture_manifest(&pool, chain_id, &manifest, 1_000)
        .await
        .unwrap();
    let hash2 = insert_capture_manifest(&pool, chain_id, &manifest, 2_000)
        .await
        .unwrap();

    assert_eq!(
        hash1, hash2,
        "identical graph must hash identically regardless of wall-clock"
    );

    // ON CONFLICT DO NOTHING: the row from the FIRST insert (generated_at =
    // 1000) wins; the second insert is a no-op against the same primary key.
    let stored = generated_at_for(&pool, hash1).await.unwrap();
    assert_eq!(stored, Some(1_000));
}

#[tokio::test]
async fn capture_manifest_hash_changes_when_the_graph_actually_changes() {
    let pool = fresh_test_db().await;
    let chain_id = 200010i64;
    let token = addr(0xA0);
    let router = addr(0xB0);
    let router_v2 = addr(0xB1);

    let asset_id = format!("{chain_id}:0x{}", hex::encode(token));
    let m1 = CaptureManifest::from_graph(
        asset_id.clone(),
        "deadbeef".to_string(),
        &sample_graph(token, router, 100),
    );
    let m2 = CaptureManifest::from_graph(
        asset_id,
        "deadbeef".to_string(),
        &sample_graph(token, router_v2, 100),
    );

    let hash1 = insert_capture_manifest(&pool, chain_id, &m1, 1_000)
        .await
        .unwrap();
    let hash2 = insert_capture_manifest(&pool, chain_id, &m2, 1_000)
        .await
        .unwrap();
    assert_ne!(hash1, hash2);
}

// ---- asset_event C1: shared-source event maps into BOTH assets ----

#[tokio::test]
async fn router_event_maps_into_both_assets_asset_event_rows_when_they_share_a_router() {
    let pool = fresh_test_db().await;
    let chain_id = 200010i64;

    let token_a = addr(0xA0);
    let token_b = addr(0xA1);
    let shared_router = addr(0xB0); // both tokens use the SAME router

    // A router event that pertains to every token whose graph includes this
    // router (capture §1.2 C1 — e.g. ModuleTypeRegistered).
    let router_event_id =
        seed_event(&pool, chain_id, shared_router, "ModuleTypeRegistered", 150).await;
    // A token-A-only event (ArcToken Transfer) must NOT leak into token B's rows.
    let token_a_only_event_id = seed_event(&pool, chain_id, token_a, "Transfer", 160).await;

    let graph_a = sample_graph(token_a, shared_router, 100);
    let graph_b = sample_graph(token_b, shared_router, 100);

    let asset_a = format!("{chain_id}:0x{}", hex::encode(token_a));
    let asset_b = format!("{chain_id}:0x{}", hex::encode(token_b));
    let manifest_a = CaptureManifest::from_graph(asset_a.clone(), "deadbeef".to_string(), &graph_a);
    let manifest_b = CaptureManifest::from_graph(asset_b.clone(), "deadbeef".to_string(), &graph_b);
    let hash_a = insert_capture_manifest(&pool, chain_id, &manifest_a, 1_000)
        .await
        .unwrap();
    let hash_b = insert_capture_manifest(&pool, chain_id, &manifest_b, 1_000)
        .await
        .unwrap();

    rebuild_asset_event(
        &pool,
        chain_id,
        &[
            AssetManifest {
                asset_id: asset_a.clone(),
                manifest_hash: hash_a,
                graph: graph_a,
            },
            AssetManifest {
                asset_id: asset_b.clone(),
                manifest_hash: hash_b,
                graph: graph_b,
            },
        ],
    )
    .await
    .unwrap();

    let router_event_assets: Vec<String> = sqlx::query_scalar(
        "SELECT asset_id FROM audit.asset_event WHERE event_id = $1 ORDER BY asset_id",
    )
    .bind(router_event_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(router_event_assets, vec![asset_a.clone(), asset_b.clone()]);

    let token_a_event_assets: Vec<String> = sqlx::query_scalar(
        "SELECT asset_id FROM audit.asset_event WHERE event_id = $1 ORDER BY asset_id",
    )
    .bind(token_a_only_event_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(token_a_event_assets, vec![asset_a]);
}

// ---- asset_event rebuild determinism ----

#[tokio::test]
async fn asset_event_rebuild_is_deterministic_drop_and_rebuild() {
    let pool = fresh_test_db().await;
    let chain_id = 200010i64;
    let token = addr(0xA0);
    let router = addr(0xB0);

    seed_event(&pool, chain_id, token, "Transfer", 120).await;
    seed_event(&pool, chain_id, router, "ModuleTypeRegistered", 130).await;
    seed_event(&pool, chain_id, token, "Transfer", 200).await;

    let graph = sample_graph(token, router, 100);
    let asset_id = format!("{chain_id}:0x{}", hex::encode(token));
    let manifest = CaptureManifest::from_graph(asset_id.clone(), "deadbeef".to_string(), &graph);
    let hash = insert_capture_manifest(&pool, chain_id, &manifest, 1_000)
        .await
        .unwrap();

    let assets = vec![AssetManifest {
        asset_id: asset_id.clone(),
        manifest_hash: hash,
        graph: graph.clone(),
    }];

    rebuild_asset_event(&pool, chain_id, &assets).await.unwrap();
    let first: Vec<(String, i64, Vec<u8>)> = sqlx::query_as(
        "SELECT asset_id, event_id, manifest_hash FROM audit.asset_event ORDER BY asset_id, event_id",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        first.len(),
        3,
        "all three seeded events fall in the resolved graph's scope"
    );

    // Drop + rebuild from the SAME chain_event + manifest inputs.
    rebuild_asset_event(&pool, chain_id, &assets).await.unwrap();
    let second: Vec<(String, i64, Vec<u8>)> = sqlx::query_as(
        "SELECT asset_id, event_id, manifest_hash FROM audit.asset_event ORDER BY asset_id, event_id",
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(first, second, "rebuild must reproduce byte-identical rows");
}

// ---- G4: `run_resolve_pass` (P3c a.3) ----

/// Wires a minimal, resolvable token into `rpc`: registered on `factory` at
/// `registered_block`, with an immutable `router` — enough for `resolve()`
/// to succeed with no further configuration (every other read this fixture
/// doesn't set defaults to "not present", which `resolve()` treats as a
/// normal negative, never an error).
async fn setup_minimal_token(
    rpc: &FakeRpc,
    factory: [u8; 20],
    token: [u8; 20],
    router: [u8; 20],
    registered_block: i64,
) {
    let impl_addr = [0xD0u8; 20];
    rpc.set_token_implementation(factory, token, impl_addr);
    rpc.set_router(token, router);

    let mut token_word = [0u8; 32];
    token_word[12..].copy_from_slice(&token);
    let mut impl_word = [0u8; 32];
    impl_word[12..].copy_from_slice(&impl_addr);

    rpc.add_log(
        factory,
        rome_audit::abi::factory::TOKEN_REGISTERED_TOPIC0,
        rome_audit::resolve::rpc::LogEntry {
            block_number: registered_block,
            tx_index: 0,
            log_index: 0,
            address: factory,
            topics: vec![
                rome_audit::abi::factory::TOKEN_REGISTERED_TOPIC0,
                token_word,
                impl_word,
            ],
            data: vec![],
        },
    );
}

#[tokio::test]
async fn resolve_pass_persists_manifests_and_rebuilds_asset_event_for_all_tokens() {
    let pool = fresh_test_db().await;
    let chain_id = 200010i64;
    let abi = rome_audit::build_registry();

    let factory = [0xF0u8; 20];
    let storefront = [0xC0u8; 20];
    let token_a = [0xA0u8; 20];
    let token_b = [0xB0u8; 20];
    let router_a = [0xA1u8; 20];
    let router_b = [0xB1u8; 20];

    let registry = FakeRegistry::new(
        "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
        factory,
        storefront,
    );
    let rpc = FakeRpc::new();
    setup_minimal_token(&rpc, factory, token_a, router_a, 100).await;
    setup_minimal_token(&rpc, factory, token_b, router_b, 100).await;

    // A real Transfer event on token_a — must land in token_a's asset_event
    // scope after the rebuild, and NOT leak into token_b's.
    let seeded_event_id = seed_event(&pool, chain_id, token_a, "Transfer", 150).await;

    let resolved = run_resolve_pass(&pool, chain_id, &[token_a, token_b], &registry, &rpc, &abi, -1)
        .await
        .unwrap();

    assert_eq!(resolved.assets.len(), 2, "one AssetManifest per token");

    let manifest_count: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM audit.capture_manifest WHERE chain_id = $1")
            .bind(chain_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(manifest_count.0, 2, "one manifest row per token");

    let asset_a = format!("{chain_id}:0x{}", hex::encode(token_a));
    let mapped: Vec<String> =
        sqlx::query_scalar("SELECT asset_id FROM audit.asset_event WHERE event_id = $1")
            .bind(seeded_event_id)
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(
        mapped,
        vec![asset_a],
        "the rebuild must have run against the FULL asset list, not a partial one"
    );

    let key = |a: [u8; 20]| format!("0x{}", hex::encode(a));
    assert_eq!(
        resolved.sources.get(&key(token_a)).map(|s| s.kind),
        Some(rome_audit::SourceKind::ArcToken)
    );
    assert_eq!(
        resolved.sources.get(&key(token_b)).map(|s| s.kind),
        Some(rome_audit::SourceKind::ArcToken)
    );
}

#[tokio::test]
async fn resolve_pass_is_idempotent_at_the_same_pin() {
    let pool = fresh_test_db().await;
    let chain_id = 200020i64;
    let abi = rome_audit::build_registry();

    let factory = [0xF1u8; 20];
    let storefront = [0xC1u8; 20];
    let token = [0xA2u8; 20];
    let router = [0xB2u8; 20];

    let registry = FakeRegistry::new(
        "cafebabecafebabecafebabecafebabecafebabe",
        factory,
        storefront,
    );
    let rpc = FakeRpc::new();
    setup_minimal_token(&rpc, factory, token, router, 200).await;

    let first = run_resolve_pass(&pool, chain_id, &[token], &registry, &rpc, &abi, -1)
        .await
        .unwrap();
    // A real wall-clock gap, so a `generated_at` that (wrongly) leaked into
    // the hash would produce a DIFFERENT hash on the second call.
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let second = run_resolve_pass(&pool, chain_id, &[token], &registry, &rpc, &abi, -1)
        .await
        .unwrap();

    assert_eq!(
        first.assets[0].manifest_hash, second.assets[0].manifest_hash,
        "the same graph at the same pin must hash identically regardless of wall-clock"
    );

    let manifest_count: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM audit.capture_manifest WHERE chain_id = $1")
            .bind(chain_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        manifest_count.0, 1,
        "the second pass's manifest must land on the SAME row (ON CONFLICT DO NOTHING), never a duplicate"
    );
}

#[tokio::test]
async fn resolve_pass_with_one_failing_token_writes_nothing() {
    let pool = fresh_test_db().await;
    let chain_id = 200030i64;
    let abi = rome_audit::build_registry();

    let factory = [0xF2u8; 20];
    let storefront = [0xC2u8; 20];
    let good_token = [0xA3u8; 20];
    let router = [0xB3u8; 20];
    let bad_token = [0xEEu8; 20]; // deliberately never configured in `rpc`/`registry`

    let registry = FakeRegistry::new(
        "0123456789012345678901234567890123456789",
        factory,
        storefront,
    );
    let rpc = FakeRpc::new();
    setup_minimal_token(&rpc, factory, good_token, router, 100).await;

    let err = run_resolve_pass(
        &pool,
        chain_id,
        &[good_token, bad_token],
        &registry,
        &rpc,
        &abi,
        -1,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, PassError::Resolve { .. }),
        "expected a Resolve error for the unconfigured token, got {err:?}"
    );

    let manifest_count: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM audit.capture_manifest WHERE chain_id = $1")
            .bind(chain_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        manifest_count.0, 0,
        "all-or-nothing: the GOOD token's manifest must not have been persisted either"
    );
}

// ---- G5: backfill-gap detection (P3c a.6) ----

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

#[tokio::test]
async fn new_source_behind_the_watermark_records_a_backfill_gap() {
    let pool = fresh_test_db().await;
    let chain_id = 200040i64;
    let abi = rome_audit::build_registry();

    let factory = [0xF3u8; 20];
    let storefront = [0xC3u8; 20];
    let token = [0xA4u8; 20];
    let router = [0xB4u8; 20];

    let registry = FakeRegistry::new(
        "1111111111111111111111111111111111111111",
        factory,
        storefront,
    );
    let rpc = FakeRpc::new();
    // The token's own registration (and therefore its router/ArcToken
    // interval anchor) is at block 50 — well BEHIND a watermark of 1000.
    setup_minimal_token(&rpc, factory, token, router, 50).await;

    let watermark = 1_000i64;
    run_resolve_pass(&pool, chain_id, &[token], &registry, &rpc, &abi, watermark)
        .await
        .unwrap();

    let gaps = backfill_gap_rows(&pool, chain_id).await;
    // Every newly-discovered source anchored at block 50 (token, router,
    // factory-at-genesis, storefront-at-genesis) is behind the watermark —
    // the token's own address is enough to prove the mechanism fires.
    assert!(
        gaps.iter().any(|(addr, from_block, wm)| {
            addr.as_slice() == token.as_slice() && *from_block == 50 && *wm == watermark
        }),
        "expected a backfill_gap row for the token itself behind the watermark, got {gaps:?}"
    );
}

#[tokio::test]
async fn a_source_anchored_exactly_at_the_watermark_is_still_a_gap() {
    // P3c M1: `from_block == watermark` means that block was already
    // scanned under the OLD (pre-discovery) map — a strict `<` would miss
    // this boundary. Must be `<=`.
    let pool = fresh_test_db().await;
    let chain_id = 200042i64;
    let abi = rome_audit::build_registry();

    let factory = [0xF6u8; 20];
    let storefront = [0xC6u8; 20];
    let token = [0xA6u8; 20];
    let router = [0xB6u8; 20];

    let registry = FakeRegistry::new(
        "8888888888888888888888888888888888888888",
        factory,
        storefront,
    );
    let rpc = FakeRpc::new();
    setup_minimal_token(&rpc, factory, token, router, 50).await;

    let watermark = 50i64; // exactly equal to the token's own anchor block
    run_resolve_pass(&pool, chain_id, &[token], &registry, &rpc, &abi, watermark)
        .await
        .unwrap();

    let gaps = backfill_gap_rows(&pool, chain_id).await;
    assert!(
        gaps.iter()
            .any(|(addr, from_block, wm)| addr.as_slice() == token.as_slice()
                && *from_block == 50
                && *wm == watermark),
        "from_block == watermark must still record a gap (already-scanned block), got {gaps:?}"
    );
}

#[tokio::test]
async fn fresh_db_records_no_gaps() {
    let pool = fresh_test_db().await;
    let chain_id = 200041i64;
    let abi = rome_audit::build_registry();

    let factory = [0xF4u8; 20];
    let storefront = [0xC4u8; 20];
    let token = [0xA5u8; 20];
    let router = [0xB5u8; 20];

    let registry = FakeRegistry::new(
        "2222222222222222222222222222222222222222",
        factory,
        storefront,
    );
    let rpc = FakeRpc::new();
    setup_minimal_token(&rpc, factory, token, router, 50).await;

    // Fresh chain ⇒ the caller (in production, `AuditWorker::tick`) passes
    // `-1` — no `audit.ingest_watermark` row yet, so the Hercules-side
    // frontier computation (`ingest::block_frontier_through_slot`) never
    // even runs (P3c H1) ⇒ `from_block <= -1` never holds for any realistic
    // (non-negative) `from_block`.
    let watermark = -1i64;

    run_resolve_pass(&pool, chain_id, &[token], &registry, &rpc, &abi, watermark)
        .await
        .unwrap();

    let gaps = backfill_gap_rows(&pool, chain_id).await;
    assert!(
        gaps.is_empty(),
        "a fresh DB must never record a gap, got {gaps:?}"
    );
}

// ---- H2: the manifest-insert / asset_event-rebuild / gap-insert primitives
// share ONE transaction inside run_resolve_pass — proven directly at the
// primitive level: an open, never-committed transaction whose LATER
// statement fails must roll back its EARLIER (individually successful)
// statement too. ----

#[tokio::test]
async fn h2_an_uncommitted_transactions_earlier_statement_rolls_back_with_its_later_failure() {
    let pool = fresh_test_db().await;
    let chain_id = 500001i64;
    let token = addr(0xA9);
    let router = addr(0xB9);
    let graph = sample_graph(token, router, 50);
    let asset_id = format!("{chain_id}:0x{}", hex::encode(token));
    let manifest = CaptureManifest::from_graph(asset_id, "deadbeef".to_string(), &graph);

    let mut tx = pool.begin().await.unwrap();
    // Step 1 (mirrors run_resolve_pass's manifest insert): succeeds, WOULD
    // be visible to a same-transaction read right now.
    let hash = insert_capture_manifest(&mut *tx, chain_id, &manifest, 1_000)
        .await
        .unwrap();

    // Step 2 (mirrors a LATER step in the same pass, e.g. gap detection):
    // fails for real — an FK violation against a manifest_hash that was
    // never inserted (a stand-in for "something later in the pass genuinely
    // errors").
    let bogus_hash = [0xFFu8; 32];
    let later_step_result = insert_backfill_gap(
        &mut *tx,
        chain_id,
        addr(0x01),
        "ARC_TOKEN",
        10,
        20,
        bogus_hash,
        1_000,
    )
    .await;
    assert!(
        later_step_result.is_err(),
        "setup: the FK violation must actually fail"
    );

    // `run_resolve_pass` reacts to this exact shape of failure with `?`,
    // which drops `tx` without ever calling `.commit()` — Postgres rolls
    // back the WHOLE transaction, including step 1's otherwise-successful
    // insert.
    drop(tx);

    let stored = generated_at_for(&pool, hash).await.unwrap();
    assert_eq!(
        stored, None,
        "step 1's manifest insert must NOT be visible — it was only ever staged inside the \
         transaction that step 2's failure (and the resulting drop-without-commit) rolled back"
    );
}

// ---- M3: rebuild_asset_event is chain-SCOPED, not a global TRUNCATE ----
//
// `audit` lives in the SHARED rome_via_db — a second chain's own resolve
// pass must never wipe another chain's already-built asset_event rows.

#[tokio::test]
async fn rebuild_is_scoped_to_its_own_chain_other_chains_rows_survive() {
    let pool = fresh_test_db().await;
    let chain_a = 400001i64;
    let chain_b = 400002i64;

    let token_a = addr(0xA0);
    let router_a = addr(0xB0);
    seed_event(&pool, chain_a, token_a, "Transfer", 100).await;
    let graph_a = sample_graph(token_a, router_a, 90);
    let asset_a = format!("{chain_a}:0x{}", hex::encode(token_a));
    let manifest_a = CaptureManifest::from_graph(asset_a.clone(), "deadbeef".to_string(), &graph_a);
    let hash_a = insert_capture_manifest(&pool, chain_a, &manifest_a, 1_000)
        .await
        .unwrap();
    rebuild_asset_event(
        &pool,
        chain_a,
        &[AssetManifest {
            asset_id: asset_a,
            manifest_hash: hash_a,
            graph: graph_a,
        }],
    )
    .await
    .unwrap();

    let count_a_before: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM audit.asset_event WHERE chain_id = $1")
            .bind(chain_a)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count_a_before.0, 1, "setup: chain A's row landed");

    // A DIFFERENT chain's resolve pass rebuilds ITS OWN asset_event rows —
    // must not touch chain A's.
    let token_b = addr(0xC0);
    let router_b = addr(0xD0);
    seed_event(&pool, chain_b, token_b, "Transfer", 100).await;
    let graph_b = sample_graph(token_b, router_b, 90);
    let asset_b = format!("{chain_b}:0x{}", hex::encode(token_b));
    let manifest_b = CaptureManifest::from_graph(asset_b.clone(), "deadbeef".to_string(), &graph_b);
    let hash_b = insert_capture_manifest(&pool, chain_b, &manifest_b, 1_000)
        .await
        .unwrap();
    rebuild_asset_event(
        &pool,
        chain_b,
        &[AssetManifest {
            asset_id: asset_b,
            manifest_hash: hash_b,
            graph: graph_b,
        }],
    )
    .await
    .unwrap();

    let count_a_after: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM audit.asset_event WHERE chain_id = $1")
            .bind(chain_a)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        count_a_after.0, 1,
        "chain A's asset_event row must survive chain B's rebuild"
    );
}
