//! G6 — the resolve→ingest `AuditWorker` (P3c a.5), against a REAL local
//! Postgres holding BOTH the Hercules-shaped fixture schema (ingest's read
//! side) and the real `audit` schema (both resolve's and ingest's write
//! side) in one DB — exactly what production does (two logical roles, one
//! `target` pool for the audit schema; `source` happens to be the SAME pool
//! here since the fixture colocates both schemas, same convention as
//! `tests/ingest_db.rs`).
//!
//! CONTRACT UNDER TEST (P3c a.5): `SourceMode::Resolved` holds ingest
//! (never advances the watermark) until the FIRST resolve pass succeeds;
//! once resolved, ingest captures the FULL resolved source set (every
//! `SourceKind` the graph touched, not just the seed token); periodic
//! re-resolution swaps in newly-discovered sources; a re-resolution
//! FAILURE never stalls ingest — the last-good source map keeps working,
//! and the failure is surfaced on `TickOutcome`, never only as a log line;
//! `SourceMode::Static` is byte-for-byte the pre-P3c behavior.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;

use rome_audit::ingest::{IngestConfig, SourceSpec};
use rome_audit::resolve::rpc::LogEntry;
use rome_audit::resolve::{run_resolve_pass, PassError};
use rome_audit::run::{AuditWorker, SourceMode, TickOutcome};
use rome_audit::types::SourceKind;

mod common;
use common::{fresh_hercules_audit_db, seed_contiguous_finalized_chain, seed_tx_with_logs, FakeRegistry, FakeRpc};

fn addr(b: u8) -> [u8; 20] {
    [b; 20]
}

fn hex_addr(a: [u8; 20]) -> String {
    format!("0x{}", hex::encode(a))
}

fn address_word(a: [u8; 20]) -> [u8; 32] {
    let mut w = [0u8; 32];
    w[12..].copy_from_slice(&a);
    w
}

fn hex_word(w: &[u8; 32]) -> String {
    format!("0x{}", hex::encode(w))
}

fn address_topic(a: [u8; 20]) -> String {
    hex_word(&address_word(a))
}

fn bool_word(v: bool) -> [u8; 32] {
    let mut w = [0u8; 32];
    w[31] = v as u8;
    w
}

fn hex_data(words: &[[u8; 32]]) -> String {
    let mut bytes = Vec::with_capacity(32 * words.len());
    for w in words {
        bytes.extend_from_slice(w);
    }
    format!("0x{}", hex::encode(bytes))
}

/// Wires a minimal, resolvable token into `rpc`/`registry`: registered on
/// `factory` at `registered_block`, with an immutable `router` — enough for
/// `resolve()` to succeed with no further configuration.
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

/// Ticks `worker` until `target_slot` shows up in an ingest outcome's
/// `slots_passed` (or panics after a generous bound). Extracts the ingest
/// outcome from EITHER `TickOutcome::Ingested` or
/// `TickOutcome::ReResolveFailed` — a.5 point 4's "later pass failures
/// never stall ingest" means ingest still ran (and may have advanced) on a
/// tick that ALSO reports a resolve failure, so a caller only interested in
/// ingest progress must not special-case that variant away.
async fn tick_until_slot_passes(worker: &mut AuditWorker, target_slot: i64) -> Vec<i64> {
    let mut passed = Vec::new();
    for _ in 0..(target_slot + 100) {
        let outcome = match worker.tick().await {
            TickOutcome::Ingested(outcome) => Some(outcome),
            TickOutcome::ReResolveFailed { ingest, .. } => Some(ingest),
            TickOutcome::AwaitingFirstResolution => None,
        };
        if let Some(outcome) = outcome {
            passed.extend(outcome.slots_passed);
            if passed.contains(&target_slot) {
                return passed;
            }
        }
    }
    panic!("slot {target_slot} never passed within the tick budget — the worker is stuck");
}

async fn chain_event_source_kinds(pool: &PgPool, chain_id: i64) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT DISTINCT source_kind FROM audit.chain_event WHERE chain_id = $1 ORDER BY source_kind",
    )
    .bind(chain_id)
    .fetch_all(pool)
    .await
    .unwrap()
}

async fn capture_manifest_hashes(pool: &PgPool, chain_id: i64, asset_id: &str) -> Vec<Vec<u8>> {
    sqlx::query_scalar(
        "SELECT manifest_hash FROM audit.capture_manifest WHERE chain_id = $1 AND asset_id = $2 ORDER BY generated_at",
    )
    .bind(chain_id)
    .bind(asset_id)
    .fetch_all(pool)
    .await
    .unwrap()
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

async fn quarantine_count(pool: &PgPool, chain_id: i64) -> i64 {
    let (n,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM audit.quarantine WHERE chain_id = $1")
        .bind(chain_id)
        .fetch_one(pool)
        .await
        .unwrap();
    n
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

// ---- 13: the headline — the FULL resolved source set gets ingested ----

#[tokio::test]
async fn resolved_mode_ingests_the_full_resolved_source_set() {
    let pool = fresh_hercules_audit_db().await;
    let chain_id = 300010i64;

    let factory = addr(0xF0);
    let storefront = addr(0xC0);
    let token = addr(0xA0);
    let router = addr(0xB0);
    let pair = addr(0xD1);

    let registry = Arc::new(FakeRegistry::new(
        "3333333333333333333333333333333333333333",
        factory,
        storefront,
    ));
    let rpc = Arc::new(FakeRpc::new());
    setup_minimal_token(&rpc, factory, token, router, 20).await;
    // UV2 discovery: token's own Transfer log points at `pair`, confirmed by
    // `pair.token0()==token`.
    rpc.set_pair(pair, token, addr(0xE0));
    rpc.add_log(
        token,
        rome_audit::abi::arc_token::TRANSFER_TOPIC0,
        LogEntry {
            block_number: 20,
            tx_index: 0,
            log_index: 0,
            address: token,
            topics: vec![
                rome_audit::abi::arc_token::TRANSFER_TOPIC0,
                address_word(addr(0x99)),
                address_word(pair),
            ],
            data: vec![0u8; 32],
        },
    );

    let mut worker = AuditWorker::new(
        pool.clone(),
        pool.clone(),
        chain_id,
        2,
        30,
        SourceMode::Resolved {
            registry,
            rpc,
            tokens: vec![token],
            reresolve_interval: Duration::from_secs(3600),
            discovery_enabled: false,
        },
    );

    // B1 (forward-tip semantics): the FIRST tick initializes the watermark
    // at `min(tip, max_produced) - lag`, so a low tip must already exist for
    // resolution (and everything after) to proceed — seed a short finalized
    // chain first. Its tip (5) is well BELOW the token's registration block
    // (20), so the initial resolve records NO backfill gap; the slot-40
    // events seeded below (once the tip is extended to 50) are then captured
    // by NORMAL forward LIVE ingest, exactly as this test intends.
    seed_contiguous_finalized_chain(&pool, 0, 5).await;

    // Resolve pass 1 (this tick) discovers ArcToken(token) + Router(router)
    // + Factory(factory) + Storefront(storefront) + Uv2Pair(pair) — five
    // distinct SourceKinds, none of them hardcoded by the ingest side. It
    // succeeds immediately (the fixture is fully wired), so `resolved_once`
    // flips true WITHIN this same tick and the hold check below no longer
    // applies — this tick both resolves AND runs an (empty-window) ingest
    // pass in one call, which is why the outcome is `Ingested`, not
    // `AwaitingFirstResolution` (that variant is for a tick where resolution
    // has NEVER yet succeeded — see `ingest_holds_watermark_until_first_resolution_succeeds`).
    worker.tick().await;

    // Now seed a REAL Hercules-shaped log for each of the five, in ONE tx at
    // slot 40, using ONLY events registered in src/abi/* (else they'd quarantine).
    seed_contiguous_finalized_chain(&pool, 0, 39).await;
    let block_hash = format!("0x{:064x}", 40);
    common::seed_sol_slot(&pool, 40, 39, "Finalized", &block_hash, 1_700_000_040).await;
    seed_tx_with_logs(
        &pool,
        40,
        &block_hash,
        40,
        &hex_addr(addr(0x55)),
        &[
            // ArcToken.Transfer(from,to,value) — two indexed + one non-indexed.
            (
                hex_addr(token),
                hex_word(&rome_audit::abi::arc_token::TRANSFER_TOPIC0),
                Some(address_topic(addr(0x99))),
                Some(address_topic(pair)),
                None,
                Some(hex_data(&[address_word(addr(1))])), // value=1 (reusing address_word as a nonzero 32-byte word)
            ),
            // Router.ModuleTypeRegistered(typeId, isGlobal, globalImplementation) —
            // one indexed + two non-indexed.
            (
                hex_addr(router),
                hex_word(&rome_audit::abi::router::MODULE_TYPE_REGISTERED_TOPIC0),
                Some(hex_word(&[0x7bu8; 32])),
                None,
                None,
                Some(hex_data(&[bool_word(true), address_word(addr(0x61))])),
            ),
            // Factory.TokenRegistered(token, implementation) — two indexed, no data.
            (
                hex_addr(factory),
                hex_word(&rome_audit::abi::factory::TOKEN_REGISTERED_TOPIC0),
                Some(address_topic(token)),
                Some(address_topic(addr(0xD0))),
                None,
                None,
            ),
            // Storefront.PurchaseTokenUpdated(newPurchaseToken) — one indexed, no data.
            (
                hex_addr(storefront),
                hex_word(&rome_audit::abi::storefront::PURCHASE_TOKEN_UPDATED_TOPIC0),
                Some(address_topic(addr(0x62))),
                None,
                None,
                None,
            ),
            // Uv2Pair.Transfer(from,to,value) — same topic0 as ArcToken's own
            // Transfer, registered under a DIFFERENT SourceKind (Uv2Pair).
            (
                hex_addr(pair),
                hex_word(&rome_audit::abi::arc_token::TRANSFER_TOPIC0),
                Some(address_topic(pair)),
                Some(address_topic(addr(0x63))),
                None,
                Some(hex_data(&[address_word(addr(2))])),
            ),
        ],
    )
    .await;

    // Head-clearance + stability for slot 40.
    seed_contiguous_finalized_chain(&pool, 41, 50).await;

    tick_until_slot_passes(&mut worker, 40).await;

    let kinds = chain_event_source_kinds(&pool, chain_id).await;
    assert_eq!(
        kinds,
        vec!["ARC_TOKEN", "FACTORY", "ROUTER", "STOREFRONT", "UV2_PAIR"],
        "every resolved SourceKind must have a captured chain_event row — got {kinds:?}"
    );
}

// ---- 14: hold-until-first-resolution ----

#[tokio::test]
async fn ingest_holds_watermark_until_first_resolution_succeeds() {
    let pool = fresh_hercules_audit_db().await;
    let chain_id = 300011i64;

    let factory = addr(0xF1);
    let storefront = addr(0xC1);
    let token = addr(0xA1);
    let router = addr(0xB1);

    let registry = Arc::new(FakeRegistry::new(
        "4444444444444444444444444444444444444444",
        factory,
        storefront,
    ));
    let rpc = Arc::new(FakeRpc::new());
    // Scripted to fail every RPC call up front — resolve() cannot succeed.
    rpc.set_fail(true);

    // A perfectly healthy, stably-produced Hercules chain sits there the
    // WHOLE TIME — proving any hold is due to the resolution gate, not a
    // missing/unstable slot.
    seed_contiguous_finalized_chain(&pool, 0, 40).await; // trailing slots give slot 30 head-clearance

    let mut worker = AuditWorker::new(
        pool.clone(),
        pool.clone(),
        chain_id,
        2,
        30,
        SourceMode::Resolved {
            registry,
            rpc: rpc.clone(),
            tokens: vec![token],
            reresolve_interval: Duration::from_secs(3600),
            discovery_enabled: false,
        },
    );

    for _ in 0..5 {
        let outcome = worker.tick().await;
        assert!(
            matches!(outcome, TickOutcome::AwaitingFirstResolution),
            "resolve() is scripted to fail — every tick must hold, got {outcome:?}"
        );
    }
    // B1 (forward-tip semantics): the watermark ROW is initialized at the
    // forward tip (min(tip, max_produced) - lag = 40 - 2 = 38) on the very
    // first tick, BEFORE the resolve gate — that's the pure forward tip, not
    // captured progress. The invariant the resolution gate actually protects
    // is that NOTHING is captured and NO manifest is written while resolution
    // has never succeeded.
    assert_eq!(
        watermark(&pool, chain_id).await,
        Some(38),
        "the watermark initializes at the forward tip (40 - lag 2); it never advances PAST it while held"
    );
    assert_eq!(chain_event_count(&pool, chain_id).await, 0, "nothing captured while resolution is held");
    assert_eq!(capture_manifest_count(&pool, chain_id).await, 0, "no manifest written while resolution is held");

    // Now let resolution succeed, and extend the chain so live ingest has
    // head-room to advance forward from the init point (38).
    rpc.set_fail(false);
    setup_minimal_token(&rpc, factory, token, router, 5).await;
    seed_contiguous_finalized_chain(&pool, 41, 60).await; // tip -> 60

    let mut advanced = false;
    for _ in 0..40 {
        worker.tick().await;
        if watermark(&pool, chain_id).await.unwrap_or(-1) > 38 {
            advanced = true;
            break;
        }
    }
    assert!(advanced, "once resolution succeeds, live ingest must advance the watermark past the init point");
    assert!(
        capture_manifest_count(&pool, chain_id).await >= 1,
        "resolution actually succeeded — a manifest must now exist"
    );
}

// ---- 15: periodic re-resolution swaps in a newly-discovered source ----

#[tokio::test]
async fn reresolution_swaps_in_a_newly_discovered_source() {
    let pool = fresh_hercules_audit_db().await;
    let chain_id = 300012i64;

    let factory = addr(0xF2);
    let storefront = addr(0xC2);
    let token = addr(0xA2);
    let router = addr(0xB2);

    let registry = Arc::new(FakeRegistry::new(
        "5555555555555555555555555555555555555555",
        factory,
        storefront,
    ));
    let rpc = Arc::new(FakeRpc::new());
    setup_minimal_token(&rpc, factory, token, router, 5).await;

    let mut worker = AuditWorker::new(
        pool.clone(),
        pool.clone(),
        chain_id,
        2,
        30,
        SourceMode::Resolved {
            registry,
            rpc: rpc.clone(),
            tokens: vec![token],
            reresolve_interval: Duration::from_millis(1),
            discovery_enabled: false,
        },
    );

    // B1: a low source tip must exist for the first tick to initialize the
    // watermark and proceed to resolution. Tip (5) is at/above the token's
    // registration block (5), so the initial resolve records no gap.
    seed_contiguous_finalized_chain(&pool, 0, 5).await;

    // Resolve pass 1 succeeds immediately (fixture is fully wired).
    worker.tick().await;

    let asset_id = format!("{chain_id}:0x{}", hex::encode(token));
    let hashes_after_first = capture_manifest_hashes(&pool, chain_id, &asset_id).await;
    assert_eq!(hashes_after_first.len(), 1);

    // A NEW UV2 pair appears — mutate the already-shared FakeRpc (its
    // setters are `&self`, per a.8) so the NEXT resolve pass discovers it.
    tokio::time::sleep(Duration::from_millis(5)).await; // cross the 1ms interval
    let pair = addr(0xD2);
    rpc.set_pair(pair, token, addr(0xE2));
    rpc.add_log(
        token,
        rome_audit::abi::arc_token::TRANSFER_TOPIC0,
        LogEntry {
            block_number: 6,
            tx_index: 0,
            log_index: 0,
            address: token,
            topics: vec![
                rome_audit::abi::arc_token::TRANSFER_TOPIC0,
                address_word(addr(0x99)),
                address_word(pair),
            ],
            data: vec![0u8; 32],
        },
    );

    let second = worker.tick().await;
    // P3c M4: `resolved_once` is ALREADY true from pass 1 — a re-resolution
    // that succeeds must never regress to `AwaitingFirstResolution`.
    assert!(
        matches!(second, TickOutcome::Ingested(_)),
        "expected Ingested (resolved_once was already true), got {second:?}"
    );

    let hashes_after_second = capture_manifest_hashes(&pool, chain_id, &asset_id).await;
    assert_eq!(
        hashes_after_second.len(),
        2,
        "re-resolution discovering a new source must persist a SECOND manifest row, not overwrite the first"
    );
    assert_ne!(
        hashes_after_second[0], hashes_after_second[1],
        "the two manifests must hash DIFFERENTLY — the graph actually changed"
    );

    // P3c M4: the swap must have reached INGEST, not just the manifest —
    // seed a real Hercules log for the newly-discovered pair and confirm a
    // UV2_PAIR chain_event actually lands.
    seed_contiguous_finalized_chain(&pool, 0, 19).await;
    let block_hash = format!("0x{:064x}", 20);
    common::seed_sol_slot(&pool, 20, 19, "Finalized", &block_hash, 1_700_000_020).await;
    seed_tx_with_logs(
        &pool,
        20,
        &block_hash,
        20,
        &hex_addr(addr(0x55)),
        &[(
            hex_addr(pair),
            hex_word(&rome_audit::abi::arc_token::TRANSFER_TOPIC0),
            Some(address_topic(pair)),
            Some(address_topic(addr(0x63))),
            None,
            Some(hex_data(&[address_word(addr(1))])),
        )],
    )
    .await;
    seed_contiguous_finalized_chain(&pool, 21, 30).await;

    tick_until_slot_passes(&mut worker, 20).await;

    let kinds = chain_event_source_kinds(&pool, chain_id).await;
    assert!(
        kinds.contains(&"UV2_PAIR".to_string()),
        "the newly-discovered pair's own log must have actually been captured, got {kinds:?}"
    );
}

// ---- 16: a re-resolution FAILURE never stalls ingest ----

#[tokio::test]
async fn reresolution_failure_keeps_last_good_sources() {
    let pool = fresh_hercules_audit_db().await;
    let chain_id = 300013i64;

    let factory = addr(0xF3);
    let storefront = addr(0xC3);
    let token = addr(0xA3);
    let router = addr(0xB3);

    let registry = Arc::new(FakeRegistry::new(
        "6666666666666666666666666666666666666666",
        factory,
        storefront,
    ));
    let rpc = Arc::new(FakeRpc::new());
    setup_minimal_token(&rpc, factory, token, router, 5).await;

    // B1: start with a low tip (5) so the first tick initializes the
    // watermark low and the initial resolve records no gap (token block 5 is
    // at the tip); the chain is then EXTENDED so live ingest has head-room to
    // advance forward — that forward advance is what must keep working across
    // a later re-resolution FAILURE.
    seed_contiguous_finalized_chain(&pool, 0, 5).await;

    let mut worker = AuditWorker::new(
        pool.clone(),
        pool.clone(),
        chain_id,
        2,
        30,
        SourceMode::Resolved {
            registry,
            rpc: rpc.clone(),
            tokens: vec![token],
            reresolve_interval: Duration::from_millis(1),
            discovery_enabled: false,
        },
    );

    // Resolve pass 1 succeeds immediately (fixture is fully wired) —
    // resolved_once flips true within this same tick.
    worker.tick().await;

    // Extend the tip and ingest a good stretch on the last-good (successfully
    // resolved) map.
    seed_contiguous_finalized_chain(&pool, 6, 40).await; // tip -> 40
    for _ in 0..20 {
        worker.tick().await;
    }
    let watermark_before_failure = watermark(&pool, chain_id).await.unwrap();
    assert!(
        watermark_before_failure > 5,
        "live ingest must have advanced on the last-good map before the failure, got {watermark_before_failure}"
    );

    // Now force every subsequent resolve attempt to fail, cross the
    // re-resolve interval, and give live ingest MORE head-room.
    rpc.set_fail(true);
    tokio::time::sleep(Duration::from_millis(5)).await;
    seed_contiguous_finalized_chain(&pool, 41, 60).await; // tip -> 60

    let outcome = worker.tick().await;
    match outcome {
        TickOutcome::ReResolveFailed { error, .. } => {
            let _ = format!("{error}"); // the error type itself is the assertion target
        }
        other => panic!("expected ReResolveFailed carrying the PassError, got {other:?}"),
    }

    // Ingest must keep working on the last-good map — the watermark advances
    // PAST where it already was, never rewinding or freezing, despite every
    // re-resolution now failing.
    let mut advanced_past = false;
    for _ in 0..40 {
        worker.tick().await;
        if watermark(&pool, chain_id).await.unwrap() > watermark_before_failure {
            advanced_past = true;
            break;
        }
    }
    assert!(
        advanced_past,
        "live ingest must have advanced the watermark past {watermark_before_failure} despite the re-resolution failures"
    );
}

// ---- 17: `SourceMode::Static` is unchanged from pre-P3c behavior ----

#[tokio::test]
async fn static_mode_behavior_is_unchanged() {
    let pool = fresh_hercules_audit_db().await;
    let chain_id = 300014i64;

    let sanctions_module = addr(0x11);
    let mut sources = BTreeMap::new();
    sources.insert(hex_addr(sanctions_module), SourceSpec::new(SourceKind::GlobalSanctions));

    // B1: Static mode has no resolve/backfill lane, so a captured event must
    // sit ABOVE the forward-tip init point. Seed a low tip first, tick to
    // initialize the watermark low (min(5,5) - lag 2 = 3), THEN place the
    // event at slot 10 and extend the tip to 20 — slot 10 is now in the
    // (init, tip - lag] = (3, 18] window normal forward LIVE ingest captures.
    seed_contiguous_finalized_chain(&pool, 0, 5).await;
    let mut worker = AuditWorker::new(
        pool.clone(),
        pool.clone(),
        chain_id,
        2,
        30,
        SourceMode::Static(sources),
    );
    worker.tick().await; // initializes the watermark at the forward tip (3)

    seed_contiguous_finalized_chain(&pool, 6, 20).await; // tip -> 20
    let block_hash = format!("0x{:064x}", 10);
    seed_tx_with_logs(
        &pool,
        10,
        &block_hash,
        10,
        &hex_addr(addr(0x55)),
        &[(
            hex_addr(sanctions_module),
            hex_word(&rome_audit::abi::global_sanctions::SANCTIONED_TOPIC0),
            Some(address_topic(addr(0xAA))),
            None,
            None,
            None,
        )],
    )
    .await;

    tick_until_slot_passes(&mut worker, 10).await;
    assert_eq!(
        chain_event_count(&pool, chain_id).await,
        1,
        "static mode must decode+capture exactly the caller-supplied map, same as pre-P3c P1"
    );
}

// ---- P4b: GlobalSanctions V2 surface (`SanctionedSet`) captures end-to-end
// through the real resolve→ingest→decode pipeline, not just the in-memory
// decoder unit tests in `src/decode.rs`. ----

#[tokio::test]
async fn static_mode_captures_sanctioned_set_v2_event() {
    let pool = fresh_hercules_audit_db().await;
    let chain_id = 300021i64;

    let sanctions_module = addr(0x11);
    let mut sources = BTreeMap::new();
    sources.insert(hex_addr(sanctions_module), SourceSpec::new(SourceKind::GlobalSanctions));

    seed_contiguous_finalized_chain(&pool, 0, 5).await;
    let mut worker = AuditWorker::new(
        pool.clone(),
        pool.clone(),
        chain_id,
        2,
        30,
        SourceMode::Static(sources),
    );
    worker.tick().await; // initializes the watermark at the forward tip (3)

    seed_contiguous_finalized_chain(&pool, 6, 20).await; // tip -> 20
    let block_hash = format!("0x{:064x}", 10);
    seed_tx_with_logs(
        &pool,
        10,
        &block_hash,
        10,
        &hex_addr(addr(0x55)),
        &[(
            hex_addr(sanctions_module),
            hex_word(&rome_audit::abi::global_sanctions::SANCTIONED_SET_TOPIC0),
            Some(address_topic(addr(0xAA))),
            None,
            None,
            Some(hex_word(&bool_word(true))),
        )],
    )
    .await;

    tick_until_slot_passes(&mut worker, 10).await;
    assert_eq!(
        chain_event_count(&pool, chain_id).await,
        1,
        "the V2 SanctionedSet log must decode+capture through the real pipeline"
    );

    let (source_kind, event_name): (String, String) = sqlx::query_as(
        "SELECT source_kind, event_name FROM audit.chain_event WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(source_kind, "GLOBAL_SANCTIONS");
    assert_eq!(event_name, "SanctionedSet");
}

// ---- P4a: YieldToken now HAS a real descriptor (closing the C1 gap this
// test originally guarded from the OTHER side) — the live ingest map now
// INCLUDES it, and THE LANDMINE-safety mechanism moves to
// `ingest::IngestFilter`: a chain-wide yield token's foreign transfers must
// still never reach `audit.chain_event`, now via Layer 2 filtering at
// capture time rather than via C1's blanket address exclusion. ----

#[tokio::test]
async fn ingest_filter_yield_token_drops_foreign_at_capture() {
    let pool = fresh_hercules_audit_db().await;
    let chain_id = 300020i64;
    let abi = rome_audit::build_registry();

    let factory = addr(0xF5);
    let storefront = addr(0xC5);
    let token = addr(0xA5);
    let router = addr(0xB5);
    let yield_token = addr(0xE5); // wUSDC-like — re-pointed via YieldTokenUpdated

    let registry = FakeRegistry::new(
        "9999999999999999999999999999999999999999",
        factory,
        storefront,
    );
    let rpc = FakeRpc::new();
    setup_minimal_token(&rpc, factory, token, router, 5).await;
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

    let resolved = run_resolve_pass(&pool, chain_id, &[token], &registry, &rpc, &abi, -1)
        .await
        .unwrap();

    assert!(
        resolved.assets[0]
            .graph
            .sources
            .iter()
            .any(|s| s.address == yield_token && s.source_kind == SourceKind::YieldToken),
        "the resolved graph must record the YieldToken source"
    );
    // P4a: YieldToken now HAS a registered descriptor (abi::erc20) — C1's
    // retain no longer drops it from the live ingest map.
    let key = hex_addr(yield_token);
    let spec = resolved
        .sources
        .get(&key)
        .expect("YieldToken must now be IN the live ingest map (it has a real descriptor)");
    assert_eq!(spec.kind, SourceKind::YieldToken);
    match &spec.ingest_filter {
        rome_audit::ingest::IngestFilter::TransferFrom(set) => {
            assert!(set.contains(&token), "the filter must key on THIS asset's own ArcToken address");
        }
        other => panic!("expected a TransferFrom ingest filter, got {other:?}"),
    }

    // THE landmine scenario: THREE wUSDC transfers, chain-wide, seeded as
    // REAL Hercules logs at the yield-token address — only the one FROM
    // this asset's own ArcToken may land.
    seed_contiguous_finalized_chain(&pool, 0, 9).await;
    let block_hash = format!("0x{:064x}", 10);
    common::seed_sol_slot(&pool, 10, 9, "Finalized", &block_hash, 1_700_000_010).await;
    seed_tx_with_logs(
        &pool,
        10,
        &block_hash,
        10,
        &hex_addr(addr(0x55)),
        &[
            (
                hex_addr(yield_token),
                hex_word(&rome_audit::abi::arc_token::TRANSFER_TOPIC0),
                Some(address_topic(token)), // from = this asset's ArcToken — MUST land
                Some(address_topic(addr(0x12))),
                None,
                Some(hex_data(&[address_word(addr(1))])),
            ),
            (
                hex_addr(yield_token),
                hex_word(&rome_audit::abi::arc_token::TRANSFER_TOPIC0),
                Some(address_topic(addr(0x11))), // from = an unrelated stranger — MUST NOT land
                Some(address_topic(addr(0x12))),
                None,
                Some(hex_data(&[address_word(addr(1))])),
            ),
            (
                hex_addr(yield_token),
                hex_word(&rome_audit::abi::arc_token::TRANSFER_TOPIC0),
                Some(address_topic(addr(0x13))), // from = a DIFFERENT unrelated stranger — MUST NOT land
                Some(address_topic(addr(0x14))),
                None,
                Some(hex_data(&[address_word(addr(1))])),
            ),
        ],
    )
    .await;
    seed_contiguous_finalized_chain(&pool, 11, 20).await;

    let mut tracker = rome_audit::ingest::WatermarkTracker::new();
    let ingest_config = IngestConfig {
        chain_id,
        confirmation_lag: 2,
        sources: resolved.sources.clone(),
    };
    for _ in 0..30 {
        rome_audit::ingest::run_ingest_once(&pool, &pool, &abi, &mut tracker, &ingest_config, 30)
            .await
            .unwrap();
    }

    assert_eq!(
        chain_event_count(&pool, chain_id).await,
        1,
        "exactly ONE of the three wUSDC transfers (from=this asset's ArcToken) must land"
    );
    assert_eq!(
        quarantine_count(&pool, chain_id).await,
        0,
        "the two filtered-out foreign transfers are neither errors nor quarantined — they were \
         correctly matched by address+topic0 and just aren't in THIS asset's scope"
    );
}

// ---- H1 (HIGH): the resolve-pass frontier is Hercules-derived, not
// MAX(chain_event.block_number) — a chain_event row far behind the true
// scan frontier must not suppress a real gap. ----

#[tokio::test]
async fn gap_detection_uses_the_hercules_derived_frontier_not_the_last_captured_event() {
    let pool = fresh_hercules_audit_db().await;
    let chain_id = 300021i64;

    let factory = addr(0xF6);
    let storefront = addr(0xC6);
    let token = addr(0xA6);
    let router = addr(0xB6);

    let registry = Arc::new(FakeRegistry::new(
        "1010101010101010101010101010101010101010",
        factory,
        storefront,
    ));
    let rpc = Arc::new(FakeRpc::new());
    // The token's own anchor (block 50) sits WAY BEHIND the true Hercules
    // scan frontier this test sets up below (block/slot 200) — but AHEAD of
    // the last captured chain_event (block 10). The OLD (buggy)
    // MAX(chain_event.block_number)-based watermark would compute 10 and
    // MISS this gap (50 is not < 10); the FIX must compute 200 and catch it.
    setup_minimal_token(&rpc, factory, token, router, 50).await;

    // A chain_event row far behind the true frontier — proves the fix does
    // NOT derive the watermark from this table.
    seed_bare_chain_event(&pool, chain_id, addr(0x01), 10).await;

    // The real Hercules scan frontier: slot/block 0..200, all finalized +
    // produced (block number == slot in this fixture, matching every other
    // test's convention).
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
            tokens: vec![token],
            reresolve_interval: Duration::from_secs(3600),
            discovery_enabled: false,
        },
    );

    worker.tick().await; // the resolve pass itself; may also run an ingest tick

    let gaps: Vec<(Vec<u8>, i64, i64)> = sqlx::query_as(
        "SELECT source_contract, from_block, watermark_at_detection FROM audit.backfill_gap \
         WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_all(&pool)
    .await
    .unwrap();

    assert!(
        gaps.iter().any(|(addr, from_block, wm)| {
            addr.as_slice() == token.as_slice() && *from_block == 50 && *wm == 200
        }),
        "expected a gap at the Hercules-derived frontier (200), not the chain_event-derived one \
         (10) which would have missed it entirely — got {gaps:?}"
    );
}

async fn seed_bare_chain_event(pool: &PgPool, chain_id: i64, source_contract: [u8; 20], block_number: i64) {
    sqlx::query(
        r#"
        INSERT INTO audit.chain_event
            (chain_id, source_contract, source_kind, event_name, projection_tag, topic0,
             block_number, block_hash, block_timestamp, tx_hash, tx_index, log_index, tx_signer, args)
        VALUES ($1,$2,'TEST_SOURCE','Test','primary',$3,$4,$5,$6,$7,0,0,$8,'{}')
        "#,
    )
    .bind(chain_id)
    .bind(source_contract.as_slice())
    .bind(vec![0u8; 32])
    .bind(block_number)
    .bind(vec![0u8; 32])
    .bind(1_700_000_000i64)
    .bind(vec![0u8; 32])
    .bind(addr(0x99).as_slice())
    .execute(pool)
    .await
    .unwrap();
}

// ---- H2 (HIGH): steps 2-5 of run_resolve_pass are ONE transaction ----
//
// The PRIMITIVE-level proof that the manifest-insert / asset_event-rebuild /
// gap-insert calls actually share one transaction (and that a later
// statement's failure rolls back an earlier one within it) lives in
// `tests/resolve_db.rs::h2_an_uncommitted_transactions_earlier_statement_rolls_back_with_its_later_failure`
// — driving that scenario through `run_resolve_pass`'s OWN public surface
// isn't possible without a fault-injection seam this crate doesn't have
// (every write inside the transaction is a straight-line, always-succeeds
// SQL statement given valid inputs). This test instead covers the
// COMPLEMENTARY, also load-bearing property: a pass that fails at the
// EARLIER resolve step (a.3's separate all-or-nothing guarantee, before
// the write transaction even opens) must leave no residue that corrupts a
// LATER, successful pass's gap detection.

#[tokio::test]
async fn a_failed_resolve_attempt_leaves_no_residue_for_a_later_successful_pass() {
    let pool = fresh_hercules_audit_db().await;
    let chain_id = 300022i64;
    let abi = rome_audit::build_registry();

    let factory = addr(0xF7);
    let storefront = addr(0xC7);
    let token_good = addr(0xA7);
    let router_good = addr(0xB7);
    // A SECOND token, never configured in `rpc`/`registry` — `resolve()`
    // fails its authoritativeness gate, forcing `run_resolve_pass` to error
    // out of STEP 1 (resolve-all) entirely, before the write transaction
    // ever opens.
    let token_bad = addr(0xEE);

    let registry = FakeRegistry::new(
        "1212121212121212121212121212121212121212",
        factory,
        storefront,
    );
    let rpc = FakeRpc::new();
    setup_minimal_token(&rpc, factory, token_good, router_good, 50).await;

    let err = run_resolve_pass(
        &pool,
        chain_id,
        &[token_good, token_bad],
        &registry,
        &rpc,
        &abi,
        200,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, PassError::Resolve { .. }));

    assert_eq!(
        capture_manifest_count(&pool, chain_id).await,
        0,
        "all-or-nothing at the RESOLUTION step: token_good's manifest must not have landed"
    );

    // Now resolve token_good ALONE — it succeeds and its history (anchored
    // at block 50) is far behind the watermark (200): a gap must be
    // detected on THIS pass, proving the earlier failed attempt didn't
    // leave any half-written state that would have marked token_good's
    // sources as "previously known" and suppressed this.
    run_resolve_pass(&pool, chain_id, &[token_good], &registry, &rpc, &abi, 200)
        .await
        .unwrap();

    let gaps: Vec<(Vec<u8>, i64)> =
        sqlx::query_as("SELECT source_contract, from_block FROM audit.backfill_gap WHERE chain_id = $1")
            .bind(chain_id)
            .fetch_all(&pool)
            .await
            .unwrap();
    assert!(
        gaps.iter()
            .any(|(addr, from_block)| addr.as_slice() == token_good.as_slice() && *from_block == 50),
        "the gap must still be detected once the pass actually succeeds — got {gaps:?}"
    );
    assert_eq!(
        capture_manifest_count(&pool, chain_id).await,
        1,
        "exactly one manifest (token_good's) must exist after the successful pass"
    );
}

// ---- M2: a frontier-read failure propagates — the pass never even runs ----

#[tokio::test]
async fn frontier_read_failure_holds_the_tick_and_writes_nothing() {
    // `target` has the Hercules fixture schema (so we can pre-seed a
    // watermark row) but `source` does NOT — `ingest::block_frontier_through_slot`'s
    // query against `source` must fail loudly (missing `eth_block` table),
    // and that failure must propagate all the way out to `tick()` rather
    // than being swallowed into the "-1 / fresh chain" sentinel.
    let target = fresh_hercules_audit_db().await;
    let source = common::fresh_audit_db().await; // audit schema only — NO Hercules tables
    let chain_id = 300023i64;

    set_ingest_watermark(&target, chain_id, 5).await;

    let factory = addr(0xF8);
    let storefront = addr(0xC8);
    let token = addr(0xA8);
    let router = addr(0xB8);
    let registry = Arc::new(FakeRegistry::new(
        "1313131313131313131313131313131313131313",
        factory,
        storefront,
    ));
    let rpc = Arc::new(FakeRpc::new());
    setup_minimal_token(&rpc, factory, token, router, 1).await; // fully resolvable — isolates the frontier read as the ONLY failure

    let mut worker = AuditWorker::new(
        source.clone(),
        target.clone(),
        chain_id,
        2,
        30,
        SourceMode::Resolved {
            registry,
            rpc,
            tokens: vec![token],
            reresolve_interval: Duration::from_secs(3600),
            discovery_enabled: false,
        },
    );

    let outcome = worker.tick().await;
    assert!(
        matches!(outcome, TickOutcome::AwaitingFirstResolution),
        "a frontier-read failure before ever resolving must hold (never run the pass), got {outcome:?}"
    );

    assert_eq!(
        capture_manifest_count(&target, chain_id).await,
        0,
        "the pass must never have run — nothing written"
    );
    assert_eq!(
        watermark(&target, chain_id).await,
        Some(5),
        "the pre-seeded watermark must be untouched — retryable, not corrupted"
    );
}

// ---- P4a CRITICAL-1: a shared address resolved under TWO different
// filter-bearing kinds (the default Bloom shape — one asset's own
// chain-wide token is BOTH its YieldToken leg AND its PurchaseToken leg)
// must keep BOTH legs' filters, never drop the merge-conflict loser's. ----

async fn chain_event_count_for_address(pool: &PgPool, chain_id: i64, address: [u8; 20]) -> i64 {
    let (n,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM audit.chain_event WHERE chain_id = $1 AND source_contract = $2",
    )
    .bind(chain_id)
    .bind(address.as_slice())
    .fetch_one(pool)
    .await
    .unwrap();
    n
}

async fn asset_event_count(pool: &PgPool, chain_id: i64, asset_id: &str) -> i64 {
    let (n,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM audit.asset_event WHERE chain_id = $1 AND asset_id = $2",
    )
    .bind(chain_id)
    .bind(asset_id)
    .fetch_one(pool)
    .await
    .unwrap();
    n
}

#[tokio::test]
async fn shared_address_dual_role_keeps_both_legs_filters() {
    let pool = fresh_hercules_audit_db().await;
    let chain_id = 300030i64;
    let abi = rome_audit::build_registry();

    let factory = addr(0xF9);
    let storefront = addr(0xC9);
    let token = addr(0xA9);
    let router = addr(0xB9);
    let shared = addr(0xE9); // BOTH this asset's YieldToken AND PurchaseToken

    let registry = FakeRegistry::new(
        "d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0",
        factory,
        storefront,
    );
    let rpc = FakeRpc::new();
    setup_minimal_token(&rpc, factory, token, router, 5).await;
    // Yield leg: token's own YieldTokenUpdated points at `shared`.
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
                address_word(shared),
            ],
            data: vec![],
        },
    );
    // Purchase leg: storefront's own PurchaseTokenUpdated ALSO points at
    // the SAME `shared` address.
    rpc.add_log(
        storefront,
        rome_audit::abi::storefront::PURCHASE_TOKEN_UPDATED_TOPIC0,
        LogEntry {
            block_number: 7,
            tx_index: 0,
            log_index: 0,
            address: storefront,
            topics: vec![
                rome_audit::abi::storefront::PURCHASE_TOKEN_UPDATED_TOPIC0,
                address_word(shared),
            ],
            data: vec![],
        },
    );

    let resolved = run_resolve_pass(&pool, chain_id, &[token], &registry, &rpc, &abi, -1)
        .await
        .unwrap();

    let key = hex_addr(shared);
    let spec = resolved
        .sources
        .get(&key)
        .expect("the shared address must be in the live ingest map (both kinds are descriptor-ful)");
    match &spec.ingest_filter {
        rome_audit::ingest::IngestFilter::Any(members) => {
            assert_eq!(members.len(), 2, "expected exactly the yield leg + purchase leg, got {members:?}");
        }
        other => panic!("expected an Any filter unioning both legs, got {other:?}"),
    }

    // Three real Hercules Transfer logs at `shared`: a buyer→storefront
    // purchase payment (purchase leg, MUST land), a token→x yield credit
    // (yield leg, MUST land), and a totally unrelated stranger transfer
    // (neither leg, MUST NOT land — the filter must still exclude it).
    let buyer = addr(0x77);
    seed_contiguous_finalized_chain(&pool, 0, 9).await;
    let block_hash = format!("0x{:064x}", 10);
    common::seed_sol_slot(&pool, 10, 9, "Finalized", &block_hash, 1_700_000_010).await;
    seed_tx_with_logs(
        &pool,
        10,
        &block_hash,
        10,
        &hex_addr(addr(0x55)),
        &[
            // (a) buyer -> storefront: purchase leg.
            (
                hex_addr(shared),
                hex_word(&rome_audit::abi::arc_token::TRANSFER_TOPIC0),
                Some(address_topic(buyer)),
                Some(address_topic(storefront)),
                None,
                Some(hex_data(&[address_word(addr(1))])),
            ),
            // (b) token -> someone: yield leg (from == this ArcToken).
            (
                hex_addr(shared),
                hex_word(&rome_audit::abi::arc_token::TRANSFER_TOPIC0),
                Some(address_topic(token)),
                Some(address_topic(addr(0x88))),
                None,
                Some(hex_data(&[address_word(addr(1))])),
            ),
            // (c) a genuine stranger — neither leg.
            (
                hex_addr(shared),
                hex_word(&rome_audit::abi::arc_token::TRANSFER_TOPIC0),
                Some(address_topic(addr(0x11))),
                Some(address_topic(addr(0x12))),
                None,
                Some(hex_data(&[address_word(addr(1))])),
            ),
        ],
    )
    .await;
    seed_contiguous_finalized_chain(&pool, 11, 20).await;

    let mut tracker = rome_audit::ingest::WatermarkTracker::new();
    let ingest_config = IngestConfig {
        chain_id,
        confirmation_lag: 2,
        sources: resolved.sources.clone(),
    };
    for _ in 0..30 {
        rome_audit::ingest::run_ingest_once(&pool, &pool, &abi, &mut tracker, &ingest_config, 30)
            .await
            .unwrap();
    }

    assert_eq!(
        chain_event_count_for_address(&pool, chain_id, shared).await,
        2,
        "both the purchase-leg AND yield-leg transfers must land; the stranger transfer must not"
    );

    // Layer 1 (per-asset join): rebuild `asset_event` now that the events
    // have actually landed (the earlier `run_resolve_pass` rebuilt it
    // BEFORE ingest ever ran, so it's stale until re-rebuilt — same
    // Tier-2-style "rebuildable, pure function of chain_event + manifest"
    // doctrine `resolve::asset_event`'s own doc describes). Proves
    // `rebuild_asset_event`'s per-interval predicate still slices correctly
    // per asset after Layer 2 keeps the union (it operates on
    // `asset.graph.intervals` directly, independent of the merged
    // `IngestFilter`).
    rome_audit::resolve::rebuild_asset_event(&pool, chain_id, &resolved.assets)
        .await
        .unwrap();
    let asset_id = format!("{chain_id}:0x{}", hex::encode(token));
    assert_eq!(
        asset_event_count(&pool, chain_id, &asset_id).await,
        2,
        "both legs' events must join to this asset via asset_event"
    );
}

// ---- P4a NEW-CRITICAL: `IngestFilter::None` (a REAL pass-all interval —
// an asset's own token) must be UNION-ABSORBING, never identity, when it
// shares an address with ANOTHER asset's filtered leg. ----

#[tokio::test]
async fn none_scoped_own_token_stays_pass_all_when_shared_as_another_assets_yield_leg() {
    let pool = fresh_hercules_audit_db().await;
    let chain_id = 300032i64;
    let abi = rome_audit::build_registry();

    let factory = addr(0xFA);
    let storefront = addr(0xCA);
    let token_a = addr(0xAA); // asset A's OWN token — also becomes asset B's yield leg
    let router_a = addr(0xBA);
    let token_b = addr(0xAB);
    let router_b = addr(0xBB);

    let registry = FakeRegistry::new(
        "e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0",
        factory,
        storefront,
    );
    let rpc = FakeRpc::new();
    setup_minimal_token(&rpc, factory, token_a, router_a, 5).await;
    setup_minimal_token(&rpc, factory, token_b, router_b, 6).await;
    // token_b's YieldTokenUpdated points AT token_a — token_a is now ALSO
    // asset B's chain-wide yield leg (TransferFrom{token_b}), on TOP of
    // being asset A's own (None-scoped, pass-all) token.
    rpc.add_log(
        token_b,
        rome_audit::abi::arc_token::YIELD_TOKEN_UPDATED_TOPIC0,
        LogEntry {
            block_number: 7,
            tx_index: 0,
            log_index: 0,
            address: token_b,
            topics: vec![
                rome_audit::abi::arc_token::YIELD_TOKEN_UPDATED_TOPIC0,
                address_word(token_a),
            ],
            data: vec![],
        },
    );

    let resolved = run_resolve_pass(&pool, chain_id, &[token_a, token_b], &registry, &rpc, &abi, -1)
        .await
        .unwrap();

    let key = hex_addr(token_a);
    let spec = resolved
        .sources
        .get(&key)
        .expect("token_a must be in the live ingest map");
    assert_eq!(spec.kind, SourceKind::ArcToken, "ArcToken outranks YieldToken by precedence");
    assert_eq!(
        spec.ingest_filter,
        rome_audit::ingest::IngestFilter::None,
        "the None (pass-all) interval must ABSORB the union, not get narrowed to TransferFrom{{token_b}}"
    );

    // A genuine stranger Transfer, AND a non-Transfer ArcToken event
    // (RoleGranted, fully indexed) at token_a — under the OLD (buggy)
    // identity semantics, BOTH would have been silently dropped (a
    // stranger transfer fails TransferFrom{token_b}; RoleGranted has no
    // `from` topic to even check).
    let stranger = addr(0x77);
    seed_contiguous_finalized_chain(&pool, 0, 9).await;
    let block_hash = format!("0x{:064x}", 10);
    common::seed_sol_slot(&pool, 10, 9, "Finalized", &block_hash, 1_700_000_010).await;
    seed_tx_with_logs(
        &pool,
        10,
        &block_hash,
        10,
        &hex_addr(addr(0x55)),
        &[
            (
                hex_addr(token_a),
                hex_word(&rome_audit::abi::arc_token::TRANSFER_TOPIC0),
                Some(address_topic(stranger)),
                Some(address_topic(addr(0x88))),
                None,
                Some(hex_data(&[address_word(addr(1))])),
            ),
            (
                hex_addr(token_a),
                hex_word(&rome_audit::abi::arc_token::ROLE_GRANTED_TOPIC0),
                Some(hex_word(&[0x11u8; 32])),
                Some(address_topic(addr(0x22))),
                Some(address_topic(addr(0x33))),
                None,
            ),
        ],
    )
    .await;
    seed_contiguous_finalized_chain(&pool, 11, 20).await;

    let mut tracker = rome_audit::ingest::WatermarkTracker::new();
    let ingest_config = IngestConfig {
        chain_id,
        confirmation_lag: 2,
        sources: resolved.sources.clone(),
    };
    for _ in 0..30 {
        rome_audit::ingest::run_ingest_once(&pool, &pool, &abi, &mut tracker, &ingest_config, 30)
            .await
            .unwrap();
    }

    assert_eq!(
        chain_event_count_for_address(&pool, chain_id, token_a).await,
        2,
        "BOTH the stranger transfer and the non-Transfer event must land — pass-all, no fail-closed drop"
    );

    // Layer 1: asset A's OWN (pass-all) interval must join BOTH events;
    // asset B's (TransferFrom{token_b}) interval must join NEITHER — the
    // union is capture-ONLY (Layer 2), never join-widening (Layer 1) — each
    // asset's own scope_filter is untouched and still slices independently.
    rome_audit::resolve::rebuild_asset_event(&pool, chain_id, &resolved.assets)
        .await
        .unwrap();
    let asset_a_id = format!("{chain_id}:0x{}", hex::encode(token_a));
    let asset_b_id = format!("{chain_id}:0x{}", hex::encode(token_b));
    assert_eq!(
        asset_event_count(&pool, chain_id, &asset_a_id).await,
        2,
        "asset A's own pass-all interval must join both events"
    );
    assert_eq!(
        asset_event_count(&pool, chain_id, &asset_b_id).await,
        0,
        "asset B's filtered leg must join neither — the stranger transfer isn't from token_b, \
         and RoleGranted has no 'from' at all"
    );
}

// ---- Pinned Sanctions-Router coverage-gap closer ----
//
// (6b) `run_resolve_pass` with `FakeRegistry::global_sanctions_router` set
// must put BOTH the pinned router AND its discovered module into the live
// ingest map — even though no seed token's own router routes to either.
// Absent config must add neither key.

#[tokio::test]
async fn pinned_sanctions_router_is_present_in_the_live_ingest_map_when_configured() {
    let pool = fresh_hercules_audit_db().await;
    let chain_id = 300040i64;
    let abi = rome_audit::build_registry();

    let factory = addr(0xFB);
    let storefront = addr(0xCB);
    let token = addr(0xAB);
    let router = addr(0xBB); // this asset's OWN router — distinct from the pin
    let pinned_router = addr(0x80); // chain-global, NOT reachable via any token's own router
    let pinned_module = addr(0x81);

    let mut registry = FakeRegistry::new(
        "2020202020202020202020202020202020202020",
        factory,
        storefront,
    );
    registry.global_sanctions_router = Some(pinned_router);

    let rpc = FakeRpc::new();
    setup_minimal_token(&rpc, factory, token, router, 5).await;
    rpc.set_global_module(
        pinned_router,
        rome_audit::abi::router::GLOBAL_SANCTIONS_TYPE,
        pinned_module,
    );

    let resolved = run_resolve_pass(&pool, chain_id, &[token], &registry, &rpc, &abi, -1)
        .await
        .unwrap();

    assert_eq!(
        resolved.sources.get(&hex_addr(pinned_router)).map(|s| s.kind),
        Some(SourceKind::Router),
        "the pinned router must be in the live ingest map even though no token's own router \
         routes to it"
    );
    assert_eq!(
        resolved.sources.get(&hex_addr(pinned_module)).map(|s| s.kind),
        Some(SourceKind::GlobalSanctions),
        "the pinned router's discovered module must be in the live ingest map"
    );
}

#[tokio::test]
async fn absent_global_sanctions_router_leaves_neither_key_in_the_ingest_map() {
    let pool = fresh_hercules_audit_db().await;
    let chain_id = 300041i64;
    let abi = rome_audit::build_registry();

    let factory = addr(0xFC);
    let storefront = addr(0xCC);
    let token = addr(0xAC);
    let router = addr(0xBC);
    let would_be_pinned_router = addr(0x82); // never configured on the registry

    let registry = FakeRegistry::new(
        "3030303030303030303030303030303030303030",
        factory,
        storefront,
    ); // global_sanctions_router stays None (Default)

    let rpc = FakeRpc::new();
    setup_minimal_token(&rpc, factory, token, router, 5).await;

    let resolved = run_resolve_pass(&pool, chain_id, &[token], &registry, &rpc, &abi, -1)
        .await
        .unwrap();

    assert!(
        !resolved.sources.contains_key(&hex_addr(would_be_pinned_router)),
        "absent config must never synthesize a pinned-router source"
    );
    assert!(
        !resolved
            .sources
            .values()
            .any(|s| s.kind == SourceKind::GlobalSanctions),
        "absent config must never synthesize a GlobalSanctions source"
    );
}

// (6c) End-to-end via the running `AuditWorker`: the pinned router's
// discovered module's LIVE Sanctioned event actually reaches
// `audit.chain_event`.

#[tokio::test]
async fn pinned_sanctions_module_live_sanctioned_event_is_captured() {
    let pool = fresh_hercules_audit_db().await;
    let chain_id = 300042i64;

    let factory = addr(0xFD);
    let storefront = addr(0xCD);
    let token = addr(0xAD);
    let router = addr(0xBD);
    let pinned_router = addr(0x83);
    let pinned_module = addr(0x84);

    let mut registry = FakeRegistry::new(
        "4040404040404040404040404040404040404040",
        factory,
        storefront,
    );
    registry.global_sanctions_router = Some(pinned_router);
    let registry = Arc::new(registry);

    let rpc = Arc::new(FakeRpc::new());
    setup_minimal_token(&rpc, factory, token, router, 5).await;
    rpc.set_global_module(
        pinned_router,
        rome_audit::abi::router::GLOBAL_SANCTIONS_TYPE,
        pinned_module,
    );

    let mut worker = AuditWorker::new(
        pool.clone(),
        pool.clone(),
        chain_id,
        2,
        30,
        SourceMode::Resolved {
            registry,
            rpc,
            tokens: vec![token],
            reresolve_interval: Duration::from_secs(3600),
            discovery_enabled: false,
        },
    );

    seed_contiguous_finalized_chain(&pool, 0, 5).await;
    worker.tick().await; // resolves; the pin's own graph flows through the SAME pass

    // Live-ingest event, well AHEAD of the (low, 5) watermark this fixture
    // starts at — captured via the normal forward scan, never a backfill.
    seed_contiguous_finalized_chain(&pool, 0, 39).await;
    let block_hash = format!("0x{:064x}", 40);
    common::seed_sol_slot(&pool, 40, 39, "Finalized", &block_hash, 1_700_000_040).await;
    seed_tx_with_logs(
        &pool,
        40,
        &block_hash,
        40,
        &hex_addr(addr(0x55)),
        &[(
            hex_addr(pinned_module),
            hex_word(&rome_audit::abi::global_sanctions::SANCTIONED_TOPIC0),
            Some(address_topic(addr(0x99))),
            None,
            None,
            None,
        )],
    )
    .await;
    seed_contiguous_finalized_chain(&pool, 41, 50).await;

    tick_until_slot_passes(&mut worker, 40).await;

    let kinds = chain_event_source_kinds(&pool, chain_id).await;
    assert!(
        kinds.contains(&"GLOBAL_SANCTIONS".to_string()),
        "the pinned router's discovered module's live Sanctioned event must be captured, got {kinds:?}"
    );
}

// (6d) THE critical test — the pin's historical events (behind the ingest
// watermark) backfill ONLY because the pin enters as a real `ResolvedGraph`
// pushed into `graphs` (with a `from_block == 0` interval) BEFORE
// merge/gap-detection. A post-merge "poke it into the sources map" shape
// would leave `detect_backfill_gaps`'s `earliest` map with no entry for the
// pinned module at all (nothing in any asset's `graph.intervals` mentions
// it), so no `backfill_gap` row would ever be written and this test would
// fail RIGHT HERE, before backfill even runs.

#[tokio::test]
async fn pinned_sanctions_module_historical_event_behind_watermark_is_backfilled() {
    let pool = fresh_hercules_audit_db().await;
    let chain_id = 300043i64;

    let factory = addr(0xFE);
    let storefront = addr(0xCE);
    let token = addr(0xAE);
    let router = addr(0xBE);
    let pinned_router = addr(0x85);
    let pinned_module = addr(0x86);

    let mut registry = FakeRegistry::new(
        "5050505050505050505050505050505050505050",
        factory,
        storefront,
    );
    registry.global_sanctions_router = Some(pinned_router);
    let registry = Arc::new(registry);

    let rpc = Arc::new(FakeRpc::new());
    setup_minimal_token(&rpc, factory, token, router, 5).await;
    rpc.set_global_module(
        pinned_router,
        rome_audit::abi::router::GLOBAL_SANCTIONS_TYPE,
        pinned_module,
    );

    // A HISTORICAL Sanctioned event at slot/block 50 — BEHIND the frontier
    // (200) set up below, but a real, receipted Hercules log (the "114
    // historical OFAC events" the locked spec describes).
    seed_contiguous_finalized_chain(&pool, 0, 200).await;
    let block_hash_50 = format!("0x{:064x}", 50);
    common::seed_sol_slot(&pool, 50, 49, "Finalized", &block_hash_50, 1_700_000_050).await;
    seed_tx_with_logs(
        &pool,
        50,
        &block_hash_50,
        50,
        &hex_addr(addr(0x55)),
        &[(
            hex_addr(pinned_module),
            hex_word(&rome_audit::abi::global_sanctions::SANCTIONED_TOPIC0),
            Some(address_topic(addr(0x77))),
            None,
            None,
            None,
        )],
    )
    .await;
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
            tokens: vec![token],
            reresolve_interval: Duration::from_secs(3600),
            discovery_enabled: false,
        },
    );

    worker.tick().await; // the resolve pass — must write a backfill_gap row for pinned_module

    let gaps: Vec<(Vec<u8>, i64, i64)> = sqlx::query_as(
        "SELECT source_contract, from_block, watermark_at_detection FROM audit.backfill_gap \
         WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_all(&pool)
    .await
    .unwrap();

    assert!(
        gaps.iter().any(|(a, from_block, wm)| {
            a.as_slice() == pinned_module.as_slice() && *from_block == 0 && *wm == 200
        }),
        "expected a backfill_gap row for the pinned module at from_block=0 (its interval anchors \
         at genesis) with watermark_at_detection=200 — got {gaps:?}. A missing row here means the \
         pin was injected AFTER merge (a post-merge sources-map poke) rather than as a real \
         ResolvedGraph pushed into `graphs` before merge/gap-detection."
    );

    // Now let the backfill executor actually run (it fires once per tick,
    // after live ingest) — the slot-50 event must land in chain_event.
    for _ in 0..30 {
        worker.tick().await;
    }

    assert!(
        chain_event_count_for_address(&pool, chain_id, pinned_module).await >= 1,
        "the historical (behind-watermark) Sanctioned event at the pinned module must have been \
         backfilled into audit.chain_event"
    );
}

// (6e) Regression — absent `global_sanctions_router` produces exactly the
// pre-change sources/manifest set: no synthetic asset, no synthetic
// manifest, no extra Router/GlobalSanctions source beyond the token walk's
// own.

#[tokio::test]
async fn absent_global_sanctions_router_produces_no_synthetic_asset_or_manifest() {
    let pool = fresh_hercules_audit_db().await;
    let chain_id = 300044i64;
    let abi = rome_audit::build_registry();

    let factory = addr(0x87);
    let storefront = addr(0x88);
    let token = addr(0x89);
    let router = addr(0x8A);

    let registry = FakeRegistry::new(
        "6060606060606060606060606060606060606060",
        factory,
        storefront,
    ); // global_sanctions_router stays None (Default)

    let rpc = FakeRpc::new();
    setup_minimal_token(&rpc, factory, token, router, 5).await;

    let resolved = run_resolve_pass(&pool, chain_id, &[token], &registry, &rpc, &abi, -1)
        .await
        .unwrap();

    assert_eq!(
        resolved.assets.len(),
        1,
        "exactly one asset (the token) — no synthetic pinned-router asset when the config is absent"
    );
    assert_eq!(
        capture_manifest_count(&pool, chain_id).await,
        1,
        "exactly one capture_manifest row — no synthetic manifest for an absent pin"
    );
    let router_count = resolved
        .sources
        .values()
        .filter(|s| s.kind == SourceKind::Router)
        .count();
    assert_eq!(
        router_count, 1,
        "only the token's own router — no extra pinned Router source"
    );
    assert!(
        !resolved.sources.values().any(|s| s.kind == SourceKind::GlobalSanctions),
        "no GlobalSanctions source at all — the token's own router has none configured"
    );
}

// ---- rome-apps#? — operator-configurable `global_sanctions_router_from_block` ----
//
// (6f) THE CORE test: on a clean audit-first chain, the pinned router's
// `from_block` anchors AT (or after) the deploy block, at-or-above the
// current watermark ⇒ ZERO backfill_gap rows for the pinned router/module —
// pure forward capture, no wasteful [0, watermark] scan. Reverting
// `from_block` to 0 (the pre-fix hardcode) would make 0 <= 999 true and open
// a gap — this is the mutation this test kills.

#[tokio::test]
async fn pinned_floor_deploy_block_at_tip_opens_no_wasteful_gap() {
    let pool = fresh_hercules_audit_db().await;
    let chain_id = 300045i64;
    let abi = rome_audit::build_registry();

    let factory = addr(0x8B);
    let storefront = addr(0x8C);
    let token = addr(0x8D);
    let router = addr(0x8E);
    let pinned_router = addr(0x8F);
    let pinned_module = addr(0x90);

    let mut registry = FakeRegistry::new(
        "7070707070707070707070707070707070707070",
        factory,
        storefront,
    );
    registry.global_sanctions_router = Some(pinned_router);
    registry.global_sanctions_router_from_block = Some(1000);

    let rpc = FakeRpc::new();
    setup_minimal_token(&rpc, factory, token, router, 5).await;
    rpc.set_global_module(
        pinned_router,
        rome_audit::abi::router::GLOBAL_SANCTIONS_TYPE,
        pinned_module,
    );

    // current_watermark = 999 < from_block = 1000 => a clean, audit-first
    // chain whose router deploy sits AHEAD of (at-tip of) the ingest scan.
    run_resolve_pass(&pool, chain_id, &[token], &registry, &rpc, &abi, 999)
        .await
        .unwrap();

    let gaps: Vec<(Vec<u8>, i64)> = sqlx::query_as(
        "SELECT source_contract, from_block FROM audit.backfill_gap WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_all(&pool)
    .await
    .unwrap();

    // Scoped to the PINNED router + module only — the seed token's own
    // sources (Router/Factory/ArcToken anchored at registered_block=5, which
    // IS behind watermark 999) legitimately open their own gaps; that's not
    // under test here.
    assert!(
        !gaps
            .iter()
            .any(|(addr, _)| addr.as_slice() == pinned_router.as_slice()),
        "the pinned router's from_block (1000) sits AHEAD of watermark 999 — no gap expected, \
         got {gaps:?}"
    );
    assert!(
        !gaps
            .iter()
            .any(|(addr, _)| addr.as_slice() == pinned_module.as_slice()),
        "the pinned module inherits the SAME from_block (1000) — no gap expected, got {gaps:?}"
    );
}

// (6g) The other side: the deploy block sits BEHIND the current watermark
// (a retrofit chain) ⇒ exactly ONE bounded gap for the pinned module, opened
// at from_block=485 (the configured deploy block), never at 0 — reverting to
// the from_block=0 hardcode would still open a gap, but at from_block=0
// (unbounded from genesis) instead of 485 (bounded to the deploy block);
// this test's `from_block == 485` assertion is what catches that reversion.

#[tokio::test]
async fn pinned_floor_deploy_block_behind_watermark_opens_one_bounded_gap() {
    let pool = fresh_hercules_audit_db().await;
    let chain_id = 300046i64;
    let abi = rome_audit::build_registry();

    let factory = addr(0x91);
    let storefront = addr(0x92);
    let token = addr(0x93);
    let router = addr(0x94);
    let pinned_router = addr(0x95);
    let pinned_module = addr(0x96);

    let mut registry = FakeRegistry::new(
        "8080808080808080808080808080808080808080",
        factory,
        storefront,
    );
    registry.global_sanctions_router = Some(pinned_router);
    registry.global_sanctions_router_from_block = Some(485);

    let rpc = FakeRpc::new();
    setup_minimal_token(&rpc, factory, token, router, 5).await;
    rpc.set_global_module(
        pinned_router,
        rome_audit::abi::router::GLOBAL_SANCTIONS_TYPE,
        pinned_module,
    );

    // current_watermark = 1000 > from_block = 485 => a retrofit chain: real
    // history sits behind the watermark, one bounded gap must open.
    run_resolve_pass(&pool, chain_id, &[token], &registry, &rpc, &abi, 1000)
        .await
        .unwrap();

    let module_gaps: Vec<(i64, i64)> = sqlx::query_as(
        "SELECT from_block, watermark_at_detection FROM audit.backfill_gap \
         WHERE chain_id = $1 AND source_contract = $2",
    )
    .bind(chain_id)
    .bind(pinned_module.to_vec())
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(
        module_gaps.len(),
        1,
        "expected exactly one backfill_gap row for the pinned module, got {module_gaps:?}"
    );
    assert_eq!(
        module_gaps[0],
        (485, 1000),
        "the gap must be bounded to the configured deploy block (485), never from_block=0 \
         (which the pre-fix hardcode would have produced) — got {module_gaps:?}"
    );
}
