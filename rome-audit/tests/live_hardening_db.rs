//! Live-integration hardening cycle — the tests that model
//! what the live Hadrian deploy actually surfaced, which the fixture-only P1
//! suite never did: Hercules retains `sol_slot` only from a HIGH min slot
//! (~470.7M on Hadrian) with a finality lag, slot 0 DOES NOT EXIST, and a
//! source's history routinely starts BEHIND wherever the forward watermark
//! first anchors.
//!
//! Contracts under test:
//! - **T1 [B1]** first-boot watermark init at the forward tip (never slot 0).
//! - **T2 [B2]** first-boot completeness — a source with history behind the
//!   init point has that history recorded as a backfill gap and landed by the
//!   backfill executor (the structural invariant: first boot never silently
//!   drops pre-boot history).
//! - **T3 [B2]** the live-Hadrian latched-state self-heal migration 0911.
//! - **T4 [B3]** the ingest window is clamped to the tip (steady-state
//!   evaluate-count is bounded, not `max_slots_per_tick`).
//! - **T5 [H1]** a gap whose `from_block` predates the retained floor is
//!   loudly flagged `source_pruned`, never falsely clean-remediated.
//! - **T6** a persisted watermark is respected on restart (init is a no-op).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;

use rome_audit::backfill::{run_backfill_once, BackfillOutcome};
use rome_audit::ingest::{
    ensure_watermark_initialized, run_ingest_once, IngestConfig, SourceSpec, WatermarkTracker,
};
use rome_audit::resolve::run_resolve_pass;
use rome_audit::run::{AuditWorker, SourceMode};
use rome_audit::types::SourceKind;

mod common;
use common::{
    count_chain_event_rows, fresh_hercules_audit_db, seed_contiguous_finalized_chain,
    seed_tx_with_logs, watermark, FakeRegistry, FakeRpc,
};

// Hercules retains history only from a HIGH min slot on the live deploy —
// slot 0 never exists. Every fixture here anchors far above 0.
const MIN: i64 = 470_711_000;
const LAG: i64 = 2;

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
fn hex_data(words: &[[u8; 32]]) -> String {
    let mut bytes = Vec::with_capacity(32 * words.len());
    for w in words {
        bytes.extend_from_slice(w);
    }
    format!("0x{}", hex::encode(bytes))
}

fn address_word_for(a: [u8; 20]) -> String {
    address_topic(a)
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

async fn chain_event_count_for(pool: &PgPool, chain_id: i64, address: [u8; 20]) -> i64 {
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

/// One SANCTIONED(GlobalSanctions) log — one indexed arg, no data — at `slot`.
async fn seed_sanctioned_log(pool: &PgPool, slot: i64, module: [u8; 20], account: [u8; 20]) {
    let block_hash = format!("0x{:064x}", slot);
    seed_tx_with_logs(
        pool,
        slot,
        &block_hash,
        slot,
        &hex_addr(addr(0x55)),
        &[(
            hex_addr(module),
            hex_word(&rome_audit::abi::global_sanctions::SANCTIONED_TOPIC0),
            Some(address_topic(account)),
            None,
            None,
            None,
        )],
    )
    .await;
}

/// Wires a minimal, resolvable ArcToken (registered on `factory` at
/// `registered_block`, immutable `router`) — enough for `resolve()` to succeed.
async fn setup_minimal_token(
    rpc: &FakeRpc,
    factory: [u8; 20],
    token: [u8; 20],
    router: [u8; 20],
    registered_block: i64,
) {
    use rome_audit::resolve::rpc::LogEntry;
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

// ---- T1 [B1]: first boot inits the watermark at the forward tip, NEVER 0 ----

#[tokio::test]
async fn t1_first_boot_inits_watermark_at_forward_tip_not_zero() {
    let pool = fresh_hercules_audit_db().await;
    let chain_id = 900_001i64;
    let module = addr(0x11);

    let mut sources = BTreeMap::new();
    sources.insert(hex_addr(module), SourceSpec::new(SourceKind::GlobalSanctions));

    // A source retained ONLY from a high min slot (slot 0 never exists).
    seed_contiguous_finalized_chain(&pool, MIN, MIN + 10).await; // tip MIN+10

    let mut worker = AuditWorker::new(
        pool.clone(),
        pool.clone(),
        chain_id,
        LAG,
        30,
        SourceMode::Static(sources),
    );

    // First tick MUST initialize the watermark at the forward tip
    // (min(tip, max_produced) - lag = MIN+10 - 2 = MIN+8), NOT at -1/slot 0.
    worker.tick().await;
    assert_eq!(
        watermark(&pool, chain_id).await,
        Some(MIN + 8),
        "first boot must anchor the watermark at the forward tip, never slot 0"
    );

    // Post-init activity ABOVE the init point, with the tip extended, must be
    // captured by normal forward LIVE ingest — and subsequent ticks advance.
    seed_contiguous_finalized_chain(&pool, MIN + 11, MIN + 30).await; // tip MIN+30
    seed_sanctioned_log(&pool, MIN + 15, module, addr(0xAA)).await;

    for _ in 0..30 {
        worker.tick().await;
        if count_chain_event_rows(&pool, chain_id).await >= 1 {
            break;
        }
    }

    assert_eq!(
        count_chain_event_rows(&pool, chain_id).await,
        1,
        "post-init activity above the init point must land in chain_event"
    );
    assert!(
        watermark(&pool, chain_id).await.unwrap() > MIN + 8,
        "subsequent ticks must advance the watermark forward from the init point"
    );
}

// ---- T2 [B2]: first-boot completeness — history BEHIND the init point is
// gap-recorded and landed by the backfill executor (the structural invariant). ----

#[tokio::test]
async fn t2_first_boot_backfills_history_behind_the_init_point() {
    let pool = fresh_hercules_audit_db().await;
    let chain_id = 900_002i64;

    let factory = addr(0xF2);
    let storefront = addr(0xC2);
    let token = addr(0xA2);
    let router = addr(0xB2);

    let registry = Arc::new(FakeRegistry::new(
        "2222222222222222222222222222222222222222",
        factory,
        storefront,
    ));
    let rpc = Arc::new(FakeRpc::new());
    // The token's own history begins at block MIN+5 — BEHIND where the
    // forward watermark first anchors (MIN+28).
    setup_minimal_token(&rpc, factory, token, router, MIN + 5).await;

    seed_contiguous_finalized_chain(&pool, MIN, MIN + 30).await; // tip MIN+30 -> init MIN+28

    // Two REAL pre-boot ArcToken Transfer logs, both BELOW the init point.
    for slot in [MIN + 5, MIN + 10] {
        let block_hash = format!("0x{:064x}", slot);
        seed_tx_with_logs(
            &pool,
            slot,
            &block_hash,
            slot,
            &hex_addr(addr(0x55)),
            &[(
                hex_addr(token),
                hex_word(&rome_audit::abi::arc_token::TRANSFER_TOPIC0),
                Some(address_word_for(addr(0x91))),
                Some(address_word_for(addr(0x92))),
                None,
                Some(hex_data(&[address_word(addr(1))])),
            )],
        )
        .await;
    }

    let mut worker = AuditWorker::new(
        pool.clone(),
        pool.clone(),
        chain_id,
        LAG,
        30,
        SourceMode::Resolved {
            registry,
            rpc,
            tokens: vec![token],
            reresolve_interval: Duration::from_secs(3600),
            discovery_enabled: false,
        },
    );

    // Drive enough ticks for: init (MIN+28) -> first resolve records the gap
    // [MIN+5, MIN+28] -> the backfill executor lands the two historical events.
    for _ in 0..30 {
        worker.tick().await;
        if chain_event_count_for(&pool, chain_id, token).await >= 2 {
            break;
        }
    }

    // The gap was honestly recorded from the token's earliest interval, not
    // from genesis.
    let gaps: Vec<(i64, i64)> = sqlx::query_as(
        "SELECT from_block, watermark_at_detection FROM audit.backfill_gap \
         WHERE chain_id = $1 AND source_contract = $2",
    )
    .bind(chain_id)
    .bind(token.as_slice())
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(
        gaps.iter().any(|(from_block, wm)| *from_block == MIN + 5 && *wm == MIN + 28),
        "first resolve must record a backfill gap [MIN+5, MIN+28] for the token — got {gaps:?}"
    );

    // The pre-boot history is COMPLETE — both events below the init point
    // landed via backfill (live ingest, a pure forward tip, never sees them).
    assert_eq!(
        chain_event_count_for(&pool, chain_id, token).await,
        2,
        "both pre-boot ArcToken transfers must be backfilled — first boot never drops history"
    );
}

// ---- T3 [B2]: the live-Hadrian latched-state self-heal migration 0911 ----

#[tokio::test]
async fn t3_migration_0911_voids_preingest_scope_then_gaps_re_record() {
    let pool = fresh_hercules_audit_db().await;
    let chain_id = 900_003i64;

    let factory = addr(0xF3);
    let storefront = addr(0xC3);
    let token = addr(0xA3);
    let router = addr(0xB3);
    let abi = rome_audit::build_registry();

    // Seed today's EXACT broken state: `ingest_scope` marks the token's own
    // (address, "none") pair "previously ingestable", but there is NO
    // `ingest_watermark` row — the resolve-before-ingest latch (resolve ran
    // against frontier -1, recorded no gaps, but upserted scope).
    sqlx::query(
        "INSERT INTO audit.ingest_scope (chain_id, address, source_kind, filter_fingerprint, first_seen_at) \
         VALUES ($1, $2, 'ARC_TOKEN', 'none', 1700000000)",
    )
    .bind(chain_id)
    .bind(token.as_slice())
    .execute(&pool)
    .await
    .unwrap();

    let scope_before: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM audit.ingest_scope WHERE chain_id = $1")
            .bind(chain_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(scope_before, 1, "the latched scope row must be present before the self-heal");
    assert_eq!(watermark(&pool, chain_id).await, None, "and no watermark row exists (the latch)");

    // Apply migration 0911's self-heal (the fix under test) — the exact up.sql.
    let up = include_str!("../migrations/0911_void_preingest_scope.up.sql");
    sqlx::raw_sql(up).execute(&pool).await.unwrap();

    let scope_after: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM audit.ingest_scope WHERE chain_id = $1")
            .bind(chain_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(scope_after, 0, "migration 0911 must void every scope row on a chain with no watermark");

    // Now that B1 would have since initialized the watermark, the NEXT resolve
    // pass (frontier at the forward tip) re-records the honest gap the latch
    // had permanently suppressed.
    let rpc = FakeRpc::new();
    let registry = FakeRegistry::new(
        "3333333333333333333333333333333333333333",
        factory,
        storefront,
    );
    setup_minimal_token(&rpc, factory, token, router, MIN + 5).await;

    run_resolve_pass(&pool, chain_id, &[token], &registry, &rpc, &abi, MIN + 28)
        .await
        .unwrap();

    let token_gap: Option<(i64, i64)> = sqlx::query_as(
        "SELECT from_block, watermark_at_detection FROM audit.backfill_gap \
         WHERE chain_id = $1 AND source_contract = $2",
    )
    .bind(chain_id)
    .bind(token.as_slice())
    .fetch_optional(&pool)
    .await
    .unwrap();
    assert_eq!(
        token_gap,
        Some((MIN + 5, MIN + 28)),
        "after the self-heal, the next resolve pass must re-record the token's suppressed gap"
    );
}

// ---- T4 [B3]: the ingest window is clamped to the tip ----

#[tokio::test]
async fn t4_ingest_window_is_clamped_to_the_tip() {
    let pool = fresh_hercules_audit_db().await;
    let chain_id = 900_004i64;
    let registry = rome_audit::build_registry();
    let module = addr(0x11);

    let mut sources = BTreeMap::new();
    sources.insert(hex_addr(module), SourceSpec::new(SourceKind::GlobalSanctions));
    let config = IngestConfig {
        chain_id,
        confirmation_lag: LAG,
        sources,
    };
    let max_slots = 30usize;

    seed_contiguous_finalized_chain(&pool, MIN, MIN + 50).await; // tip = MIN+50

    // Steady state — watermark NEAR the tip (MIN+45). window_end must clamp to
    // the tip (MIN+50), so exactly 5 slots (MIN+46..=MIN+50) are evaluated —
    // NOT the flat max_slots_per_tick=30 that would probe ~25 slots BEYOND
    // the tip (each of which can only ever return Wait).
    set_ingest_watermark(&pool, chain_id, MIN + 45).await;
    let mut tracker = WatermarkTracker::new();
    let near = run_ingest_once(&pool, &pool, &registry, &mut tracker, &config, max_slots)
        .await
        .unwrap();
    assert_eq!(
        near.slots_evaluated,
        5,
        "near the tip the window clamps to (tip - last_verified) = 5, never max_slots_per_tick"
    );
    assert!(
        near.slots_evaluated < max_slots,
        "steady-state evaluate-count must be bounded well below max_slots_per_tick"
    );

    // Far from the tip — the OTHER side of the min(): here max_slots binds
    // (30 < gap-to-tip 40), and the window never reaches past the tip.
    set_ingest_watermark(&pool, chain_id, MIN + 10).await;
    let mut tracker2 = WatermarkTracker::new();
    let far = run_ingest_once(&pool, &pool, &registry, &mut tracker2, &config, max_slots)
        .await
        .unwrap();
    assert_eq!(
        far.slots_evaluated, max_slots,
        "far below the tip, max_slots binds; window_end = min(last_verified+max, tip)"
    );
    assert!(
        far.slots_evaluated as i64 <= (MIN + 50) - (MIN + 10),
        "the window must never exceed min(finalized_tip, max_produced)"
    );
}

// ---- T5 [H1]: a gap whose from_block predates the retained floor is loudly
// flagged source_pruned, never falsely clean-remediated. ----

async fn seed_bare_manifest(pool: &PgPool, chain_id: i64, hash: [u8; 32]) {
    sqlx::query(
        "INSERT INTO audit.capture_manifest \
            (manifest_hash, chain_id, asset_id, registry_commit_sha, resolved_sources, source_intervals, generated_at) \
         VALUES ($1,$2,'t5-asset','deadbeef','[]'::jsonb,'[]'::jsonb,1700000000) \
         ON CONFLICT (manifest_hash) DO NOTHING",
    )
    .bind(hash.as_slice())
    .bind(chain_id)
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn t5_gap_predating_retained_floor_is_flagged_source_pruned() {
    let pool = fresh_hercules_audit_db().await;
    let chain_id = 900_005i64;
    let abi = rome_audit::build_registry();
    let module = addr(0x51);

    // The source retains blocks ONLY from MIN (a high floor) upward — exactly
    // the live Hadrian shape. Nothing below MIN exists.
    seed_contiguous_finalized_chain(&pool, MIN, MIN + 50).await; // retained floor = MIN

    // A gap whose from_block (MIN-100) predates the retained floor: its low
    // end was pruned and can never be captured.
    let manifest_hash = [0x51u8; 32];
    seed_bare_manifest(&pool, chain_id, manifest_hash).await;
    rome_audit::resolve::store::insert_backfill_gap(
        &pool,
        chain_id,
        module,
        SourceKind::GlobalSanctions.as_db_str(),
        MIN - 100,
        MIN + 20,
        manifest_hash,
        1_700_000_000,
    )
    .await
    .unwrap();

    let mut sources = BTreeMap::new();
    sources.insert(hex_addr(module), SourceSpec::new(SourceKind::GlobalSanctions));
    let config = IngestConfig {
        chain_id,
        confirmation_lag: LAG,
        sources,
    };

    let outcome = run_backfill_once(
        &pool,
        &pool,
        &abi,
        &config,
        rome_audit::backfill::DEFAULT_MAX_SLOTS_PER_BACKFILL_CALL,
    )
    .await
    .unwrap();

    match outcome {
        BackfillOutcome::SourcePruned {
            source_contract,
            from_block,
            retained_floor,
        } => {
            assert_eq!(source_contract, module);
            assert_eq!(from_block, MIN - 100);
            assert_eq!(retained_floor, MIN, "the retained floor is the min produced block");
        }
        other => panic!("expected SourcePruned for a gap below the retained floor, got {other:?}"),
    }

    // The row is a VISIBLY-unrecoverable terminal state, NOT a silent clean
    // remediation.
    let (source_pruned, remediated_at): (bool, Option<i64>) = sqlx::query_as(
        "SELECT source_pruned, remediated_at FROM audit.backfill_gap \
         WHERE chain_id = $1 AND source_contract = $2",
    )
    .bind(chain_id)
    .bind(module.as_slice())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(source_pruned, "the pruned gap must be flagged source_pruned");
    assert_eq!(
        remediated_at, None,
        "a pruned gap must NOT be falsely marked clean-remediated"
    );
}

// ---- T6: a persisted watermark is respected on restart (init is a no-op) ----

#[tokio::test]
async fn t6_persisted_watermark_is_respected_on_restart() {
    let pool = fresh_hercules_audit_db().await;
    let chain_id = 900_006i64;
    let module = addr(0x11);

    let mut sources = BTreeMap::new();
    sources.insert(hex_addr(module), SourceSpec::new(SourceKind::GlobalSanctions));

    seed_contiguous_finalized_chain(&pool, MIN, MIN + 30).await; // tip MIN+30 (init WOULD be MIN+28)
    seed_sanctioned_log(&pool, MIN + 6, module, addr(0xAA)).await;

    // A restart: a watermark row already persisted at MIN+5.
    set_ingest_watermark(&pool, chain_id, MIN + 5).await;

    // Direct no-op proof: ensure_watermark_initialized must NOT overwrite it
    // with the tip-derived value (MIN+28).
    let existed = ensure_watermark_initialized(&pool, &pool, chain_id, LAG)
        .await
        .unwrap();
    assert!(existed, "an existing watermark row must report initialized");
    assert_eq!(
        watermark(&pool, chain_id).await,
        Some(MIN + 5),
        "restart: a persisted watermark is respected exactly, never re-initialized to the tip"
    );

    // End-to-end proof: live ingest resumes from MIN+5 and captures the event
    // at MIN+6 — which it could ONLY do if init did not jump the watermark to
    // MIN+28 (that would have skipped MIN+6 forever).
    let mut worker = AuditWorker::new(
        pool.clone(),
        pool.clone(),
        chain_id,
        LAG,
        30,
        SourceMode::Static(sources),
    );
    for _ in 0..30 {
        worker.tick().await;
        if count_chain_event_rows(&pool, chain_id).await >= 1 {
            break;
        }
    }
    assert_eq!(
        count_chain_event_rows(&pool, chain_id).await,
        1,
        "the event just above the persisted watermark must be captured — the init was a true no-op"
    );
}
