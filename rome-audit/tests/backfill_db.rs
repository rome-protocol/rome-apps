//! P4a deliverable 2 — the backfill EXECUTOR (`backfill::run_backfill_once`)
//! + `ingest::hercules_reads::slot_range_for_blocks`, against a REAL local
//! Postgres holding both the Hercules-shaped fixture schema and the real
//! `audit` schema (same convention as `tests/resolve_ingest_db.rs`).

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use sqlx::PgPool;

use rome_audit::backfill::{run_backfill_once, BackfillOutcome};
use rome_audit::ingest::{IngestConfig, IngestFilter, SourceSpec};
use rome_audit::resolve::store::insert_backfill_gap;
use rome_audit::run::{AuditWorker, SourceMode, TickOutcome};
use rome_audit::types::SourceKind;

mod common;
use common::{fresh_hercules_audit_db, seed_contiguous_finalized_chain, seed_tx_with_logs};

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

/// Seeds a bare `capture_manifest` row (the FK `audit.backfill_gap` needs) —
/// its content is irrelevant to these tests, only its `manifest_hash`'s
/// existence.
async fn seed_bare_manifest(pool: &PgPool, chain_id: i64, hash: [u8; 32]) {
    sqlx::query(
        r#"
        INSERT INTO audit.capture_manifest
            (manifest_hash, chain_id, asset_id, registry_commit_sha, resolved_sources, source_intervals, generated_at)
        VALUES ($1,$2,'test-asset','deadbeef','[]'::jsonb,'[]'::jsonb,1700000000)
        ON CONFLICT (manifest_hash) DO NOTHING
        "#,
    )
    .bind(hash.as_slice())
    .bind(chain_id)
    .execute(pool)
    .await
    .unwrap();
}

async fn seed_gap(
    pool: &PgPool,
    chain_id: i64,
    address: [u8; 20],
    source_kind: SourceKind,
    from_block: i64,
    watermark_at_detection: i64,
) {
    let manifest_hash = [address[0]; 32]; // deterministic-enough, unique per test address
    seed_bare_manifest(pool, chain_id, manifest_hash).await;
    insert_backfill_gap(
        pool,
        chain_id,
        address,
        source_kind.as_db_str(),
        from_block,
        watermark_at_detection,
        manifest_hash,
        1_700_000_000,
    )
    .await
    .unwrap();
}

async fn gap_remediated_at(pool: &PgPool, chain_id: i64, address: [u8; 20]) -> Option<i64> {
    let row: Option<(Option<i64>,)> = sqlx::query_as(
        "SELECT remediated_at FROM audit.backfill_gap WHERE chain_id = $1 AND source_contract = $2",
    )
    .bind(chain_id)
    .bind(address.as_slice())
    .fetch_optional(pool)
    .await
    .unwrap();
    row.and_then(|(r,)| r)
}

async fn gap_watermark(pool: &PgPool, chain_id: i64, address: [u8; 20]) -> i64 {
    let (wm,): (i64,) = sqlx::query_as(
        "SELECT watermark_at_detection FROM audit.backfill_gap WHERE chain_id = $1 AND source_contract = $2",
    )
    .bind(chain_id)
    .bind(address.as_slice())
    .fetch_one(pool)
    .await
    .unwrap();
    wm
}

async fn gap_resume_from_slot(pool: &PgPool, chain_id: i64, address: [u8; 20]) -> Option<i64> {
    let (r,): (Option<i64>,) = sqlx::query_as(
        "SELECT resume_from_slot FROM audit.backfill_gap WHERE chain_id = $1 AND source_contract = $2",
    )
    .bind(chain_id)
    .bind(address.as_slice())
    .fetch_one(pool)
    .await
    .unwrap();
    r
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

// ---- headline: idempotent, filter-respecting backfill over a real range ----

#[tokio::test]
async fn backfill_executor_ingests_gap_range_idempotently() {
    let pool = fresh_hercules_audit_db().await;
    let chain_id = 500001i64;
    let abi = rome_audit::build_registry();

    let yield_token = addr(0xE0);
    let in_scope_party = addr(0xA0); // this asset's own ArcToken
    let foreign_1 = addr(0x11);
    let foreign_2 = addr(0x13);

    // The Hercules-shaped chain covering the gap's [from_block, watermark]
    // range, block number == slot number (this fixture's convention).
    seed_contiguous_finalized_chain(&pool, 0, 30).await;
    let block_hash = format!("0x{:064x}", 10);
    common::seed_sol_slot(&pool, 10, 9, "Finalized", &block_hash, 1_700_000_010).await;
    seed_tx_with_logs(
        &pool,
        10,
        &block_hash,
        10,
        &hex_addr(addr(0x55)),
        &[
            // Lands: from == in_scope_party.
            (
                hex_addr(yield_token),
                hex_word(&rome_audit::abi::arc_token::TRANSFER_TOPIC0),
                Some(address_topic(in_scope_party)),
                Some(address_topic(addr(0x12))),
                None,
                Some(hex_data(&[address_word(addr(1))])),
            ),
            // Foreign — must NOT land.
            (
                hex_addr(yield_token),
                hex_word(&rome_audit::abi::arc_token::TRANSFER_TOPIC0),
                Some(address_topic(foreign_1)),
                Some(address_topic(addr(0x12))),
                None,
                Some(hex_data(&[address_word(addr(1))])),
            ),
            // Foreign — must NOT land.
            (
                hex_addr(yield_token),
                hex_word(&rome_audit::abi::arc_token::TRANSFER_TOPIC0),
                Some(address_topic(foreign_2)),
                Some(address_topic(addr(0x14))),
                None,
                Some(hex_data(&[address_word(addr(1))])),
            ),
        ],
    )
    .await;

    seed_gap(&pool, chain_id, yield_token, SourceKind::YieldToken, 5, 20).await;

    let mut sources = BTreeMap::new();
    sources.insert(
        hex_addr(yield_token),
        SourceSpec {
            kind: SourceKind::YieldToken,
            ingest_filter: IngestFilter::TransferFrom(BTreeSet::from([in_scope_party])),
        },
    );
    let config = IngestConfig {
        chain_id,
        confirmation_lag: 2,
        sources,
    };

    let outcome = run_backfill_once(&pool, &pool, &abi, &config, rome_audit::backfill::DEFAULT_MAX_SLOTS_PER_BACKFILL_CALL).await.unwrap();
    match outcome {
        BackfillOutcome::Remediated {
            source_contract,
            events_inserted,
            ..
        } => {
            assert_eq!(source_contract, yield_token);
            assert_eq!(events_inserted, 1, "only the in-scope transfer must land");
        }
        other => panic!("expected Remediated, got {other:?}"),
    }
    assert_eq!(chain_event_count_for(&pool, chain_id, yield_token).await, 1);
    assert!(gap_remediated_at(&pool, chain_id, yield_token).await.is_some());

    // 2nd call: no pending gaps left, and re-running is a pure no-op.
    let outcome2 = run_backfill_once(&pool, &pool, &abi, &config, rome_audit::backfill::DEFAULT_MAX_SLOTS_PER_BACKFILL_CALL).await.unwrap();
    assert_eq!(outcome2, BackfillOutcome::NoGapsPending);
    assert_eq!(
        chain_event_count_for(&pool, chain_id, yield_token).await,
        1,
        "idempotent re-run must not duplicate the already-landed row"
    );
}

// ---- backfill never backfills past watermark_at_detection ----

#[tokio::test]
async fn backfill_stops_at_watermark_at_detection() {
    let pool = fresh_hercules_audit_db().await;
    let chain_id = 500002i64;
    let abi = rome_audit::build_registry();

    let sanctions_module = addr(0x21);
    let account = addr(0x99);

    seed_contiguous_finalized_chain(&pool, 0, 30).await;

    // In-range (slot 10, within [5, 15]) — must land.
    let bh10 = format!("0x{:064x}", 10);
    common::seed_sol_slot(&pool, 10, 9, "Finalized", &bh10, 1_700_000_010).await;
    seed_tx_with_logs(
        &pool,
        10,
        &bh10,
        10,
        &hex_addr(addr(0x55)),
        &[(
            hex_addr(sanctions_module),
            hex_word(&rome_audit::abi::global_sanctions::SANCTIONED_TOPIC0),
            Some(address_topic(account)),
            None,
            None,
            None,
        )],
    )
    .await;

    // Beyond watermark_at_detection (slot 20 > 15) — must NOT be backfilled.
    let bh20 = format!("0x{:064x}", 20);
    common::seed_sol_slot(&pool, 20, 19, "Finalized", &bh20, 1_700_000_020).await;
    seed_tx_with_logs(
        &pool,
        20,
        &bh20,
        20,
        &hex_addr(addr(0x55)),
        &[(
            hex_addr(sanctions_module),
            hex_word(&rome_audit::abi::global_sanctions::SANCTIONED_TOPIC0),
            Some(address_topic(addr(0x98))),
            None,
            None,
            None,
        )],
    )
    .await;

    seed_gap(&pool, chain_id, sanctions_module, SourceKind::GlobalSanctions, 5, 15).await;

    let mut sources = BTreeMap::new();
    sources.insert(hex_addr(sanctions_module), SourceSpec::new(SourceKind::GlobalSanctions));
    let config = IngestConfig {
        chain_id,
        confirmation_lag: 2,
        sources,
    };

    let outcome = run_backfill_once(&pool, &pool, &abi, &config, rome_audit::backfill::DEFAULT_MAX_SLOTS_PER_BACKFILL_CALL).await.unwrap();
    assert!(matches!(
        outcome,
        BackfillOutcome::Remediated { events_inserted: 1, .. }
    ));
    assert_eq!(
        chain_event_count_for(&pool, chain_id, sanctions_module).await,
        1,
        "only the in-range (slot 10) event must land — the slot-20 event is past watermark_at_detection"
    );
}

// ---- a transient failure mid-range never sets remediated_at ----

#[tokio::test]
async fn backfill_transient_failure_leaves_remediated_at_null() {
    let pool = fresh_hercules_audit_db().await;
    let chain_id = 500003i64;
    let abi = rome_audit::build_registry();

    let sanctions_module = addr(0x31);

    seed_contiguous_finalized_chain(&pool, 0, 30).await;

    // slot 10 (earlier, in-range): a NORMAL, fully-receipted event — proves
    // partial progress before the failure isn't silently lost either way
    // (idempotent ON CONFLICT DO NOTHING re-lands it on any retry).
    let bh10 = format!("0x{:064x}", 10);
    common::seed_sol_slot(&pool, 10, 9, "Finalized", &bh10, 1_700_000_010).await;
    seed_tx_with_logs(
        &pool,
        10,
        &bh10,
        10,
        &hex_addr(addr(0x55)),
        &[(
            hex_addr(sanctions_module),
            hex_word(&rome_audit::abi::global_sanctions::SANCTIONED_TOPIC0),
            Some(address_topic(addr(0x99))),
            None,
            None,
            None,
        )],
    )
    .await;

    // slot 15 (later, still in-range): a matched `evm_log` row with NO
    // corresponding `evm_tx_result`/`evm_tx` — `ingest_slot`'s own
    // `resolve_tx_receipt` surfaces this as `MissingReceiptInfo`
    // (Transient, per `IngestError::classify`), which must propagate all
    // the way out of `run_backfill_once` — the SAME real mechanism live
    // ingest uses, not a mocked error.
    common::seed_produced_empty_block(&pool, 15, &format!("0x{:064x}", 900_015), 15).await;
    let broken_tx_hash = "0x0000000000000000000000000000000000000000000000000000000000dead";
    sqlx::query(
        "INSERT INTO evm_log (slot_number, tx_hash, log_ordinal, address, topic0, topic1) \
         VALUES (15, $1, 0, $2, $3, $4)",
    )
    .bind(broken_tx_hash)
    .bind(hex_addr(sanctions_module))
    .bind(hex_word(&rome_audit::abi::global_sanctions::SANCTIONED_TOPIC0))
    .bind(address_topic(addr(0x98)))
    .execute(&pool)
    .await
    .unwrap();

    seed_gap(&pool, chain_id, sanctions_module, SourceKind::GlobalSanctions, 5, 20).await;

    let mut sources = BTreeMap::new();
    sources.insert(hex_addr(sanctions_module), SourceSpec::new(SourceKind::GlobalSanctions));
    let config = IngestConfig {
        chain_id,
        confirmation_lag: 2,
        sources,
    };

    let result = run_backfill_once(&pool, &pool, &abi, &config, rome_audit::backfill::DEFAULT_MAX_SLOTS_PER_BACKFILL_CALL).await;
    assert!(result.is_err(), "a transient mid-range failure must propagate as Err");

    assert_eq!(
        gap_remediated_at(&pool, chain_id, sanctions_module).await,
        None,
        "remediated_at must stay NULL — the range did not complete"
    );
    assert_eq!(
        chain_event_count_for(&pool, chain_id, sanctions_module).await,
        1,
        "the earlier slot's event landed (idempotent partial progress) even though the later slot failed"
    );
}

// ---- a backfill failure must never stall LIVE ingest ----

#[tokio::test]
async fn backfill_failure_does_not_stall_live_ingest() {
    let pool = fresh_hercules_audit_db().await;
    let chain_id = 500004i64;

    let broken_module = addr(0x41); // carries the gap that will fail to backfill
    let healthy_module = addr(0x42); // live ingest must still advance on this one

    // Phase 1: a chain covering ONLY the gap's own range plus head
    // clearance — advance the LIVE watermark past slot 20 with
    // `broken_module` absent from the sources map entirely (exactly the
    // real-world precondition a `backfill_gap` row always represents, P3c
    // a.6/P4a §3: live ingest already scanned PAST the gap's range under a
    // map that didn't even know the source existed). Deliberately does NOT
    // yet reach slot 40 — that must stay AHEAD of the watermark so phase 2
    // captures it via its own normal forward scan, not a backfill.
    seed_contiguous_finalized_chain(&pool, 0, 25).await;
    let mut phase1_sources = BTreeMap::new();
    phase1_sources.insert(hex_addr(healthy_module), SourceSpec::new(SourceKind::GlobalSanctions));
    let mut worker1 = AuditWorker::new(
        pool.clone(),
        pool.clone(),
        chain_id,
        2,
        30,
        SourceMode::Static(phase1_sources),
    );
    for _ in 0..5 {
        worker1.tick().await;
    }
    let watermark_after_phase1 = common::watermark(&pool, chain_id).await.unwrap();
    assert!(
        watermark_after_phase1 >= 20 && watermark_after_phase1 < 40,
        "phase 1 must advance PAST the gap's range but stay BEHIND slot 40, got {watermark_after_phase1}"
    );

    // NOW seed the gap's own broken log (slot 12, in [5, 20] — already
    // BEHIND the advanced watermark) and record the gap. Only `backfill`
    // will ever look at slot 12 again.
    common::seed_produced_empty_block(&pool, 12, &format!("0x{:064x}", 900_012), 12).await;
    let broken_tx_hash = "0x00000000000000000000000000000000000000000000000000000000beef01";
    sqlx::query(
        "INSERT INTO evm_log (slot_number, tx_hash, log_ordinal, address, topic0, topic1) \
         VALUES (12, $1, 0, $2, $3, $4)",
    )
    .bind(broken_tx_hash)
    .bind(hex_addr(broken_module))
    .bind(hex_word(&rome_audit::abi::global_sanctions::SANCTIONED_TOPIC0))
    .bind(address_topic(addr(0x11)))
    .execute(&pool)
    .await
    .unwrap();
    seed_gap(&pool, chain_id, broken_module, SourceKind::GlobalSanctions, 5, 20).await;

    // Chain continuation + trailing head-clearance for slot 40, still AHEAD
    // of the watermark — this is what phase 2's OWN forward scan will walk.
    seed_contiguous_finalized_chain(&pool, 26, 50).await;

    // A completely separate, healthy live-ingest event at slot 40 — must
    // land normally even though the SAME tick's backfill call errors on
    // the broken gap.
    let bh40 = format!("0x{:064x}", 40);
    common::seed_sol_slot(&pool, 40, 39, "Finalized", &bh40, 1_700_000_040).await;
    seed_tx_with_logs(
        &pool,
        40,
        &bh40,
        40,
        &hex_addr(addr(0x55)),
        &[(
            hex_addr(healthy_module),
            hex_word(&rome_audit::abi::global_sanctions::SANCTIONED_TOPIC0),
            Some(address_topic(addr(0x77))),
            None,
            None,
            None,
        )],
    )
    .await;

    // Phase 2: a NEW worker (resumes from the persisted watermark) whose
    // sources now include BOTH modules — `broken_module` only matters to
    // `backfill` from here on.
    let mut phase2_sources = BTreeMap::new();
    phase2_sources.insert(hex_addr(broken_module), SourceSpec::new(SourceKind::GlobalSanctions));
    phase2_sources.insert(hex_addr(healthy_module), SourceSpec::new(SourceKind::GlobalSanctions));
    let mut worker2 = AuditWorker::new(
        pool.clone(),
        pool.clone(),
        chain_id,
        2,
        30,
        SourceMode::Static(phase2_sources),
    );

    let mut healthy_event_seen = false;
    for _ in 0..60 {
        match worker2.tick().await {
            TickOutcome::Ingested(outcome) => {
                if outcome.slots_passed.contains(&40) {
                    healthy_event_seen = true;
                }
            }
            other => panic!("Static mode never resolves — unexpected {other:?}"),
        }
        if healthy_event_seen {
            break;
        }
    }

    assert!(
        healthy_event_seen,
        "live ingest must have advanced through slot 40 despite the SAME tick's backfill call erroring on the broken gap"
    );
    assert_eq!(
        chain_event_count_for(&pool, chain_id, healthy_module).await,
        1,
        "the healthy module's event must have landed via normal live ingest"
    );
    assert_eq!(
        gap_remediated_at(&pool, chain_id, broken_module).await,
        None,
        "the broken gap must still be unremediated — its own failure never got silently marked done"
    );
}

// ---- P4a HIGH-1: a 2nd (and 3rd) widening of the SAME address must
// RE-ARM the same `backfill_gap` row (`from_block` is pinned at the MIN
// across contributing assets, so it collides with the earlier row's own
// primary key) rather than silently no-op via the old `DO NOTHING`. ----

#[tokio::test]
async fn second_widening_re_arms_the_same_backfill_gap_row() {
    let pool = fresh_hercules_audit_db().await;
    let chain_id = 500005i64;
    let abi = rome_audit::build_registry();
    let shared = addr(0x51);

    seed_contiguous_finalized_chain(&pool, 0, 100).await;

    // Pass 1's own gap: [5, 20] — remediate it (empty range, no events yet).
    seed_gap(&pool, chain_id, shared, SourceKind::GlobalSanctions, 5, 20).await;
    let mut sources = BTreeMap::new();
    sources.insert(hex_addr(shared), SourceSpec::new(SourceKind::GlobalSanctions));
    let config = IngestConfig {
        chain_id,
        confirmation_lag: 2,
        sources,
    };
    let outcome1 = run_backfill_once(
        &pool,
        &pool,
        &abi,
        &config,
        rome_audit::backfill::DEFAULT_MAX_SLOTS_PER_BACKFILL_CALL,
    )
    .await
    .unwrap();
    assert!(matches!(outcome1, BackfillOutcome::Remediated { .. }), "got {outcome1:?}");
    assert!(gap_remediated_at(&pool, chain_id, shared).await.is_some());

    // Widening #1 (a 2nd party re-detects the SAME address at the SAME
    // from_block=5 — pinned MIN — but a WIDER watermark, 50) — must RE-ARM
    // the row, not silently no-op.
    seed_gap(&pool, chain_id, shared, SourceKind::GlobalSanctions, 5, 50).await;
    assert_eq!(gap_watermark(&pool, chain_id, shared).await, 50, "watermark must extend to the new, wider value");
    assert!(
        gap_remediated_at(&pool, chain_id, shared).await.is_none(),
        "re-arm must reset remediated_at to NULL — the newly-exposed slice hasn't been walked yet"
    );

    // A real event in the NEWLY exposed slice (21..50) — never captured by
    // pass 1's own (narrower) remediation.
    let bh30 = format!("0x{:064x}", 30);
    common::seed_sol_slot(&pool, 30, 29, "Finalized", &bh30, 1_700_000_030).await;
    seed_tx_with_logs(
        &pool,
        30,
        &bh30,
        30,
        &hex_addr(addr(0x55)),
        &[(
            hex_addr(shared),
            hex_word(&rome_audit::abi::global_sanctions::SANCTIONED_TOPIC0),
            Some(address_topic(addr(0x66))),
            None,
            None,
            None,
        )],
    )
    .await;

    let outcome2 = run_backfill_once(
        &pool,
        &pool,
        &abi,
        &config,
        rome_audit::backfill::DEFAULT_MAX_SLOTS_PER_BACKFILL_CALL,
    )
    .await
    .unwrap();
    assert!(
        matches!(outcome2, BackfillOutcome::Remediated { events_inserted: 1, .. }),
        "widening #1's own newly-exposed event must land after the re-arm, got {outcome2:?}"
    );

    // Widening #2 (a 3rd party re-widens AGAIN, watermark -> 80) —
    // between-remediation collision #2, proving this isn't a one-shot fix.
    seed_gap(&pool, chain_id, shared, SourceKind::GlobalSanctions, 5, 80).await;
    assert_eq!(gap_watermark(&pool, chain_id, shared).await, 80);
    assert!(gap_remediated_at(&pool, chain_id, shared).await.is_none());

    let bh60 = format!("0x{:064x}", 60);
    common::seed_sol_slot(&pool, 60, 59, "Finalized", &bh60, 1_700_000_060).await;
    seed_tx_with_logs(
        &pool,
        60,
        &bh60,
        60,
        &hex_addr(addr(0x55)),
        &[(
            hex_addr(shared),
            hex_word(&rome_audit::abi::global_sanctions::SANCTIONED_TOPIC0),
            Some(address_topic(addr(0x77))),
            None,
            None,
            None,
        )],
    )
    .await;

    let outcome3 = run_backfill_once(
        &pool,
        &pool,
        &abi,
        &config,
        rome_audit::backfill::DEFAULT_MAX_SLOTS_PER_BACKFILL_CALL,
    )
    .await
    .unwrap();
    assert!(
        matches!(outcome3, BackfillOutcome::Remediated { events_inserted: 1, .. }),
        "the 3rd party's pre-pass-3 event must land after the 2nd widening re-arms + backfills, got {outcome3:?}"
    );

    assert_eq!(
        chain_event_count_for(&pool, chain_id, shared).await,
        2,
        "both widenings' own newly-exposed events must have landed, exactly once each"
    );
}

// ---- P4a HIGH-2: a range exceeding one chunk advances `resume_from_slot`
// without remediating; a later call finishes it; a transient failure
// resumes from the CURSOR, never restarts from `from_block`. ----

#[tokio::test]
async fn backfill_chunks_a_wide_range_and_resumes_from_the_cursor() {
    let pool = fresh_hercules_audit_db().await;
    let chain_id = 500006i64;
    let abi = rome_audit::build_registry();
    let sanctions_module = addr(0x61);

    // A range spanning 3 chunks of size 10: [0, 29].
    seed_contiguous_finalized_chain(&pool, 0, 40).await;
    seed_gap(&pool, chain_id, sanctions_module, SourceKind::GlobalSanctions, 0, 29).await;

    let mut sources = BTreeMap::new();
    sources.insert(hex_addr(sanctions_module), SourceSpec::new(SourceKind::GlobalSanctions));
    let config = IngestConfig {
        chain_id,
        confirmation_lag: 2,
        sources,
    };
    const CHUNK: i64 = 10;

    // Call 1: walks [0, 9] — must NOT remediate yet.
    let outcome1 = run_backfill_once(&pool, &pool, &abi, &config, CHUNK).await.unwrap();
    match outcome1 {
        BackfillOutcome::Progressed { resumed_through_slot, .. } => assert_eq!(resumed_through_slot, 9),
        other => panic!("expected Progressed after chunk 1, got {other:?}"),
    }
    assert_eq!(gap_resume_from_slot(&pool, chain_id, sanctions_module).await, Some(10));
    assert!(gap_remediated_at(&pool, chain_id, sanctions_module).await.is_none());

    // Call 2: resumes from slot 10 (NOT from from_block=0) — walks [10, 19].
    let outcome2 = run_backfill_once(&pool, &pool, &abi, &config, CHUNK).await.unwrap();
    match outcome2 {
        BackfillOutcome::Progressed { resumed_through_slot, .. } => assert_eq!(resumed_through_slot, 19),
        other => panic!("expected Progressed after chunk 2, got {other:?}"),
    }
    assert_eq!(gap_resume_from_slot(&pool, chain_id, sanctions_module).await, Some(20));

    // Call 3: walks [20, 29] — reaches watermark_at_detection=29, remediates.
    let outcome3 = run_backfill_once(&pool, &pool, &abi, &config, CHUNK).await.unwrap();
    assert!(matches!(outcome3, BackfillOutcome::Remediated { .. }), "got {outcome3:?}");
    assert!(gap_remediated_at(&pool, chain_id, sanctions_module).await.is_some());
}

#[tokio::test]
async fn backfill_transient_failure_resumes_from_cursor_not_from_scratch() {
    let pool = fresh_hercules_audit_db().await;
    let chain_id = 500007i64;
    let abi = rome_audit::build_registry();
    let sanctions_module = addr(0x62);

    seed_contiguous_finalized_chain(&pool, 0, 40).await;
    seed_gap(&pool, chain_id, sanctions_module, SourceKind::GlobalSanctions, 0, 29).await;

    let mut sources = BTreeMap::new();
    sources.insert(hex_addr(sanctions_module), SourceSpec::new(SourceKind::GlobalSanctions));
    let config = IngestConfig {
        chain_id,
        confirmation_lag: 2,
        sources,
    };
    const CHUNK: i64 = 10;

    // Chunk 1 completes cleanly: cursor advances to slot 10.
    run_backfill_once(&pool, &pool, &abi, &config, CHUNK).await.unwrap();
    assert_eq!(gap_resume_from_slot(&pool, chain_id, sanctions_module).await, Some(10));

    // Break slot 15 (inside chunk 2's [10,19] window) — no receipt info, a
    // real Transient `IngestError::MissingReceiptInfo`.
    common::seed_produced_empty_block(&pool, 15, &format!("0x{:064x}", 900_015), 15).await;
    let broken_tx_hash = "0x0000000000000000000000000000000000000000000000000000000000ca11";
    sqlx::query(
        "INSERT INTO evm_log (slot_number, tx_hash, log_ordinal, address, topic0, topic1) \
         VALUES (15, $1, 0, $2, $3, $4)",
    )
    .bind(broken_tx_hash)
    .bind(hex_addr(sanctions_module))
    .bind(hex_word(&rome_audit::abi::global_sanctions::SANCTIONED_TOPIC0))
    .bind(address_topic(addr(0x99)))
    .execute(&pool)
    .await
    .unwrap();

    let failed = run_backfill_once(&pool, &pool, &abi, &config, CHUNK).await;
    assert!(failed.is_err(), "chunk 2 must fail on the broken slot 15 log");
    assert_eq!(
        gap_resume_from_slot(&pool, chain_id, sanctions_module).await,
        Some(10),
        "a mid-chunk failure must NOT advance the cursor past its last successfully-persisted value"
    );

    // Heal it (delete the broken log) and retry — the NEXT call must
    // resume from slot 10 (the cursor), never re-walk from from_block=0.
    sqlx::query("DELETE FROM evm_log WHERE slot_number = 15 AND tx_hash = $1")
        .bind(broken_tx_hash)
        .execute(&pool)
        .await
        .unwrap();
    let outcome = run_backfill_once(&pool, &pool, &abi, &config, CHUNK).await.unwrap();
    match outcome {
        BackfillOutcome::Progressed { resumed_through_slot, .. } => assert_eq!(resumed_through_slot, 19),
        other => panic!("expected Progressed resuming through slot 19, got {other:?}"),
    }
}

// ---- P4a MED-1: a non-ingestable gap must not head-of-line-block a later,
// INGESTABLE one. ----

#[tokio::test]
async fn non_ingestable_gap_does_not_block_a_later_ingestable_one() {
    let pool = fresh_hercules_audit_db().await;
    let chain_id = 500008i64;
    let abi = rome_audit::build_registry();

    let not_ingestable = addr(0x01); // sorts FIRST (lowest byte) — would head-of-line-block under LIMIT-1-take-first
    let ingestable = addr(0x71);

    seed_contiguous_finalized_chain(&pool, 0, 30).await;
    seed_gap(&pool, chain_id, not_ingestable, SourceKind::GlobalSanctions, 5, 20).await;
    seed_gap(&pool, chain_id, ingestable, SourceKind::GlobalSanctions, 5, 20).await;

    // ONLY `ingestable` is in the current config — `not_ingestable` is a
    // real pending gap the caller currently doesn't recognize (e.g. a
    // re-resolution transiently dropped it).
    let mut sources = BTreeMap::new();
    sources.insert(hex_addr(ingestable), SourceSpec::new(SourceKind::GlobalSanctions));
    let config = IngestConfig {
        chain_id,
        confirmation_lag: 2,
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
    assert!(
        matches!(outcome, BackfillOutcome::Remediated { source_contract, .. } if source_contract == ingestable),
        "the ingestable gap must be remediated THIS call despite sorting after the non-ingestable one, got {outcome:?}"
    );
    assert!(gap_remediated_at(&pool, chain_id, ingestable).await.is_some());
    assert!(
        gap_remediated_at(&pool, chain_id, not_ingestable).await.is_none(),
        "the non-ingestable gap is untouched, not silently marked done"
    );
}
