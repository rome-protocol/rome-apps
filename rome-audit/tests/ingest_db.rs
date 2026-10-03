//! P1 ingest — DB-backed tests against a REAL local Postgres (see the P1
//! report for how it's provisioned: a disposable Docker container, one
//! fresh database per test). Each test seeds a Hercules-shaped schema
//! (`tests/fixtures/hercules_schema.sql`) plus the real `audit` schema
//! migration, then drives `rome_audit::ingest::run_ingest_once`.
//!
//! CONTRACT UNDER TEST (restated, IMPL-PLAN §3 P1 / §5.2 / §5.4): a slot's
//! events land in `audit.chain_event` ONLY after the verified-finality
//! watermark passes it (finalized+lag AND head-cleared AND a
//! two-consecutive-read-stable digest); a genuinely empty/skipped slot
//! passes without stalling and stores nothing; a transient gap (head
//! dropped below the slot) or an unstable digest (content changed between
//! reads) both WAIT and never let anything into the append-only table; the
//! table itself rejects UPDATE/DELETE; re-ingesting the same finalized
//! slots is idempotent; and a fully-indexed event never needs the
//! `tx_result` JSONB read that a non-indexed event does.
//!
//! IMPORTANT seeding invariant: the pipeline walks slots strictly in order
//! from the watermark and NEVER skips an unobserved one — an unseeded gap
//! at slot 1 would permanently block every later slot, which would make a
//! "must Wait forever" assertion pass for the WRONG reason. Every test
//! therefore seeds an unbroken, stably-empty, Finalized+produced chain from
//! slot 1 up to just before whatever it's actually testing.

use std::collections::BTreeMap;

use rome_audit::ingest::{run_ingest_once, IngestConfig, SourceSpec, WatermarkTracker};
use rome_audit::{build_registry, SourceKind};

mod common;
use common::{
    address_topic, count_chain_event_rows, delete_produced_block, fresh_hercules_audit_db as fresh_test_db,
    hex_addr, hex_topic, quarantine_rows, run_until_slot_passes, seed_contiguous_finalized_chain,
    seed_produced_empty_block, seed_sol_slot, seed_tx_with_empty_tx_result, seed_tx_with_logs, watermark,
};

// ---- ArcToken/GlobalSanctions test fixtures (reuse the REAL P0 registry) --

const SANCTIONS_MODULE: u8 = 0x11;
const ALICE: u8 = 0xAA;
const BOB: u8 = 0xBB;

fn base_config(chain_id: i64) -> IngestConfig {
    let mut sources = BTreeMap::new();
    sources.insert(hex_addr(SANCTIONS_MODULE), SourceSpec::new(SourceKind::GlobalSanctions));
    IngestConfig {
        chain_id,
        confirmation_lag: 2,
        sources,
    }
}

// ---- (a) below-watermark not materialized; clears exactly once ----------
//
// A′ (finality-prefix fix): finality comes from the ROOTED PREFIX
// (`finalized_tip`), never from this slot's own per-row status label — so
// this test's negative phase isolates INSUFFICIENT LAG (not "still
// Confirmed") as the thing that must hold the slot back, then proves a
// slot that sits `lag` slots below the tip materializes even while its own
// `sol_slot.status` STAYS `Confirmed` (never promoted to `Finalized`) —
// exactly the finality-promotion gap that wedged live Hadrian ingest.

#[tokio::test]
async fn a_events_below_watermark_not_materialized_then_appear_exactly_once() {
    let pool = fresh_test_db().await;
    let registry = build_registry();
    let config = base_config(1001);
    let mut tracker = WatermarkTracker::new();

    let slot = 10i64;
    let lag = config.confirmation_lag; // 2
    seed_contiguous_finalized_chain(&pool, 0, slot - 1).await;
    let block_hash = format!("0x{:064x}", slot);
    seed_sol_slot(&pool, slot, slot - 1, "Confirmed", "0xdead", 1700000010).await;
    seed_tx_with_logs(
        &pool,
        slot,
        &block_hash,
        slot,
        &hex_addr(0x55),
        &[(
            hex_addr(SANCTIONS_MODULE),
            hex_topic(&rome_audit::abi::global_sanctions::SANCTIONED_TOPIC0),
            Some(address_topic(ALICE)),
            None,
            None,
            None,
        )],
    )
    .await;
    // Head-clearance is satisfied well beyond `slot` (produced blocks all
    // the way to slot+10) — but only ONE `Finalized` `sol_slot` row exists,
    // at slot+1, pinning `finalized_tip` at slot+1 (tip - slot = 1 < lag).
    // This isolates "insufficient lag" as the SOLE reason the slot must
    // wait, independent of its own (Confirmed) status label.
    assert_eq!(lag, 2, "this fixture's arithmetic assumes lag=2");
    seed_sol_slot(&pool, slot + 1, slot, "Finalized", "0xtip1", 1700000011).await;
    for s in (slot + 1)..=(slot + 10) {
        seed_produced_empty_block(&pool, s, &format!("0x{:064x}", 900_000 + s), s).await;
    }

    // H1 (P1 review): walk the cursor to EXACTLY target-1 first, so
    // the negative loop below is GUARANTEED to actually engage `slot`, not
    // merely fail to reach it (which would pass "vacuously" for a
    // cursor-position reason instead of a condition reason).
    if slot > 0 {
        run_until_slot_passes(&pool, &pool, &registry, &mut tracker, &config, slot - 1).await;
    }
    assert_eq!(
        watermark(&pool, 1001).await,
        Some(slot - 1),
        "setup: cursor must be at target-1 before the negative loop"
    );

    // finalized_tip - slot = 1 < lag=2 — must not materialize no matter how
    // many ticks, regardless of the slot's own (Confirmed) status label.
    for _ in 0..5 {
        let out = run_ingest_once(&pool, &pool, &registry, &mut tracker, &config, 30)
            .await
            .unwrap();
        assert!(
            !out.slots_passed.contains(&slot),
            "insufficient lag below the finalized tip must never let this slot pass"
        );
    }
    assert_eq!(
        count_chain_event_rows(&pool, 1001).await,
        0,
        "insufficient lag must never pass"
    );
    assert_eq!(
        watermark(&pool, 1001).await,
        Some(slot - 1),
        "the negative loop must have ENGAGED and REFUSED the target slot, never advancing past target-1"
    );

    // Advance the ROOTED tip past the lag threshold — WITHOUT ever promoting
    // `slot` itself to `Finalized`. tip - slot = 2 >= lag=2: the slot must
    // now materialize on its own Confirmed-but-rooted merit (the A′ fix).
    seed_sol_slot(&pool, slot + 2, slot + 1, "Finalized", "0xtip2", 1700000012).await;

    let out = run_until_slot_passes(&pool, &pool, &registry, &mut tracker, &config, slot).await;
    assert_eq!(out.events_inserted, 1);
    assert_eq!(
        count_chain_event_rows(&pool, 1001).await,
        1,
        "must materialize once the rooted tip clears it by `lag`, even while still Confirmed"
    );

    // Hercules eventually catches its own label up to Finalized — a re-tick
    // across that transition must not duplicate the already-landed row.
    seed_sol_slot(&pool, slot, slot - 1, "Finalized", "0xdead", 1700000010).await;
    run_ingest_once(&pool, &pool, &registry, &mut tracker, &config, 30)
        .await
        .unwrap();
    assert_eq!(
        count_chain_event_rows(&pool, 1001).await,
        1,
        "must appear EXACTLY once, not duplicated across the pending→finalized re-reads"
    );
}

// ---- (a2) THE incident regression: a Confirmed band below a Finalized tip
// advances and captures — A′ (finality-prefix fix). Reproduces the actual
// Hadrian shape: a contiguous run of slots Hercules never promoted past
// `Confirmed` in `sol_slot`, sitting far below an already-advanced
// `finalized_tip`. The old per-row conjunct (`status == Finalized`) would
// wedge the contiguous-prefix walk at the very first slot of this band
// forever; A′ lets the whole rooted band through on its own merit and
// captures the log it carries. --------------------------------------------

#[tokio::test]
async fn a2_confirmed_band_below_finalized_tip_advances_and_captures() {
    let pool = fresh_test_db().await;
    let registry = build_registry();
    let config = base_config(3003);
    let mut tracker = WatermarkTracker::new();
    let lag = config.confirmation_lag; // 2
    assert_eq!(lag, 2, "this fixture's arithmetic assumes lag=2");

    // The band: slots 0..=9, ALL labeled `Confirmed` (never `Finalized`) in
    // `sol_slot`, each with a produced (empty) `eth_block`.
    for s in 0i64..=9 {
        seed_sol_slot(&pool, s, s - 1, "Confirmed", &format!("0x{:064x}", s), 1700000000 + s).await;
        seed_produced_empty_block(&pool, s, &format!("0x{:064x}", 900_000 + s), s).await;
    }

    // One decodable log inside the band, at slot 5 — proves capture, not
    // just watermark advance.
    let block_hash = format!("0x{:064x}", 5i64);
    seed_tx_with_logs(
        &pool,
        5,
        &block_hash,
        5,
        &hex_addr(0x55),
        &[(
            hex_addr(SANCTIONS_MODULE),
            hex_topic(&rome_audit::abi::global_sanctions::SANCTIONED_TOPIC0),
            Some(address_topic(ALICE)),
            None,
            None,
            None,
        )],
    )
    .await;

    // Padding: produced (head-clearance) blocks at 10 and 11, deliberately
    // WITHOUT a `sol_slot` row at 10 (status stays genuinely unobserved —
    // the discriminator that must still hold the walk, per A′'s guard) and
    // exactly ONE `Finalized` row at 11, which sets `finalized_tip = 11`.
    // `11 - 9 = lag` — the band's last slot clears the tip by exactly `lag`.
    seed_produced_empty_block(&pool, 10, "0xa2-pad10", 10).await;
    seed_sol_slot(&pool, 11, 10, "Finalized", "0xa2-tip", 1700000011).await;
    seed_produced_empty_block(&pool, 11, "0xa2-pad11", 11).await;

    let outcome = run_until_slot_passes(&pool, &pool, &registry, &mut tracker, &config, 9).await;

    assert_eq!(
        outcome.slots_passed,
        (0..=9).collect::<Vec<i64>>(),
        "the whole Confirmed band must advance contiguously in one stable tick, \
         stopping at slot 10 because it's within the lag window of the tip \
         (finalized_tip=11, 11-10=1 < lag=2) — NOT because slot 10 lacks a \
         sol_slot row (a below-tip missing row now passes; see the c2 test)"
    );
    assert_eq!(
        outcome.events_inserted, 1,
        "the band's one decodable log must be captured"
    );
    assert_eq!(
        watermark(&pool, 3003).await,
        Some(9),
        "the watermark must land exactly at the end of the rooted-but-Confirmed band"
    );
    assert_eq!(
        count_chain_event_rows(&pool, 3003).await,
        1,
        "exactly the band's one log must have landed in audit.chain_event"
    );
}

// ---- (b1) content change inside the lag window never enters chain_event -

#[tokio::test]
async fn b1_content_change_before_stability_never_enters_chain_event() {
    let pool = fresh_test_db().await;
    let registry = build_registry();
    let config = base_config(1002);
    let mut tracker = WatermarkTracker::new();

    let slot = 12i64;
    let lag = config.confirmation_lag; // 2
                                       // H1 (P1 review): seed and walk to EXACTLY target-1 BEFORE the
                                       // target slot's own content exists at all — otherwise the wide-window
                                       // evaluation (C1 fix) would see target's content is ALREADY stable and
                                       // let the walk overshoot straight through it. Only once the cursor is
                                       // pinned at target-1 do we introduce target's (about-to-be-mutated)
                                       // content, so the negative ticks below are guaranteed to engage it.
    seed_contiguous_finalized_chain(&pool, 0, slot - 1).await;
    // Give target-1 exactly enough head-clearance room WITHOUT touching
    // `slot` itself: skip straight to slot+1 (Solana slots are sparsely
    // produced anyway — a skip is normal, per the watermark's own
    // "genuinely empty/skipped slot" case). `slot` staying entirely absent
    // (status=None) is what pins the walk at target-1 — it can't evaluate
    // past a slot Hercules hasn't observed at all.
    seed_sol_slot(&pool, slot + 1, slot, "Finalized", "0xb1-tip", 1700000021).await;
    seed_produced_empty_block(&pool, slot + 1, "0xb1-tip-eth", slot + 1).await;
    assert_eq!(lag, 2, "this fixture's arithmetic assumes lag=2 — update the offsets above if base_config's lag ever changes");
    if slot > 0 {
        run_until_slot_passes(&pool, &pool, &registry, &mut tracker, &config, slot - 1).await;
    }
    assert_eq!(
        watermark(&pool, 1002).await,
        Some(slot - 1),
        "setup: cursor must be at target-1 before target's content exists"
    );

    let block_hash = format!("0x{:064x}", slot);
    seed_sol_slot(&pool, slot, slot - 1, "Finalized", "0xaaa1", 1700000020).await;
    seed_tx_with_logs(
        &pool,
        slot,
        &block_hash,
        slot,
        &hex_addr(0x55),
        &[(
            hex_addr(SANCTIONS_MODULE),
            hex_topic(&rome_audit::abi::global_sanctions::SANCTIONED_TOPIC0),
            Some(address_topic(ALICE)),
            None,
            None,
            None,
        )],
    )
    .await;
    seed_contiguous_finalized_chain(&pool, slot + 1, slot + 10).await;

    let first = run_ingest_once(&pool, &pool, &registry, &mut tracker, &config, 30)
        .await
        .unwrap();
    assert!(
        !first.slots_passed.contains(&slot),
        "first observation of the target slot must Wait"
    );

    // Simulate a clean_from_slot + regenerate with DIFFERENT content: the
    // produced eth_block's blockhash at this slot changes.
    seed_produced_empty_block(&pool, slot, "0xaaa1-REGENERATED", slot).await;

    let second = run_ingest_once(&pool, &pool, &registry, &mut tracker, &config, 30)
        .await
        .unwrap();
    assert!(
        !second.slots_passed.contains(&slot),
        "changed digest must reset first-seen and Wait, never Pass on this tick"
    );
    assert_eq!(
        count_chain_event_rows(&pool, 1002).await,
        0,
        "the changed-content slot must never have entered chain_event"
    );
    assert_eq!(
        watermark(&pool, 1002).await,
        Some(slot - 1),
        "target must have been ENGAGED and REFUSED — watermark must not have advanced past target-1"
    );
}

// ---- (b2) empty-gap: MAX dropped below S must WAIT, not vacuously pass --

#[tokio::test]
async fn b2_empty_gap_head_dropped_below_slot_waits_not_passes() {
    let pool = fresh_test_db().await;
    let registry = build_registry();
    let config = base_config(1003);
    let mut tracker = WatermarkTracker::new();

    let slot = 14i64;
    let lag = config.confirmation_lag; // 2
    seed_contiguous_finalized_chain(&pool, 0, slot - 1).await;
    // sol_slot says Finalized, but NO eth_block ever at `slot` — the
    // production head never advances far enough past it, exactly like a
    // first-seen landing inside a deleted `clean_from_slot` window.
    seed_sol_slot(&pool, slot, slot - 1, "Finalized", "0xgap", 1700000030).await;
    // slot+1 IS produced — just enough for target-1 (`slot-1`) to clear
    // its OWN head-clearance (needs max_produced ≥ (slot-1)+lag = slot+1),
    // without EVER reaching what `slot` itself needs (max_produced ≥
    // slot+lag = slot+2). This isolates condition (ii) as the one thing
    // failing for `slot`, while `slot-1` genuinely passes during the walk.
    seed_sol_slot(&pool, slot + 1, slot, "Finalized", "0xgap-tip1", 1700000031).await;
    seed_produced_empty_block(&pool, slot + 1, "0xgap-tip1-eth", slot + 1).await;
    // slot+2: status-only (pushes finalized_tip further so `slot`'s
    // finalized_with_lag condition holds too — isolating head-clearance as
    // the SOLE failing condition), but deliberately NO eth_block — this is
    // what keeps max_produced pinned at slot+1 forever.
    seed_sol_slot(
        &pool,
        slot + 2,
        slot + 1,
        "Finalized",
        "0xgap-tip2",
        1700000032,
    )
    .await;
    assert_eq!(lag, 2, "this fixture's arithmetic assumes lag=2 — update the offsets above if base_config's lag ever changes");

    // H1: walk to EXACTLY target-1 first — guarantees the ticks below
    // actually engage `slot`, not merely never reach it.
    if slot > 0 {
        run_until_slot_passes(&pool, &pool, &registry, &mut tracker, &config, slot - 1).await;
    }
    assert_eq!(
        watermark(&pool, 1003).await,
        Some(slot - 1),
        "setup: cursor must be at target-1 before the negative loop"
    );

    for _ in 0..5 {
        let out = run_ingest_once(&pool, &pool, &registry, &mut tracker, &config, 30)
            .await
            .unwrap();
        assert!(
            !out.slots_passed.contains(&slot),
            "head-clearance must fail regardless of digest stability across ticks"
        );
    }
    assert_eq!(count_chain_event_rows(&pool, 1003).await, 0);
    assert_eq!(
        watermark(&pool, 1003).await,
        Some(slot - 1),
        "target must have been ENGAGED and REFUSED — watermark must not have advanced past target-1"
    );
}

// ---- (c) a genuinely empty/skipped slot passes, stores nothing ----------

#[tokio::test]
async fn c_genuinely_empty_skipped_slot_passes_without_stalling() {
    let pool = fresh_test_db().await;
    let registry = build_registry();
    let config = base_config(1004);
    let mut tracker = WatermarkTracker::new();

    let slot = 16i64;
    seed_contiguous_finalized_chain(&pool, 0, slot - 1).await;
    // Finalized, but no eth_block at this slot at all — a real empty/skipped slot.
    seed_sol_slot(&pool, slot, slot - 1, "Finalized", "0xempty", 1700000040).await;
    // Head advances well past it via later PRODUCED slots.
    seed_contiguous_finalized_chain(&pool, slot + 1, slot + 10).await;

    let out = run_until_slot_passes(&pool, &pool, &registry, &mut tracker, &config, slot).await;
    assert_eq!(out.events_inserted, 0);
    assert_eq!(
        count_chain_event_rows(&pool, 1004).await,
        0,
        "no placeholder rows for an empty slot"
    );
}

// ---- (c2) THE incident: a SKIPPED slot with NO sol_slot row at all (not
// just no eth_block, per (c) above) must not wedge the watermark below a
// later band of finalized, event-bearing slots. This is what the deployed
// #540 guard (`status.is_some()`) gets wrong — a skipped Solana slot never
// gets a `sol_slot` row in the first place, so requiring one wedges the
// whole audit at the skip, capturing nothing above it. Under the fix, the
// watermark must advance THROUGH the skip and still capture the finalized
// log sitting above the gap. -----------------------------------------------

#[tokio::test]
async fn c2_skipped_slot_with_no_sol_slot_row_advances_and_captures_above_the_gap() {
    let pool = fresh_test_db().await;
    let registry = build_registry();
    let config = base_config(3004);
    let mut tracker = WatermarkTracker::new();
    let lag = config.confirmation_lag; // 2
    assert_eq!(lag, 2, "this fixture's arithmetic assumes lag=2");

    // Rooted prefix.
    seed_contiguous_finalized_chain(&pool, 0, 15).await;

    // Slot 16 = THE SKIP: no `seed_sol_slot`, no `seed_produced_empty_block`
    // at all — exactly what a leader-skipped Solana slot leaves behind (no
    // row anywhere), NOT the "Finalized-but-no-eth_block" shape test (c)
    // above already covers.

    // Finalized, produced band above the gap. Runs to 26 so the LAST slot
    // we actually assert on (24) has its own `lag`-slot headroom below the
    // tip — 25/26 are pure padding, the same shape `a2` uses.
    seed_contiguous_finalized_chain(&pool, 17, 26).await;

    // One decodable log ABOVE the gap — proves capture, not just advance.
    let block_hash = format!("0x{:064x}", 18i64);
    seed_tx_with_logs(
        &pool,
        18,
        &block_hash,
        18,
        &hex_addr(0x55),
        &[(
            hex_addr(SANCTIONS_MODULE),
            hex_topic(&rome_audit::abi::global_sanctions::SANCTIONED_TOPIC0),
            Some(address_topic(ALICE)),
            None,
            None,
            None,
        )],
    )
    .await;

    // finalized_tip = 26 ⇒ slot 16 sits 26-16=10 ≥ lag below the tip, and
    // target slot 24 sits 26-24=2 == lag below it (its own headroom, via
    // the 25/26 padding); max_produced = 26 clears both slots' head
    // -clearance (16+lag=18 and 24+lag=26, both ≤ 26).
    let outcome = run_until_slot_passes(&pool, &pool, &registry, &mut tracker, &config, 24).await;

    assert_eq!(
        outcome.slots_passed.last(),
        Some(&24),
        "the walk must reach slot 24, stepping THROUGH the skip at slot 16"
    );
    assert!(
        outcome.slots_passed.contains(&16),
        "slot 16 (the skip itself) must be among the slots the watermark advanced through"
    );
    assert_eq!(
        watermark(&pool, 3004).await,
        Some(24),
        "the watermark must advance through the skip, not wedge at 15"
    );
    assert_eq!(
        count_chain_event_rows(&pool, 3004).await,
        1,
        "the finalized log ABOVE the gap must have been captured"
    );
}

// ---- (d) reorg strictly below the watermark leaves chain_event untouched -

#[tokio::test]
async fn d_reorg_below_watermark_leaves_chain_event_untouched() {
    let pool = fresh_test_db().await;
    let registry = build_registry();
    let config = base_config(1005);
    let mut tracker = WatermarkTracker::new();

    let earlier_slot = 5i64;
    let target_slot = 10i64;
    seed_contiguous_finalized_chain(&pool, 0, earlier_slot - 1).await;
    seed_sol_slot(
        &pool,
        earlier_slot,
        earlier_slot - 1,
        "Finalized",
        "0xearlier",
        1700000045,
    )
    .await;
    seed_produced_empty_block(&pool, earlier_slot, "0xearliereth", earlier_slot).await;
    // Stably-empty fill between earlier_slot and target_slot.
    seed_contiguous_finalized_chain(&pool, earlier_slot + 1, target_slot - 1).await;

    let target_block_hash = format!("0x{:064x}", target_slot);
    seed_sol_slot(
        &pool,
        target_slot,
        target_slot - 1,
        "Finalized",
        "0xtarget",
        1700000050,
    )
    .await;
    seed_tx_with_logs(
        &pool,
        target_slot,
        &target_block_hash,
        target_slot,
        &hex_addr(0x55),
        &[(
            hex_addr(SANCTIONS_MODULE),
            hex_topic(&rome_audit::abi::global_sanctions::SANCTIONED_TOPIC0),
            Some(address_topic(BOB)),
            None,
            None,
            None,
        )],
    )
    .await;
    seed_contiguous_finalized_chain(&pool, target_slot + 1, target_slot + 10).await;

    run_until_slot_passes(&pool, &pool, &registry, &mut tracker, &config, target_slot).await;
    let before: Vec<(String, serde_json::Value)> = sqlx::query_as(
        "SELECT event_name, args FROM audit.chain_event WHERE chain_id = $1 ORDER BY event_id",
    )
    .bind(1005i64)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        before.len(),
        1,
        "target slot's Sanctioned event must have landed"
    );

    // Simulate a reorg STRICTLY BELOW the (now-passed) watermark: mutate the
    // earlier slot's content directly (as clean_from_slot + regenerate would).
    delete_produced_block(&pool, earlier_slot).await;
    seed_produced_empty_block(&pool, earlier_slot, "0xearlier-REORGED", earlier_slot).await;

    // The pipeline only ever walks FORWARD from the watermark — it must
    // never re-touch the earlier slot, so chain_event is byte-identical.
    run_ingest_once(&pool, &pool, &registry, &mut tracker, &config, 30)
        .await
        .unwrap();
    let after: Vec<(String, serde_json::Value)> = sqlx::query_as(
        "SELECT event_name, args FROM audit.chain_event WHERE chain_id = $1 ORDER BY event_id",
    )
    .bind(1005i64)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        before, after,
        "a reorg strictly below the watermark must leave chain_event byte-identical"
    );
}

// ---- (e) UPDATE/DELETE on chain_event RAISES (append-only trigger) ------

#[tokio::test]
async fn e_update_and_delete_on_chain_event_both_raise() {
    let pool = fresh_test_db().await;
    let registry = build_registry();
    let config = base_config(1006);
    let mut tracker = WatermarkTracker::new();

    let slot = 8i64;
    seed_contiguous_finalized_chain(&pool, 0, slot - 1).await;
    let block_hash = format!("0x{:064x}", slot);
    seed_sol_slot(&pool, slot, slot - 1, "Finalized", "0xrow", 1700000060).await;
    seed_tx_with_logs(
        &pool,
        slot,
        &block_hash,
        slot,
        &hex_addr(0x55),
        &[(
            hex_addr(SANCTIONS_MODULE),
            hex_topic(&rome_audit::abi::global_sanctions::SANCTIONED_TOPIC0),
            Some(address_topic(ALICE)),
            None,
            None,
            None,
        )],
    )
    .await;
    seed_contiguous_finalized_chain(&pool, slot + 1, slot + 10).await;
    run_until_slot_passes(&pool, &pool, &registry, &mut tracker, &config, slot).await;
    assert_eq!(
        count_chain_event_rows(&pool, 1006).await,
        1,
        "setup: exactly one row landed"
    );

    let update_err =
        sqlx::query("UPDATE audit.chain_event SET event_name = 'x' WHERE chain_id = 1006")
            .execute(&pool)
            .await;
    assert!(
        update_err.is_err(),
        "UPDATE must be rejected by the append-only trigger"
    );

    let delete_err = sqlx::query("DELETE FROM audit.chain_event WHERE chain_id = 1006")
        .execute(&pool)
        .await;
    assert!(
        delete_err.is_err(),
        "DELETE must be rejected by the append-only trigger"
    );

    assert_eq!(
        count_chain_event_rows(&pool, 1006).await,
        1,
        "row must survive both rejected attempts, untouched"
    );
}

// ---- (f) fully-indexed event doesn't depend on `tx_result.logs`; a
// non-indexed event genuinely cannot decode without it ------------------
//
// No mock/trait/call-counter (matching the shape correction — plain
// `&PgPool` reads, not a bespoke abstraction to wrap). Instead: seed a real
// tx whose `tx_result.logs` array is DELIBERATELY EMPTY (so `log_data`'s
// `tx_result->'logs'->N->>'data'` resolves to SQL NULL for any ordinal),
// and observe the two events behave completely differently — that
// difference in real behavior IS the proof, not an instrumentation count.

#[tokio::test]
async fn f_fully_indexed_event_ingests_without_tx_result_non_indexed_fails_without_it() {
    let pool = fresh_test_db().await;
    let registry = build_registry();

    // Part 1: Sanctioned (fully indexed, zero non-indexed args) with an
    // EMPTY tx_result.logs array. It must land anyway — it never reads it.
    {
        let config = base_config(2001);
        let mut tracker = WatermarkTracker::new();
        let slot = 9i64;
        seed_contiguous_finalized_chain(&pool, 0, slot - 1).await;
        let block_hash = format!("0x{:064x}", slot);
        seed_sol_slot(&pool, slot, slot - 1, "Finalized", "0xfully1", 1700000070).await;
        seed_tx_with_empty_tx_result(
            &pool,
            slot,
            &block_hash,
            slot,
            &hex_addr(0x55),
            &(
                hex_addr(SANCTIONS_MODULE),
                hex_topic(&rome_audit::abi::global_sanctions::SANCTIONED_TOPIC0),
                Some(address_topic(ALICE)),
                None,
                None,
                None,
            ),
        )
        .await;
        seed_contiguous_finalized_chain(&pool, slot + 1, slot + 10).await;

        let outcome =
            run_until_slot_passes(&pool, &pool, &registry, &mut tracker, &config, slot).await;
        assert_eq!(
            outcome.events_inserted, 1,
            "Sanctioned must land even with an empty tx_result.logs array — it never depends on it"
        );
    }

    // Part 2: TransferScreened (has a non-indexed `amount`) with the SAME
    // kind of empty tx_result. There is nowhere else `amount` could come
    // from, and this is a DETERMINISTIC content problem (H2, P1
    // review): it must never silently coerce a missing word into zero,
    // but it must ALSO never wedge the watermark forever — the valve
    // doctrine quarantines the log (a real, inspectable row saying "saw
    // it, couldn't decode it") and lets the slot advance.
    //
    // Fresh DB (not the same `pool` as Part 1): Hercules' tables aren't
    // chain_id-scoped (one Hercules DB = one chain, per the module doc), so
    // reusing Part 1's pool would let this part's independent from-genesis
    // watermark walk re-discover Part 1's real, valid Sanctioned log at
    // slot 9 too — a test-fixture-sharing artifact, not a decoder bug, but
    // exactly the kind of cross-contamination an isolated DB avoids.
    {
        let pool = fresh_test_db().await;
        let registry = build_registry();
        let config = base_config(2002);
        let mut tracker = WatermarkTracker::new();
        let slot = 19i64;
        seed_contiguous_finalized_chain(&pool, 0, slot - 1).await;
        let block_hash = format!("0x{:064x}", slot);
        seed_sol_slot(&pool, slot, slot - 1, "Finalized", "0xfully2", 1700000071).await;
        seed_tx_with_empty_tx_result(
            &pool,
            slot,
            &block_hash,
            slot,
            &hex_addr(0x55),
            &(
                hex_addr(SANCTIONS_MODULE),
                hex_topic(&rome_audit::abi::global_sanctions::TRANSFER_SCREENED_TOPIC0),
                Some(address_topic(ALICE)),
                Some(address_topic(BOB)),
                None,
                None,
            ),
        )
        .await;
        seed_contiguous_finalized_chain(&pool, slot + 1, slot + 10).await;

        run_until_slot_passes(&pool, &pool, &registry, &mut tracker, &config, slot).await;

        assert_eq!(
            count_chain_event_rows(&pool, 2002).await,
            0,
            "the undecodable log must never land in chain_event"
        );
        let quarantined = quarantine_rows(&pool, 2002).await;
        assert_eq!(
            quarantined.len(),
            1,
            "exactly the one undecodable log must be quarantined"
        );
        assert_eq!(quarantined[0], (slot, "undecodable log".to_string()));
        assert!(
            watermark(&pool, 2002).await.unwrap_or(-1) >= slot,
            "a quarantined (terminal) log must NOT wedge the watermark — the slot (and beyond) still advances"
        );
    }
}

// ---- (g) idempotent re-ingest: same rows once, even across a re-run -----

#[tokio::test]
async fn g_idempotent_reingest_same_finalized_slot_yields_rows_once() {
    let pool = fresh_test_db().await;
    let registry = build_registry();
    let config = base_config(1008);

    let slot = 7i64;
    seed_contiguous_finalized_chain(&pool, 0, slot - 1).await;
    let block_hash = format!("0x{:064x}", slot);
    seed_sol_slot(&pool, slot, slot - 1, "Finalized", "0xidem", 1700000080).await;
    seed_tx_with_logs(
        &pool,
        slot,
        &block_hash,
        slot,
        &hex_addr(0x55),
        &[(
            hex_addr(SANCTIONS_MODULE),
            hex_topic(&rome_audit::abi::global_sanctions::SANCTIONED_TOPIC0),
            Some(address_topic(ALICE)),
            None,
            None,
            None,
        )],
    )
    .await;
    seed_contiguous_finalized_chain(&pool, slot + 1, slot + 10).await;

    let mut tracker = WatermarkTracker::new();
    run_until_slot_passes(&pool, &pool, &registry, &mut tracker, &config, slot).await;
    assert_eq!(count_chain_event_rows(&pool, 1008).await, 1);

    // Simulate a worker restart with a stale watermark: rewind the
    // PERSISTED watermark and use a FRESH (in-memory-state-lost) tracker —
    // exactly what a real restart looks like per the module doc.
    sqlx::query("UPDATE audit.ingest_watermark SET verified_through_slot = $1 WHERE chain_id = $2")
        .bind(slot - 1)
        .bind(1008i64)
        .execute(&pool)
        .await
        .unwrap();
    let mut fresh_tracker = WatermarkTracker::new();
    let outcome =
        run_until_slot_passes(&pool, &pool, &registry, &mut fresh_tracker, &config, slot).await;

    assert_eq!(
        count_chain_event_rows(&pool, 1008).await,
        1,
        "re-ingesting the same finalized slot must not duplicate the row"
    );
    assert_eq!(
        outcome.events_inserted, 0,
        "the second pass's own insert must report 0 NEW rows (ON CONFLICT DO NOTHING)"
    );
}

// ---- (h_c1) C1 fix: one stable tick advances MANY slots, not one --------

#[tokio::test]
async fn h_c1_stable_window_advances_multiple_slots_in_one_tick() {
    let pool = fresh_test_db().await;
    let registry = build_registry();
    let config = base_config(3001);
    let mut tracker = WatermarkTracker::new();

    // A contiguous, stably-produced run — no special content needed, just
    // Finalized+produced+stable. Head-clearance (lag=2) caps how far the
    // LAST couple of slots in the range can clear, so this range naturally
    // yields a partial-but-still-multi-slot advance.
    seed_contiguous_finalized_chain(&pool, 0, 19).await;

    // Tick 1: every candidate in the window is a FIRST observation — must
    // record first-seen for the WHOLE window (the C1 fix) but advance
    // nothing (digest-stability can't be proven on a first read).
    let first = run_ingest_once(&pool, &pool, &registry, &mut tracker, &config, 30)
        .await
        .unwrap();
    assert!(
        first.slots_passed.is_empty(),
        "first observation of every candidate must Wait"
    );

    // Tick 2: the SAME window is now stable everywhere it can be — this
    // ONE tick must advance the whole contiguous run at once, not one slot.
    let second = run_ingest_once(&pool, &pool, &registry, &mut tracker, &config, 30)
        .await
        .unwrap();
    assert!(
        second.slots_passed.len() > 1,
        "C1: a single tick over a stable window must advance MORE than one slot at a time — got {:?}",
        second.slots_passed
    );
    for (i, &s) in second.slots_passed.iter().enumerate() {
        assert_eq!(
            s, i as i64,
            "the advanced run must be exactly contiguous from slot 0, no gaps"
        );
    }
    assert_eq!(
        watermark(&pool, 3001).await,
        Some(second.slots_passed.len() as i64 - 1)
    );
}

// ---- (h_h2) H2 fix: a TRANSIENT miss holds — never quarantines, never advances

#[tokio::test]
async fn h_h2_transient_missing_receipt_holds_without_quarantine_or_advance() {
    let pool = fresh_test_db().await;
    let registry = build_registry();
    let config = base_config(3002);
    let mut tracker = WatermarkTracker::new();

    let slot = 9i64;
    seed_contiguous_finalized_chain(&pool, 0, slot - 1).await;
    // Head-clearance room for target-1, without touching `slot` itself
    // (same pattern as b1/b2 — `slot` staying entirely absent is what pins
    // the walk at target-1).
    seed_sol_slot(&pool, slot + 1, slot, "Finalized", "0xh2-tip", 1700000091).await;
    seed_produced_empty_block(&pool, slot + 1, "0xh2-tip-eth", slot + 1).await;
    run_until_slot_passes(&pool, &pool, &registry, &mut tracker, &config, slot - 1).await;
    assert_eq!(
        watermark(&pool, 3002).await,
        Some(slot - 1),
        "setup: cursor must be at target-1 before target's content exists"
    );

    // Now seed slot 9 for real: a matched evm_log with NO evm_tx_result/
    // evm_tx ever written for its tx — tx_receipt_info() returns None,
    // MissingReceiptInfo, classified Transient (no deterministic signal
    // here proves "will never resolve" vs. "hasn't caught up yet", so it
    // defaults to the safe side: hold and retry, never quarantine).
    let block_hash = format!("0x{:064x}", slot);
    seed_sol_slot(
        &pool,
        slot,
        slot - 1,
        "Finalized",
        "0xtransient",
        1700000090,
    )
    .await;
    seed_produced_empty_block(&pool, slot, &block_hash, slot).await;
    let tx_hash = format!("0x{:064x}", slot as u64 * 1000 + 7);
    sqlx::query(
        "INSERT INTO evm_log (slot_number, tx_hash, log_ordinal, address, topic0, topic1, topic2, topic3)
         VALUES ($1,$2,0,$3,$4,$5,NULL,NULL)",
    )
    .bind(slot)
    .bind(&tx_hash)
    .bind(hex_addr(SANCTIONS_MODULE))
    .bind(hex_topic(&rome_audit::abi::global_sanctions::SANCTIONED_TOPIC0))
    .bind(address_topic(ALICE))
    .execute(&pool)
    .await
    .unwrap();
    seed_contiguous_finalized_chain(&pool, slot + 2, slot + 10).await;

    for _ in 0..5 {
        match run_ingest_once(&pool, &pool, &registry, &mut tracker, &config, 30).await {
            Ok(out) => assert!(
                !out.slots_passed.contains(&slot),
                "a transient miss must never let the slot pass"
            ),
            Err(rome_audit::ingest::IngestError::MissingReceiptInfo { .. }) => {} // expected — hold
            Err(other) => panic!("expected MissingReceiptInfo (Transient), got {other:?}"),
        }
    }

    assert_eq!(count_chain_event_rows(&pool, 3002).await, 0);
    assert_eq!(
        quarantine_rows(&pool, 3002).await.len(),
        0,
        "a TRANSIENT miss must NEVER quarantine"
    );
    assert_eq!(
        watermark(&pool, 3002).await,
        Some(slot - 1),
        "a TRANSIENT miss must hold — the watermark must not advance past it"
    );
}

// ---- (i) quarantine must be idempotent across a re-tick (reviewer-found
// bug: `audit.quarantine` had no UNIQUE constraint and the INSERT had no
// `ON CONFLICT`) ----------------------------------------------------------
//
// Reproduces the exact failure mode from the module doc: one slot carries
// TWO tx groups — tx A's log is a deterministic (Terminal) decode failure,
// tx B's log has no receipt info yet (Transient). `by_tx` is a `BTreeMap`,
// so tx A (hash suffix `...007`) is processed — and quarantined — BEFORE tx
// B (hash suffix `...008`) blows up and makes the whole `ingest_slot` call
// return `Err`, which means `run_ingest_once` never persists the watermark
// for this slot. Re-ticking therefore re-quarantines tx A's log every time,
// forever, until B's receipt shows up — exactly the "double alarms" bug.

#[tokio::test]
async fn i_quarantine_reinsert_across_re_tick_is_idempotent() {
    let pool = fresh_test_db().await;
    let registry = build_registry();
    let config = base_config(4001);
    let mut tracker = WatermarkTracker::new();

    let slot = 9i64;
    seed_contiguous_finalized_chain(&pool, 0, slot - 1).await;
    // Head-clearance room for target-1, without touching `slot` itself yet
    // (same pattern as h_h2 — `slot` staying entirely absent is what pins
    // the watermark walk at target-1 first).
    seed_sol_slot(
        &pool,
        slot + 1,
        slot,
        "Finalized",
        "0xquardup-tip",
        1700000094,
    )
    .await;
    seed_produced_empty_block(&pool, slot + 1, "0xquardup-tip-eth", slot + 1).await;
    run_until_slot_passes(&pool, &pool, &registry, &mut tracker, &config, slot - 1).await;
    assert_eq!(
        watermark(&pool, 4001).await,
        Some(slot - 1),
        "setup: cursor must be at target-1 before target's content exists"
    );

    // Now seed slot 9 for real.
    let block_hash = format!("0x{:064x}", slot);
    seed_sol_slot(&pool, slot, slot - 1, "Finalized", "0xquardup", 1700000095).await;

    // tx A (tx_hash suffix 007, sorts first): TransferScreened with NO data
    // recoverable (`tx_result.logs` deliberately empty) — a deterministic
    // decode failure, Terminal, quarantined.
    seed_tx_with_empty_tx_result(
        &pool,
        slot,
        &block_hash,
        slot,
        &hex_addr(0x55),
        &(
            hex_addr(SANCTIONS_MODULE),
            hex_topic(&rome_audit::abi::global_sanctions::TRANSFER_SCREENED_TOPIC0),
            Some(address_topic(ALICE)),
            Some(address_topic(BOB)),
            None,
            None,
        ),
    )
    .await;

    // tx B (tx_hash suffix 008, sorts second): a matched evm_log with NO
    // evm_tx/evm_tx_result ever written — MissingReceiptInfo, Transient,
    // propagates and holds the whole tick's watermark advance.
    let tx_b_hash = format!("0x{:064x}", slot as u64 * 1000 + 8);
    sqlx::query(
        "INSERT INTO evm_log (slot_number, tx_hash, log_ordinal, address, topic0, topic1, topic2, topic3)
         VALUES ($1,$2,0,$3,$4,$5,NULL,NULL)",
    )
    .bind(slot)
    .bind(&tx_b_hash)
    .bind(hex_addr(SANCTIONS_MODULE))
    .bind(hex_topic(&rome_audit::abi::global_sanctions::SANCTIONED_TOPIC0))
    .bind(address_topic(BOB))
    .execute(&pool)
    .await
    .unwrap();

    seed_contiguous_finalized_chain(&pool, slot + 2, slot + 10).await;

    // The slot's FIRST observation always returns `Wait` (digest-stability
    // needs two reads, per the pipeline module doc's C1 note) — so the
    // first tick or two may be a no-op `Ok` before `ingest_slot` actually
    // runs. Tick until `ingest_slot` has ACTUALLY run (and errored on tx B)
    // exactly twice — each such tick is a full re-processing of the slot
    // from scratch, re-quarantining tx A's log before hitting tx B again.
    let mut process_attempts = 0;
    while process_attempts < 2 {
        match run_ingest_once(&pool, &pool, &registry, &mut tracker, &config, 30).await {
            Ok(out) => assert!(
                !out.slots_passed.contains(&slot),
                "a transient miss on tx B must never let the slot pass"
            ),
            Err(rome_audit::ingest::IngestError::MissingReceiptInfo { .. }) => {
                process_attempts += 1;
            }
            Err(other) => panic!("expected MissingReceiptInfo (Transient), got {other:?}"),
        }
    }
    assert_eq!(
        watermark(&pool, 4001).await,
        Some(slot - 1),
        "the transient miss on tx B must hold the watermark below `slot` throughout"
    );

    let quarantined = quarantine_rows(&pool, 4001).await;
    assert_eq!(
        quarantined.len(),
        1,
        "tx A's log must be quarantined EXACTLY ONCE across the two re-ticks, not once per tick \
         (pre-fix: no UNIQUE constraint + no ON CONFLICT means a duplicate row per re-tick)"
    );
    assert_eq!(quarantined[0], (slot, "undecodable log".to_string()));
}

// ---- (j) `audit.quarantine` is append-only, same doctrine as (e) --------

#[tokio::test]
async fn j_update_and_delete_on_quarantine_both_raise() {
    let pool = fresh_test_db().await;
    let registry = build_registry();
    let config = base_config(4002);
    let mut tracker = WatermarkTracker::new();

    let slot = 19i64;
    seed_contiguous_finalized_chain(&pool, 0, slot - 1).await;
    let block_hash = format!("0x{:064x}", slot);
    seed_sol_slot(&pool, slot, slot - 1, "Finalized", "0xquarrow", 1700000096).await;
    seed_tx_with_empty_tx_result(
        &pool,
        slot,
        &block_hash,
        slot,
        &hex_addr(0x55),
        &(
            hex_addr(SANCTIONS_MODULE),
            hex_topic(&rome_audit::abi::global_sanctions::TRANSFER_SCREENED_TOPIC0),
            Some(address_topic(ALICE)),
            None,
            None,
            None,
        ),
    )
    .await;
    seed_contiguous_finalized_chain(&pool, slot + 1, slot + 10).await;
    run_until_slot_passes(&pool, &pool, &registry, &mut tracker, &config, slot).await;
    assert_eq!(
        quarantine_rows(&pool, 4002).await.len(),
        1,
        "setup: exactly one row landed"
    );

    let update_err = sqlx::query("UPDATE audit.quarantine SET reason = 'x' WHERE chain_id = 4002")
        .execute(&pool)
        .await;
    assert!(
        update_err.is_err(),
        "UPDATE must be rejected by the append-only trigger"
    );

    let delete_err = sqlx::query("DELETE FROM audit.quarantine WHERE chain_id = 4002")
        .execute(&pool)
        .await;
    assert!(
        delete_err.is_err(),
        "DELETE must be rejected by the append-only trigger"
    );

    assert_eq!(
        quarantine_rows(&pool, 4002).await.len(),
        1,
        "row must survive both rejected attempts, untouched"
    );
}
