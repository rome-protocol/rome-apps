//! Direct decode tests for the Hercules source reads whose column types are
//! the ones most easily faked wrong in a fixture. The regression this guards
//! is concrete: `sol_slot.status` is Hercules' custom `slotstatus` Postgres
//! ENUM, and the crate decodes it into `Option<String>`. sqlx REFUSES to
//! decode an enum column straight into a Rust String — it errors
//!   `mismatched types; Rust Option<String> (as SQL TEXT) is not compatible
//!    with SQL type `slotstatus``
//! which is exactly what broke every live forward-tick on Hadrian. The old
//! fixture declared `status` as plain TEXT, so no test could ever have caught
//! it. With the fixture now type-faithful (real `slotstatus` ENUM), these
//! tests call `slot_observation` / `finalized_tip` directly and prove the
//! `status::text` cast decodes a real enum column.
//!
//! NON-VACUOUSNESS: revert the `::text` cast in
//! `src/ingest/hercules_reads.rs::slot_observation` (back to `SELECT status,
//! …`) and `enum_status_decodes_*` FAILS with the production enum-vs-String
//! decode error — the fixture is the real enum type now.

use rome_audit::ingest::hercules_reads::{finalized_tip, slot_observation};
use rome_audit::ingest::SlotStatusKind;

mod common;
use common::{fresh_hercules_audit_db, seed_produced_empty_block, seed_sol_slot};

#[tokio::test]
async fn enum_status_decodes_finalized_and_processed_through_the_text_cast() {
    let pool = fresh_hercules_audit_db().await;

    // A Finalized slot (with a produced block) and a Processed slot.
    seed_sol_slot(&pool, 100, 99, "Finalized", "0xf100", 1_700_000_100).await;
    seed_produced_empty_block(&pool, 100, "0xeth100", 100).await;
    seed_sol_slot(&pool, 101, 100, "Processed", "0xp101", 1_700_000_101).await;

    // The read that broke live ingest: decoding the `slotstatus` ENUM column.
    let finalized = slot_observation(&pool, 100)
        .await
        .expect("slot_observation must decode a `slotstatus` ENUM row (was: enum-vs-String error)");
    assert_eq!(
        finalized.status,
        Some(SlotStatusKind::Finalized),
        "the Finalized enum value must decode via the status::text cast"
    );
    assert_eq!(
        finalized.digest.sol_blockhash.as_deref(),
        Some("0xf100"),
        "blockhash must come back alongside the status in the same read"
    );

    let processed = slot_observation(&pool, 101)
        .await
        .expect("Processed enum row must also decode");
    assert_eq!(processed.status, Some(SlotStatusKind::Processed));

    // A slot Hercules has no row for at all → None status (not an error).
    let absent = slot_observation(&pool, 999).await.expect("absent slot is Ok(None status)");
    assert_eq!(absent.status, None);
}

#[tokio::test]
async fn finalized_tip_bare_literal_compares_against_the_real_enum_column() {
    let pool = fresh_hercules_audit_db().await;

    // No finalized slot yet → None.
    assert_eq!(
        finalized_tip(&pool).await.expect("finalized_tip on empty DB is Ok(None)"),
        None
    );

    // Finalized at 200 and 220; a Confirmed (non-final) slot at 230 must NOT
    // be counted — this is the `WHERE status = 'Finalized'` bare-literal
    // comparison running against the real `slotstatus` ENUM column.
    seed_sol_slot(&pool, 200, 199, "Finalized", "0xf200", 1_700_000_200).await;
    seed_sol_slot(&pool, 220, 219, "Finalized", "0xf220", 1_700_000_220).await;
    seed_sol_slot(&pool, 230, 229, "Confirmed", "0xc230", 1_700_000_230).await;

    assert_eq!(
        finalized_tip(&pool).await.expect("finalized_tip must run its enum-literal comparison"),
        Some(220),
        "MAX finalized slot is 220; the later Confirmed slot 230 must be excluded"
    );
}
