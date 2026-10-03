//! Differential correctness proof for the `slot_range_for_blocks` perf fix.
//!
//! `ingest::hercules_reads::slot_range_for_blocks` was rewritten from a
//! `MIN(slot_number)/MAX(slot_number) … WHERE (params).number BETWEEN` scan
//! (which the planner served as an ordered LIMIT-1 over `eth_block_pkey` in
//! slot order, filter-scanning millions of rows and ignoring the
//! `eth_block_number` btree on `((params).number)`) into two ordered
//! index-served subqueries under ONE snapshot. Correctness rests on the
//! producer invariant: **block number is strictly increasing in
//! `(slot_number, slot_block_idx)` order** — so the slot of the lowest
//! in-range block number == `MIN(slot_number)` in range, and likewise the
//! highest number == `MAX(slot_number)`.
//!
//! This test seeds producer-shaped `eth_block` rows (multiple blocks per
//! slot, sequential plain-mode numbering, slot gaps, out-of-range blocks on
//! both sides, and a `params IS NULL` row inside the slot range) and asserts
//! the OLD query and the NEW query return the IDENTICAL `(min_slot, max_slot)`
//! for every range — the honest guard is a differential test on
//! producer-shaped data (a non-monotonic fixture is unreachable by the
//! producer, so seeding one would prove nothing about the real system).

use sqlx::PgPool;

mod common;
use common::fresh_hercules_audit_db;

/// Seeds one produced `eth_block` at `(slot, idx)` carrying `number` — the
/// plain single_state shape: a `blockparams` composite with a real number.
async fn seed_block(pool: &PgPool, slot: i64, idx: i32, number: i64) {
    sqlx::query(
        "INSERT INTO eth_block (slot_number, slot_block_idx, block_gas_used, slot_timestamp, params)
         VALUES ($1, $2, 0, 1700000000, ROW($3, $4, $5, 1700000000)::blockparams)",
    )
    .bind(slot)
    .bind(idx)
    .bind(format!("0x{:064x}", number)) // blockhash
    .bind(format!("0x{:064x}", 0)) // parent_hash (irrelevant)
    .bind(number)
    .execute(pool)
    .await
    .unwrap();
}

/// Seeds a `params IS NULL` `eth_block` row at `(slot, idx)` — an unproduced
/// placeholder that carries no block number and must be excluded by BOTH the
/// old and new queries.
async fn seed_null_params_block(pool: &PgPool, slot: i64, idx: i32) {
    sqlx::query(
        "INSERT INTO eth_block (slot_number, slot_block_idx, block_gas_used, slot_timestamp, params)
         VALUES ($1, $2, 0, 1700000000, NULL)",
    )
    .bind(slot)
    .bind(idx)
    .execute(pool)
    .await
    .unwrap();
}

/// The ORIGINAL query, kept verbatim as the differential oracle.
async fn old_query(pool: &PgPool, from_block: i64, to_block: i64) -> Option<(i64, i64)> {
    let row: (Option<i64>, Option<i64>) = sqlx::query_as(
        r#"
        SELECT MIN(slot_number), MAX(slot_number)
        FROM eth_block
        WHERE params IS NOT NULL AND (params).number BETWEEN $1 AND $2
        "#,
    )
    .bind(from_block)
    .bind(to_block)
    .fetch_one(pool)
    .await
    .unwrap();
    match row {
        (Some(min), Some(max)) => Some((min, max)),
        _ => None,
    }
}

/// The NEW query — the ACTUAL shipped production function, so a mutation of
/// its SQL is caught by this differential test (binds the guard to the
/// deployed path, not a copy of it).
async fn new_query(pool: &PgPool, from_block: i64, to_block: i64) -> Option<(i64, i64)> {
    rome_audit::ingest::slot_range_for_blocks(pool, from_block, to_block)
        .await
        .unwrap()
}

async fn seed_producer_shaped(pool: &PgPool) {
    // Plain single_state: block number strictly increasing in
    // (slot_number, slot_block_idx) order. Layout:
    //
    //   slot  idx  number
    //     5    0     50    <- below every query range
    //    10    0    100    <- multiple blocks per slot (idx 0 and 1)
    //    10    1    101
    //    11    0    102
    //   (gap: slots 12, 13 produce no block)
    //    14    0    103    <- multiple blocks per slot across a gap
    //    14    1    104
    //    20    0    105
    //    20    1    NULL   <- params IS NULL inside the slot range (excluded)
    //    25    0    106
    //    30    0    200    <- above every query range
    seed_block(pool, 5, 0, 50).await;
    seed_block(pool, 10, 0, 100).await;
    seed_block(pool, 10, 1, 101).await;
    seed_block(pool, 11, 0, 102).await;
    seed_block(pool, 14, 0, 103).await;
    seed_block(pool, 14, 1, 104).await;
    seed_block(pool, 20, 0, 105).await;
    seed_null_params_block(pool, 20, 1).await;
    seed_block(pool, 25, 0, 106).await;
    seed_block(pool, 30, 0, 200).await;
}

#[tokio::test]
async fn new_query_matches_old_query_across_ranges() {
    let pool = fresh_hercules_audit_db().await;
    seed_producer_shaped(&pool).await;

    // (from_block, to_block, human-readable expectation for a sanity anchor)
    let cases: &[(i64, i64, Option<(i64, i64)>)] = &[
        // Spans a slot gap: 102@slot11 .. 104@slot14.
        (102, 104, Some((11, 14))),
        // Full produced span 100..106 → slots 10..25.
        (100, 106, Some((10, 25))),
        // Within-slot + adjacent-slot: 101@slot10.idx1, 102@slot11.
        (101, 102, Some((10, 11))),
        // Single block that sits at a slot whose OTHER idx is params-NULL —
        // the NULL row (slot 20 idx 1) must be excluded by both.
        (105, 105, Some((20, 20))),
        // Range covering NO produced block → None from both.
        (107, 150, None),
        // Degenerate from > to → BETWEEN-empty / subquery-empty → None.
        (104, 102, None),
        // A degenerate range entirely below the retained blocks.
        (0, 49, None),
        // Touches the low out-of-range block only.
        (50, 99, Some((5, 5))),
        // Touches the high out-of-range block only.
        (150, 250, Some((30, 30))),
    ];

    for (from, to, expected) in cases {
        let old = old_query(&pool, *from, *to).await;
        let new = new_query(&pool, *from, *to).await;
        // The correctness proof: NEW == OLD on producer-shaped data.
        assert_eq!(
            new, old,
            "NEW != OLD for range [{from}, {to}]: new={new:?} old={old:?}"
        );
        // Anchor the differential to the hand-computed truth so a bug that
        // happens to break BOTH queries identically can't pass silently.
        assert_eq!(
            old, *expected,
            "OLD query wrong for range [{from}, {to}]: got {old:?}, expected {expected:?}"
        );
    }
}

#[tokio::test]
async fn new_query_matches_old_query_on_empty_source() {
    // No blocks at all: both must return None (mirrors a genuinely empty
    // source / fresh chain).
    let pool = fresh_hercules_audit_db().await;
    for (from, to) in [(0i64, 1_000i64), (100, 100), (5, 1)] {
        assert_eq!(
            new_query(&pool, from, to).await,
            old_query(&pool, from, to).await,
            "NEW != OLD on empty source for range [{from}, {to}]"
        );
        assert_eq!(new_query(&pool, from, to).await, None);
    }
}
