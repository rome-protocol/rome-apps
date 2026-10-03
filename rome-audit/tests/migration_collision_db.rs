//! Regression test for the shared-`rome_via_db` migration-version collision
//! that broke every rome-audit boot on Hadrian.
//!
//! rome-audit runs its sqlx migrations against the SAME database that hosts
//! rome-via-sync's migrations, both tracked in the default
//! `public._sqlx_migrations` table. When rome-audit numbered its migrations
//! `0001–0010` they collided head-on with rome-via-sync's `0001–0019`: sqlx
//! found "version 1 already applied (by rome-via) but the checksum differs
//! (rome-audit's 0001)" and aborted boot with
//! `migration 1 was previously applied but has been modified`.
//!
//! CI never caught this because the CI/test DB is isolated — only rome-audit's
//! own migrations are ever present, so version 1 IS rome-audit's 0001 and its
//! checksum matches. This test reproduces the PRODUCTION topology: it seeds a
//! foreign `_sqlx_migrations` row (simulating rome-via-sync's version 1)
//! BEFORE running rome-audit's migrator, exactly as the shared DB looks on a
//! real boot.
//!
//! With rome-audit's migrations renumbered into the free `0901–0910` band,
//! version 1 is no longer in rome-audit's own set, so `set_ignore_missing(true)`
//! (mirroring `src/main.rs`) makes sqlx ignore it and apply 901–910 cleanly.
//! If the migrations were back at `0001` this test FAILS with the
//! "has been modified" error — that is the whole point.

mod common;

use sqlx::Row;

/// Seeds a `public._sqlx_migrations` row that looks like rome-via-sync already
/// applied ITS migration version 1, then runs rome-audit's own migrator over
/// the same pool (byte-for-byte the `src/main.rs` invocation) and asserts it
/// succeeds and lands the `audit` schema.
#[tokio::test]
async fn rome_audit_migrations_survive_rome_via_sync_version_collision() {
    let pool = common::fresh_empty_db().await;

    // Mirror sqlx's own Postgres migration-tracking schema exactly, because we
    // seed a row before any migrator call would create it. (sqlx creates this
    // same table lazily on `run`; creating it ourselves first is a no-op for
    // the migrator, which uses CREATE TABLE IF NOT EXISTS.)
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS _sqlx_migrations (
             version BIGINT PRIMARY KEY,
             description TEXT NOT NULL,
             installed_on TIMESTAMPTZ NOT NULL DEFAULT now(),
             success BOOLEAN NOT NULL,
             checksum BYTEA NOT NULL,
             execution_time BIGINT NOT NULL
         )",
    )
    .execute(&pool)
    .await
    .expect("create _sqlx_migrations table");

    // Simulate rome-via-sync having already applied its migration version 1 on
    // this shared DB. The checksum is ARBITRARY bytes that deliberately do NOT
    // equal rome-audit's own migration checksum — this is precisely the state
    // that made pre-renumber sqlx abort with "migration 1 ... has been
    // modified" when rome-audit ALSO owned a version 1.
    let foreign_checksum: &[u8] = &[0xde, 0xad, 0xbe, 0xef, 0x00, 0x11, 0x22, 0x33];
    sqlx::query(
        "INSERT INTO _sqlx_migrations
             (version, description, installed_on, success, checksum, execution_time)
         VALUES (1, 'rome_via_sync_0001', now(), true, $1, 0)",
    )
    .bind(foreign_checksum)
    .execute(&pool)
    .await
    .expect("seed the foreign rome-via-sync version-1 row");

    // Run rome-audit's migrator EXACTLY as src/main.rs does: from
    // ./migrations, with ignore_missing(true). Post-renumber (0901–0910) this
    // must succeed despite the foreign version 1 in the tracking table.
    let mut migrator = sqlx::migrate!("./migrations");
    migrator.set_ignore_missing(true);
    migrator
        .run(&pool)
        .await
        .expect("rome-audit migrations must apply cleanly alongside rome-via-sync's version 1 (renumbered 0901–0910; the collision is fixed)");

    // The foreign row must be untouched — rome-audit never owned version 1.
    let via_desc: String =
        sqlx::query("SELECT description FROM _sqlx_migrations WHERE version = 1")
            .fetch_one(&pool)
            .await
            .expect("foreign version-1 row must still be present")
            .get("description");
    assert_eq!(
        via_desc, "rome_via_sync_0001",
        "rome-audit must not have clobbered rome-via-sync's version-1 row"
    );

    // The audit schema + a representative table must now exist.
    let (schema_exists,): (bool,) = sqlx::query_as(
        "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'audit')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(schema_exists, "the `audit` schema must exist after migrating");

    let (table_exists,): (bool,) = sqlx::query_as(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='audit' AND table_name='chain_event')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        table_exists,
        "audit.chain_event must exist after rome-audit's migrations apply"
    );

    // And rome-audit's own migrations (901–910) are all recorded as applied.
    let (applied,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM _sqlx_migrations WHERE version BETWEEN 901 AND 910")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        applied, 10,
        "all 10 rome-audit migrations (901–910) must be recorded applied"
    );

    pool.close().await;
}
