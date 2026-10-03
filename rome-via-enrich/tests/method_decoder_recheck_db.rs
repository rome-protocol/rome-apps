// Integration test: `unknown_selectors_sql` must re-include a selector whose
// only row is a STALE raw placeholder — otherwise a transient 4byte.directory
// failure (or a genuine miss recorded long ago) poisons the selector forever.
//
// See rome-via-enrich/src/workers/method_decoder.rs module doc: a raw
// placeholder is a row with `source = '4byte' AND signature = selector`
// (4byte had nothing for this selector, or a bug once wrote the raw selector
// on a transient error). Once older than METHOD_SIG_RECHECK_TTL it must be
// treated as "unknown" again so the decoder revisits it. Resolved rows
// (signature != selector) and 'seed' rows must NEVER be re-included.
//
// Requires HERCULES_TEST_DATABASE_URL pointing at a throwaway/dev Postgres —
// same gate rome-via-api's kind_filter_db.rs / throughput_db.rs use. Runs the
// sync + enrich migrations (idempotent), seeds fixtures under a throwaway
// chain_id, and always cleans up. Skips cleanly when unset.

use rome_via_enrich::workers::method_decoder::{
    unknown_selectors_sql, UPSERT_RAW_PLACEHOLDER_SQL, UPSERT_RESOLVED_SQL,
};
use sqlx::postgres::PgPoolOptions;

const THROWAWAY_CHAIN_ID: i64 = 990013;

fn test_db_url() -> Option<String> {
    std::env::var("HERCULES_TEST_DATABASE_URL").ok()
}

async fn setup_schema(pool: &sqlx::PgPool) {
    // Both migrators share one `_sqlx_migrations` table (sync owns versions
    // 1-20, enrich owns 100+) — `ignore_missing` is required so each migrator
    // doesn't choke on the other's versions it can't see in its own folder.
    let mut sync_migrator = sqlx::migrate!("../rome-via-sync/migrations");
    sync_migrator.set_ignore_missing(true);
    sync_migrator.run(pool).await.expect("rome-via-sync migrations");

    let mut enrich_migrator = sqlx::migrate!("../rome-via-enrich/migrations");
    enrich_migrator.set_ignore_missing(true);
    enrich_migrator.run(pool).await.expect("rome-via-enrich migrations");
}

async fn cleanup(pool: &sqlx::PgPool) {
    sqlx::query("DELETE FROM rome_via.evm_tx WHERE chain_id=$1")
        .bind(THROWAWAY_CHAIN_ID)
        .execute(pool)
        .await
        .expect("cleanup evm_tx");
    sqlx::query(
        "DELETE FROM rome_via.method_signatures WHERE selector = ANY($1)",
    )
    .bind(&[
        "0xaaaa0001", // (a) resolved
        "0xaaaa0002", // (b) fresh raw placeholder
        "0xaaaa0003", // (c) stale raw placeholder
        "0xaaaa0004", // (d) no row
    ] as &[&str])
    .execute(pool)
    .await
    .expect("cleanup method_signatures");
}

async fn seed_evm_tx(pool: &sqlx::PgPool, selector: &str, tx_hash: &str) {
    sqlx::query(
        "INSERT INTO rome_via.evm_tx (chain_id, tx_hash, method_id)
         VALUES ($1, $2, $3)
         ON CONFLICT (chain_id, tx_hash) DO UPDATE SET method_id = EXCLUDED.method_id",
    )
    .bind(THROWAWAY_CHAIN_ID)
    .bind(tx_hash)
    .bind(selector)
    .execute(pool)
    .await
    .expect("seed evm_tx");
}

#[tokio::test]
async fn stale_raw_placeholders_are_rechecked_fresh_ones_and_resolved_rows_are_not() {
    let Some(url) = test_db_url() else {
        eprintln!("SKIP: HERCULES_TEST_DATABASE_URL unset");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .expect("connect");
    setup_schema(&pool).await;
    cleanup(&pool).await;

    // (a) resolved signature — must NOT be returned regardless of age.
    sqlx::query(
        "INSERT INTO rome_via.method_signatures (selector, signature, source, added_at)
         VALUES ($1, $2, '4byte', NOW() - INTERVAL '365 days')",
    )
    .bind("0xaaaa0001")
    .bind("transfer(address,uint256)")
    .execute(&pool)
    .await
    .expect("seed resolved");

    // (b) fresh raw placeholder (added_at = now) — must NOT be returned.
    sqlx::query(
        "INSERT INTO rome_via.method_signatures (selector, signature, source, added_at)
         VALUES ($1, $1, '4byte', NOW())",
    )
    .bind("0xaaaa0002")
    .execute(&pool)
    .await
    .expect("seed fresh placeholder");

    // (c) stale raw placeholder (added_at = 31 days ago, past the 30-day TTL)
    // — MUST be returned so the decoder revisits it.
    sqlx::query(
        "INSERT INTO rome_via.method_signatures (selector, signature, source, added_at)
         VALUES ($1, $1, '4byte', NOW() - INTERVAL '31 days')",
    )
    .bind("0xaaaa0003")
    .execute(&pool)
    .await
    .expect("seed stale placeholder");

    // (d) no row at all — MUST be returned (genuinely never seen).
    // (nothing to insert into method_signatures)

    for (selector, tx_hash) in [
        ("0xaaaa0001", "0xrecheck000000000000000000000000000000000000000000000000000001"),
        ("0xaaaa0002", "0xrecheck000000000000000000000000000000000000000000000000000002"),
        ("0xaaaa0003", "0xrecheck000000000000000000000000000000000000000000000000000003"),
        ("0xaaaa0004", "0xrecheck000000000000000000000000000000000000000000000000000004"),
    ] {
        seed_evm_tx(&pool, selector, tx_hash).await;
    }

    let sql = unknown_selectors_sql();
    let rows: Vec<(String,)> = sqlx::query_as(&sql)
        .bind(THROWAWAY_CHAIN_ID)
        .bind(100i64)
        .fetch_all(&pool)
        .await
        .expect("unknown_selectors_sql query");

    let returned: std::collections::HashSet<String> = rows.into_iter().map(|(s,)| s).collect();

    assert!(
        !returned.contains("0xaaaa0001"),
        "resolved row must never be re-included: {returned:?}"
    );
    assert!(
        !returned.contains("0xaaaa0002"),
        "fresh raw placeholder must not be re-included: {returned:?}"
    );
    assert!(
        returned.contains("0xaaaa0003"),
        "stale raw placeholder must be re-included so it self-heals: {returned:?}"
    );
    assert!(
        returned.contains("0xaaaa0004"),
        "selector with no row must be included (genuine unknown): {returned:?}"
    );

    cleanup(&pool).await;
}

/// Curated rows (`source = 'seed'` AND `source = 'manual'`) must survive BOTH
/// runtime 4byte upserts; genuine `4byte` rows must still update.
///
/// `method_signatures` is shared across every per-chain enrich process (no
/// `chain_id` column), so a sibling process on an OLDER image can be mid-batch
/// — it SELECTed unknowns before its own startup `seed_static` re-ran — and try
/// to overwrite a freshly-curated selector. Without the `source = '4byte'`
/// allow-list guard on the DO UPDATE, that demotes a curated selector to
/// `4byte` (raw placeholder on the Ok(None) path, collision-spam on Ok(Some)),
/// and it stays demoted until the next PROCESS restart re-runs seed_static —
/// the supervisor only restarts `run()`. Silent, days-long. `manual` rows are
/// curated too (migration 0207), hence allow-list not `<> 'seed'`.
///
/// This executes the SAME upsert consts `run()` uses (not a re-typed copy), so
/// a regression in the deployed SQL is caught here. Every non-vacuous branch is
/// covered: seed+manual protected on BOTH paths, and BOTH paths still mutate a
/// real 4byte row (else an over-broad guard would silently freeze resolution).
#[tokio::test]
async fn curated_rows_survive_4byte_upserts_but_4byte_rows_still_update() {
    let Some(url) = test_db_url() else {
        eprintln!("SKIP: HERCULES_TEST_DATABASE_URL unset");
        return;
    };
    let pool = PgPoolOptions::new().connect(&url).await.expect("connect");
    setup_schema(&pool).await;

    let seeded = "0xcccc0001"; // curated seed row — must be protected
    let manual = "0xcccc0002"; // operator-applied manual row — must be protected
    let ph_resolved = "0xcccc0003"; // 4byte placeholder — resolved upsert must update it
    let ph_refresh = "0xcccc0004"; // 4byte placeholder — placeholder upsert must refresh it
    async fn clean(p: &sqlx::PgPool) {
        sqlx::query("DELETE FROM rome_via.method_signatures WHERE selector = ANY($1)")
            .bind(&["0xcccc0001", "0xcccc0002", "0xcccc0003", "0xcccc0004"] as &[&str])
            .execute(p)
            .await
            .expect("cleanup");
    }
    clean(&pool).await;

    // Two curated rows (seed + manual) + two genuine 4byte placeholders, both
    // stale (added_at 31 days ago) so a refresh is observable.
    sqlx::query(
        "INSERT INTO rome_via.method_signatures (selector, signature, source, added_at) VALUES
            ($1, 'romeInvoke(bytes32,uint256)', 'seed',   NOW()),
            ($2, 'operatorNamed(uint256)',      'manual', NOW()),
            ($3, $3,                            '4byte',  NOW() - INTERVAL '31 days'),
            ($4, $4,                            '4byte',  NOW() - INTERVAL '31 days')",
    )
    .bind(seeded)
    .bind(manual)
    .bind(ph_resolved)
    .bind(ph_refresh)
    .execute(&pool)
    .await
    .expect("seed fixtures");

    // A sibling old-image worker tries to demote each curated row via BOTH
    // paths: Ok(None) genuine-miss placeholder (the likelier demote for Rome
    // selectors 4byte doesn't know) and Ok(Some) collision-spam.
    for curated in [seeded, manual] {
        sqlx::query(UPSERT_RAW_PLACEHOLDER_SQL)
            .bind(curated)
            .bind(curated)
            .execute(&pool)
            .await
            .expect("placeholder upsert on curated");
        sqlx::query(UPSERT_RESOLVED_SQL)
            .bind(curated)
            .bind("join_tg_invmru_haha_617eab6(address,uint256,bool)")
            .execute(&pool)
            .await
            .expect("resolved upsert on curated");
    }
    // The resolved upsert against a real 4byte row MUST take effect — the guard
    // must not freeze legitimate resolution.
    sqlx::query(UPSERT_RESOLVED_SQL)
        .bind(ph_resolved)
        .bind("transfer(address,uint256)")
        .execute(&pool)
        .await
        .expect("resolved upsert on 4byte row");
    // The placeholder upsert against a real 4byte row MUST refresh added_at —
    // this is what lets a stale placeholder re-settle instead of being
    // re-queried every poll. Guards the Ok(None) path against an over-broad
    // guard that would freeze it (the mutation the seed-only assertions miss).
    sqlx::query(UPSERT_RAW_PLACEHOLDER_SQL)
        .bind(ph_refresh)
        .bind(ph_refresh)
        .execute(&pool)
        .await
        .expect("placeholder upsert on 4byte row");

    // Curated rows: source + signature untouched.
    for (sel, want_src, want_sig) in [
        (seeded, "seed", "romeInvoke(bytes32,uint256)"),
        (manual, "manual", "operatorNamed(uint256)"),
    ] {
        let (sig, src): (String, String) = sqlx::query_as(
            "SELECT signature, source FROM rome_via.method_signatures WHERE selector = $1",
        )
        .bind(sel)
        .fetch_one(&pool)
        .await
        .expect("read curated row");
        assert_eq!(src, want_src, "{sel} ({want_src}) must not be demoted: got source={src}");
        assert_eq!(sig, want_sig, "{sel} signature must be untouched: got {sig}");
    }

    // 4byte row resolved by Ok(Some): signature updated, still 4byte.
    let (rsig, rsrc): (String, String) = sqlx::query_as(
        "SELECT signature, source FROM rome_via.method_signatures WHERE selector = $1",
    )
    .bind(ph_resolved)
    .fetch_one(&pool)
    .await
    .expect("read resolved placeholder");
    assert_eq!(rsrc, "4byte");
    assert_eq!(
        rsig, "transfer(address,uint256)",
        "4byte row must still be updated by the resolved upsert: got {rsig}"
    );

    // 4byte row hit by Ok(None): added_at refreshed to ~now (was 31 days old).
    let fresh: bool = sqlx::query_scalar(
        "SELECT added_at > NOW() - INTERVAL '1 day' FROM rome_via.method_signatures WHERE selector = $1",
    )
    .bind(ph_refresh)
    .fetch_one(&pool)
    .await
    .expect("read refreshed placeholder");
    assert!(fresh, "placeholder upsert must refresh added_at on a real 4byte row");

    clean(&pool).await;
}

/// A 'seed' row must never be re-included, even if it happens to be old and
/// even in the (should-never-happen) case its signature equals its own
/// selector — seed rows are curated, not 4byte fallback output.
#[tokio::test]
async fn seed_rows_are_never_rechecked() {
    let Some(url) = test_db_url() else {
        eprintln!("SKIP: HERCULES_TEST_DATABASE_URL unset");
        return;
    };
    let pool = PgPoolOptions::new().connect(&url).await.expect("connect");
    setup_schema(&pool).await;

    sqlx::query(
        "DELETE FROM rome_via.evm_tx WHERE chain_id=$1 AND tx_hash=$2",
    )
    .bind(THROWAWAY_CHAIN_ID)
    .bind("0xrecheck000000000000000000000000000000000000000000000000000005")
    .execute(&pool)
    .await
    .ok();
    sqlx::query("DELETE FROM rome_via.method_signatures WHERE selector = $1")
        .bind("0xbbbb0001")
        .execute(&pool)
        .await
        .ok();

    sqlx::query(
        "INSERT INTO rome_via.method_signatures (selector, signature, source, added_at)
         VALUES ($1, $1, 'seed', NOW() - INTERVAL '365 days')",
    )
    .bind("0xbbbb0001")
    .execute(&pool)
    .await
    .expect("seed old seed-sourced row");

    seed_evm_tx(&pool, "0xbbbb0001", "0xrecheck000000000000000000000000000000000000000000000000000005").await;

    let sql = unknown_selectors_sql();
    let rows: Vec<(String,)> = sqlx::query_as(&sql)
        .bind(THROWAWAY_CHAIN_ID)
        .bind(100i64)
        .fetch_all(&pool)
        .await
        .expect("unknown_selectors_sql query");

    assert!(
        rows.iter().all(|(s,)| s != "0xbbbb0001"),
        "a 'seed' row must never be re-included regardless of age: {rows:?}"
    );

    sqlx::query(
        "DELETE FROM rome_via.evm_tx WHERE chain_id=$1 AND tx_hash=$2",
    )
    .bind(THROWAWAY_CHAIN_ID)
    .bind("0xrecheck000000000000000000000000000000000000000000000000000005")
    .execute(&pool)
    .await
    .ok();
    sqlx::query("DELETE FROM rome_via.method_signatures WHERE selector = $1")
        .bind("0xbbbb0001")
        .execute(&pool)
        .await
        .ok();
}
