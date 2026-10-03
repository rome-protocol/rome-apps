// Integration tests for the verified-labels design: the rank-guarded
// `contract_labels` upsert (the core correctness property — a higher-tier
// label must survive a lower-tier writer's later pass, in both directions)
// and the `verified_labels` worker's candidate-selection query.
//
// Requires HERCULES_TEST_DATABASE_URL pointing at a throwaway/dev Postgres —
// same gate as `method_decoder_recheck_db.rs`. Runs the sync + enrich
// migrations (idempotent), seeds fixtures under a per-test throwaway
// chain_id (tests run concurrently by default under `cargo test`, and
// `cleanup` is a whole-chain delete, so distinct chain_ids are load-bearing
// isolation — not just cosmetic), and always cleans up. Skips cleanly when
// unset.

use rome_via_enrich::workers::contract_labels::reassert_registry_labels;
use rome_via_enrich::workers::label_provenance::GUARD_WHERE_CLAUSE;
use rome_via_enrich::workers::verified_labels::verified_candidates_sql;
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;

fn test_db_url() -> Option<String> {
    std::env::var("HERCULES_TEST_DATABASE_URL").ok()
}

async fn setup_schema(pool: &sqlx::PgPool) {
    let mut sync_migrator = sqlx::migrate!("../rome-via-sync/migrations");
    sync_migrator.set_ignore_missing(true);
    sync_migrator.run(pool).await.expect("rome-via-sync migrations");

    let mut enrich_migrator = sqlx::migrate!("../rome-via-enrich/migrations");
    enrich_migrator.set_ignore_missing(true);
    enrich_migrator.run(pool).await.expect("rome-via-enrich migrations");
}

async fn cleanup(pool: &sqlx::PgPool, chain_id: i64) {
    sqlx::query("DELETE FROM rome_via.contract_labels WHERE chain_id = $1")
        .bind(chain_id)
        .execute(pool)
        .await
        .expect("cleanup contract_labels");
}

/// Insert (or overwrite) a bare contract_labels row for a fixture, bypassing
/// the guard (direct INSERT), so tests can set up an arbitrary starting
/// state.
async fn seed_row(
    pool: &sqlx::PgPool,
    chain_id: i64,
    address: &str,
    label: &str,
    provenance: &str,
    has_code: bool,
    verified_checked_at: Option<&str>, // e.g. Some("NOW() - INTERVAL '10 days'") or None
) {
    let checked_expr = verified_checked_at.unwrap_or("NULL");
    let sql = format!(
        "INSERT INTO rome_via.contract_labels
            (chain_id, address, display_label, provenance, has_code, verified_checked_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, {checked_expr}, NOW())
         ON CONFLICT (chain_id, address) DO UPDATE
           SET display_label = EXCLUDED.display_label,
               provenance    = EXCLUDED.provenance,
               has_code      = EXCLUDED.has_code,
               verified_checked_at = {checked_expr},
               updated_at    = NOW()"
    );
    sqlx::query(&sql)
        .bind(chain_id)
        .bind(address)
        .bind(label)
        .bind(provenance)
        .bind(has_code)
        .execute(pool)
        .await
        .expect("seed_row");
}

/// The exact shape `contract_labels.rs::upsert_label` / `verified_labels.rs::
/// upsert_verified` use: an `ON CONFLICT DO UPDATE ... WHERE
/// {GUARD_WHERE_CLAUSE}` upsert against `(chain_id, address)`, guarded on
/// provenance rank. Exercises the real production constant, not a re-typed
/// copy.
async fn guarded_upsert(pool: &sqlx::PgPool, chain_id: i64, address: &str, label: &str, provenance: &str) {
    let sql = format!(
        "INSERT INTO rome_via.contract_labels
            (chain_id, address, display_label, provenance, has_code, updated_at)
         VALUES ($1, $2, $3, $4, true, NOW())
         ON CONFLICT (chain_id, address) DO UPDATE
           SET display_label = EXCLUDED.display_label,
               provenance    = EXCLUDED.provenance,
               has_code      = EXCLUDED.has_code,
               updated_at    = NOW()
         WHERE {GUARD_WHERE_CLAUSE}"
    );
    sqlx::query(&sql)
        .bind(chain_id)
        .bind(address)
        .bind(label)
        .bind(provenance)
        .execute(pool)
        .await
        .expect("guarded_upsert");
}

async fn read_label(pool: &sqlx::PgPool, chain_id: i64, address: &str) -> (Option<String>, String) {
    sqlx::query_as(
        "SELECT display_label, provenance FROM rome_via.contract_labels
          WHERE chain_id = $1 AND address = $2",
    )
    .bind(chain_id)
    .bind(address)
    .fetch_one(pool)
    .await
    .expect("read_label")
}

/// THE core correctness property. A contract already labeled `onchain` gets
/// promoted by a `verified` write (verified > onchain — wins), a subsequent
/// `onchain` re-resolution against the now-`verified` row is a no-op (must
/// NOT clobber a higher tier), and a `registry` write then wins over
/// `verified` (registry > verified). Proves the guard both directions.
#[tokio::test]
async fn rank_guard_verified_wins_onchain_noop_registry_wins() {
    let Some(url) = test_db_url() else {
        eprintln!("SKIP: HERCULES_TEST_DATABASE_URL unset");
        return;
    };
    let chain_id = 990014;
    let pool = PgPoolOptions::new().connect(&url).await.expect("connect");
    setup_schema(&pool).await;
    cleanup(&pool, chain_id).await;

    let addr = "0xd8af070d901e2988964b77450c14917963cf78d";
    seed_row(&pool, chain_id, addr, "old", "onchain", true, None).await;

    // 1. verified beats onchain.
    guarded_upsert(&pool, chain_id, addr, "ArcTokenFactoryV2", "verified").await;
    let (label, prov) = read_label(&pool, chain_id, addr).await;
    assert_eq!(label, Some("ArcTokenFactoryV2".to_string()));
    assert_eq!(prov, "verified");

    // 2. a later onchain re-resolution must NOT clobber the verified row.
    guarded_upsert(&pool, chain_id, addr, "junk", "onchain").await;
    let (label, prov) = read_label(&pool, chain_id, addr).await;
    assert_eq!(
        label,
        Some("ArcTokenFactoryV2".to_string()),
        "onchain must not clobber a verified label"
    );
    assert_eq!(prov, "verified");

    // 3. registry beats verified.
    guarded_upsert(&pool, chain_id, addr, "RegistryCurated", "registry").await;
    let (label, prov) = read_label(&pool, chain_id, addr).await;
    assert_eq!(label, Some("RegistryCurated".to_string()));
    assert_eq!(prov, "registry");

    // 4. a later onchain OR verified re-resolution must NOT clobber registry.
    guarded_upsert(&pool, chain_id, addr, "junk2", "onchain").await;
    guarded_upsert(&pool, chain_id, addr, "junk3", "verified").await;
    let (label, prov) = read_label(&pool, chain_id, addr).await;
    assert_eq!(label, Some("RegistryCurated".to_string()), "registry is the ceiling");
    assert_eq!(prov, "registry");

    cleanup(&pool, chain_id).await;
}

/// A same-tier re-resolution still refreshes the label (>=, not >) — matches
/// the pre-guard unconditional-upsert behavior for the single-writer case.
#[tokio::test]
async fn rank_guard_same_tier_still_refreshes() {
    let Some(url) = test_db_url() else {
        eprintln!("SKIP: HERCULES_TEST_DATABASE_URL unset");
        return;
    };
    let chain_id = 990015;
    let pool = PgPoolOptions::new().connect(&url).await.expect("connect");
    setup_schema(&pool).await;
    cleanup(&pool, chain_id).await;

    let addr = "0x00000000000000000000000000000000000abc";
    seed_row(&pool, chain_id, addr, "Compound cached 9-asset", "onchain", true, None).await;

    // A fresh on-chain read renaming the same contract must still apply.
    guarded_upsert(&pool, chain_id, addr, "Compound", "onchain").await;
    let (label, prov) = read_label(&pool, chain_id, addr).await;
    assert_eq!(label, Some("Compound".to_string()), "a fresh onchain read for an unchanged tier must still update");
    assert_eq!(prov, "onchain");

    cleanup(&pool, chain_id).await;
}

/// Candidate query: only non-verified/non-registry contracts with code, and
/// only those whose `verified_checked_at` is NULL or past the TTL, are
/// returned.
#[tokio::test]
async fn candidate_query_selects_only_unverified_contracts_past_ttl() {
    let Some(url) = test_db_url() else {
        eprintln!("SKIP: HERCULES_TEST_DATABASE_URL unset");
        return;
    };
    let chain_id = 990016;
    let pool = PgPoolOptions::new().connect(&url).await.expect("connect");
    setup_schema(&pool).await;
    cleanup(&pool, chain_id).await;

    let never_checked = "0x0000000000000000000000000000000000a001"; // onchain, has_code, never checked -> candidate
    let ttl_expired = "0x0000000000000000000000000000000000a002"; // onchain, has_code, checked 10d ago (TTL=1d) -> candidate
    let recently_checked = "0x0000000000000000000000000000000000a003"; // onchain, has_code, checked just now -> NOT a candidate
    let already_verified = "0x0000000000000000000000000000000000a004"; // provenance=verified -> NOT a candidate
    let already_registry = "0x0000000000000000000000000000000000a005"; // provenance=registry -> NOT a candidate
    let eoa = "0x0000000000000000000000000000000000a006"; // has_code=false -> NOT a candidate

    seed_row(&pool, chain_id, never_checked, "n", "onchain", true, None).await;
    seed_row(&pool, chain_id, ttl_expired, "n", "onchain", true, Some("NOW() - INTERVAL '10 days'")).await;
    seed_row(&pool, chain_id, recently_checked, "n", "onchain", true, Some("NOW()")).await;
    seed_row(&pool, chain_id, already_verified, "V", "verified", true, Some("NOW() - INTERVAL '10 days'")).await;
    seed_row(&pool, chain_id, already_registry, "R", "registry", true, Some("NOW() - INTERVAL '10 days'")).await;
    seed_row(&pool, chain_id, eoa, "n", "onchain", false, None).await;

    let ttl_secs: i64 = 24 * 3600; // 1 day
    let rows: Vec<(String,)> = sqlx::query_as(verified_candidates_sql())
        .bind(chain_id)
        .bind(ttl_secs)
        .bind(100i64)
        .fetch_all(&pool)
        .await
        .expect("verified_candidates_sql query");
    let returned: std::collections::HashSet<String> = rows.into_iter().map(|(a,)| a).collect();

    assert!(returned.contains(never_checked), "never-checked contract must be a candidate: {returned:?}");
    assert!(returned.contains(ttl_expired), "TTL-expired contract must be re-polled: {returned:?}");
    assert!(!returned.contains(recently_checked), "recently-checked contract must NOT be re-polled yet: {returned:?}");
    assert!(!returned.contains(already_verified), "already-verified row must never be re-checked: {returned:?}");
    assert!(!returned.contains(already_registry), "registry-curated row must never be checked: {returned:?}");
    assert!(!returned.contains(eoa), "an EOA (no code) must never be a candidate: {returned:?}");

    cleanup(&pool, chain_id).await;
}

/// B1 (review BLOCKER): migration 0235 defaults EVERY pre-existing row —
/// including a registry row this worker's registry tier already wrote —
/// to provenance='onchain'. Simulate that: seed a registry-map address with
/// a stale on-chain label at provenance='onchain' (as if the row predated
/// the provenance column). Run the startup re-assert. The row must be healed
/// to provenance='registry' with the registry map's name, and — because the
/// verified-labels candidate query excludes provenance IN ('verified',
/// 'registry') — the healed row must no longer be a verified-check
/// candidate (else it stays clobberable by a lower-confidence Sourcify
/// write forever).
#[tokio::test]
async fn registry_reassert_heals_pre_migration_rows_and_excludes_from_verified_candidates() {
    let Some(url) = test_db_url() else {
        eprintln!("SKIP: HERCULES_TEST_DATABASE_URL unset");
        return;
    };
    let chain_id = 990018;
    let pool = PgPoolOptions::new().connect(&url).await.expect("connect");
    setup_schema(&pool).await;
    cleanup(&pool, chain_id).await;

    let addr = "0xb342f70d56855f11b0721fcbe2804a200d0f0533"; // registry-map address (e.g. UniswapV2Router02)
    // Simulate the pre-migration-0235 state: a registry row that defaulted to
    // provenance='onchain' when the column was added, with a stale on-chain
    // read still sitting in display_label.
    seed_row(&pool, chain_id, addr, "stale onchain junk", "onchain", true, None).await;

    let mut registry: HashMap<String, String> = HashMap::new();
    registry.insert(addr.to_string(), "UniswapV2Router02".to_string());

    reassert_registry_labels(&pool, chain_id, &registry).await;

    let (label, prov) = read_label(&pool, chain_id, addr).await;
    assert_eq!(
        label,
        Some("UniswapV2Router02".to_string()),
        "startup re-assert must overwrite the stale onchain label with the registry name"
    );
    assert_eq!(prov, "registry", "startup re-assert must heal provenance to 'registry'");

    // The healed row must be excluded from the verified-labels candidate set.
    let ttl_secs: i64 = 24 * 3600;
    let rows: Vec<(String,)> = sqlx::query_as(verified_candidates_sql())
        .bind(chain_id)
        .bind(ttl_secs)
        .bind(100i64)
        .fetch_all(&pool)
        .await
        .expect("verified_candidates_sql query");
    assert!(
        rows.iter().all(|(a,)| a != addr),
        "a healed registry row must never be a verified-check candidate (clobber risk): {rows:?}"
    );

    cleanup(&pool, chain_id).await;
}
