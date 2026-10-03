// Integration test: GET /api/v1/addresses must accept an optional `q=`
// search term that filters the list to rows where the address matches as a
// PREFIX (case-insensitive) OR the joined contract label matches as a
// SUBSTRING (case-insensitive) — so a user can find a contract by name
// ("ARC") or by address prefix ("0x0000"). Absent/empty `q` is unfiltered.
// Must compose with the existing `kind` filter.
//
// Requires HERCULES_TEST_DATABASE_URL pointing at a throwaway/dev Postgres —
// same gate addresses_label_db.rs / kind_filter_db.rs use. Skips cleanly
// when unset.

use axum::extract::{Query, State};
use rome_via_api::api::addresses::{list_addresses, PaginationQuery as AddrQuery};
use rome_via_api::state::AppState;
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use std::sync::Arc;

// Distinct from the other *_db tests' throwaway chain ids (990012, 990013).
const THROWAWAY_CHAIN_ID: i64 = 990014;

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

fn app_state(pool: sqlx::PgPool) -> AppState {
    AppState::new(
        pool,
        THROWAWAY_CHAIN_ID,
        vec![0u8; 32],
        "http://127.0.0.1:1".to_string(),
        None,
        Arc::new(HashMap::new()),
        Arc::new(HashMap::new()),
    )
}

async fn cleanup(pool: &sqlx::PgPool) {
    for sql in [
        "DELETE FROM rome_via.contract_labels WHERE chain_id=$1",
        "DELETE FROM rome_via.address_stats WHERE chain_id=$1",
    ] {
        sqlx::query(sql)
            .bind(THROWAWAY_CHAIN_ID)
            .execute(pool)
            .await
            .expect("cleanup");
    }
}

const ADDR_AAAA: &str = "0x000000000000000000000000000000000000aaaa";
const ADDR_BBBB: &str = "0x000000000000000000000000000000000000bbbb";
const ADDR_CCCC: &str = "0x000000000000000000000000000000000000cccc";

async fn seed(pool: &sqlx::PgPool) {
    // (a) labeled contract "ArcToken Factory" @ ...aaaa
    // (b) plain address, no label @ ...bbbb
    // (c) labeled contract "Compound" @ ...cccc
    for (addr, is_contract) in [
        (ADDR_AAAA, true),
        (ADDR_BBBB, false),
        (ADDR_CCCC, true),
    ] {
        sqlx::query(
            "INSERT INTO rome_via.address_stats (chain_id, address, tx_count, is_contract)
             VALUES ($1, $2, 1, $3)
             ON CONFLICT (chain_id, address) DO UPDATE SET is_contract = EXCLUDED.is_contract",
        )
        .bind(THROWAWAY_CHAIN_ID)
        .bind(addr)
        .bind(is_contract)
        .execute(pool)
        .await
        .expect("seed address_stats");
    }

    for (addr, label) in [(ADDR_AAAA, "ArcToken Factory"), (ADDR_CCCC, "Compound")] {
        sqlx::query(
            "INSERT INTO rome_via.contract_labels (chain_id, address, display_label)
             VALUES ($1, $2, $3)
             ON CONFLICT (chain_id, address) DO UPDATE SET display_label = EXCLUDED.display_label",
        )
        .bind(THROWAWAY_CHAIN_ID)
        .bind(addr)
        .bind(label)
        .execute(pool)
        .await
        .expect("seed contract_labels");
    }
}

fn q(kind: Option<&str>, term: Option<&str>) -> AddrQuery {
    AddrQuery {
        cursor: None,
        limit: Some(100),
        kind: kind.map(|s| s.to_string()),
        q: term.map(|s| s.to_string()),
    }
}

// Both tests share THROWAWAY_CHAIN_ID and each does a table-wide cleanup by
// chain_id, so they must not run concurrently (else one wipes the other's
// fixtures mid-flight). #[serial] serializes them.
#[tokio::test]
#[serial_test::serial]
async fn addresses_search_filters_by_label_substring_and_address_prefix() {
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
    seed(&pool).await;
    let state = app_state(pool.clone());

    // q="arc" (case-insensitive) must match ONLY the ArcToken row, via its
    // label — not the plain or Compound rows.
    let page = list_addresses(State(state.clone()), Query(q(None, Some("arc"))))
        .await
        .expect("list_addresses q=arc");
    assert_eq!(
        page.items.len(),
        1,
        "q=arc must return exactly the ArcToken row, got {:?}",
        page.items.iter().map(|a| &a.address).collect::<Vec<_>>()
    );
    assert_eq!(page.items[0].address, ADDR_AAAA);

    // q=<address prefix> must match by address prefix (case-insensitive).
    let page = list_addresses(State(state.clone()), Query(q(None, Some("0x000000000000000000000000000000000000aa"))))
        .await
        .expect("list_addresses q=prefix");
    assert_eq!(
        page.items.len(),
        1,
        "q=<prefix of aaaa> must return exactly the aaaa row"
    );
    assert_eq!(page.items[0].address, ADDR_AAAA);

    // Address arm must be case-INSENSITIVE (ILIKE, not LIKE): an uppercase
    // prefix still matches the lowercase-stored address. Guards an ILIKE→LIKE
    // regression on the address arm (the label arm's case-insensitivity is
    // covered by "arc" matching "ArcToken").
    let page = list_addresses(State(state.clone()), Query(q(None, Some("0x000000000000000000000000000000000000AA"))))
        .await
        .expect("list_addresses q=UPPER prefix");
    assert_eq!(page.items.len(), 1, "uppercase address prefix must match lowercase-stored address");
    assert_eq!(page.items[0].address, ADDR_AAAA);

    // q=None -> unfiltered, all three rows present.
    let page = list_addresses(State(state.clone()), Query(q(None, None)))
        .await
        .expect("list_addresses q=None");
    assert_eq!(page.items.len(), 3, "absent q must be unfiltered");

    // q="" -> unfiltered too (empty string treated as absent).
    let page = list_addresses(State(state.clone()), Query(q(None, Some(""))))
        .await
        .expect("list_addresses q=empty");
    assert_eq!(page.items.len(), 3, "empty q must be unfiltered");

    // Compose: q="compound" + kind=contract must return the Compound row.
    let page = list_addresses(State(state.clone()), Query(q(Some("contract"), Some("compound"))))
        .await
        .expect("list_addresses q=compound kind=contract");
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].address, ADDR_CCCC);

    // Compose: q="compound" + kind=eoa (Compound is a contract, not an eoa)
    // must return nothing — the two predicates AND together.
    let page = list_addresses(State(state.clone()), Query(q(Some("eoa"), Some("compound"))))
        .await
        .expect("list_addresses q=compound kind=eoa");
    assert_eq!(
        page.items.len(),
        0,
        "q=compound + non-matching kind must AND to zero results"
    );

    cleanup(&pool).await;
}

/// A user typing literal `%` or `_` (SQL LIKE metacharacters) into the search
/// box must NOT get pattern-matching behavior — those characters must be
/// escaped and treated as literal text, so "%" doesn't match every row.
#[tokio::test]
#[serial_test::serial]
async fn addresses_search_escapes_like_wildcards() {
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
    seed(&pool).await;
    let state = app_state(pool.clone());

    // A bare "%" must not match everything via unescaped LIKE.
    let page = list_addresses(State(state.clone()), Query(q(None, Some("%"))))
        .await
        .expect("list_addresses q=%");
    assert_eq!(
        page.items.len(),
        0,
        "a literal '%' must not act as a wildcard matching every row"
    );

    cleanup(&pool).await;
}
