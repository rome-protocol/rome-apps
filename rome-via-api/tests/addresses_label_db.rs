// Integration test: GET /api/v1/addresses must surface each listed address's
// clean protocol label (`rome_via.contract_labels`), mirroring the join shape
// `get_address` already uses for the detail endpoint
// (`cl.chain_id = $1 AND cl.address = <lowercased address>`), so the
// explorer's /addresses list can show contract names instead of bare hex.
//
// Requires HERCULES_TEST_DATABASE_URL pointing at a throwaway/dev Postgres —
// same gate kind_filter_db.rs / throughput_db.rs use. Skips cleanly when unset.

use axum::extract::{Query, State};
use rome_via_api::api::addresses::{list_addresses, PaginationQuery as AddrQuery};
use rome_via_api::state::AppState;
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use std::sync::Arc;

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

/// GET /addresses must return `label`/`labelDetail` for a row that has a
/// `contract_labels` entry, and `None` for a row that doesn't — the negative
/// case proves the join is a LEFT JOIN keyed correctly, not a stray default.
#[tokio::test]
async fn addresses_list_surfaces_contract_label() {
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

    let labeled_addr = "0x000000000000000000000000000000000000c001";
    let unlabeled_addr = "0x000000000000000000000000000000000000c002";

    for addr in [labeled_addr, unlabeled_addr] {
        sqlx::query(
            "INSERT INTO rome_via.address_stats (chain_id, address, tx_count, is_contract)
             VALUES ($1, $2, 1, true)
             ON CONFLICT (chain_id, address) DO UPDATE SET is_contract = EXCLUDED.is_contract",
        )
        .bind(THROWAWAY_CHAIN_ID)
        .bind(addr)
        .execute(&pool)
        .await
        .expect("seed address_stats");
    }

    sqlx::query(
        "INSERT INTO rome_via.contract_labels (chain_id, address, display_label, display_label_detail)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (chain_id, address) DO UPDATE SET
             display_label = EXCLUDED.display_label,
             display_label_detail = EXCLUDED.display_label_detail",
    )
    .bind(THROWAWAY_CHAIN_ID)
    .bind(labeled_addr)
    .bind("TestProtocol")
    .bind("detail-x")
    .execute(&pool)
    .await
    .expect("seed contract_labels");

    let state = app_state(pool.clone());
    let page = list_addresses(
        State(state),
        Query(AddrQuery { cursor: None, limit: Some(100), kind: None, q: None }),
    )
    .await
    .expect("list_addresses");

    let labeled_row = page
        .items
        .iter()
        .find(|a| a.address == labeled_addr)
        .expect("labeled address present in list");
    assert_eq!(
        labeled_row.label.as_deref(),
        Some("TestProtocol"),
        "address with a contract_labels row must surface its display_label"
    );
    assert_eq!(
        labeled_row.label_detail.as_deref(),
        Some("detail-x"),
        "address with a contract_labels row must surface its display_label_detail"
    );

    let unlabeled_row = page
        .items
        .iter()
        .find(|a| a.address == unlabeled_addr)
        .expect("unlabeled address present in list");
    assert_eq!(
        unlabeled_row.label, None,
        "address with NO contract_labels row must have label=None, not a stray join artifact"
    );
    assert_eq!(unlabeled_row.label_detail, None);

    cleanup(&pool).await;
}
