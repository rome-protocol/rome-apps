// Integration test: the `kind=` filter on GET /tokens and GET /addresses must
// return EXACTLY the subset the explorer's header stat tiles count, so a tile
// can link straight to its filtered list without the numbers disagreeing.
//
// Hard correctness bar: for each kind value, `list.items.len()` must equal the
// corresponding count from `stats.rs::overview_compute` (exercised here via the
// real `overview` handler — not a re-typed copy of its SQL, so the two can't
// drift silently).
//
// Requires HERCULES_TEST_DATABASE_URL pointing at a throwaway/dev Postgres —
// same gate `throughput_db.rs` / `sol_legs_order_db.rs` use. Runs the sync +
// enrich migrations (idempotent — no-ops if already applied), seeds fixtures
// under a throwaway chain_id, and always cleans up. Skips cleanly when unset.

use axum::extract::{Query, State};
use rome_via_api::api::addresses::{list_addresses, PaginationQuery as AddrQuery};
use rome_via_api::api::stats::overview_compute;
use rome_via_api::api::tokens::{list_tokens, PaginationQuery as TokenQuery};
use rome_via_api::state::AppState;
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use std::sync::Arc;

const THROWAWAY_CHAIN_ID: i64 = 990012;

fn test_db_url() -> Option<String> {
    std::env::var("HERCULES_TEST_DATABASE_URL").ok()
}

async fn setup_schema(pool: &sqlx::PgPool) {
    // Both migrators share one `_sqlx_migrations` table (sync owns versions
    // 1-20, enrich owns 100+) — `ignore_missing` is required so each migrator
    // doesn't choke on the other's versions it can't see in its own folder.
    // Mirrors rome-via-sync/src/main.rs and rome-via-enrich/src/main.rs.
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
        // redis=None ⇒ `stats::overview`'s cache layer is a transparent
        // pass-through (see cache.rs docs) — this exercises the REAL
        // `overview_compute` query, not a mock.
        None,
        Arc::new(HashMap::new()),
        Arc::new(HashMap::new()),
    )
}

async fn cleanup(pool: &sqlx::PgPool) {
    for sql in [
        "DELETE FROM rome_via.token_metadata WHERE chain_id=$1",
        "DELETE FROM rome_via.evm_tx WHERE chain_id=$1",
        "DELETE FROM rome_via.address_stats WHERE chain_id=$1",
    ] {
        sqlx::query(sql)
            .bind(THROWAWAY_CHAIN_ID)
            .execute(pool)
            .await
            .expect("cleanup");
    }
}

/// GET /tokens?kind=X count must equal stats.rs's token_erc20/token_spl/token_token2022.
#[tokio::test]
async fn tokens_kind_filter_matches_stats_tile_counts() {
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

    // 2 SPL, 1 ERC-20, 1 Token-2022, 1 unclassified (NULL kind — must count
    // toward none of the three, per stats.rs's FILTER semantics).
    for (addr, kind) in [
        ("0x0000000000000000000000000000000000a001", Some("SPL")),
        ("0x0000000000000000000000000000000000a002", Some("SPL")),
        ("0x0000000000000000000000000000000000a003", Some("ERC-20")),
        ("0x0000000000000000000000000000000000a004", Some("Token-2022")),
        ("0x0000000000000000000000000000000000a005", None::<&str>),
    ] {
        sqlx::query(
            "INSERT INTO rome_via.token_metadata (chain_id, address, kind) VALUES ($1,$2,$3)
             ON CONFLICT (chain_id, address) DO UPDATE SET kind = EXCLUDED.kind",
        )
        .bind(THROWAWAY_CHAIN_ID)
        .bind(addr)
        .bind(kind)
        .execute(&pool)
        .await
        .expect("seed token_metadata");
    }

    let state = app_state(pool.clone());

    // The tile: the real production aggregate query, called directly (not a
    // re-typed copy of its SQL) so the two can't silently drift apart.
    let stats = overview_compute(&pool, THROWAWAY_CHAIN_ID)
        .await
        .expect("overview_compute");
    let tile = &stats["tokenKindCounts"];

    for (kind, tile_key, expected_count) in
        [("SPL", "spl", 2i64), ("ERC-20", "erc20", 1), ("Token-2022", "token2022", 1)]
    {
        let tile_count = tile[tile_key].as_i64().expect("tile field present");
        assert_eq!(tile_count, expected_count, "sanity: seed vs tile for {kind}");

        let page = list_tokens(
            State(state.clone()),
            Query(TokenQuery {
                cursor: None,
                limit: Some(100),
                factory: None,
                kind: Some(kind.to_string()),
            }),
        )
        .await
        .unwrap_or_else(|e| panic!("list_tokens kind={kind} failed: {e:?}"));

        assert_eq!(
            page.items.len() as i64,
            tile_count,
            "kind={kind}: filtered /tokens count must equal the stats tile count"
        );
        assert!(
            page.items.iter().all(|t| t.kind.as_deref() == Some(kind)),
            "kind={kind}: every returned row must actually have that kind"
        );
    }

    cleanup(&pool).await;
}

/// An invalid `kind=` on /tokens must 400, not silently return everything.
#[tokio::test]
async fn tokens_invalid_kind_is_bad_request() {
    let Some(url) = test_db_url() else {
        eprintln!("SKIP: HERCULES_TEST_DATABASE_URL unset");
        return;
    };
    let pool = PgPoolOptions::new().connect(&url).await.expect("connect");
    setup_schema(&pool).await;
    let state = app_state(pool);

    let err = list_tokens(
        State(state),
        Query(TokenQuery {
            cursor: None,
            limit: None,
            factory: None,
            kind: Some("NFT".to_string()),
        }),
    )
    .await
    .expect_err("bad kind must be rejected");
    assert!(matches!(err, rome_via_api::error::AppError::BadRequest(_)));
}

/// GET /addresses?kind=X count must equal stats.rs's contracts/synthetics, and
/// kind=eoa must equal the derived residual (active − contracts − synthetics).
#[tokio::test]
async fn addresses_kind_filter_matches_stats_tile_counts() {
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

    let contract_addr = "0x0000000000000000000000000000000000b001";
    let eoa_addr = "0x0000000000000000000000000000000000b002";
    let synthetic_addr = "0x0000000000000000000000000000000000b003";

    for (addr, is_contract) in [
        (contract_addr, true),
        (eoa_addr, false),
        (synthetic_addr, false),
    ] {
        sqlx::query(
            "INSERT INTO rome_via.address_stats (chain_id, address, tx_count, is_contract)
             VALUES ($1, $2, 1, $3)
             ON CONFLICT (chain_id, address) DO UPDATE SET is_contract = EXCLUDED.is_contract",
        )
        .bind(THROWAWAY_CHAIN_ID)
        .bind(addr)
        .bind(is_contract)
        .execute(&pool)
        .await
        .expect("seed address_stats");
    }

    // Mark `synthetic_addr` as a Solana-controlled sender the same way a real
    // DoTxUnsigned tx does: non-ecdsa origination + a solana_signer, `from_addr`
    // = the synthetic address. This is the exact predicate stats.rs's
    // `synthetics` DISTINCT-count and the `/addresses` LATERAL probe both key on.
    sqlx::query(
        "INSERT INTO rome_via.evm_tx (chain_id, tx_hash, from_addr, origination, solana_signer)
         VALUES ($1, $2, $3, 'solana_unsigned', 'So1anaPubKey1111111111111111111111111111')
         ON CONFLICT (chain_id, tx_hash) DO NOTHING",
    )
    .bind(THROWAWAY_CHAIN_ID)
    .bind("0xfeed000000000000000000000000000000000000000000000000000000000001")
    .bind(synthetic_addr)
    .execute(&pool)
    .await
    .expect("seed synthetic evm_tx");

    let state = app_state(pool.clone());
    let stats = overview_compute(&pool, THROWAWAY_CHAIN_ID)
        .await
        .expect("overview_compute");
    let tile = &stats["addressTypeCounts"];
    let contracts = tile["contracts"].as_i64().expect("contracts tile");
    let synthetics = tile["synthetics"].as_i64().expect("synthetics tile");
    let eoas = tile["eoas"].as_i64().expect("eoas tile");

    assert_eq!(contracts, 1, "sanity: exactly one seeded contract");
    assert_eq!(synthetics, 1, "sanity: exactly one seeded synthetic");
    assert_eq!(eoas, 1, "sanity: exactly one seeded plain EOA");

    for (kind, expected) in [("contract", contracts), ("eoa", eoas), ("synthetic", synthetics)] {
        let page = list_addresses(
            State(state.clone()),
            Query(AddrQuery {
                cursor: None,
                limit: Some(100),
                kind: Some(kind.to_string()),
                q: None,
            }),
        )
        .await
        .unwrap_or_else(|e| panic!("list_addresses kind={kind} failed: {e:?}"));

        assert_eq!(
            page.items.len() as i64,
            expected,
            "kind={kind}: filtered /addresses count must equal the stats tile count"
        );
    }

    // Cross-check the actual identities, not just counts.
    let contract_page = list_addresses(
        State(state.clone()),
        Query(AddrQuery { cursor: None, limit: Some(100), kind: Some("contract".to_string()), q: None }),
    )
    .await
    .expect("list contracts");
    assert_eq!(contract_page.items[0].address, contract_addr);
    assert!(contract_page.items[0].is_contract);

    let synthetic_page = list_addresses(
        State(state.clone()),
        Query(AddrQuery { cursor: None, limit: Some(100), kind: Some("synthetic".to_string()), q: None }),
    )
    .await
    .expect("list synthetics");
    assert_eq!(synthetic_page.items[0].address, synthetic_addr);
    assert!(synthetic_page.items[0].controlled_by_solana);

    let eoa_page = list_addresses(
        State(state.clone()),
        Query(AddrQuery { cursor: None, limit: Some(100), kind: Some("eoa".to_string()), q: None }),
    )
    .await
    .expect("list eoas");
    assert_eq!(eoa_page.items[0].address, eoa_addr);
    assert!(!eoa_page.items[0].is_contract);
    assert!(!eoa_page.items[0].controlled_by_solana);

    cleanup(&pool).await;
}

/// An invalid `kind=` on /addresses must 400, not silently return everything.
#[tokio::test]
async fn addresses_invalid_kind_is_bad_request() {
    let Some(url) = test_db_url() else {
        eprintln!("SKIP: HERCULES_TEST_DATABASE_URL unset");
        return;
    };
    let pool = PgPoolOptions::new().connect(&url).await.expect("connect");
    setup_schema(&pool).await;
    let state = app_state(pool);

    let err = list_addresses(
        State(state),
        Query(AddrQuery { cursor: None, limit: None, kind: Some("Contract".to_string()), q: None }),
    )
    .await
    .expect_err("bad kind must be rejected");
    assert!(matches!(err, rome_via_api::error::AppError::BadRequest(_)));
}
