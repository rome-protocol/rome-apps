// Integration test: throughput oracle-counting regression.
//
// Headline guarantee: the total TPS count INCLUDES oracle txs (method_id='0xf8ac93e8');
// the application figure EXCLUDES exactly them.
//
// Requires a live Postgres reachable via HERCULES_TEST_DATABASE_URL with the
// rome_via schema already migrated. When the env var is absent the tests skip
// cleanly (no-op pass). Fixture data is written under chain_id=990010 (throwaway)
// and always cleaned up, so this is safe to run against a shared dev DB.

use sqlx::postgres::PgPoolOptions;

const THROWAWAY_CHAIN_ID: i64 = 990010;
const ORACLE_METHOD_ID: &str = "0xf8ac93e8";

fn test_db_url() -> Option<String> {
    std::env::var("HERCULES_TEST_DATABASE_URL").ok()
}

/// Insert one eth_block row, one eth_block_txs row per method_id, and one
/// evm_tx row per method_id under `chain_id`. Slot and block numbering are
/// caller-supplied so multiple blocks can coexist without PK collision.
async fn seed_block(
    pool: &sqlx::PgPool,
    chain_id: i64,
    block_number: i64,
    slot: i64,
    ts_epoch_secs: i64,
    method_ids: &[&str],
) {
    // Insert the block (slot_block_idx=0; block_gas_used=0; nullable fields omitted).
    sqlx::query(
        "INSERT INTO rome_via.eth_block \
         (chain_id, slot_number, slot_block_idx, block_gas_used, \
          params_number, params_block_timestamp) \
         VALUES ($1, $2, 0, 0, $3, $4) \
         ON CONFLICT DO NOTHING",
    )
    .bind(chain_id)
    .bind(slot)
    .bind(block_number)
    .bind(ts_epoch_secs as f64) // NUMERIC — cast from f64 satisfies sqlx
    .execute(pool)
    .await
    .expect("seed eth_block failed");

    for (i, method_id) in method_ids.iter().enumerate() {
        // Unique tx hash: encode chain_id + slot + index into a 32-byte hex string.
        let tx_hash = format!(
            "0x{chain_id:016x}{slot:016x}{i:016x}{:016x}",
            0u64,
        );

        // eth_block_txs row
        sqlx::query(
            "INSERT INTO rome_via.eth_block_txs \
             (chain_id, slot_number, slot_block_idx, tx_hash, tx_idx) \
             VALUES ($1, $2, 0, $3, $4) \
             ON CONFLICT DO NOTHING",
        )
        .bind(chain_id)
        .bind(slot)
        .bind(&tx_hash)
        .bind(i as i32)
        .execute(pool)
        .await
        .expect("seed eth_block_txs failed");

        // evm_tx row — only NOT NULL columns that lack a server-side default.
        // `origination` defaults to 'ecdsa' (migration 0011); `created_at`
        // defaults to NOW(); `rlp`, `from_address`, and all 0010 columns are nullable.
        sqlx::query(
            "INSERT INTO rome_via.evm_tx \
             (chain_id, tx_hash, method_id) \
             VALUES ($1, $2, $3) \
             ON CONFLICT DO NOTHING",
        )
        .bind(chain_id)
        .bind(&tx_hash)
        .bind(method_id)
        .execute(pool)
        .await
        .expect("seed evm_tx failed");
    }
}

/// Remove all fixture rows for the throwaway chain. Order: eth_block_txs /
/// evm_tx before eth_block (FK-safe; the schema has no FK constraints, but
/// this order is correct by convention).
async fn cleanup(pool: &sqlx::PgPool, chain_id: i64) {
    sqlx::query("DELETE FROM rome_via.eth_block_txs WHERE chain_id=$1")
        .bind(chain_id)
        .execute(pool)
        .await
        .expect("cleanup eth_block_txs failed");

    sqlx::query("DELETE FROM rome_via.evm_tx WHERE chain_id=$1")
        .bind(chain_id)
        .execute(pool)
        .await
        .expect("cleanup evm_tx failed");

    sqlx::query("DELETE FROM rome_via.eth_block WHERE chain_id=$1")
        .bind(chain_id)
        .execute(pool)
        .await
        .expect("cleanup eth_block failed");
}

/// Core correctness guarantee: total counts oracle txs; application figure
/// (total − oracle) excludes exactly them.
#[tokio::test]
async fn current_tps_total_counts_oracle_application_excludes() {
    let Some(url) = test_db_url() else {
        eprintln!("SKIP: HERCULES_TEST_DATABASE_URL unset");
        return;
    };

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("failed to connect to test DB");

    // Idempotent pre-clean (guard against a previous interrupted run).
    cleanup(&pool, THROWAWAY_CHAIN_ID).await;

    // Seed: 2 oracle txs + 3 non-oracle txs in one block.
    seed_block(
        &pool,
        THROWAWAY_CHAIN_ID,
        1,           // block_number
        900_000_001, // slot (arbitrary, unique)
        1_700_000_000,
        &[
            ORACLE_METHOD_ID,
            ORACLE_METHOD_ID,
            "0xaaaaaaaa",
            "0xbbbbbbbb",
            "0xcccccccc",
        ],
    )
    .await;

    // The oracle-split query the handler uses.
    let (total, oracle): (i64, i64) = sqlx::query_as(
        "SELECT COUNT(*)::BIGINT, \
                COUNT(*) FILTER (WHERE method_id = '0xf8ac93e8')::BIGINT \
         FROM rome_via.evm_tx \
         WHERE chain_id = $1",
    )
    .bind(THROWAWAY_CHAIN_ID)
    .fetch_one(&pool)
    .await
    .expect("oracle-split query failed");

    assert_eq!(total, 5, "total must count oracle txs");
    assert_eq!(oracle, 2, "oracle subset must be exactly the 0xf8ac93e8 txs");
    assert_eq!(total - oracle, 3, "application figure excludes oracle txs");

    cleanup(&pool, THROWAWAY_CHAIN_ID).await;
}

/// Secondary fixture check: one packed block with 3 txs groups to 3 in
/// eth_block_txs. Exercises the seed helper with a different block.
#[tokio::test]
async fn top_blocks_excludes_empty_but_counts_packed() {
    let Some(url) = test_db_url() else {
        eprintln!("SKIP: HERCULES_TEST_DATABASE_URL unset");
        return;
    };

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("failed to connect to test DB");

    cleanup(&pool, THROWAWAY_CHAIN_ID).await;

    // Seed a packed block with 3 txs.
    seed_block(
        &pool,
        THROWAWAY_CHAIN_ID,
        2,
        900_000_002,
        1_700_000_060,
        &["0x11111111", "0x22222222", "0x33333333"],
    )
    .await;

    let count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*)::BIGINT \
         FROM rome_via.eth_block_txs \
         WHERE chain_id = $1",
    )
    .bind(THROWAWAY_CHAIN_ID)
    .fetch_one(&pool)
    .await
    .expect("block tx count query failed");

    assert_eq!(count.0, 3, "packed block must show 3 txs in eth_block_txs");

    cleanup(&pool, THROWAWAY_CHAIN_ID).await;
}
