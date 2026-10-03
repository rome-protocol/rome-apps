// pg-gated: an iterative EVM tx's Solana legs must read back in
// (slot_number, tx_idx, instr_idx) execution order — across AND within Solana
// blocks — regardless of insertion order. Requires HERCULES_TEST_DATABASE_URL;
// skips cleanly otherwise. Writes under a throwaway chain_id and cleans up.
use sqlx::postgres::PgPoolOptions;

const THROWAWAY_CHAIN_ID: i64 = 990011;

fn test_db_url() -> Option<String> {
    std::env::var("HERCULES_TEST_DATABASE_URL").ok()
}

#[tokio::test]
async fn sol_legs_ordered_by_execution() {
    let Some(url) = test_db_url() else {
        eprintln!("SKIP: HERCULES_TEST_DATABASE_URL unset (needs a live Postgres)");
        return;
    };
    let pool = PgPoolOptions::new().connect(&url).await.expect("connect");

    // Self-contained schema (matches migrations 0008 + 0012).
    sqlx::query("CREATE SCHEMA IF NOT EXISTS rome_via")
        .execute(&pool).await.unwrap();
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS rome_via.evm_tx_sol_tx (
            chain_id BIGINT NOT NULL, evm_tx_hash VARCHAR(66) NOT NULL,
            sol_signature TEXT NOT NULL, slot_number BIGINT NOT NULL,
            tx_idx INTEGER NOT NULL DEFAULT 0, instr_idx INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (chain_id, evm_tx_hash, sol_signature))",
    ).execute(&pool).await.unwrap();
    let _ = sqlx::query("ALTER TABLE rome_via.evm_tx_sol_tx ADD COLUMN IF NOT EXISTS tx_idx INTEGER NOT NULL DEFAULT 0").execute(&pool).await;
    let _ = sqlx::query("ALTER TABLE rome_via.evm_tx_sol_tx ADD COLUMN IF NOT EXISTS instr_idx INTEGER NOT NULL DEFAULT 0").execute(&pool).await;

    let hash = "0xfeedfacefeedfacefeedfacefeedfacefeedfacefeedfacefeedfacefeedface";
    sqlx::query("DELETE FROM rome_via.evm_tx_sol_tx WHERE chain_id=$1")
        .bind(THROWAWAY_CHAIN_ID).execute(&pool).await.unwrap();

    // Insert OUT OF ORDER: slot 1001 first, then slot 1000's two legs (tx_idx 7, 3).
    for (sig, slot, tx_idx, instr_idx) in
        [("sigB", 1001i64, 1i32, 0i32), ("sigC", 1000, 7, 0), ("sigA", 1000, 3, 0)]
    {
        sqlx::query(
            "INSERT INTO rome_via.evm_tx_sol_tx
                (chain_id, evm_tx_hash, sol_signature, slot_number, tx_idx, instr_idx)
             VALUES ($1,$2,$3,$4,$5,$6)",
        )
        .bind(THROWAWAY_CHAIN_ID).bind(hash).bind(sig).bind(slot).bind(tx_idx).bind(instr_idx)
        .execute(&pool).await.unwrap();
    }

    let legs = rome_via_api::api::txs::fetch_sol_legs(&pool, THROWAWAY_CHAIN_ID, hash)
        .await.expect("fetch_sol_legs");
    let got: Vec<(i64, i32, String)> =
        legs.iter().map(|l| (l.slot_number, l.tx_idx, l.sol_signature.clone())).collect();
    assert_eq!(
        got,
        vec![
            (1000, 3, "sigA".to_string()),
            (1000, 7, "sigC".to_string()),
            (1001, 1, "sigB".to_string()),
        ],
        "legs must be ordered by (slot_number, tx_idx, instr_idx), not insertion order",
    );

    sqlx::query("DELETE FROM rome_via.evm_tx_sol_tx WHERE chain_id=$1")
        .bind(THROWAWAY_CHAIN_ID).execute(&pool).await.unwrap();
}
