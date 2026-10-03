//! One-shot: rebuild `rome_via.address_stats` from `rome_via.evm_tx` +
//! `rome_via.evm_tx_result` so historical fee-recipient addresses are counted.
//!
//! This complements the address_stats worker, which only advances forward and
//! won't retro-count rows it already processed before fee-recipient counting
//! was added.
//!
//! Idempotent: DELETEs and rebuilds in one transaction. Pins the worker
//! cursor to MAX(slot_number) at rebuild time so the live worker only
//! processes txs strictly after the snapshot (avoiding double-counting).
//!
//! Usage:
//!   DATABASE_URL=postgres://... cargo run -p rome-via-enrich --example rebuild_address_stats -- --chain-id 121301

use std::env;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db_url = env::var("DATABASE_URL")
        .map_err(|_| anyhow::anyhow!("set DATABASE_URL to a rome_via_db connection string"))?;

    let chain_id: i64 = std::env::args()
        .skip_while(|a| a != "--chain-id")
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| anyhow::anyhow!("pass --chain-id <id>"))?;

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&db_url)
        .await?;

    let mut tx = pool.begin().await?;

    sqlx::query("DELETE FROM rome_via.address_stats WHERE chain_id = $1")
        .bind(chain_id)
        .execute(&mut *tx)
        .await?;

    // Counts every (tx, role) where role ∈ {from, to, fee_recipient}, summed per address.
    // Each tx contributes at most 1 to an address (de-dup via UNION).
    let inserted = sqlx::query(
        r#"
        INSERT INTO rome_via.address_stats (chain_id, address, tx_count, first_seen, last_seen)
        SELECT
            $1,
            address,
            COUNT(DISTINCT tx_hash)::BIGINT AS tx_count,
            MIN(ts) AS first_seen,
            MAX(ts) AS last_seen
        FROM (
            SELECT et.tx_hash, et.from_addr AS address,
                   to_timestamp(eb.params_block_timestamp::DOUBLE PRECISION) AS ts
            FROM rome_via.evm_tx et
            JOIN rome_via.eth_block_txs ebt
                ON ebt.tx_hash = et.tx_hash AND ebt.chain_id = et.chain_id
            JOIN rome_via.eth_block eb
                ON eb.slot_number = ebt.slot_number
                AND eb.slot_block_idx = ebt.slot_block_idx
                AND eb.chain_id = ebt.chain_id
            WHERE et.chain_id = $1 AND et.from_addr IS NOT NULL

            UNION

            SELECT et.tx_hash, et.to_addr,
                   to_timestamp(eb.params_block_timestamp::DOUBLE PRECISION)
            FROM rome_via.evm_tx et
            JOIN rome_via.eth_block_txs ebt
                ON ebt.tx_hash = et.tx_hash AND ebt.chain_id = et.chain_id
            JOIN rome_via.eth_block eb
                ON eb.slot_number = ebt.slot_number
                AND eb.slot_block_idx = ebt.slot_block_idx
                AND eb.chain_id = ebt.chain_id
            WHERE et.chain_id = $1 AND et.to_addr IS NOT NULL

            UNION

            SELECT et.tx_hash,
                   LOWER(r.tx_result->'gas_report'->>'gas_recipient'),
                   to_timestamp(eb.params_block_timestamp::DOUBLE PRECISION)
            FROM rome_via.evm_tx et
            JOIN rome_via.evm_tx_result r
                ON r.tx_hash = et.tx_hash AND r.chain_id = et.chain_id
            JOIN rome_via.eth_block_txs ebt
                ON ebt.tx_hash = et.tx_hash AND ebt.chain_id = et.chain_id
            JOIN rome_via.eth_block eb
                ON eb.slot_number = ebt.slot_number
                AND eb.slot_block_idx = ebt.slot_block_idx
                AND eb.chain_id = ebt.chain_id
            WHERE et.chain_id = $1
              AND r.tx_result->'gas_report'->>'gas_recipient' IS NOT NULL
        ) flat
        WHERE address IS NOT NULL
        GROUP BY address
        "#,
    )
    .bind(chain_id)
    .execute(&mut *tx)
    .await?
    .rows_affected();

    // Pin the cursor to the current MAX slot so the live worker only
    // processes txs strictly after the rebuild snapshot. Setting it to 0
    // would cause every historical tx to be re-counted, doubling existing
    // address tx_count values as the worker catches back up.
    let max_slot: Option<i64> = sqlx::query_scalar(
        "SELECT MAX(slot_number) FROM rome_via.eth_block_txs WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_one(&mut *tx)
    .await?;
    let cursor_slot = max_slot.unwrap_or(0);

    sqlx::query(
        "INSERT INTO rome_via.enrich_cursors (chain_id, worker, last_processed, last_processed_at)
         VALUES ($1, 'address_stats', $2, NOW())
         ON CONFLICT (chain_id, worker) DO UPDATE
             SET last_processed = EXCLUDED.last_processed, last_processed_at = NOW()",
    )
    .bind(chain_id)
    .bind(cursor_slot)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    let total: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM rome_via.address_stats WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_one(&pool)
    .await?;

    println!("rebuilt address_stats for chain_id {chain_id}: {inserted} rows inserted, {total} total addresses");

    // Spot-check a few high-count addresses.
    let top: Vec<(String, i64)> = sqlx::query_as(
        "SELECT address, tx_count FROM rome_via.address_stats
         WHERE chain_id = $1 ORDER BY tx_count DESC LIMIT 5",
    )
    .bind(chain_id)
    .fetch_all(&pool)
    .await?;
    for (addr, n) in top {
        println!("  {addr}  tx_count={n}");
    }

    Ok(())
}
