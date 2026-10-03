//! One-shot backfill: re-run `decode_signed_tx` over rows whose denormalized
//! columns (`tx_type_byte`, `to_addr`, etc.) are NULL — typically rows ingested
//! before the decoder gained support for the envelope type they use (e.g.
//! 0x7E DepositTransaction).
//!
//! Idempotent: only touches rows where decode currently fails or hasn't been
//! attempted, and only writes columns that successfully decode this time.
//!
//! Usage:
//!   DATABASE_URL=postgres://... cargo run -p rome-via-sync --example redecode_failed
//!   DATABASE_URL=postgres://... cargo run -p rome-via-sync --example redecode_failed -- --chain-id 121301

use rome_via_sync::rlp_decode;
use sqlx::Row;
use std::env;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db_url = env::var("DATABASE_URL")
        .map_err(|_| anyhow::anyhow!("set DATABASE_URL to a rome_via_db connection string"))?;

    let chain_id: Option<i64> = std::env::args()
        .skip_while(|a| a != "--chain-id")
        .nth(1)
        .and_then(|s| s.parse().ok());

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(&db_url)
        .await?;

    let rows = sqlx::query(
        r#"
        SELECT chain_id, tx_hash, rlp, origination
        FROM rome_via.evm_tx
        WHERE rlp IS NOT NULL
          AND tx_type_byte IS NULL
          AND ($1::BIGINT IS NULL OR chain_id = $1)
        "#,
    )
    .bind(chain_id)
    .fetch_all(&pool)
    .await?;

    println!("found {} candidate rows to re-decode", rows.len());
    let mut ok = 0usize;
    let mut still_failing = 0usize;

    for row in rows {
        let chain_id: i64 = row.get("chain_id");
        let tx_hash: String = row.get("tx_hash");
        let rlp: Vec<u8> = row.get("rlp");
        // Solana-origin txs have zeroed v/r/s — skip ECDSA recovery for them.
        // 'ecdsa' is the default for rows pre-dating the origination column.
        let origination: String = row
            .get::<Option<String>, _>("origination")
            .unwrap_or_else(|| "ecdsa".to_string());
        let recover_sender = origination != "solana_unsigned";

        match rlp_decode::decode_signed_tx(&rlp, recover_sender) {
            Ok(d) => {
                sqlx::query(
                    r#"
                    UPDATE rome_via.evm_tx
                    SET from_addr     = $3,
                        to_addr       = $4,
                        value_wei     = $5::NUMERIC,
                        nonce         = $6,
                        gas_price     = $7::NUMERIC,
                        gas_limit     = $8,
                        method_id     = $9,
                        input_len     = $10,
                        tx_type_byte  = $11
                    WHERE chain_id = $1 AND tx_hash = $2
                    "#,
                )
                .bind(chain_id)
                .bind(&tx_hash)
                .bind(&d.from)
                .bind(d.to.as_deref())
                .bind(d.value_wei.to_string())
                .bind(d.nonce)
                .bind(d.gas_price.to_string())
                .bind(d.gas_limit)
                .bind(d.method_id.as_deref())
                .bind(d.input_len)
                .bind(d.tx_type_byte)
                .execute(&pool)
                .await?;
                ok += 1;
                if ok <= 3 {
                    println!("  re-decoded {} → tx_type=0x{:02x} to={:?}", tx_hash, d.tx_type_byte, d.to);
                }
            }
            Err(e) => {
                still_failing += 1;
                if still_failing <= 3 {
                    println!("  still failing {}: {}", tx_hash, e);
                }
            }
        }
    }

    println!("done: re-decoded {ok}, still failing {still_failing}");
    Ok(())
}
