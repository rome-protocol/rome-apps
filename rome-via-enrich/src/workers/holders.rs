/// Token holder and transfer tracking worker.
///
/// Tails `evm_tx_result.tx_result` JSONB for ERC-20 Transfer logs
/// (topic0 = 0xddf252ad…).  For each log:
/// - Inserts into `token_transfers`
/// - Debits from sender in `token_holders`, deletes row when balance reaches 0
/// - Credits recipient in `token_holders`
/// - Updates `token_holder_counts` (pre-aggregated WHERE balance > 0)
///
/// Also triggers `token_metadata` ingestion for newly-seen token addresses
/// (handled by the metadata worker sharing the same pool).
use sqlx::PgPool;
use std::time::Duration;
use tracing::{debug, warn};

/// ERC-20 Transfer event topic0 (keccak256("Transfer(address,address,uint256)"))
const TRANSFER_TOPIC: &str =
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

/// Convert a Unix-epoch-seconds value (as read from `eth_block.params_block_timestamp`,
/// a NUMERIC epoch fetched `::FLOAT8`) into a UTC datetime. `None` epoch (block not
/// yet indexed) → `None`. Same decode as `address_stats` / `cross_chain`; used to
/// stamp `token_transfers.timestamp` with the block's real time instead of
/// ingest-time `NOW()` (E-CO1).
pub fn epoch_to_dt(secs: Option<f64>) -> Option<chrono::DateTime<chrono::Utc>> {
    secs.and_then(|e| chrono::DateTime::from_timestamp(e as i64, 0))
}

/// Whether a `token_transfers` INSERT (ON CONFLICT DO NOTHING) actually
/// inserted the row. rows_affected()==1 ⇒ first time we've recorded this
/// (chain,tx,log) in the current committed txn ⇒ apply the balance delta.
/// rows_affected()==0 ⇒ ON CONFLICT no-op (re-processed batch) ⇒ skip; the
/// delta was already committed. The gate that makes holder balances
/// exactly-once under at-least-once batch redelivery. [E-CO3]
fn transfer_was_newly_inserted(insert_rows_affected: u64) -> bool {
    insert_rows_affected == 1
}

pub async fn run(
    pool: PgPool,
    chain_id: i64,
    poll_interval: Duration,
    batch_size: i64,
) -> anyhow::Result<()> {
    const MAX_HOLD: u32 = 30;
    let mut hold = super::rpc_verdict::HoldState::clear();

    loop {
        let cursor: i64 = sqlx::query_scalar(
            "SELECT COALESCE(last_processed, 0)
             FROM rome_via.enrich_cursors
             WHERE chain_id = $1 AND worker = 'holders'",
        )
        .bind(chain_id)
        .fetch_optional(&pool)
        .await?
        .unwrap_or(0);

        // Fetch the next batch of evm_tx_result rows. Logs live in `tx_result`
        // (keys: logs/footprint/timestamp/gas_report/exit_reason/slot_number) —
        // NOT in `receipt_params` (keys: blockhash/block_number/tx_index/
        // block_gas_used/first_log_index, no logs). `tx_result` is also the
        // NOT-NULL column, so this gate includes solana_unsigned rows whose
        // `receipt_params` was historically NULL.
        // We use slot_number as the cursor dimension (from eth_block_txs join).
        //
        // `AND r.chain_id = $1` is LOAD-BEARING: rome_via_db is SHARED across
        // every chain on the cluster (Hadrian 200010 + Trajan 121302 today), and
        // evm_tx_result's PK is (chain_id, slot_number, tx_hash). Without the
        // chain_id predicate this worker would tail another chain's Transfer
        // logs and credit/debit holders + seed token_metadata under THIS
        // chain_id — cross-chain pollution. (chain_id is the leading PK + index
        // column, so the filter is index-backed.)
        let rows: Vec<(String, i64, serde_json::Value)> = sqlx::query_as(
            "SELECT r.tx_hash, r.slot_number, r.tx_result
             FROM rome_via.evm_tx_result r
             WHERE r.chain_id = $1
               AND r.slot_number > $2
               AND r.tx_result IS NOT NULL
             ORDER BY r.slot_number ASC, r.tx_hash ASC
             LIMIT $3",
        )
        .bind(chain_id)
        .bind(cursor)
        .bind(batch_size)
        .fetch_all(&pool)
        .await
        .unwrap_or_default();

        if rows.is_empty() {
            tokio::time::sleep(poll_interval).await;
            continue;
        }

        // E-C1: defer the (possibly LIMIT-cut) top slot so a full batch never
        // skips a boundary slot's tail. Fetch query unchanged; only what we
        // process + persist changes.
        let slots: Vec<i64> = rows.iter().map(|r| r.1).collect();
        let plan = super::batch_cursor::plan_batch(&slots, batch_size as usize);
        if plan.giant_slot {
            warn!(worker = "holders", slot = slots[0],
                  "single slot fills an entire batch; tail beyond batch_size cannot be deferred (unexpected on one-block-per-slot chains)");
        }

        let mut earliest_miss: Option<(i64, super::rpc_verdict::MissKind)> = None;

        for (tx_hash, slot_number, tx_result) in &rows[..plan.process_count] {
            // Decode all ERC-20 Transfer logs from this row's `tx_result` JSON.
            // Pure extraction (no I/O) so it can be unit-tested directly against
            // the `tx_result` shape; the DB writes below stay in the loop.
            let transfers = extract_transfers(tx_result);
            if transfers.is_empty() {
                continue;
            }

            // Resolve block_number + block timestamp for this tx from eth_block.
            // `params_block_timestamp` is a NUMERIC unix-epoch (rome-via-sync
            // migration 0004) — fetch it ::FLOAT8 and convert via epoch_to_dt
            // (same decode as address_stats / cross_chain). This is the block's
            // real time, bound into token_transfers below — NOT ingest-time
            // NOW() (E-CO1). Block not yet indexed → None → NULL timestamp.
            let block_lookup: Result<Option<(i64, Option<f64>)>, sqlx::Error> = sqlx::query_as(
                "SELECT COALESCE(b.params_number, 0), b.params_block_timestamp::FLOAT8
                 FROM rome_via.eth_block_txs t
                 JOIN rome_via.eth_block b
                     ON b.chain_id = t.chain_id
                     AND b.slot_number = t.slot_number
                     AND b.slot_block_idx = t.slot_block_idx
                 WHERE t.tx_hash = $1 AND t.chain_id = $2
                 LIMIT 1",
            )
            .bind(tx_hash)
            .bind(chain_id)
            .fetch_optional(&pool)
            .await;
            let (block_number, block_ts): (i64, Option<chrono::DateTime<chrono::Utc>>) =
                match block_lookup {
                    Ok(Some((n, epoch))) => (n, epoch_to_dt(epoch)),
                    // F6: block not indexed yet is a sync-lag race, not a
                    // genuine absence — defer rather than fabricating
                    // block_number=0 for this transfer.
                    Ok(None) => {
                        if earliest_miss.is_none() {
                            earliest_miss =
                                Some((*slot_number, super::rpc_verdict::MissKind::Transient));
                        }
                        continue;
                    }
                    Err(e) => {
                        warn!(%tx_hash, error = %e, "eth_block lookup failed — deferring tx");
                        if earliest_miss.is_none() {
                            earliest_miss =
                                Some((*slot_number, super::rpc_verdict::db_miss_kind(&e)));
                        }
                        continue;
                    }
                };

            for transfer in &transfers {
                let DecodedTransfer {
                    log_index,
                    token_address,
                    from_addr,
                    to_addr,
                    amount,
                } = transfer;
                let log_index = *log_index;

                // Insert the transfer + apply its balance deltas in ONE
                // transaction. The INSERT's rows_affected gates the deltas
                // (applied only on a fresh insert, skipped on an ON CONFLICT
                // no-op), and the atomic commit makes balance updates
                // EXACTLY-ONCE under at-least-once batch redelivery: a crash
                // before the cursor advances re-fetches the batch, committed
                // transfers no-op so their deltas are NOT re-applied, and a
                // mid-row crash rolls the whole row back to replay cleanly. The
                // old code applied deltas unconditionally after a bare INSERT,
                // so a replayed batch double-counted (transfers dedup via PK,
                // balances don't). [E-CO3] (F3/A″ later dropped the GREATEST(0,…)
                // clamp that used to hide such drift — it is now a visible signal.)
                let mut txn = match pool.begin().await {
                    Ok(t) => t,
                    Err(e) => {
                        warn!(%tx_hash, log_index, error = %e, "failed to begin token_transfer txn");
                        if earliest_miss.is_none() {
                            earliest_miss =
                                Some((*slot_number, super::rpc_verdict::db_miss_kind(&e)));
                        }
                        continue;
                    }
                };

                let ins = sqlx::query(
                    "INSERT INTO rome_via.token_transfers
                         (chain_id, tx_hash, log_index, token_address, from_addr, to_addr,
                          amount, block_number, timestamp)
                     VALUES ($1, $2, $3, $4, $5, $6, $7::NUMERIC, $8, $9)
                     ON CONFLICT (chain_id, tx_hash, log_index) DO NOTHING",
                )
                .bind(chain_id)
                .bind(tx_hash)
                .bind(log_index as i32)
                .bind(token_address)
                .bind(from_addr)
                .bind(to_addr)
                .bind(amount)
                .bind(block_number)
                .bind(block_ts)
                .execute(&mut *txn)
                .await;

                let inserted = match ins {
                    Ok(r) => transfer_was_newly_inserted(r.rows_affected()),
                    Err(e) => {
                        warn!(%tx_hash, log_index, error = %e, "failed to insert token_transfer");
                        let _ = txn.rollback().await;
                        if earliest_miss.is_none() {
                            earliest_miss =
                                Some((*slot_number, super::rpc_verdict::db_miss_kind(&e)));
                        }
                        continue;
                    }
                };

                if inserted {
                    // Debit from_addr / credit to_addr — applied exactly once.
                    if from_addr != "0x0000000000000000000000000000000000000000" {
                        if let Err(e) =
                            apply_balance_delta(&mut txn, chain_id, token_address, from_addr, &format!("-{amount}")).await
                        {
                            warn!(%tx_hash, %token_address, %from_addr, error = %e, "failed to debit; rolling back transfer");
                            let _ = txn.rollback().await;
                            if earliest_miss.is_none() {
                                earliest_miss =
                                    Some((*slot_number, super::rpc_verdict::db_miss_kind(&e)));
                            }
                            continue;
                        }
                    }
                    if to_addr != "0x0000000000000000000000000000000000000000" {
                        if let Err(e) =
                            apply_balance_delta(&mut txn, chain_id, token_address, to_addr, amount).await
                        {
                            warn!(%tx_hash, %token_address, %to_addr, error = %e, "failed to credit; rolling back transfer");
                            let _ = txn.rollback().await;
                            if earliest_miss.is_none() {
                                earliest_miss =
                                    Some((*slot_number, super::rpc_verdict::db_miss_kind(&e)));
                            }
                            continue;
                        }
                    }
                }

                if let Err(e) = txn.commit().await {
                    warn!(%tx_hash, log_index, error = %e, "failed to commit token_transfer txn");
                    if earliest_miss.is_none() {
                        earliest_miss = Some((*slot_number, super::rpc_verdict::db_miss_kind(&e)));
                    }
                    continue;
                }

                // Ensure token_metadata row exists (name/symbol/decimals/kind
                // populated by the metadata worker). Seed `kind = NULL` so the
                // metadata worker's `name IS NULL OR kind IS NULL` poll
                // classifies it on first probe. Idempotent (ON CONFLICT DO
                // NOTHING) and marks a first-seen token — safe to run
                // unconditionally, outside the balance txn.
                let _ = sqlx::query(
                    "INSERT INTO rome_via.token_metadata (chain_id, address, kind)
                     VALUES ($1, $2, NULL)
                     ON CONFLICT (chain_id, address) DO NOTHING",
                )
                .bind(chain_id)
                .bind(token_address)
                .execute(&pool)
                .await;

                // Refresh the pre-aggregated holder count UNCONDITIONALLY (not gated
                // on `inserted`) so a crash between the balance-txn commit and this
                // refresh self-heals on replay. The refresh is an idempotent recompute
                // (COUNT WHERE balance>0); a redelivered no-op just recomputes the same
                // value. [F7]
                refresh_holder_count(&pool, chain_id, token_address).await;
            }

            debug!(%tx_hash, "processed transfer logs");
        }

        // F6: don't commit past a swallowed per-row write; hold the cursor just
        // below the earliest miss so it re-processes next poll (the writes
        // above are ON CONFLICT-idempotent / exactly-once-gated, so replay is
        // safe).
        let outcome =
            super::rpc_verdict::valve_persist_cursor(plan.new_cursor, earliest_miss, hold, MAX_HOLD);
        hold = outcome.hold;
        if outcome.gave_up {
            warn!(worker = "holders",
                "terminal miss unresolved for MAX_HOLD polls; advancing past stuck slot [F6]");
        }

        let _ = sqlx::query(
            "INSERT INTO rome_via.enrich_cursors (chain_id, worker, last_processed, last_processed_at)
             VALUES ($1, 'holders', $2, NOW())
             ON CONFLICT (chain_id, worker) DO UPDATE
                 SET last_processed    = EXCLUDED.last_processed,
                     last_processed_at = NOW()",
        )
        .bind(chain_id)
        .bind(outcome.persist)
        .execute(&pool)
        .await;

        if earliest_miss.is_some() {
            tokio::time::sleep(poll_interval).await;
        }
    }
}

/// One decoded ERC-20 Transfer, ready for the DB writes in [`run`].
///
/// Addresses are 20-byte 0x-hex lowercase; `amount` is a decimal string suitable
/// for a NUMERIC bind. `log_index` is the position within the row's `logs` array.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedTransfer {
    pub log_index: usize,
    pub token_address: String,
    pub from_addr: String,
    pub to_addr: String,
    pub amount: String,
}

/// Extract every ERC-20 Transfer from a row's **result JSON** (`evm_tx_result.tx_result`).
///
/// The logs that hold Transfer events live under `tx_result.logs` (alongside
/// `footprint`/`exit_reason`/`gas_report`/…). They are NOT in `receipt_params`,
/// whose keys are `blockhash`/`block_number`/`tx_index`/`block_gas_used`/
/// `first_log_index` — no `logs` array at all. So feeding this a
/// `receipt_params`-shaped value yields an empty vec, while a `tx_result`-shaped
/// value yields one [`DecodedTransfer`] per Transfer log.
///
/// `log_index` is the index within the `logs` array (not just the Transfer
/// subset), matching the value previously used as the `token_transfers` PK.
///
/// Pure function — no I/O, fully unit-testable.
pub fn extract_transfers(result_json: &serde_json::Value) -> Vec<DecodedTransfer> {
    let logs = match result_json.get("logs") {
        Some(serde_json::Value::Array(a)) => a,
        _ => return Vec::new(),
    };

    let mut out = Vec::new();
    for (log_index, log) in logs.iter().enumerate() {
        let topics = match log.get("topics") {
            Some(serde_json::Value::Array(t)) => t,
            _ => continue,
        };

        let topic0 = topics
            .first()
            .and_then(|v| v.as_str())
            .unwrap_or_default();

        if topic0.to_lowercase() != TRANSFER_TOPIC {
            continue;
        }

        // ERC-20 Transfer has exactly 3 topics (topic0 + indexed from + indexed
        // to) with the value in `data`. ERC-721 Transfer(from,to,tokenId) shares
        // topic0 but indexes tokenId as a 4th topic and carries empty data —
        // skip it, else it decodes as an ERC-20 transfer of amount 0 and
        // pollutes token_transfers / token_metadata for every NFT contract.
        if topics.len() != 3 {
            continue;
        }

        // ERC-20 Transfer: topics[1]=from, topics[2]=to, data=amount
        let from_raw = topics
            .get(1)
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let to_raw = topics
            .get(2)
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let data = log.get("data").and_then(|v| v.as_str()).unwrap_or("0x");

        // Convert 32-byte padded addresses to 20-byte (take last 40 hex chars + 0x prefix).
        let from_addr = topic_to_address(from_raw);
        let to_addr = topic_to_address(to_raw);

        // Amount is a 256-bit big-endian integer in the `data` field.
        let amount = hex_to_numeric(data);

        // Token address is `log.address`.
        let token_address = log
            .get("address")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_lowercase();

        if token_address.is_empty() || from_addr.is_empty() || to_addr.is_empty() {
            continue;
        }

        out.push(DecodedTransfer {
            log_index,
            token_address,
            from_addr,
            to_addr,
            amount,
        });
    }
    out
}

/// Convert a 32-byte padded EVM topic to a 20-byte address string (0x-prefixed, lowercase).
fn topic_to_address(topic: &str) -> String {
    let hex = topic.trim_start_matches("0x");
    if hex.len() < 40 {
        return String::new();
    }
    format!("0x{}", &hex[hex.len() - 40..]).to_lowercase()
}

/// Convert a hex-encoded 256-bit integer (the `data` field in Transfer logs) to a
/// decimal numeric string suitable for NUMERIC columns.
fn hex_to_numeric(data: &str) -> String {
    let hex = data.trim_start_matches("0x");
    if hex.is_empty() {
        return "0".to_string();
    }
    // Parse the FULL uint256 word, not just the low 128 bits. The old code took
    // `hex[len-32..]` (last 128 bits), silently truncating any amount ≥ 2^128
    // (huge-supply tokens) — a real corruption of token_transfers.amount. U256's
    // 256-bit cap matches EVM word semantics, so a malformed over-long field
    // can't mint an absurd NUMERIC either.
    match ethers::types::U256::from_str_radix(hex, 16) {
        Ok(n) => n.to_string(),
        Err(e) => {
            warn!(data, error = %e, "failed to parse Transfer amount hex — recording 0");
            "0".to_string()
        }
    }
}

/// Adjust a holder's balance by `delta` (signed decimal string, e.g. "100"
/// or "-100"), then delete the row if the balance reached 0. Runs inside the
/// caller's transaction so the balance write commits atomically with the
/// transfer INSERT that gated it (see the per-transfer txn in `run`).
/// Propagates errors so the caller can roll the whole transfer back.
///
/// F3 (A″): balance = the running SUM of signed deltas, applied WITHOUT clamping.
/// Addition commutes, so the result is independent of the order deltas arrive in
/// — which is what makes the `slot ASC, tx_hash ASC` batch order (lexicographic,
/// NOT execution order) safe: a same-slot send-before-receive nets correctly
/// instead of being floored to 0 by the old `GREATEST(0, …)` (which silently
/// LOST the clamped amount and made balances order-dependent). A transient
/// negative during a batch nets back up; a PERSISTENT negative is a real signal
/// (a missing transfer) — surfaced, not hidden. Readers never see it: every
/// `token_holders` read filters `balance > 0` (the partial index), so a negative
/// row is invisible to holder lists/counts while remaining a data-quality tell.
async fn apply_balance_delta(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    chain_id: i64,
    token_address: &str,
    holder_address: &str,
    delta: &str,
) -> Result<(), sqlx::Error> {
    // Upsert: credit / debit in a single statement.
    sqlx::query(
        "INSERT INTO rome_via.token_holders
             (chain_id, token_address, holder_address, balance, updated_at)
         VALUES ($1, $2, $3, $4::NUMERIC, NOW())
         ON CONFLICT (chain_id, token_address, holder_address) DO UPDATE
             SET balance    = token_holders.balance + $4::NUMERIC,
                 updated_at = NOW()",
    )
    .bind(chain_id)
    .bind(token_address)
    .bind(holder_address)
    .bind(delta)
    .execute(&mut **txn)
    .await?;

    // Delete rows at exactly 0 (a full exit). A NEGATIVE balance (A″: no clamp)
    // is deliberately NOT deleted — it's kept as a data-quality signal (a missing
    // transfer), invisible to readers via the `balance > 0` partial index.
    sqlx::query(
        "DELETE FROM rome_via.token_holders
         WHERE chain_id = $1 AND token_address = $2 AND holder_address = $3 AND balance = 0",
    )
    .bind(chain_id)
    .bind(token_address)
    .bind(holder_address)
    .execute(&mut **txn)
    .await?;

    Ok(())
}

/// Refresh the pre-aggregated holder count for a token.
async fn refresh_holder_count(pool: &PgPool, chain_id: i64, token_address: &str) {
    let _ = sqlx::query(
        "INSERT INTO rome_via.token_holder_counts
             (chain_id, token_address, holder_count, updated_at)
         SELECT $1, $2, COUNT(*), NOW()
         FROM rome_via.token_holders
         WHERE chain_id = $1 AND token_address = $2 AND balance > 0
         ON CONFLICT (chain_id, token_address) DO UPDATE
             SET holder_count = EXCLUDED.holder_count,
                 updated_at   = NOW()",
    )
    .bind(chain_id)
    .bind(token_address)
    .execute(pool)
    .await;
}

// ─────────────────────────────────────────────────────────────
// Unit tests
// ─────────────────────────────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_to_address_strips_padding() {
        let padded = "0x000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
        let addr = topic_to_address(padded);
        assert_eq!(addr, "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48");
    }

    #[test]
    fn topic_to_address_short_returns_empty() {
        let addr = topic_to_address("0x1234");
        assert!(addr.is_empty(), "short topic should yield empty address");
    }

    #[test]
    fn hex_to_numeric_converts_correctly() {
        // 1 USDC = 1_000_000 (6 decimals)
        let hex = "0x00000000000000000000000000000000000000000000000000000000000f4240";
        assert_eq!(hex_to_numeric(hex), "1000000");
    }

    #[test]
    fn hex_to_numeric_zero_data() {
        assert_eq!(hex_to_numeric("0x"), "0");
        assert_eq!(hex_to_numeric("0x0"), "0");
    }

    /// A value > u128::MAX must NOT truncate. The old code kept only the low 128
    /// bits (`hex[len-32..]`), so 2^128 (1 followed by 32 hex zeros) read as 0.
    /// A uint256 data word is 64 hex chars; parse the FULL word. 2^128 =
    /// 340282366920938463463374607431768211456.
    #[test]
    fn hex_to_numeric_full_uint256_no_truncation() {
        // 2^128 padded to a 32-byte word: 31 zeros, '1', 32 zeros.
        let two_pow_128 =
            "0x0000000000000000000000000000000100000000000000000000000000000000";
        assert_eq!(
            hex_to_numeric(two_pow_128),
            "340282366920938463463374607431768211456"
        );
        // max uint256 round-trips fully (not the low-128 2^128-1).
        let max = "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
        assert_eq!(
            hex_to_numeric(max),
            "115792089237316195423570985008687907853269984665640564039457584007913129639935"
        );
    }

    /// ERC-721 `Transfer(from,to,tokenId)` shares topic0 with ERC-20
    /// `Transfer(from,to,value)` but indexes tokenId as topics[3] (len==4) with
    /// empty data. It must NOT be decoded as an ERC-20 transfer of amount 0
    /// (which polluted token_transfers/token_metadata for NFT contracts).
    #[test]
    fn extract_transfers_skips_erc721_four_topic() {
        let from = "0x000000000000000000000000aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let to = "0x000000000000000000000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let token_id = "0x0000000000000000000000000000000000000000000000000000000000000007";
        let tx_result = serde_json::json!({
            "logs": [
                {
                    "address": "0x9a8b4cb7cf9d9f6d8c0f0e0c0d0e0f0102030405",
                    "topics": [TRANSFER_TOPIC, from, to, token_id],
                    "data": "0x"
                }
            ]
        });
        assert!(
            extract_transfers(&tx_result).is_empty(),
            "a 4-topic ERC-721 Transfer must not decode as an ERC-20 transfer"
        );
    }

    /// F3 (A″) invariant lock: with the `GREATEST(0, …)` clamp removed, a balance
    /// is the plain SUM of signed deltas, which is ORDER-INDEPENDENT (addition
    /// commutes) — so the `slot, tx_hash` batch order can't corrupt it. The old
    /// clamp was order-DEPENDENT (it floored an intra-slot underflow to 0 and lost
    /// the amount), which is exactly the F3 bug.
    ///
    /// NB: the real write is SQL, so the *binding* regression is the live Hadrian
    /// trace (a same-slot send-before-receive nets correctly). This test locks the
    /// commutativity reasoning + demonstrates the removed clamp's order-dependence.
    #[test]
    fn balance_deltas_commute_without_clamp() {
        let sum = |ds: &[i64]| ds.iter().sum::<i64>();
        // Same deltas (receive 100, send 100, receive 50), two arrival orders:
        assert_eq!(sum(&[100, -100, 50]), sum(&[-100, 50, 100]));
        assert_eq!(sum(&[100, -100, 50]), 50);

        // The REMOVED clamp did NOT commute — send-first inflated the result:
        let clamped = |ds: &[i64]| ds.iter().fold(0i64, |b, d| (b + d).max(0));
        assert_eq!(clamped(&[100, -100, 50]), 50, "receive-first: correct");
        assert_eq!(clamped(&[-100, 50, 100]), 150, "send-first under clamp: WRONG");
        assert_ne!(
            clamped(&[-100, 50, 100]),
            clamped(&[100, -100, 50]),
            "the removed GREATEST(0,…) clamp was order-dependent — the F3 bug"
        );
    }

    // ── extract_transfers: logs come from the `tx_result` shape, not `receipt_params` ──
    //
    // This is the keystone-bug regression lock. The worker historically read
    // `receipt_params.logs`, which is ALWAYS absent (receipt_params keys are
    // blockhash/block_number/tx_index/block_gas_used/first_log_index). Logs
    // live in `tx_result.logs`. These tests assert extraction works against the
    // shape that actually carries logs and yields NOTHING against the one that
    // doesn't — so a regression back to the wrong column fails here.

    /// A single ERC-20 Transfer log inside a `tx_result`-shaped value (the keys a
    /// real `evm_tx_result.tx_result` carries) is decoded into one transfer.
    #[test]
    fn extract_from_tx_result_shape_decodes_transfer() {
        // 1 USDC (6 decimals) = 1_000_000 = 0x..0f4240.
        let from = "0x000000000000000000000000aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let to = "0x000000000000000000000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let tx_result = serde_json::json!({
            "exit_reason": "Succeed",
            "footprint": [],
            "gas_report": {},
            "slot_number": 42,
            "timestamp": 0,
            "logs": [
                {
                    "address": "0x9A8B4cB7Cf9D9F6D8c0F0e0c0d0e0f0102030405",
                    "topics": [TRANSFER_TOPIC, from, to],
                    "data": "0x00000000000000000000000000000000000000000000000000000000000f4240"
                }
            ]
        });

        let transfers = extract_transfers(&tx_result);
        assert_eq!(transfers.len(), 1, "one Transfer log → one decoded transfer");
        let t = &transfers[0];
        assert_eq!(t.log_index, 0);
        // Token address is lowercased.
        assert_eq!(t.token_address, "0x9a8b4cb7cf9d9f6d8c0f0e0c0d0e0f0102030405");
        assert_eq!(t.from_addr, "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        assert_eq!(t.to_addr, "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        assert_eq!(t.amount, "1000000");
    }

    /// A `receipt_params`-shaped value (the column the worker used to read) has
    /// NO `logs` key — so extraction yields nothing. This is the bug: feeding
    /// receipt_params produced zero transfers chain-wide.
    #[test]
    fn extract_from_receipt_params_shape_yields_nothing() {
        let receipt_params = serde_json::json!({
            "blockhash": "0xabc",
            "block_number": 100,
            "tx_index": 0,
            "block_gas_used": "0x5208",
            "first_log_index": 0
        });
        assert!(
            extract_transfers(&receipt_params).is_empty(),
            "receipt_params has no `logs` key → no transfers extracted (the bug)"
        );
    }

    /// Non-Transfer logs (different topic0) are ignored; only Transfer topic0
    /// rows are decoded, preserving the original log_index of the Transfer.
    #[test]
    fn extract_ignores_non_transfer_topics() {
        let from = "0x000000000000000000000000aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let to = "0x000000000000000000000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let tx_result = serde_json::json!({
            "logs": [
                {
                    // Approval(address,address,uint256) — not Transfer.
                    "address": "0x9a8b4cb7cf9d9f6d8c0f0e0c0d0e0f0102030405",
                    "topics": ["0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925", from, to],
                    "data": "0x00000000000000000000000000000000000000000000000000000000000f4240"
                },
                {
                    "address": "0x9a8b4cb7cf9d9f6d8c0f0e0c0d0e0f0102030405",
                    "topics": [TRANSFER_TOPIC, from, to],
                    "data": "0x0000000000000000000000000000000000000000000000000000000000000001"
                }
            ]
        });

        let transfers = extract_transfers(&tx_result);
        assert_eq!(transfers.len(), 1, "only the Transfer log is decoded");
        assert_eq!(transfers[0].log_index, 1, "log_index is the array position");
        assert_eq!(transfers[0].amount, "1");
    }

    /// A mint (from = zero address) is still extracted — the zero-address debit
    /// skip happens at the DB-write layer in `run`, not in extraction.
    #[test]
    fn extract_keeps_mint_from_zero_address() {
        let zero = "0x0000000000000000000000000000000000000000000000000000000000000000";
        let to = "0x000000000000000000000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let tx_result = serde_json::json!({
            "logs": [
                {
                    "address": "0x9a8b4cb7cf9d9f6d8c0f0e0c0d0e0f0102030405",
                    "topics": [TRANSFER_TOPIC, zero, to],
                    "data": "0x00000000000000000000000000000000000000000000000000000000000f4240"
                }
            ]
        });

        let transfers = extract_transfers(&tx_result);
        assert_eq!(transfers.len(), 1);
        assert_eq!(transfers[0].from_addr, "0x0000000000000000000000000000000000000000");
    }

    /// `logs` present but empty → no transfers (graceful, not a panic).
    #[test]
    fn extract_empty_logs_array_yields_nothing() {
        let tx_result = serde_json::json!({ "logs": [] });
        assert!(extract_transfers(&tx_result).is_empty());
    }

    // ── E-CO1 regression lock: token_transfers.timestamp is the block's real
    // time, NOT ingest-time NOW(). ──
    //
    // The worker historically bound SQL `NOW()` for `token_transfers.timestamp`
    // while discarding the block ts it fetched — so a transfer got a wall-clock
    // stamp ~seconds after (and, on a backfill, DAYS after) the block it
    // belongs to. `eth_block.params_block_timestamp` is a NUMERIC unix-epoch
    // (rome-via-sync migration 0004); the fix fetches it `::FLOAT8` and converts
    // via `epoch_to_dt` (identical decode to address_stats / cross_chain), then
    // binds that value. A `None` epoch (block not yet indexed) → `None` → NULL
    // column (honest absence), never a fabricated stamp. These lock the
    // conversion the bind depends on; the behavioural claim (stored transfer ts
    // == block ts) is verified by the live Hadrian trace.

    /// A block mined at a given unix epoch converts to exactly that instant —
    /// the value bound into `token_transfers.timestamp`, not the wall clock.
    #[test]
    fn epoch_to_dt_binds_block_time_not_now() {
        let dt = epoch_to_dt(Some(1_780_558_086.0)).expect("a valid epoch must convert");
        assert_eq!(
            dt.timestamp(),
            1_780_558_086,
            "bound transfer ts must equal the block epoch, not NOW()"
        );
    }

    /// No indexed block ts → `None` → NULL column. The fix must never fall back
    /// to a fabricated wall-clock stamp when the block time is unknown.
    #[test]
    fn epoch_to_dt_none_binds_null_not_now() {
        assert!(
            epoch_to_dt(None).is_none(),
            "absent block ts must bind NULL (honest), not a fabricated NOW()"
        );
    }

    /// Fractional epoch (sub-second) truncates to the whole second — matches the
    /// address_stats / cross_chain decode, not rounding.
    #[test]
    fn epoch_to_dt_truncates_fraction() {
        let dt = epoch_to_dt(Some(1_780_558_086.987)).expect("fractional epoch converts");
        assert_eq!(dt.timestamp(), 1_780_558_086, "sub-second is truncated, not rounded");
    }

    // ── E-CO3 regression lock: holder-balance updates are EXACTLY-ONCE ──
    //
    // Balance deltas are NON-idempotent (a repeated debit/credit double-counts).
    // [F3/A″] The GREATEST(0,…) clamp that used to hide the resulting underflow
    // drift is gone — drift now surfaces as a visible negative (a missing
    // transfer), invisible to readers via the balance>0 index but auditable.
    // The `token_transfers` INSERT (ON CONFLICT DO NOTHING) is the
    // idempotency guard: rows_affected()==1 means this (chain,tx,log) is being
    // recorded for the first time in THIS committed transaction, so the delta
    // must be applied; rows_affected()==0 means a re-processed batch (crash
    // before the cursor advanced) whose transfer row already exists — the delta
    // was already committed, so it must be skipped. The insert + deltas run in
    // one transaction so the gate + the writes commit atomically (a mid-row
    // crash rolls back cleanly and replays as a fresh insert), which is what
    // makes it exactly-once rather than "at most twice minus a race".
    //
    // These lock the gate the balance writes are conditioned on. Ship-time
    // behaviour (no double-count under redelivery) is proven by construction +
    // code review of the atomic txn; it can't be crash-replay-traced on a live
    // chain without invasive cursor surgery.

    /// A newly-inserted transfer (rows_affected==1) applies its balance delta.
    #[test]
    fn balance_delta_applies_on_fresh_insert() {
        assert!(
            transfer_was_newly_inserted(1),
            "fresh insert (rows_affected==1) → apply the balance delta"
        );
    }

    /// An ON CONFLICT DO NOTHING no-op (rows_affected==0) skips the delta — the
    /// transfer already exists, so the delta was already applied. Re-applying is
    /// the E-CO3 double-count bug.
    #[test]
    fn balance_delta_skipped_on_conflict_noop() {
        assert!(
            !transfer_was_newly_inserted(0),
            "ON CONFLICT no-op (rows_affected==0) → skip; delta already applied"
        );
    }
}
