/// Search indexer worker.
///
/// Builds and maintains `search_index` from multiple base and derived tables:
/// - Tx hashes (weight 100) — from evm_tx via eth_block_txs
/// - Block numbers (weight 90) — from eth_block
/// - Token addresses + symbols (weight 80) — from token_metadata
/// - Contract labels (weight 60) — from contract_labels (an `address`-type
///   entity labeled by the protocol name, so "Uniswap V2" finds the router)
/// - Addresses (weight 50) — from address_stats
///
/// Uses the `pg_trgm` GIN index for efficient similarity search.
/// Each entity is upserted with a display_label that the search endpoint queries.
///
/// Tx/block passes each keep a real slot cursor (`enrich_cursors` rows
/// `search_indexer` / `search_indexer_blocks`) plus the shared miss valve
/// (`rpc_verdict`) and `batch_cursor::plan_batch`, so one pass's DB failure or
/// a LIMIT-cut boundary slot can no longer drag the other's cursor past
/// unexamined rows. Token/address/label passes have no slot to key off of;
/// they walk the table by primary key via an in-memory keyset (`next_keyset`)
/// that wraps to the start once a chunk comes back short, covering the whole
/// table over successive polls instead of only its first `batch_size` rows.
use sqlx::PgPool;
use std::time::Duration;
use tracing::{debug, warn};

/// Weight for a labeled contract indexed as an `address`-type search entity.
/// Sits between token (80) and the bare-address pass (50): a contract with a
/// real protocol label should outrank the plain-address entry it upserts over
/// (both share `entity_type='address'`, `entity_id=address`), while still
/// ranking below named tokens.
const CONTRACT_LABEL_WEIGHT: i32 = 60;

/// Shape a `contract_labels` row into an `address`-type search entity:
/// `(entity_id, display_label, weight)`. Returns `None` when the row has no
/// `display_label` (an EOA cached as seen-but-unlabeled, or a
/// reachable-but-unidentified contract) — there's nothing searchable, and
/// indexing it would clobber the richer address_stats entry with an empty
/// label.
///
/// Pure function — fully unit-testable, no I/O.
fn contract_label_entity(address: &str, display_label: Option<String>) -> Option<(String, String, i32)> {
    let label = display_label?;
    if label.trim().is_empty() {
        return None;
    }
    Some((address.to_string(), label, CONTRACT_LABEL_WEIGHT))
}

/// Wrapping keyset advance for a full-table refresh pass (tokens / addresses /
/// labels). `returned` rows came back for `WHERE key > prev ORDER BY key LIMIT
/// chunk`. Fewer than `chunk` ⇒ the end of the table was reached → wrap (return
/// empty, restart from the beginning next poll); otherwise continue from
/// `last_key`. This paces full coverage of tables larger than one chunk across
/// polls (grounding: address_stats/contract_labels ≫ the per-poll chunk, so the
/// old un-paged `LIMIT` left most rows never indexed). Pure — unit-testable.
fn next_keyset(returned: usize, chunk: usize, last_key: &str) -> String {
    if returned < chunk {
        String::new()
    } else {
        last_key.to_string()
    }
}

/// Row shape for the tokens keyset pass: `(address, symbol, name)`.
type TokenRow = (String, Option<String>, Option<String>);

pub async fn run(
    pool: PgPool,
    chain_id: i64,
    poll_interval: Duration,
    batch_size: i64,
) -> anyhow::Result<()> {
    const MAX_HOLD: u32 = 30;
    let mut tx_hold = super::rpc_verdict::HoldState::clear();
    let mut block_hold = super::rpc_verdict::HoldState::clear();
    // Keyset cursors for the three full-table refresh passes (tokens /
    // addresses / labels). Deliberately in-memory only — NOT persisted to
    // enrich_cursors: every upsert is ON CONFLICT DO UPDATE, so a restart
    // just resumes the wrap from the start at no correctness cost.
    let mut token_key = String::new();
    let mut addr_key = String::new();
    let mut label_key = String::new();

    loop {
        // ── Tx hashes (own cursor 'search_indexer') ─────────────────────────
        let tx_cursor: i64 = sqlx::query_scalar(
            "SELECT COALESCE(last_processed, 0)
             FROM rome_via.enrich_cursors
             WHERE chain_id = $1 AND worker = 'search_indexer'",
        )
        .bind(chain_id)
        .fetch_optional(&pool)
        .await?
        .unwrap_or(0);

        let tx_fetch: Result<Vec<(String, i64)>, sqlx::Error> = sqlx::query_as(
            "SELECT t.tx_hash, t.slot_number
             FROM rome_via.eth_block_txs t
             WHERE t.chain_id = $1 AND t.slot_number > $2
             ORDER BY t.slot_number ASC, t.tx_idx ASC
             LIMIT $3",
        )
        .bind(chain_id)
        .bind(tx_cursor)
        .bind(batch_size)
        .fetch_all(&pool)
        .await;

        let mut tx_count = 0usize;
        match tx_fetch {
            Err(e) => {
                warn!(worker = "search_indexer:tx", error = %e,
                      "fetch failed — skipping tx pass this iteration");
            }
            Ok(tx_rows) if !tx_rows.is_empty() => {
                tx_count = tx_rows.len();
                let slots: Vec<i64> = tx_rows.iter().map(|r| r.1).collect();
                let plan = super::batch_cursor::plan_batch(&slots, batch_size as usize);
                if plan.giant_slot {
                    warn!(worker = "search_indexer:tx", slot = slots[0],
                          "single slot fills an entire batch; tail beyond batch_size cannot be deferred (unexpected on one-block-per-slot chains)");
                }

                let mut earliest_miss: Option<(i64, super::rpc_verdict::MissKind)> = None;
                for (tx_hash, slot_number) in &tx_rows[..plan.process_count] {
                    if let Err(e) =
                        upsert_entry(&pool, chain_id, "tx", tx_hash, tx_hash, 100).await
                    {
                        if earliest_miss.is_none() {
                            earliest_miss =
                                Some((*slot_number, super::rpc_verdict::db_miss_kind(&e)));
                        }
                    }
                }

                let outcome = super::rpc_verdict::valve_persist_cursor(
                    plan.new_cursor,
                    earliest_miss,
                    tx_hold,
                    MAX_HOLD,
                );
                tx_hold = outcome.hold;
                if outcome.gave_up {
                    warn!(worker = "search_indexer:tx",
                        "terminal miss unresolved for MAX_HOLD polls; advancing past stuck slot [F6]");
                }

                let _ = sqlx::query(
                    "INSERT INTO rome_via.enrich_cursors
                         (chain_id, worker, last_processed, last_processed_at)
                     VALUES ($1, 'search_indexer', $2, NOW())
                     ON CONFLICT (chain_id, worker) DO UPDATE
                         SET last_processed    = EXCLUDED.last_processed,
                             last_processed_at = NOW()",
                )
                .bind(chain_id)
                .bind(outcome.persist)
                .execute(&pool)
                .await;
            }
            Ok(_) => {}
        }

        // ── Blocks (own cursor 'search_indexer_blocks' — a new worker key,
        // no migration needed; starts at 0, so the first run after this
        // change is a one-time re-index of every block, which is idempotent
        // via the upsert) ─────────────────────────────────────────────────
        let block_cursor: i64 = sqlx::query_scalar(
            "SELECT COALESCE(last_processed, 0)
             FROM rome_via.enrich_cursors
             WHERE chain_id = $1 AND worker = 'search_indexer_blocks'",
        )
        .bind(chain_id)
        .fetch_optional(&pool)
        .await?
        .unwrap_or(0);

        let block_fetch: Result<Vec<(Option<i64>, i64)>, sqlx::Error> = sqlx::query_as(
            "SELECT params_number, slot_number
             FROM rome_via.eth_block
             WHERE chain_id = $1 AND slot_number > $2 AND params_number IS NOT NULL
             ORDER BY slot_number ASC, slot_block_idx ASC
             LIMIT $3",
        )
        .bind(chain_id)
        .bind(block_cursor)
        .bind(batch_size)
        .fetch_all(&pool)
        .await;

        let mut block_count = 0usize;
        match block_fetch {
            Err(e) => {
                warn!(worker = "search_indexer:block", error = %e,
                      "fetch failed — skipping block pass this iteration");
            }
            Ok(block_rows) if !block_rows.is_empty() => {
                block_count = block_rows.len();
                let slots: Vec<i64> = block_rows.iter().map(|r| r.1).collect();
                let plan = super::batch_cursor::plan_batch(&slots, batch_size as usize);
                if plan.giant_slot {
                    warn!(worker = "search_indexer:block", slot = slots[0],
                          "single slot fills an entire batch; tail beyond batch_size cannot be deferred (unexpected on one-block-per-slot chains)");
                }

                let mut earliest_miss: Option<(i64, super::rpc_verdict::MissKind)> = None;
                for (block_number, slot_number) in &block_rows[..plan.process_count] {
                    if let Some(n) = block_number {
                        let label = format!("Block #{n}");
                        let entity_id = n.to_string();
                        if let Err(e) =
                            upsert_entry(&pool, chain_id, "block", &entity_id, &label, 90).await
                        {
                            if earliest_miss.is_none() {
                                earliest_miss =
                                    Some((*slot_number, super::rpc_verdict::db_miss_kind(&e)));
                            }
                        }
                    }
                }

                let outcome = super::rpc_verdict::valve_persist_cursor(
                    plan.new_cursor,
                    earliest_miss,
                    block_hold,
                    MAX_HOLD,
                );
                block_hold = outcome.hold;
                if outcome.gave_up {
                    warn!(worker = "search_indexer:block",
                        "terminal miss unresolved for MAX_HOLD polls; advancing past stuck slot [F6]");
                }

                let _ = sqlx::query(
                    "INSERT INTO rome_via.enrich_cursors
                         (chain_id, worker, last_processed, last_processed_at)
                     VALUES ($1, 'search_indexer_blocks', $2, NOW())
                     ON CONFLICT (chain_id, worker) DO UPDATE
                         SET last_processed    = EXCLUDED.last_processed,
                             last_processed_at = NOW()",
                )
                .bind(chain_id)
                .bind(outcome.persist)
                .execute(&pool)
                .await;
            }
            Ok(_) => {}
        }

        // ── Tokens (keyset wrap — replaces the old un-paged LIMIT, which
        // indexed only an arbitrary ~batch_size subset of the table forever)
        let token_fetch: Result<Vec<TokenRow>, sqlx::Error> =
            sqlx::query_as(
                "SELECT address, symbol, name
                 FROM rome_via.token_metadata
                 WHERE chain_id = $1
                   AND symbol IS NOT NULL
                   AND address > $2
                 ORDER BY address ASC
                 LIMIT $3",
            )
            .bind(chain_id)
            .bind(&token_key)
            .bind(batch_size)
            .fetch_all(&pool)
            .await;

        match token_fetch {
            Err(e) => {
                warn!(worker = "search_indexer:tokens", error = %e,
                      "fetch failed — skipping tokens pass this iteration");
            }
            Ok(token_rows) => {
                for (address, symbol, name) in &token_rows {
                    let sym = symbol.as_deref().unwrap_or("");
                    let nm = name.as_deref().unwrap_or("");
                    let label = format!("{sym} {nm} {address}");
                    if let Err(e) =
                        upsert_entry(&pool, chain_id, "token", address, &label, 80).await
                    {
                        // No slot cursor to hold here — a failed upsert
                        // self-heals on the next wrap through this address.
                        warn!(worker = "search_indexer:tokens", %address, error = %e,
                              "upsert failed — self-heals next wrap");
                    }
                }
                let last = token_rows.last().map(|r| r.0.as_str()).unwrap_or("");
                token_key = next_keyset(token_rows.len(), batch_size as usize, last);
            }
        }

        // ── Addresses (keyset wrap) ──────────────────────────────────────────
        let addr_fetch: Result<Vec<(String,)>, sqlx::Error> = sqlx::query_as(
            "SELECT address
             FROM rome_via.address_stats
             WHERE chain_id = $1
               AND address > $2
             ORDER BY address ASC
             LIMIT $3",
        )
        .bind(chain_id)
        .bind(&addr_key)
        .bind(batch_size)
        .fetch_all(&pool)
        .await;

        match addr_fetch {
            Err(e) => {
                warn!(worker = "search_indexer:addresses", error = %e,
                      "fetch failed — skipping addresses pass this iteration");
            }
            Ok(addr_rows) => {
                for (address,) in &addr_rows {
                    if let Err(e) =
                        upsert_entry(&pool, chain_id, "address", address, address, 50).await
                    {
                        warn!(worker = "search_indexer:addresses", %address, error = %e,
                              "upsert failed — self-heals next wrap");
                    }
                }
                let last = addr_rows.last().map(|r| r.0.as_str()).unwrap_or("");
                addr_key = next_keyset(addr_rows.len(), batch_size as usize, last);
            }
        }

        // ── Contract labels (keyset wrap) ────────────────────────────────────
        // Index each labeled contract as an `address`-type entity using its
        // protocol display_label (so "Uniswap V2" / "Compound" / a router name
        // is searchable). Runs AFTER the bare-address pass above so a labeled
        // contract's richer entry (weight 60, protocol label) wins the UPSERT
        // over the plain-address entry (weight 50, label = address) on the same
        // (chain_id, 'address', address) key. NULL-label rows (EOAs / unnamed
        // contracts) are filtered out in SQL and by `contract_label_entity`.
        let label_fetch: Result<Vec<(String, Option<String>)>, sqlx::Error> = sqlx::query_as(
            "SELECT address, display_label
             FROM rome_via.contract_labels
             WHERE chain_id = $1
               AND display_label IS NOT NULL
               AND address > $2
             ORDER BY address ASC
             LIMIT $3",
        )
        .bind(chain_id)
        .bind(&label_key)
        .bind(batch_size)
        .fetch_all(&pool)
        .await;

        match label_fetch {
            Err(e) => {
                warn!(worker = "search_indexer:labels", error = %e,
                      "fetch failed — skipping labels pass this iteration");
            }
            Ok(label_rows) => {
                for (address, display_label) in &label_rows {
                    if let Some((entity_id, label, weight)) =
                        contract_label_entity(address, display_label.clone())
                    {
                        if let Err(e) = upsert_entry(
                            &pool, chain_id, "address", &entity_id, &label, weight,
                        )
                        .await
                        {
                            warn!(worker = "search_indexer:labels", %address, error = %e,
                                  "upsert failed — self-heals next wrap");
                        }
                    }
                }
                // Keyset advances by the row's address regardless of whether
                // contract_label_entity returned Some — an empty/whitespace
                // label (filtered out above) still consumes a keyset slot
                // that must be advanced past, or the wrap would stall on it.
                let last = label_rows.last().map(|r| r.0.as_str()).unwrap_or("");
                label_key = next_keyset(label_rows.len(), batch_size as usize, last);
            }
        }

        debug!(
            chain_id,
            txs = tx_count,
            blocks = block_count,
            "search_indexer batch complete"
        );

        tokio::time::sleep(poll_interval).await;
    }
}

async fn upsert_entry(
    pool: &PgPool,
    chain_id: i64,
    entity_type: &str,
    entity_id: &str,
    display_label: &str,
    weight: i32,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO rome_via.search_index
             (chain_id, entity_type, entity_id, display_label, weight)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (chain_id, entity_type, entity_id) DO UPDATE
             SET display_label = EXCLUDED.display_label,
                 weight        = EXCLUDED.weight",
    )
    .bind(chain_id)
    .bind(entity_type)
    .bind(entity_id)
    .bind(display_label)
    .bind(weight)
    .execute(pool)
    .await
    .map(|_| ())
}

// ─────────────────────────────────────────────────────────────
// Unit tests
// ─────────────────────────────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;

    // ── next_keyset: wrapping full-table coverage ──
    #[test]
    fn keyset_continues_on_full_chunk() {
        // A full chunk means more rows remain → continue from the last key.
        assert_eq!(next_keyset(500, 500, "0xabc"), "0xabc");
    }
    #[test]
    fn keyset_wraps_on_short_chunk() {
        // Fewer than a chunk means end reached → wrap to the start.
        assert_eq!(next_keyset(200, 500, "0xabc"), "");
    }
    #[test]
    fn keyset_wraps_on_empty() {
        // No rows (empty table / past the end) → wrap.
        assert_eq!(next_keyset(0, 500, "0xdef"), "");
    }

    #[test]
    fn block_label_format() {
        let n: i64 = 12_401_118;
        let label = format!("Block #{n}");
        assert_eq!(label, "Block #12401118");
    }

    #[test]
    fn token_label_format() {
        let sym = "USDC";
        let nm = "USD Coin";
        let addr = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
        let label = format!("{sym} {nm} {addr}");
        assert!(label.contains("USDC"));
        assert!(label.contains(addr));
    }

    /// A contract-label row with a non-null display_label maps to an
    /// `address`-type search entity keyed by the address, labeled by the
    /// protocol name (so "Uniswap V2" finds the router). Weight sits between
    /// token (80) and the bare-address pass (50) so a labeled contract
    /// outranks the plain-address entry it upserts over.
    #[test]
    fn contract_label_search_entity_maps_label_and_weight() {
        let addr = "0xb342f70d56855f11b0721fcbe2804a200d0f0533";
        let entity = contract_label_entity(addr, Some("Uniswap V2".to_string()));
        let (entity_id, label, weight) = entity.expect("labeled row must index");
        assert_eq!(entity_id, addr);
        assert_eq!(label, "Uniswap V2");
        assert_eq!(weight, CONTRACT_LABEL_WEIGHT);
        assert!(weight > 50 && weight < 80, "weight between address and token");
    }

    /// A contract-label row whose display_label is NULL (an EOA cached as
    /// seen-but-unlabeled, or a reachable-but-unidentified contract) is NOT
    /// indexed — there's nothing searchable. It would also clobber the richer
    /// address_stats entry. Returns None → the pass skips it.
    #[test]
    fn contract_label_search_entity_skips_null_label() {
        let addr = "0x0000000000000000000000000000000000000001";
        assert!(contract_label_entity(addr, None).is_none());
    }
}
