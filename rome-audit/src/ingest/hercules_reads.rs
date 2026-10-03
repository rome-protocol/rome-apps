//! Plain, read-only queries against Hercules' source Postgres DB (IMPL-PLAN
//! §5.2/§5.4) — mirrors the shape rome-via-sync/rome-via-enrich already use
//! (free functions taking `&PgPool` directly, e.g. `rome-via-sync/src/sync.rs`'s
//! `sync_cycle`), NOT a bespoke trait-object abstraction. The pipeline calls
//! these directly against the real Hercules `source` pool; tests call the
//! SAME functions against a seeded local Postgres.
//!
//! Schema notes (verified against `rome-sdk/rome-evm-client/src/indexer/pg_storage`,
//! see the P1 report for the full diff against what the IMPL-PLAN assumed):
//! `evm_log` has no `chain_id` (one Hercules DB = one chain; `chain_id` is
//! supplied externally by the audit pipeline when it writes `audit.chain_event`).
//! `evm_log.address`/`topic0..3` are lowercase `0x`-hex `VARCHAR`, not `BYTEA`.
//! Block-level `log_index` is NOT a stored column — it's
//! `evm_log.log_ordinal` (0-based within the tx) + `receipt_params.first_log_index`
//! (the block-cumulative offset for that tx), which is why `tx_receipt_info`
//! exists as its own read.

use std::collections::BTreeSet;

use ethers::types::{H256, U256, U64};
use serde::Deserialize;
use sqlx::PgPool;

use super::watermark::{SlotDigest, SlotStatusKind};
use crate::resolve::rpc::LogEntry;

/// One matched row from `evm_log` — an indexed row, not the full log
/// payload. Indexed args (topic1..3) are already here; a non-indexed arg's
/// `data` is a SEPARATE, targeted read (`log_data`), fetched only when the
/// registered event descriptor actually has a non-indexed arg.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchedLog {
    pub tx_hash: String, // 0x-hex, as stored (VARCHAR(66))
    pub log_ordinal: i32,
    pub address: String,
    pub topic0: String,
    pub topic1: Option<String>,
    pub topic2: Option<String>,
    pub topic3: Option<String>,
}

/// Per-tx receipt facts needed to complete a `chain_event` row for every
/// log in that tx. ONE read serves every matched log in the same tx —
/// this is the "receipt_params" JSONB read (small: blockhash/block_number/
/// tx_index/first_log_index), NOT the big `tx_result` payload that holds
/// every log's full data. A fully-indexed event (no non-indexed args) still
/// needs THIS read (there's no other source for log_index/block_hash/
/// block_timestamp/tx_index) — it just never needs `log_data` on top of it.
/// See the P1 report for why this nuances the brief's "no JSONB read" framing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxReceiptInfo {
    pub signer: Option<String>, // evm_tx.from_address; None = not yet backfilled (pre-#tx_from_index rows)
    pub block_hash: String,     // receipt_params.blockhash
    pub block_number: i64,      // receipt_params.block_number
    pub block_timestamp: i64,   // eth_block.params.block_timestamp
    pub tx_index: i32,          // receipt_params.tx_index
    pub first_log_index: i64,   // receipt_params.first_log_index
}

/// A slot's status + digest inputs, in one read (§5.2 conditions i + iii).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotObservation {
    pub status: Option<SlotStatusKind>,
    pub digest: SlotDigest,
}

/// Mirrors `rome-sdk` `rome_evm_client::indexer::pg_storage::types::ReceiptParams`
/// field-for-field (deliberately NOT a path-dependency on rome-sdk — that
/// would pull the whole SDK in for one struct; this is the tiny duplicate,
/// flagged rather than silently diverging). Deserializes the real wire
/// format: ethers' `H256`/`U64`/`U256` serialize as `0x`-hex strings.
#[derive(Debug, Deserialize)]
struct ReceiptParamsJson {
    blockhash: H256,
    block_number: U64,
    tx_index: i64,
    first_log_index: U256,
}

/// `sol_slot` status + digest inputs for one slot. `None` status means
/// Hercules has no `sol_slot` row for it yet.
pub async fn slot_observation(source: &PgPool, slot: i64) -> anyhow::Result<SlotObservation> {
    // `status` is Hercules' custom `slotstatus` Postgres ENUM — sqlx will NOT
    // decode an enum column into `Option<String>` (it errors "Rust
    // Option<String> (as SQL TEXT) is not compatible with SQL type
    // `slotstatus`", which broke every live forward-tick). Cast enum→text in
    // SQL so the `match s.as_deref()` mapping below stays unchanged.
    let slot_row: Option<(Option<String>, Option<String>)> =
        sqlx::query_as("SELECT status::text, blockhash FROM sol_slot WHERE slot_number = $1")
            .bind(slot)
            .fetch_optional(source)
            .await?;

    let status = slot_row.as_ref().and_then(|(s, _)| match s.as_deref() {
        Some("Processed") => Some(SlotStatusKind::Processed),
        Some("Confirmed") => Some(SlotStatusKind::Confirmed),
        Some("Finalized") => Some(SlotStatusKind::Finalized),
        _ => None,
    });
    let sol_blockhash = slot_row.and_then(|(_, h)| h);

    let block_rows: Vec<(i32, Option<String>, i64)> = sqlx::query_as(
        r#"
        SELECT eb.slot_block_idx,
               (eb.params).blockhash,
               (SELECT COUNT(*) FROM eth_block_txs ebt
                 WHERE ebt.slot_number = eb.slot_number AND ebt.slot_block_idx = eb.slot_block_idx)
        FROM eth_block eb
        WHERE eb.slot_number = $1 AND eb.params IS NOT NULL
        "#,
    )
    .bind(slot)
    .fetch_all(source)
    .await?;

    let mut eth_block_hashes = std::collections::BTreeSet::new();
    let mut tx_count_by_block_idx = std::collections::BTreeMap::new();
    for (idx, hash, count) in block_rows {
        if let Some(h) = hash {
            eth_block_hashes.insert(h);
        }
        tx_count_by_block_idx.insert(idx, count);
    }

    Ok(SlotObservation {
        status,
        digest: SlotDigest {
            sol_blockhash,
            eth_block_hashes,
            tx_count_by_block_idx,
        },
    })
}

/// The EVM-block-number frontier ingest has actually SCANNED through, as of
/// Solana slot `through_slot` — `MAX((eth_block.params).number)` over every
/// produced block at or before that slot. Read from `source` (Hercules), NOT
/// `target` — P3c H1: the audit-side `audit.chain_event` table only records
/// blocks that had a MATCHED log under whatever source map was active at the
/// time, so `MAX(chain_event.block_number)` under-reports how far ingest has
/// actually scanned (a chain-wide-visible source registered between the last
/// captured event and the true scan frontier would be silently missed by
/// gap detection). `through_slot = -1` (no watermark row yet) ⇒ `None`
/// (nothing scanned) ⇒ callers default to `-1`, matching the "fresh chain ⇒
/// no gaps" fixed point.
pub async fn block_frontier_through_slot(
    source: &PgPool,
    through_slot: i64,
) -> anyhow::Result<Option<i64>> {
    if through_slot < 0 {
        return Ok(None);
    }
    let (max,): (Option<i64>,) = sqlx::query_as(
        "SELECT MAX((params).number) FROM eth_block WHERE slot_number <= $1 AND params IS NOT NULL",
    )
    .bind(through_slot)
    .fetch_one(source)
    .await?;
    Ok(max)
}

/// The Solana slot SPAN covering EVM blocks `[from_block, to_block]`
/// inclusive (P4a deliverable 2 — the backfill executor's block-range→
/// slot-range translation), via the SAME `(params).number` join
/// `block_frontier_through_slot` uses. `None` when no PRODUCED block's
/// number falls in that range (nothing to backfill — a defensively empty
/// range, not an error).
///
/// INVARIANT (mode-agnostic): block number is strictly increasing in
/// `(slot_number, slot_block_idx)` order — plain single_state assigns numbers
/// sequentially over an ascending `(slot, idx)` BTreeMap; slot_aligned uses
/// `number == slot`; reorgs are suffix-deletes, so the invariant holds at
/// every instant. Therefore the slot of the LOWEST in-range block number
/// equals `MIN(slot_number)` over that range, and the slot of the HIGHEST
/// equals `MAX(slot_number)`. That lets us replace a
/// `MIN/MAX(slot_number) … WHERE (params).number BETWEEN` aggregate — which
/// the planner served as an ordered LIMIT-1 over `eth_block_pkey`
/// (slot_number order), filter-scanning millions of rows because it can't use
/// the `eth_block_number` btree on `((params).number)` while ordering by
/// slot_number — with two ordered subqueries whose range qual AND `ORDER BY`
/// both match that indexed expression, so each is an ordered Index Scan + one
/// heap fetch (~ms vs ~2.3s on live Hadrian). One statement = one snapshot, so
/// both bounds are consistent: either both subqueries are NULL (empty range,
/// incl. `from > to`) or both are set — preserving the `None` semantics
/// unchanged (empty/degenerate range ⇒ `None` ⇒ remediated-0). The range is
/// always `≤ watermark_at_detection` — finalized territory, not the live tip.
pub async fn slot_range_for_blocks(
    source: &PgPool,
    from_block: i64,
    to_block: i64,
) -> anyhow::Result<Option<(i64, i64)>> {
    let row: (Option<i64>, Option<i64>) = sqlx::query_as(
        r#"
        SELECT
          (SELECT slot_number FROM eth_block
             WHERE (params).number >= $1 AND (params).number <= $2
             ORDER BY (params).number ASC  LIMIT 1),
          (SELECT slot_number FROM eth_block
             WHERE (params).number >= $1 AND (params).number <= $2
             ORDER BY (params).number DESC LIMIT 1)
        "#,
    )
    .bind(from_block)
    .bind(to_block)
    .fetch_one(source)
    .await?;
    Ok(match row {
        (Some(min), Some(max)) => Some((min, max)),
        _ => None,
    })
}

/// `MIN((eth_block.params).number) WHERE params IS NOT NULL` — the RETAINED
/// FLOOR: the lowest EVM block number Hercules still holds (H1). Hercules
/// prunes old `sol_slot`/`eth_block` history (on the live Hadrian deploy it
/// retains only from slot ~470.7M), so a backfill gap whose `from_block`
/// predates this floor can NEVER be fully covered — its low end was pruned.
/// `backfill::run_backfill_once` uses this to flag such a gap `source_pruned`
/// loudly rather than marking it falsely clean with 0 events. `None` = the
/// source has produced no block at all (a genuinely empty source).
pub async fn min_produced_block(source: &PgPool) -> anyhow::Result<Option<i64>> {
    let (min,): (Option<i64>,) =
        sqlx::query_as("SELECT MIN((params).number) FROM eth_block WHERE params IS NOT NULL")
            .fetch_one(source)
            .await?;
    Ok(min)
}

/// `MAX(eth_block.slot_number) WHERE params IS NOT NULL` — production head
/// clearance (§5.2-ii, the empty/gap discriminator).
pub async fn max_produced_slot(source: &PgPool) -> anyhow::Result<Option<i64>> {
    let (max,): (Option<i64>,) =
        sqlx::query_as("SELECT MAX(slot_number) FROM eth_block WHERE params IS NOT NULL")
            .fetch_one(source)
            .await?;
    Ok(max)
}

/// `MAX(sol_slot.slot_number) WHERE status = 'Finalized'` — the finalized
/// tip Hercules currently reports (§5.2-i).
pub async fn finalized_tip(source: &PgPool) -> anyhow::Result<Option<i64>> {
    let (max,): (Option<i64>,) =
        sqlx::query_as("SELECT MAX(slot_number) FROM sol_slot WHERE status = 'Finalized'")
            .fetch_one(source)
            .await?;
    Ok(max)
}

/// `evm_log` rows at exactly `slot` whose `(address, topic0)` pair is in
/// the caller-supplied filter sets (§5.4: filter by the REGISTERED topic0
/// set + the resolved address set — an unregistered event is simply never
/// fetched, which is the efficiency half of the phantom guard; the
/// decoder's loud-reject half still guards a registered-but-wrong-shape topic).
pub async fn matched_logs_at_slot(
    source: &PgPool,
    slot: i64,
    addresses: &BTreeSet<String>,
    topic0_set: &BTreeSet<String>,
) -> anyhow::Result<Vec<MatchedLog>> {
    let addr_vec: Vec<String> = addresses.iter().cloned().collect();
    let topic_vec: Vec<String> = topic0_set.iter().cloned().collect();

    type MatchedLogRow = (
        String,
        i32,
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
    );
    let rows: Vec<MatchedLogRow> = sqlx::query_as(
        r#"
        SELECT tx_hash, log_ordinal, address, topic0, topic1, topic2, topic3
        FROM evm_log
        WHERE slot_number = $1
          AND address = ANY($2::text[])
          AND topic0 = ANY($3::text[])
        ORDER BY tx_hash, log_ordinal
        "#,
    )
    .bind(slot)
    .bind(&addr_vec)
    .bind(&topic_vec)
    .fetch_all(source)
    .await?;

    Ok(rows
        .into_iter()
        .map(
            |(tx_hash, log_ordinal, address, topic0, topic1, topic2, topic3)| MatchedLog {
                tx_hash,
                log_ordinal,
                address,
                topic0,
                topic1,
                topic2,
                topic3,
            },
        )
        .collect())
}

/// The DISTINCT slots in `[from_slot, to_slot]` that carry AT LEAST ONE
/// matched `evm_log` for the caller's `(address, topic0)` sets — one ranged
/// query, ascending (H2). The backfill EXECUTOR uses this so a gap spanning
/// millions of Solana slots isn't one point-query per slot (`matched_logs_at_slot`
/// × range width) blocking the tick loop: it queries the whole range ONCE,
/// then re-drives the SAME per-slot decode/insert path (`pipeline::ingest_slot`)
/// only for the handful of NON-EMPTY slots — the empty slots (the vast
/// majority of a wide gap) are never touched. LIVE ingest keeps its per-slot
/// path unchanged (the watermark needs per-slot finality/head-clearance).
///
/// Matches `matched_logs_at_slot`'s address+topic0 predicate EXACTLY (same
/// `= ANY($_::text[])` filter) — the only difference is `slot_number BETWEEN`
/// vs `=`, and that the slot is returned so the caller can group by it.
pub async fn matched_log_slots_in_range(
    source: &PgPool,
    from_slot: i64,
    to_slot: i64,
    addresses: &BTreeSet<String>,
    topic0_set: &BTreeSet<String>,
) -> anyhow::Result<Vec<i64>> {
    let addr_vec: Vec<String> = addresses.iter().cloned().collect();
    let topic_vec: Vec<String> = topic0_set.iter().cloned().collect();

    let rows: Vec<(i64,)> = sqlx::query_as(
        r#"
        SELECT DISTINCT slot_number
        FROM evm_log
        WHERE slot_number BETWEEN $1 AND $2
          AND address = ANY($3::text[])
          AND topic0 = ANY($4::text[])
        ORDER BY slot_number
        "#,
    )
    .bind(from_slot)
    .bind(to_slot)
    .bind(&addr_vec)
    .bind(&topic_vec)
    .fetch_all(source)
    .await?;

    Ok(rows.into_iter().map(|(s,)| s).collect())
}

/// Receipt facts for one tx at one slot (see `TxReceiptInfo` docs). `None`
/// if the tx's block hasn't been produced yet (shouldn't happen for a slot
/// that already passed the watermark, but the caller must treat it as a
/// hold, not a panic, if it ever does).
pub async fn tx_receipt_info(
    source: &PgPool,
    slot: i64,
    tx_hash: &str,
) -> anyhow::Result<Option<TxReceiptInfo>> {
    let row: Option<(Option<String>, serde_json::Value, String)> = sqlx::query_as(
        r#"
        SELECT et.from_address, etr.receipt_params, (eb.params).block_timestamp::text
        FROM evm_tx et
        JOIN evm_tx_result etr ON etr.tx_hash = et.tx_hash AND etr.slot_number = $1
        JOIN eth_block_txs ebt ON ebt.slot_number = etr.slot_number AND ebt.tx_hash = etr.tx_hash
        JOIN eth_block eb ON eb.slot_number = ebt.slot_number AND eb.slot_block_idx = ebt.slot_block_idx
        WHERE et.tx_hash = $2 AND etr.slot_number = $1
        "#,
    )
    .bind(slot)
    .bind(tx_hash)
    .fetch_optional(source)
    .await?;

    let Some((signer, receipt_params_json, block_timestamp_text)) = row else {
        return Ok(None);
    };

    let receipt_params: ReceiptParamsJson = serde_json::from_value(receipt_params_json)?;
    let block_timestamp: i64 = block_timestamp_text.parse()?;

    Ok(Some(TxReceiptInfo {
        signer,
        block_hash: format!("{:#x}", receipt_params.blockhash),
        block_number: receipt_params.block_number.as_u64() as i64,
        block_timestamp,
        tx_index: receipt_params.tx_index as i32,
        first_log_index: receipt_params.first_log_index.as_u64() as i64,
    }))
}

/// Every historical `evm_log` at `address` matching `topic0`, shaped as the
/// resolver's [`LogEntry`] — the DB-backed equivalent of the proxy's
/// `eth_getLogs` over `[Earliest, Latest]`, which the production proxy caps
/// at 12,000 blocks (`-32005 block range too wide`) so the resolver's
/// first pass can never complete a real log walk through it. The proxy's
/// `eth_getLogs` is itself backed by THIS `evm_log` table, so serving the
/// resolver's log history straight from the indexed source is the same data
/// with no range cap and no rate-limited public edge (IMPL-PLAN §595
/// "nothing to re-fetch from RPC"); STATE reads (`eth_call` /
/// `eth_getStorageAt`) stay on live RPC per the #136 live-authority rule.
///
/// Mirrors the ingest path's own field derivation EXACTLY (see
/// `pipeline::try_ingest_log`): `block_number` / `tx_index` /
/// `first_log_index` come from the joined `evm_tx_result.receipt_params`,
/// `log_index = evm_log.log_ordinal + first_log_index` (the block-cumulative
/// per-tx offset), `data` from `tx_result -> 'logs' -> log_ordinal ->>
/// 'data'`, and `topics` from `topic0..3` (skipping NULLs). Ordered ascending
/// by `(block_number, tx_index, log_index)` — the fixed-point walk's ordering
/// invariant (capture §1.2).
pub async fn logs_for_address_topic0(
    source: &PgPool,
    address: [u8; 20],
    topic0: [u8; 32],
) -> anyhow::Result<Vec<LogEntry>> {
    let address_hex = format!("0x{}", hex::encode(address));
    let topic0_hex = format!("0x{}", hex::encode(topic0));

    // (log_ordinal, topic1, topic2, topic3, receipt_params, data)
    type LogRow = (
        i32,
        Option<String>,
        Option<String>,
        Option<String>,
        serde_json::Value,
        Option<String>,
    );
    let rows: Vec<LogRow> = sqlx::query_as(
        r#"
        SELECT el.log_ordinal,
               el.topic1, el.topic2, el.topic3,
               etr.receipt_params,
               etr.tx_result -> 'logs' -> el.log_ordinal ->> 'data'
        FROM evm_log el
        JOIN evm_tx_result etr
          ON etr.slot_number = el.slot_number AND etr.tx_hash = el.tx_hash
        WHERE el.address = $1 AND el.topic0 = $2
        "#,
    )
    .bind(&address_hex)
    .bind(&topic0_hex)
    .fetch_all(source)
    .await?;

    let mut entries = Vec::with_capacity(rows.len());
    for (log_ordinal, topic1, topic2, topic3, receipt_params_json, data_hex) in rows {
        let receipt: ReceiptParamsJson = serde_json::from_value(receipt_params_json)?;

        // Same shape ingest reconstructs: topic0 first, then any present
        // higher topics (NULLs skipped).
        let mut topics = vec![topic0];
        for t in [topic1, topic2, topic3].into_iter().flatten() {
            topics.push(parse_topic32(&t)?);
        }
        let data = match data_hex {
            Some(s) => hex::decode(s.trim_start_matches("0x"))?,
            None => Vec::new(),
        };

        entries.push(LogEntry {
            block_number: receipt.block_number.as_u64() as i64,
            tx_index: receipt.tx_index as i32,
            log_index: (log_ordinal as i64 + receipt.first_log_index.as_u64() as i64) as i32,
            address,
            topics,
            data,
        });
    }

    entries.sort_by_key(|e| (e.block_number, e.tx_index, e.log_index));
    Ok(entries)
}

fn parse_topic32(s: &str) -> anyhow::Result<[u8; 32]> {
    let bytes = hex::decode(s.trim_start_matches("0x"))?;
    bytes
        .try_into()
        .map_err(|v: Vec<u8>| anyhow::anyhow!("topic is {} bytes, expected 32", v.len()))
}

/// Targeted read of exactly one log's non-indexed `data` field —
/// `tx_result->'logs'->log_ordinal->>'data'`, never the whole logs array.
/// Only called for events whose registered descriptor has ≥1 non-indexed arg.
pub async fn log_data(
    source: &PgPool,
    slot: i64,
    tx_hash: &str,
    log_ordinal: i32,
) -> anyhow::Result<Option<String>> {
    let row: Option<(Option<String>,)> = sqlx::query_as(
        r#"
        SELECT tx_result -> 'logs' -> $3 ->> 'data'
        FROM evm_tx_result
        WHERE slot_number = $1 AND tx_hash = $2
        "#,
    )
    .bind(slot)
    .bind(tx_hash)
    .bind(log_ordinal)
    .fetch_optional(source)
    .await?;

    Ok(row.and_then(|(d,)| d))
}
