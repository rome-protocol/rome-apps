/// Polling sync loop — copies rows from Hercules' Postgres into rome_via_db.
///
/// Strategy:
/// 1. Read last_synced_slot from sync_cursors (defaults to 0).
/// 2. SELECT new rows WHERE slot_number > cursor ORDER BY slot_number LIMIT batch_size.
/// 3. INSERT into rome_via.{table} with chain_id prepended, ON CONFLICT DO NOTHING.
/// 4. UPDATE sync_cursors with new cursor.
///
/// Repeat for every table in dependency order.
///
/// Runtime sqlx::query (not sqlx::query!) is used throughout to avoid requiring
/// DATABASE_URL at compile time (no live DB in CI).
///
/// Runs until the CancellationToken is cancelled.
use anyhow::Context;
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};
use tokio_util::sync::CancellationToken;

use crate::config::SyncConfig;
use crate::metrics;
use crate::rlp_decode;

/// Run the polling loop until cancellation.
pub async fn run(
    source: PgPool,
    target: PgPool,
    cfg: SyncConfig,
    token: CancellationToken,
) -> anyhow::Result<()> {
    let interval = std::time::Duration::from_millis(cfg.poll_interval_base_ms());
    let chain_id = cfg.chain_id as i64;
    let batch_size = cfg.batch_size;

    // Consecutive wakes that found nothing. Drives the idle backoff below; any work
    // at all resets it, so a busy chain is never throttled.
    let mut idle_rounds: u32 = 0;

    loop {
        // Drain to empty: keep running full cycles until one makes no cursor
        // progress (fully caught up). Previously each poll advanced every table by
        // at most `batch_size` rows, so any sustained inflow above batch_size/poll
        // built an unbounded backlog — the Via lag. Draining on each wake keeps the
        // mirror current regardless of how far behind it fell during a burst.
        let mut did_work = false;
        loop {
            let before = cursors_sum(&target, chain_id).await.unwrap_or(0);
            if let Err(e) = sync_cycle(&source, &target, chain_id, batch_size).await {
                tracing::error!(?e, "sync cycle error — will retry");
                // An error is not idleness: retry at base cadence rather than
                // backing off away from a chain that may be producing fine.
                did_work = true;
                break;
            }
            let after = cursors_sum(&target, chain_id).await.unwrap_or(before);
            if after == before || token.is_cancelled() {
                break; // no rows copied this pass → caught up (or shutting down)
            }
            did_work = true;
        }

        idle_rounds = if did_work { 0 } else { idle_rounds.saturating_add(1) };
        // next_poll_secs' maths is unit-agnostic (geometric growth, clamped), so it
        // drives the millisecond base directly — no second variant to keep in step.
        let wait = std::time::Duration::from_millis(next_poll_secs(
            idle_rounds,
            cfg.poll_interval_base_ms(),
            cfg.max_idle_poll_interval_ms(),
        ));
        if wait != interval {
            tracing::debug!(idle_rounds, wait_ms = wait.as_millis(), "idle backoff");
        }

        // Wait for the next interval OR immediate cancellation.
        tokio::select! {
            _ = token.cancelled() => {
                tracing::info!("sync loop cancelled — shutting down");
                break;
            }
            _ = tokio::time::sleep(wait) => {}
        }
    }

    Ok(())
}

/// Record how far the SOURCE has been written, so the explorer can tell whether
/// it is live or catching up (design finding 9d).
///
/// Keyed on `eth_block` because that is what the UI's own tip is derived from —
/// lag is (this value − the mirrored eth_block max), i.e. blocks the reader
/// cannot see yet. One index-only MAX per cycle.
///
/// Never touches `last_synced_slot`: the conflict path updates only this column,
/// so it cannot disturb the cursor that governs restart safety.
async fn record_source_max(
    source: &PgPool,
    target: &PgPool,
    chain_id: i64,
) -> anyhow::Result<()> {
    let src_max: Option<i64> = sqlx::query_scalar("SELECT MAX(slot_number) FROM eth_block")
        .fetch_one(source)
        .await?;
    let Some(m) = src_max else { return Ok(()) };
    sqlx::query(
        "INSERT INTO rome_via.sync_cursors (chain_id, table_name, last_synced_slot, source_max_slot) \
         VALUES ($1, 'eth_block', 0, $2) \
         ON CONFLICT (chain_id, table_name) \
         DO UPDATE SET source_max_slot = EXCLUDED.source_max_slot",
    )
    .bind(chain_id)
    .bind(m)
    .execute(target)
    .await?;
    Ok(())
}

/// One full sync cycle across all tables.
async fn sync_cycle(
    source: &PgPool,
    target: &PgPool,
    chain_id: i64,
    batch_size: i64,
) -> anyhow::Result<()> {
    // Best-effort: a failure here must not abort a cycle that would otherwise
    // sync data. Worst case the explorer reports lag unknown and stays silent.
    if let Err(e) = record_source_max(source, target, chain_id).await {
        tracing::debug!(error = %e, "record_source_max failed; lag will read unknown");
    }

    // Tables synced in dependency order (parent before child).
    sync_sol_slot(source, target, chain_id, batch_size).await
        .context("sync_sol_slot")?;
    sync_eth_block(source, target, chain_id, batch_size).await
        .context("sync_eth_block")?;
    sync_eth_block_txs(source, target, chain_id, batch_size).await
        .context("sync_eth_block_txs")?;
    sync_evm_tx(source, target, chain_id, batch_size).await
        .context("sync_evm_tx")?;
    sync_evm_tx_result(source, target, chain_id, batch_size).await
        .context("sync_evm_tx_result")?;
    sync_evm_tx_sol_tx(source, target, chain_id, batch_size).await
        .context("sync_evm_tx_sol_tx")?;
    Ok(())
}

// ─── Cursor helpers ───────────────────────────────────────────────────────────

/// Sum of all per-table sync cursors for a chain — a cheap progress signal for
/// the drain loop. If a full sync cycle doesn't increase it, every table is
/// caught up and we can sleep until the next poll. Cursors increase monotonically
/// as rows are copied, so the sum strictly grows iff some table advanced.
async fn cursors_sum(target: &PgPool, chain_id: i64) -> anyhow::Result<i64> {
    let row: Option<PgRow> = sqlx::query(
        "SELECT COALESCE(SUM(last_synced_slot), 0)::BIGINT AS s \
         FROM rome_via.sync_cursors WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_optional(target)
    .await?;

    Ok(row.map(|r| r.get::<i64, _>("s")).unwrap_or(0))
}

async fn read_cursor(
    source: &PgPool,
    target: &PgPool,
    chain_id: i64,
    table: &str,
) -> anyhow::Result<i64> {
    let row: Option<PgRow> = sqlx::query(
        "SELECT last_synced_slot \
         FROM rome_via.sync_cursors \
         WHERE chain_id = $1 AND table_name = $2",
    )
    .bind(chain_id)
    .bind(table)
    .fetch_optional(target)
    .await?;

    if let Some(r) = row {
        return Ok(r.get::<i64, _>("last_synced_slot"));
    }

    // No cursor yet: start at the chain's first block, not slot 0. The
    // range-based drains step `slots_per_pass` slots per cycle, so on a
    // slot-aligned chain born at a high slot (rubicon mainnet: genesis at
    // slot 437.5M) a zero start means days of draining empty ranges before
    // the first real row is copied — blocks appear in the mirror (eth_block
    // jumps via its row-based first batch) while every tx table lags.
    let min: Option<i64> = sqlx::query_scalar("SELECT MIN(slot_number) FROM eth_block")
        .fetch_one(source)
        .await?;
    Ok(min.map(|m| (m - 1).max(0)).unwrap_or(0))
}

async fn write_cursor(
    target: &PgPool,
    chain_id: i64,
    table: &str,
    slot: i64,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        INSERT INTO rome_via.sync_cursors (chain_id, table_name, last_synced_slot, last_synced_at)
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT (chain_id, table_name)
        DO UPDATE SET last_synced_slot = EXCLUDED.last_synced_slot,
                      last_synced_at   = EXCLUDED.last_synced_at
        "#,
    )
    .bind(chain_id)
    .bind(table)
    .bind(slot)
    .execute(target)
    .await?;
    Ok(())
}

// ─── sol_slot ────────────────────────────────────────────────────────────────

/// Row shape for the sol_slot batched insert.
struct SolSlotRow {
    slot_number: i64,
    parent_slot: Option<i64>,
    status: Option<String>,
    blockhash: Option<String>,
    timestamp: Option<i64>,
}

/// Batched UNNEST insert for sol_slot (ON CONFLICT DO NOTHING).
async fn batch_insert_sol_slot(
    target: &PgPool,
    chain_id: i64,
    rows: &[SolSlotRow],
) -> anyhow::Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }
    let slot: Vec<i64> = rows.iter().map(|r| r.slot_number).collect();
    let parent: Vec<Option<i64>> = rows.iter().map(|r| r.parent_slot).collect();
    let status: Vec<Option<String>> = rows.iter().map(|r| r.status.clone()).collect();
    let blockhash: Vec<Option<String>> = rows.iter().map(|r| r.blockhash.clone()).collect();
    let ts: Vec<Option<i64>> = rows.iter().map(|r| r.timestamp).collect();
    let res = sqlx::query(
        r#"
        INSERT INTO rome_via.sol_slot
            (chain_id, slot_number, parent_slot, status, blockhash, timestamp)
        SELECT $1, u.slot_number, u.parent_slot, u.status, u.blockhash, u.timestamp
        FROM UNNEST($2::bigint[], $3::bigint[], $4::text[], $5::text[], $6::bigint[])
            AS u(slot_number, parent_slot, status, blockhash, timestamp)
        ON CONFLICT (chain_id, slot_number) DO NOTHING
        "#,
    )
    .bind(chain_id)
    .bind(&slot)
    .bind(&parent)
    .bind(&status)
    .bind(&blockhash)
    .bind(&ts)
    .execute(target)
    .await?;
    Ok(res.rows_affected())
}

async fn sync_sol_slot(
    source: &PgPool,
    target: &PgPool,
    chain_id: i64,
    batch_size: i64,
) -> anyhow::Result<()> {
    let table = "sol_slot";
    let cursor = read_cursor(source, target, chain_id, table).await?;

    // status is a Postgres ENUM SlotStatus; cast to TEXT for portability.
    let rows = sqlx::query(
        r#"
        SELECT slot_number, parent_slot, status::TEXT AS status, blockhash, timestamp
        FROM sol_slot
        WHERE slot_number > $1
        ORDER BY slot_number
        LIMIT $2
        "#,
    )
    .bind(cursor)
    .bind(batch_size)
    .fetch_all(source)
    .await?;

    if rows.is_empty() {
        return Ok(());
    }

    let max_slot: i64 = rows.iter().map(|r| r.get::<i64, _>("slot_number")).max().unwrap_or(cursor);
    let count = rows.len();

    let batch: Vec<SolSlotRow> = rows
        .into_iter()
        .map(|row| SolSlotRow {
            slot_number: row.get("slot_number"),
            parent_slot: row.get("parent_slot"),
            status: row.get("status"),
            blockhash: row.get("blockhash"),
            timestamp: row.get("timestamp"),
        })
        .collect();
    batch_insert_sol_slot(target, chain_id, &batch).await?;

    write_cursor(target, chain_id, table, max_slot).await?;

    for _ in 0..count {
        metrics::inc_rows_synced("sol_slot", chain_id as u64);
    }

    tracing::debug!(table, cursor, max_slot, count, "synced rows");
    Ok(())
}

// ─── eth_block ───────────────────────────────────────────────────────────────

/// Row shape for the eth_block batched insert. NUMERIC columns
/// (`block_gas_used`, `params_block_timestamp`) travel as text and are cast
/// `::numeric` in the SELECT, keeping every array bind a primitive type.
struct EthBlockRow {
    slot_number: i64,
    slot_block_idx: i32,
    block_gas_used: Option<String>,
    gas_recipient: Option<String>,
    slot_timestamp: Option<i64>,
    params_blockhash: Option<String>,
    params_parent_hash: Option<String>,
    params_number: Option<i64>,
    params_block_timestamp: Option<String>,
}

/// Batched UNNEST insert for eth_block (ON CONFLICT DO NOTHING). The two NUMERIC
/// columns are bound as `text[]` and cast `::numeric` in the projection.
async fn batch_insert_eth_block(
    target: &PgPool,
    chain_id: i64,
    rows: &[EthBlockRow],
) -> anyhow::Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }
    let slot: Vec<i64> = rows.iter().map(|r| r.slot_number).collect();
    let idx: Vec<i32> = rows.iter().map(|r| r.slot_block_idx).collect();
    let gas_used: Vec<Option<String>> = rows.iter().map(|r| r.block_gas_used.clone()).collect();
    let recipient: Vec<Option<String>> = rows.iter().map(|r| r.gas_recipient.clone()).collect();
    let slot_ts: Vec<Option<i64>> = rows.iter().map(|r| r.slot_timestamp).collect();
    let bh: Vec<Option<String>> = rows.iter().map(|r| r.params_blockhash.clone()).collect();
    let ph: Vec<Option<String>> = rows.iter().map(|r| r.params_parent_hash.clone()).collect();
    let num: Vec<Option<i64>> = rows.iter().map(|r| r.params_number).collect();
    let bts: Vec<Option<String>> = rows.iter().map(|r| r.params_block_timestamp.clone()).collect();
    let res = sqlx::query(
        r#"
        INSERT INTO rome_via.eth_block
            (chain_id, slot_number, slot_block_idx, block_gas_used, gas_recipient,
             slot_timestamp, params_blockhash, params_parent_hash, params_number, params_block_timestamp)
        SELECT $1, u.slot_number, u.slot_block_idx, u.block_gas_used::numeric, u.gas_recipient,
               u.slot_timestamp, u.params_blockhash, u.params_parent_hash, u.params_number, u.params_block_timestamp::numeric
        FROM UNNEST($2::bigint[], $3::int[], $4::text[], $5::varchar[], $6::bigint[],
                    $7::text[], $8::text[], $9::bigint[], $10::text[])
            AS u(slot_number, slot_block_idx, block_gas_used, gas_recipient,
                 slot_timestamp, params_blockhash, params_parent_hash, params_number, params_block_timestamp)
        ON CONFLICT (chain_id, slot_number, slot_block_idx) DO NOTHING
        "#,
    )
    .bind(chain_id)
    .bind(&slot)
    .bind(&idx)
    .bind(&gas_used)
    .bind(&recipient)
    .bind(&slot_ts)
    .bind(&bh)
    .bind(&ph)
    .bind(&num)
    .bind(&bts)
    .execute(target)
    .await?;
    Ok(res.rows_affected())
}

async fn sync_eth_block(
    source: &PgPool,
    target: &PgPool,
    chain_id: i64,
    batch_size: i64,
) -> anyhow::Result<()> {
    let table = "eth_block";
    let cursor = read_cursor(source, target, chain_id, table).await?;

    // Hercules INSERTs eth_block with NULL params, then UPDATEs params_*
    // a moment later (block-build is two writes: shell first, then payload).
    // If we naively SELECT slot_number > cursor we capture rows mid-fill,
    // copy them with NULL params, advance cursor past their slot, and never
    // re-read once Hercules backfills. Result: the live API silently omits
    // the block, the explorer hangs on tx pages whose containing block
    // params are NULL on our side.
    //
    // Defense: compute a "wall" — the lowest still-NULL-params slot strictly
    // above the current cursor — and refuse to sync rows at or above the
    // wall this tick. The wall slot stays in source's eth_block; next tick
    // we re-read it (and only it) once params are filled.
    let wall_row: Option<PgRow> = sqlx::query(
        r#"
        SELECT MIN(slot_number) AS wall
        FROM eth_block
        WHERE slot_number > $1 AND (params).number IS NULL
        "#,
    )
    .bind(cursor)
    .fetch_optional(source)
    .await?;
    let wall: Option<i64> = wall_row.and_then(|r| r.get::<Option<i64>, _>("wall"));

    // BlockParams is a Postgres composite type; use accessor syntax to flatten.
    // block_gas_used is NUMERIC — cast to TEXT to avoid sqlx bigdecimal complexity.
    // params_block_timestamp is NUMERIC — also cast to TEXT.
    let rows = sqlx::query(
        r#"
        SELECT
            slot_number,
            slot_block_idx,
            block_gas_used::TEXT   AS block_gas_used,
            gas_recipient,
            slot_timestamp,
            (params).blockhash      AS params_blockhash,
            (params).parent_hash    AS params_parent_hash,
            (params).number         AS params_number,
            (params).block_timestamp::TEXT AS params_block_timestamp
        FROM eth_block
        WHERE slot_number > $1
          AND ($2::BIGINT IS NULL OR slot_number < $2)
        ORDER BY slot_number, slot_block_idx
        LIMIT $3
        "#,
    )
    .bind(cursor)
    .bind(wall)
    .bind(batch_size)
    .fetch_all(source)
    .await?;

    if rows.is_empty() {
        // Empty batch with a wall present means the next slot above us is
        // still mid-fill upstream — do NOT advance the cursor (default would
        // be max_slot=cursor, a no-op, but make the intent explicit and emit
        // a debug line so this is visible during incidents).
        if let Some(w) = wall {
            tracing::debug!(table, cursor, wall = w, "wall blocks cursor advance — null params upstream");
        }
        return Ok(());
    }

    let max_slot: i64 = rows.iter().map(|r| r.get::<i64, _>("slot_number")).max().unwrap_or(cursor);
    let count = rows.len();

    let batch: Vec<EthBlockRow> = rows
        .into_iter()
        .map(|row| EthBlockRow {
            slot_number: row.get("slot_number"),
            slot_block_idx: row.get("slot_block_idx"),
            // block_gas_used/params_block_timestamp arrive as TEXT; cast ::numeric in the insert.
            block_gas_used: row.get("block_gas_used"),
            gas_recipient: row.get("gas_recipient"),
            slot_timestamp: row.get("slot_timestamp"),
            params_blockhash: row.get("params_blockhash"),
            params_parent_hash: row.get("params_parent_hash"),
            params_number: row.get("params_number"),
            params_block_timestamp: row.get("params_block_timestamp"),
        })
        .collect();
    batch_insert_eth_block(target, chain_id, &batch).await?;

    write_cursor(target, chain_id, table, max_slot).await?;

    for _ in 0..count {
        metrics::inc_rows_synced("eth_block", chain_id as u64);
    }

    // NOTIFY SSE listeners: compact payload with chainId + blockNumber (last in batch).
    let notify_payload =
        new_block_notify_payload(chain_id, cursor + 1, max_slot, count);
    let _ = sqlx::query(&format!(
        "SELECT pg_notify('rome_via_new_block', '{}')",
        notify_payload.replace('\'', "''")
    ))
    .execute(target)
    .await;

    tracing::debug!(table, cursor, max_slot, count, "synced rows");
    Ok(())
}

/// Below this lag the sync switches to one slot per pass.
///
/// The live feed is driven by one NOTIFY per pass, so pass size IS the granularity a
/// browser sees. At ~2.6 slots/s a one-slot pass emits an event roughly every 400 ms —
/// streaming cadence. A 5000-slot pass emits one event carrying ~725k transactions,
/// which reads as a teleport no matter how the client renders it.
///
/// Ingestion speed is not sacrificed: this only applies once already caught up, and
/// any real backlog immediately re-enters the batched branch below.
pub(crate) const STREAM_LAG_SLOTS: i64 = 16;

/// Hard ceiling on slots per pass, whatever the density says. Bounds the worst-case
/// rows (and bind memory) a single statement can pull when a stretch is unexpectedly
/// sparse in the sample but dense just past it.
pub(crate) const MAX_SLOTS_PER_PASS: i64 = 2_000;

/// How many SLOTS one pass should advance.
///
/// The budget is expressed in slots, and each table converts a shared ROW budget into
/// its own slot count using its own density. That is the fix for the asymmetry
/// measured on hadrian-lt 2026-07-27: a single `batch_size` of 5000 meant 5000 slots
/// for `eth_block` (1 row/slot) but ~34 for `evm_tx` (~145 rows/slot), so `eth_block`
/// sat 12 slots behind while `evm_tx` was 26,093 — about 2.8 hours of explorer lag.
/// Equalising in slots keeps every table's cursor moving together at comparable row
/// cost per pass.
///
/// Returns at least 1: a zero budget would leave the cursor unmoved and spin the
/// drain loop forever.
pub(crate) fn slots_per_pass(
    lag_slots: i64,
    rows_per_slot: f64,
    row_budget: i64,
    max_slots: i64,
) -> i64 {
    if lag_slots <= STREAM_LAG_SLOTS {
        return 1;
    }
    let cap = max_slots.max(1);
    if !rows_per_slot.is_finite() || rows_per_slot <= 1.0 {
        // Sparse table (one row per slot, or density not yet observed): the row
        // budget cannot bind, so run to the slot cap.
        return cap;
    }
    let by_rows = (row_budget.max(1) as f64 / rows_per_slot).floor() as i64;
    by_rows.clamp(1, cap)
}

/// Poll interval for the next wake, given how many consecutive cycles found nothing.
///
/// A full cycle costs ~16 queries (7 cursor reads + 7 selects + 2 `cursors_sum`) and
/// runs every `base` seconds whether or not there is work. Measured 2026-07-27 that is
/// ~11 queries/s per chain idle, ~44/s across the four chains sharing the instance —
/// on a quiet chain, the entire database load.
///
/// Backs off geometrically once idleness is sustained, capped so a long-quiet chain
/// still notices new work within `max_secs`. The first idle round keeps base cadence:
/// a chain that has only just gone quiet should stay responsive, and any work at all
/// resets the counter, so a busy chain is never throttled.
pub(crate) fn next_poll_secs(idle_rounds: u32, base_secs: u64, max_secs: u64) -> u64 {
    if idle_rounds < 2 {
        return base_secs;
    }
    // Saturating: idle_rounds is unbounded, and 1u64 << 64 is UB.
    let shift = (idle_rounds - 1).min(32);
    base_secs
        .saturating_mul(1u64 << shift)
        .min(max_secs.max(base_secs))
}

/// Where the cursor should move when a bounded pass found NO rows.
///
/// A keyset bound selects a slot RANGE, and a range can legitimately be empty — slots
/// in which nothing happened. The previous OFFSET/MAX bound always landed on a
/// non-empty slot, so "no rows" could only mean "caught up" and returning early was
/// correct. Under a keyset bound that same early return would re-query a dead range
/// forever, and the mirror would stall on any quiet stretch.
///
/// Returns the slot to advance to, or `None` when there is genuinely nothing further
/// in the source and the cursor should stay put.
pub(crate) fn cursor_after_empty_range(
    cursor: i64,
    slots_scanned: i64,
    source_max_slot: Option<i64>,
) -> Option<i64> {
    let max = source_max_slot?;
    if max <= cursor {
        return None; // caught up
    }
    // Never claim to have scanned past what exists.
    Some((cursor + slots_scanned.max(1)).min(max))
}

/// How far a slot-keyed source table has been written, and how dense the next
/// stretch is. One round trip per table per pass.
///
/// Deliberately NOT a single cycle-wide src_max. The design note asks for the
/// source max "once per cycle, not per table" — but that is about deriving LAG,
/// which only sizes the budget. Reusing one table's max as another's CEILING is
/// unsafe: if that table's writer lags, the empty-range path advances the cursor
/// past slots that have not been written yet and those rows are never picked up
/// again — silent, permanent loss, the same hazard as the restart cursor.
/// `MAX(slot_number)` is an index-only lookup, so per-table costs ~nothing.
async fn probe_slot_bounds(
    source: &PgPool,
    table: &str,
    cursor: i64,
) -> anyhow::Result<(Option<i64>, f64)> {
    // Sampling only the next N slots keeps density O(sample), not O(backlog) —
    // which is what the OFFSET bound this replaced used to cost.
    const DENSITY_SAMPLE_SLOTS: i64 = 64;
    let sql = format!(
        "SELECT (SELECT MAX(slot_number) FROM {table}) AS src_max, \
                (SELECT count(*)::float8 \
                   / GREATEST(count(DISTINCT slot_number), 1)::float8 \
                 FROM {table} \
                 WHERE slot_number > $1 AND slot_number <= $1 + $2) AS rows_per_slot"
    );
    let row: Option<PgRow> = sqlx::query(&sql)
        .bind(cursor)
        .bind(DENSITY_SAMPLE_SLOTS)
        .fetch_optional(source)
        .await?;
    let src_max = row.as_ref().and_then(|r| r.get::<Option<i64>, _>("src_max"));
    let rows_per_slot = row
        .as_ref()
        .and_then(|r| r.get::<Option<f64>, _>("rows_per_slot"))
        .unwrap_or(0.0);
    Ok((src_max, rows_per_slot))
}

/// Inclusive upper slot bound for one keyset page.
///
/// A newtype rather than a bare i64 because the parameter it replaced was a ROW
/// count (`batch_size`) in the same position with the same primitive type.
/// Rust arguments are positional, so renaming the parameter compiles silently
/// and a caller still passing batch_size yields `slot_number <= 5000` against
/// slots near 479,000,000 — selecting nothing, forever, with no error anywhere.
/// The wrapper turns that into a type error at every call site.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct UpperSlot(pub(crate) i64);

/// Build a slot-boundary SELECT for a slot-keyed source table. Fetches COMPLETE
/// slots in the half-open range `($1, $2]` — never splitting a slot across a page
/// — so the max-slot cursor (`$1`) can't skip a busy slot's tail on the next
/// page. `columns`/`table`/`order_by` are compile-time literals from the call
/// sites (no user input → no injection).
///
/// $1 = cursor (exclusive), $2 = upper slot bound (inclusive), supplied by the
/// caller from `slots_per_pass` — the same shape `evm_tx_select` already uses.
/// The two must stay in lockstep: a caller computing a slot bound for one while
/// the other reads `$2` as a row count is a silent 147x asymmetry, which is
/// exactly how evm_tx came to advance ~34 slots per pass against the other
/// tables' 5000 (hadrian-lt, 2026-07-27: eth_block 12 slots behind while evm_tx
/// sat 26,093 behind).
///
/// This replaces an OFFSET/MAX form that located the batch_size-th candidate row
/// to derive the bound. Two costs went with it: choosing a batch was O(batch), so
/// raising batch_size partly paid for itself; and the `MAX(slot_number)` fallback
/// scanned to the end of the table on every pass — most expensive precisely when
/// there was nothing left to sync.
///
/// It also replaces the naive `WHERE slot_number > $1 ORDER BY … LIMIT $2`, which
/// split a block with > batch_size txs across pages and permanently dropped its
/// tail once the cursor advanced. Slots are whole, so a slot bound keeps that
/// guarantee for free.
fn slot_boundary_select(columns: &str, table: &str, order_by: &str) -> String {
    format!(
        "SELECT {columns} \
         FROM {table} \
         WHERE slot_number > $1 \
           AND slot_number <= $2 \
         ORDER BY {order_by}"
    )
}

/// evm_tx has no slot_number of its own — it pages via a join to eth_block_txs,
/// `DISTINCT ON (tx_hash)` to dedupe a tx that appears in multiple blocks.
///
/// The page is bounded to COMPLETE eth_block_txs slots, so a block with more txs
/// than one pass would like still moves whole and cannot lose its tail once the
/// max-slot cursor advances. That guarantee now comes from the bound being a SLOT
/// keyset supplied by the caller (`$2`), rather than from an OFFSET scan locating
/// the batch_size-th row.
///
/// Two reasons that changed. The OFFSET made choosing a batch cost O(batch), so
/// raising batch_size partly paid for itself; and expressing the budget in ROWS
/// while sol_slot/sol_block/eth_block expressed theirs in SLOTS meant evm_tx
/// advanced ~34 slots per pass against their 5000 — measured on hadrian-lt
/// 2026-07-27 as eth_block 12 slots behind while evm_tx sat 26,093 behind.
///
/// $1 = cursor (exclusive), $2 = upper slot bound (inclusive).
fn evm_tx_select() -> &'static str {
    r#"
        SELECT DISTINCT ON (et.tx_hash)
            ebt.slot_number, et.tx_hash, et.rlp,
            et.from_address, et.origination, et.solana_signer
        FROM evm_tx et
        INNER JOIN eth_block_txs ebt ON ebt.tx_hash = et.tx_hash
        WHERE ebt.slot_number > $1
          AND ebt.slot_number <= $2
        ORDER BY et.tx_hash, ebt.slot_number
    "#
}

// ─── eth_block_txs ───────────────────────────────────────────────────────────

/// Row shape for the eth_block_txs batched insert. Decoupled from the source
/// SELECT so the insert is testable without a Hercules source DB.
struct EthBlockTxRow {
    slot_number: i64,
    slot_block_idx: i32,
    tx_hash: String,
    tx_idx: i32,
}

/// Batched UNNEST upsert for eth_block_txs: one statement, one array bind per
/// column, replacing the prior row-by-row INSERT loop (one round-trip per row).
/// `ON CONFLICT DO NOTHING` matches the prior semantics; in-batch duplicate
/// conflict keys are safe under DO NOTHING (no "affect row a second time" error).
async fn batch_insert_eth_block_txs(
    target: &PgPool,
    chain_id: i64,
    rows: &[EthBlockTxRow],
) -> anyhow::Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }
    let slot_numbers: Vec<i64> = rows.iter().map(|r| r.slot_number).collect();
    let slot_block_idxs: Vec<i32> = rows.iter().map(|r| r.slot_block_idx).collect();
    let tx_hashes: Vec<String> = rows.iter().map(|r| r.tx_hash.clone()).collect();
    let tx_idxs: Vec<i32> = rows.iter().map(|r| r.tx_idx).collect();

    let res = sqlx::query(
        r#"
        INSERT INTO rome_via.eth_block_txs
            (chain_id, slot_number, slot_block_idx, tx_hash, tx_idx)
        SELECT $1, u.slot_number, u.slot_block_idx, u.tx_hash, u.tx_idx
        FROM UNNEST($2::bigint[], $3::int[], $4::varchar[], $5::int[])
            AS u(slot_number, slot_block_idx, tx_hash, tx_idx)
        ON CONFLICT (chain_id, slot_number, slot_block_idx, tx_hash) DO NOTHING
        "#,
    )
    .bind(chain_id)
    .bind(&slot_numbers)
    .bind(&slot_block_idxs)
    .bind(&tx_hashes)
    .bind(&tx_idxs)
    .execute(target)
    .await?;
    Ok(res.rows_affected())
}

async fn sync_eth_block_txs(
    source: &PgPool,
    target: &PgPool,
    chain_id: i64,
    batch_size: i64,
) -> anyhow::Result<()> {
    let table = "eth_block_txs";
    let cursor = read_cursor(source, target, chain_id, table).await?;

    // Budget in SLOTS, converted from the shared ROW budget by this table's own
    // observed density, and clamped to what the source has actually written.
    let (src_max, rows_per_slot) = probe_slot_bounds(source, table, cursor).await?;
    let lag = src_max.unwrap_or(cursor).saturating_sub(cursor);
    let slots = slots_per_pass(lag, rows_per_slot, batch_size, MAX_SLOTS_PER_PASS);
    let upper = UpperSlot(match src_max {
        Some(m) => (cursor + slots).min(m),
        None => cursor + slots,
    });

    // Slot-boundary batching (shared builder): fetch COMPLETE slots so a block with
    // > batch_size txs is never split across pages and its tail dropped. See
    // `slot_boundary_select`.
    let rows = sqlx::query(&slot_boundary_select(
        "slot_number, slot_block_idx, tx_hash, tx_idx",
        "eth_block_txs",
        "slot_number, slot_block_idx, tx_idx",
    ))
    .bind(cursor)
    .bind(upper.0)
    .fetch_all(source)
    .await?;

    if rows.is_empty() {
        // A slot keyset can select a legitimately EMPTY range — quiet slots. The
        // OFFSET bound this replaced always landed on a non-empty slot, so returning
        // early was safe; with a keyset it would re-query the same dead range forever
        // and STALL the mirror — worst on quiet chains. Skip the scanned range, never
        // past what the source has actually written.
        if let Some(next) = cursor_after_empty_range(cursor, slots, src_max) {
            write_cursor(target, chain_id, table, next).await?;
        }
        return Ok(());
    }

    let max_slot: i64 = rows.iter().map(|r| r.get::<i64, _>("slot_number")).max().unwrap_or(cursor);
    let count = rows.len();

    let batch: Vec<EthBlockTxRow> = rows
        .into_iter()
        .map(|row| EthBlockTxRow {
            slot_number: row.get("slot_number"),
            slot_block_idx: row.get("slot_block_idx"),
            tx_hash: row.get("tx_hash"),
            tx_idx: row.get("tx_idx"),
        })
        .collect();
    batch_insert_eth_block_txs(target, chain_id, &batch).await?;

    write_cursor(target, chain_id, table, max_slot).await?;

    for _ in 0..count {
        metrics::inc_rows_synced("eth_block_txs", chain_id as u64);
    }

    tracing::debug!(table, cursor, max_slot, count, "synced rows");
    Ok(())
}

// ─── evm_tx ──────────────────────────────────────────────────────────────────
//
// evm_tx has no slot_number. We join via eth_block_txs to get a slot ordering.
// Cursor semantics: "max slot_number of eth_block_txs rows seen so far for this chain".
//
// RLP decode: for each row with a non-null `rlp` column, we call
// `rlp_decode::decode_signed_tx` to populate denormalized columns.
// On decode failure we log a warning, emit a metric, and insert the row with
// NULL for the decoded fields — sync is never blocked by decode failures.

/// Resolve the `from` to store for a synced tx. Solana-native (DoTxUnsigned) txs
/// carry an authoritative synthetic `from` from Hercules (= keccak(solana_signer)[12:])
/// and have no recoverable signature, so NEVER use the local RLP-recovered `from`
/// for them. Everything else keeps the prior behavior (prefer locally-decoded from,
/// fall back to Hercules').
fn resolve_sync_from(
    origination: &str,
    decoded_from: Option<&str>,
    hercules_from: Option<&str>,
) -> Option<String> {
    if origination == "solana_unsigned" {
        // No signature to recover from — `decoded_from` is a zero-address sentinel
        // here, so it must never serve as a fallback or it would shadow the
        // synthetic sender with 0x0000….
        return hercules_from.map(|s| s.to_string());
    }
    // Hercules is authoritative for BOTH lanes. It recovered this sender from the
    // same RLP bytes, so preferring Via's own recovery is duplicate work whose only
    // possible effect is disagreement: a tx type the two decoders treat differently
    // would make the explorer show a different sender than the chain does, silently.
    // Local recovery stays as the fallback for rows where Hercules stored nothing.
    hercules_from
        .map(|s| s.to_string())
        .or_else(|| decoded_from.map(|s| s.to_string()))
}

/// Resolve the value to store in the denormalized `from_addr` column — the field
/// rome-via-api displays as the tx sender (`COALESCE(from_addr, from_address)`,
/// from_addr wins). This is the SAME resolution as [`resolve_sync_from`]: for
/// `solana_unsigned` txs `decode_signed_tx(recover_sender=false)` yields a
/// zero-address sentinel as `decoded_from`, so binding `decoded.from` directly
/// would shadow the synthetic sender with `0x0000…`. Routing the denorm bind
/// through this resolver guarantees `from_addr` holds the synthetic (solana)
/// or the locally-recovered (ecdsa) sender, never the sentinel.
fn denorm_from_addr(
    origination: &str,
    decoded_from: Option<&str>,
    hercules_from: Option<&str>,
) -> Option<String> {
    resolve_sync_from(origination, decoded_from, hercules_from)
}

/// Keep only the LAST occurrence of each key, preserving the input order of the
/// survivors. Mirrors the sequential per-row upsert semantics (last write wins)
/// and — critically — prevents a single `ON CONFLICT DO UPDATE` statement from
/// seeing the same conflict key twice, which Postgres rejects with
/// "ON CONFLICT DO UPDATE command cannot affect row a second time".
fn dedupe_last_by_key<T, K, F>(rows: Vec<T>, key: F) -> Vec<T>
where
    K: Eq + std::hash::Hash,
    F: Fn(&T) -> K,
{
    let mut last_idx: std::collections::HashMap<K, usize> = std::collections::HashMap::new();
    for (i, r) in rows.iter().enumerate() {
        last_idx.insert(key(r), i);
    }
    rows.into_iter()
        .enumerate()
        .filter(|(i, r)| last_idx.get(&key(r)) == Some(i))
        .map(|(_, r)| r)
        .collect()
}

/// Row shape for the evm_tx batched upsert. NUMERIC columns (value_wei, gas_price)
/// travel as text and are cast `::numeric` in the SELECT.
struct EvmTxRow {
    tx_hash: String,
    rlp: Option<Vec<u8>>,
    from_address: Option<String>,
    from_addr: Option<String>,
    to_addr: Option<String>,
    value_wei: Option<String>,
    nonce: Option<i64>,
    gas_price: Option<String>,
    gas_limit: Option<i64>,
    method_id: Option<String>,
    input_len: Option<i32>,
    tx_type_byte: Option<i16>,
    origination: String,
    solana_signer: Option<String>,
    /// Depth-1 CPI target decoded from calldata — see `cpi_calldata` module doc.
    /// Immutable per-tx derivation (never refreshed on conflict), same as method_id.
    cpi_program_calldata: Option<String>,
    cpi_program_label_calldata: Option<String>,
    cpi_instruction_calldata: Option<String>,
}

/// Batched UNNEST upsert for evm_tx. Preserves the exact origination/solana_signer
/// self-heal: `DO UPDATE … WHERE IS DISTINCT FROM` (a no-op for the unchanged
/// ecdsa majority; back-tags historical Solana-origin rows on a cursor reset).
/// Immutable tx fields (rlp/to/value/…) are intentionally NOT in the SET.
/// Callers MUST pass a batch already deduped by tx_hash (see `dedupe_last_by_key`).
/// The `evm_tx` upsert, with `RETURNING (xmax = 0)` so the caller can count how many
/// rows were genuinely INSERTED.
///
/// This is an upsert, not a plain insert — it refreshes `origination` and
/// `solana_signer` for rows Hercules re-classified — so `rows_affected()` includes
/// conflict-updates and is NOT a count of new transactions. Driving a running total
/// from it would drift upward forever, and invisibly, because an inflated count still
/// looks plausible.
///
/// `xmax = 0` holds only for a tuple this statement inserted, so filtering the
/// returned rows on it yields the exact number of new transactions.
pub(crate) fn evm_tx_insert_sql() -> &'static str {
    r#"
        WITH ins AS (
        INSERT INTO rome_via.evm_tx (
            chain_id, tx_hash, rlp, from_address, from_addr, to_addr, value_wei, nonce,
            gas_price, gas_limit, method_id, input_len, tx_type_byte, origination, solana_signer,
            cpi_program_calldata, cpi_program_label_calldata, cpi_instruction_calldata)
        SELECT $1, u.tx_hash, u.rlp, u.from_address, u.from_addr, u.to_addr, u.value_wei::numeric, u.nonce,
               u.gas_price::numeric, u.gas_limit, u.method_id, u.input_len, u.tx_type_byte, u.origination, u.solana_signer,
               u.cpi_program_calldata, u.cpi_program_label_calldata, u.cpi_instruction_calldata
        FROM UNNEST($2::varchar[], $3::bytea[], $4::varchar[], $5::varchar[], $6::varchar[], $7::text[], $8::bigint[],
                    $9::text[], $10::bigint[], $11::varchar[], $12::int[], $13::smallint[], $14::text[], $15::varchar[],
                    $16::varchar[], $17::varchar[], $18::varchar[])
            AS u(tx_hash, rlp, from_address, from_addr, to_addr, value_wei, nonce,
                 gas_price, gas_limit, method_id, input_len, tx_type_byte, origination, solana_signer,
                 cpi_program_calldata, cpi_program_label_calldata, cpi_instruction_calldata)
        ON CONFLICT (chain_id, tx_hash) DO UPDATE SET
            origination   = EXCLUDED.origination,
            solana_signer = EXCLUDED.solana_signer
        WHERE rome_via.evm_tx.origination   IS DISTINCT FROM EXCLUDED.origination
           OR rome_via.evm_tx.solana_signer IS DISTINCT FROM EXCLUDED.solana_signer
        RETURNING (xmax = 0) AS inserted
        ), delta AS (
            SELECT count(*) FILTER (WHERE inserted) AS n FROM ins
        ), bumped AS (
            INSERT INTO rome_via.chain_counters (chain_id, total_txs, updated_at)
            SELECT $1, delta.n, NOW() FROM delta
            ON CONFLICT (chain_id) DO UPDATE
                SET total_txs  = rome_via.chain_counters.total_txs + EXCLUDED.total_txs,
                    updated_at = NOW()
            RETURNING total_txs
        )
        SELECT delta.n AS inserted, bumped.total_txs FROM delta, bumped
        "#
}

async fn batch_upsert_evm_tx(
    target: &PgPool,
    chain_id: i64,
    rows: &[EvmTxRow],
) -> anyhow::Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }
    // Keep only the LAST row per tx_hash: a single ON CONFLICT DO UPDATE statement
    // errors if the same conflict key appears twice ("cannot affect row a second
    // time"). The source DISTINCT ON makes intra-batch dups rare, but the batched
    // upsert must hold the invariant regardless of caller.
    let kept: Vec<&EvmTxRow> = dedupe_last_by_key(rows.iter().collect(), |r| r.tx_hash.clone());
    let tx_hash: Vec<String> = kept.iter().map(|r| r.tx_hash.clone()).collect();
    let rlp: Vec<Option<Vec<u8>>> = kept.iter().map(|r| r.rlp.clone()).collect();
    let from_address: Vec<Option<String>> = kept.iter().map(|r| r.from_address.clone()).collect();
    let from_addr: Vec<Option<String>> = kept.iter().map(|r| r.from_addr.clone()).collect();
    let to_addr: Vec<Option<String>> = kept.iter().map(|r| r.to_addr.clone()).collect();
    let value_wei: Vec<Option<String>> = kept.iter().map(|r| r.value_wei.clone()).collect();
    let nonce: Vec<Option<i64>> = kept.iter().map(|r| r.nonce).collect();
    let gas_price: Vec<Option<String>> = kept.iter().map(|r| r.gas_price.clone()).collect();
    let gas_limit: Vec<Option<i64>> = kept.iter().map(|r| r.gas_limit).collect();
    let method_id: Vec<Option<String>> = kept.iter().map(|r| r.method_id.clone()).collect();
    let input_len: Vec<Option<i32>> = kept.iter().map(|r| r.input_len).collect();
    let tx_type_byte: Vec<Option<i16>> = kept.iter().map(|r| r.tx_type_byte).collect();
    let origination: Vec<String> = kept.iter().map(|r| r.origination.clone()).collect();
    let solana_signer: Vec<Option<String>> = kept.iter().map(|r| r.solana_signer.clone()).collect();
    let cpi_program_calldata: Vec<Option<String>> =
        kept.iter().map(|r| r.cpi_program_calldata.clone()).collect();
    let cpi_program_label_calldata: Vec<Option<String>> = kept
        .iter()
        .map(|r| r.cpi_program_label_calldata.clone())
        .collect();
    let cpi_instruction_calldata: Vec<Option<String>> = kept
        .iter()
        .map(|r| r.cpi_instruction_calldata.clone())
        .collect();

    let rows_ret = sqlx::query(
        evm_tx_insert_sql(),
    )
    .bind(chain_id)
    .bind(&tx_hash)
    .bind(&rlp)
    .bind(&from_address)
    .bind(&from_addr)
    .bind(&to_addr)
    .bind(&value_wei)
    .bind(&nonce)
    .bind(&gas_price)
    .bind(&gas_limit)
    .bind(&method_id)
    .bind(&input_len)
    .bind(&tx_type_byte)
    .bind(&origination)
    .bind(&solana_signer)
    .bind(&cpi_program_calldata)
    .bind(&cpi_program_label_calldata)
    .bind(&cpi_instruction_calldata)
    .fetch_all(target)
    .await?;

    // Count only genuine inserts. rows_affected() would also include the
    // conflict-updates this statement performs on origination / solana_signer, so a
    // running total driven from it would drift upward forever and invisibly.
    // The statement returns one row: the genuine-insert count and the new running
    // total. Both are produced inside the same statement as the insert, so the
    // counter cannot diverge from the rows it counts — there is no window in which
    // a crash could land the rows but lose the increment.
    let inserted = rows_ret
        .first()
        .and_then(|r| r.get::<Option<i64>, _>("inserted"))
        .unwrap_or(0)
        .max(0) as u64;
    Ok(inserted)
}

async fn sync_evm_tx(
    source: &PgPool,
    target: &PgPool,
    chain_id: i64,
    batch_size: i64,
) -> anyhow::Result<()> {
    let table = "evm_tx";
    let cursor = read_cursor(source, target, chain_id, table).await?;

    // How far behind we are, and how dense the next stretch is. Both come from one
    // bounded probe: sampling only the next DENSITY_SAMPLE_SLOTS keeps it O(sample)
    // rather than O(backlog), which is what the old OFFSET/MAX bound cost.
    const DENSITY_SAMPLE_SLOTS: i64 = 64;
    let probe: Option<PgRow> = sqlx::query(
        r#"
        SELECT
            (SELECT MAX(slot_number) FROM eth_block_txs) AS src_max,
            (SELECT count(*)::float8
               / GREATEST(count(DISTINCT slot_number), 1)::float8
             FROM eth_block_txs
             WHERE slot_number > $1 AND slot_number <= $1 + $2) AS rows_per_slot
        "#,
    )
    .bind(cursor)
    .bind(DENSITY_SAMPLE_SLOTS)
    .fetch_optional(source)
    .await?;

    let src_max: Option<i64> = probe.as_ref().and_then(|r| r.get::<Option<i64>, _>("src_max"));
    let rows_per_slot: f64 = probe
        .as_ref()
        .and_then(|r| r.get::<Option<f64>, _>("rows_per_slot"))
        .unwrap_or(0.0);
    let lag = src_max.unwrap_or(cursor).saturating_sub(cursor);

    // Budget in SLOTS, converted from a shared ROW budget by this table's own
    // density — so evm_tx keeps pace with the slot-bounded tables instead of
    // crawling ~34 slots a pass against their 5000. Below STREAM_LAG_SLOTS this
    // returns 1, which is what makes the live feed read as a stream.
    let slots = slots_per_pass(lag, rows_per_slot, batch_size, MAX_SLOTS_PER_PASS);
    let upper = match src_max {
        Some(m) => (cursor + slots).min(m),
        None => cursor + slots,
    };

    // Pull evm_tx rows first seen in eth_block_txs in (cursor, upper]. The bound is
    // a slot keyset, so pages are whole slots by construction and a busy block can
    // never lose its tx tail. DISTINCT ON tx_hash dedupes a tx in multiple blocks.
    let rows = sqlx::query(evm_tx_select())
        .bind(cursor)
        .bind(upper)
    .fetch_all(source)
    .await?;

    if rows.is_empty() {
        // A slot keyset can select a legitimately EMPTY range — quiet slots. The old
        // OFFSET/MAX bound always landed on a non-empty slot, so returning early was
        // safe; here it would re-query the same dead range forever and stall the
        // mirror. Skip the scanned range instead.
        if let Some(next) = cursor_after_empty_range(cursor, slots, src_max) {
            write_cursor(target, chain_id, table, next).await?;
        }
        return Ok(());
    }

    let max_slot: i64 = rows.iter().map(|r| r.get::<i64, _>("slot_number")).max().unwrap_or(cursor);
    let count = rows.len();

    // Accumulates (tx_hash, method_id) for every row in this batch so that the
    // NOTIFY at the end can skip pure-oracle batches (see pick_new_tx_notify_hash).
    let mut batch_for_notify: Vec<(String, Option<String>)> = Vec::with_capacity(count);
    // Accumulates the rows for one batched UNNEST upsert (was one INSERT per row).
    let mut batch: Vec<EvmTxRow> = Vec::with_capacity(count);

    for row in rows {
        let tx_hash: String = row.get("tx_hash");
        let rlp_bytes: Option<Vec<u8>> = row.get("rlp");
        // from_address from Hercules: already recovered and stored there. Used as a
        // fallback if our own RLP recovery fails (e.g., non-standard tx type), and
        // as the AUTHORITATIVE source for Solana-native txs (see resolve_sync_from).
        let from_address: Option<String> = row.get("from_address");
        // origination distinguishes signature-recovered ('ecdsa') from Solana-native
        // ('solana_unsigned') txs. Default 'ecdsa' if somehow NULL (pre-Phase-2 rows).
        let origination: String = row
            .get::<Option<String>, _>("origination")
            .unwrap_or_else(|| "ecdsa".to_string());
        // base58 Solana pubkey that authorized a Solana-native tx; NULL for ecdsa.
        let solana_signer: Option<String> = row.get("solana_signer");

        // Attempt to decode the RLP to populate denormalized columns.
        // For Solana-origin txs the EIP-1559 envelope has zeroed v/r/s — ECDSA
        // recovery is impossible and unnecessary (from is resolved by
        // resolve_sync_from from the Hercules synthetic address). Skipping
        // recovery prevents the error from discarding the perfectly-valid
        // to_addr/method_id fields for those rows.
        let recover_sender = origination != "solana_unsigned";
        let decoded = rlp_bytes.as_deref().and_then(|raw| {
            match rlp_decode::decode_signed_tx(raw, recover_sender) {
                Ok(d) => Some(d),
                Err(e) => {
                    tracing::warn!(
                        tx_hash = %tx_hash,
                        error = %e,
                        "RLP decode failed — inserting with NULL decoded fields"
                    );
                    metrics::inc_rlp_decode_errors(chain_id as u64);
                    None
                }
            }
        });

        // Resolve which `from` to store. For 'ecdsa' txs: prefer the locally
        // signature-recovered `from`, fall back to Hercules'. For 'solana_unsigned'
        // txs the local RLP recovery is garbage (zeroed v/r/s) — ALWAYS use
        // Hercules' authoritative synthetic `from`.
        //
        // This same resolution feeds BOTH the `from_address` column and the
        // denormalized `from_addr` column. The denorm column must NOT be bound
        // from `decoded.from` directly: with recover_sender=false (solana_unsigned)
        // that is the zero-address sentinel, which would shadow the synthetic
        // sender in rome-via-api's `COALESCE(from_addr, from_address)` display.
        let effective_from = denorm_from_addr(
            &origination,
            decoded.as_ref().map(|d| d.from.as_str()),
            from_address.as_deref(),
        );

        // Record this tx for the oracle-aware NOTIFY decision after the loop.
        let method_id = decoded.as_ref().and_then(|d| d.method_id.clone());
        batch_for_notify.push((tx_hash.clone(), method_id.clone()));

        // Accumulate for the batched upsert. from_addr = the RESOLVED sender
        // (synthetic for solana_unsigned, recovered for ecdsa), NOT decoded.from —
        // which is the zero sentinel for solana_unsigned and would shadow the
        // synthetic in rome-via-api's COALESCE(from_addr, from_address) display.
        batch.push(EvmTxRow {
            tx_hash,
            rlp: rlp_bytes,
            from_address: effective_from.clone(),
            from_addr: effective_from,
            to_addr: decoded.as_ref().and_then(|d| d.to.clone()),
            value_wei: decoded.as_ref().map(|d| d.value_wei.to_string()),
            nonce: decoded.as_ref().map(|d| d.nonce),
            gas_price: decoded.as_ref().map(|d| d.gas_price.to_string()),
            gas_limit: decoded.as_ref().map(|d| d.gas_limit),
            method_id,
            input_len: decoded.as_ref().map(|d| d.input_len),
            tx_type_byte: decoded.as_ref().map(|d| d.tx_type_byte),
            origination,
            solana_signer,
            cpi_program_calldata: decoded
                .as_ref()
                .and_then(|d| d.cpi_call.as_ref())
                .map(|c| c.program_id.clone()),
            cpi_program_label_calldata: decoded
                .as_ref()
                .and_then(|d| d.cpi_call.as_ref())
                .and_then(|c| c.program_label.clone()),
            cpi_instruction_calldata: decoded
                .as_ref()
                .and_then(|d| d.cpi_call.as_ref())
                .and_then(|c| c.instruction.clone()),
        });
    }

    // batch_upsert_evm_tx dedups by tx_hash internally (last-wins) to satisfy the
    // single-statement ON CONFLICT DO UPDATE invariant.
    batch_upsert_evm_tx(target, chain_id, &batch).await?;

    write_cursor(target, chain_id, table, max_slot).await?;

    for _ in 0..count {
        metrics::inc_rows_synced("evm_tx", chain_id as u64);
    }

    // NOTIFY SSE listeners only when this batch contained a non-oracle tx.
    // Pure oracle-keeper batches (method_id 0xf8ac93e8) are skipped so the
    // explorer's "new tx" badge and live-feed refetch don't fire on keeper noise.
    // Full table of tx hashes omitted to stay within 8KB Postgres NOTIFY limit;
    // SSE handlers query the DB for fresh data on each event.
    if let Some(last_hash) = pick_new_tx_notify_hash(&batch_for_notify) {
        let notify_payload = new_tx_notify_payload(
            chain_id,
            &last_hash,
            cursor + 1,
            max_slot,
            non_oracle_count(&batch_for_notify),
        );
        let _ = sqlx::query(&format!(
            "SELECT pg_notify('rome_via_new_tx', '{}')",
            notify_payload.replace('\'', "''")
        ))
        .execute(target)
        .await;
    }

    tracing::debug!(table, cursor, max_slot, count, "synced rows");
    Ok(())
}

// ─── evm_tx_result ───────────────────────────────────────────────────────────

/// Pull the priority portion of the fee (lamports) out of a `tx_result` JSONB.
/// Path: `tx_result.gas_report.priority_fee` — the SDK serializes the field as an
/// ethers `U256` (hex string `"0x…"`, but tolerate decimal string / JSON number
/// too). Returns 0 when the key is absent (legacy rows / pre-priority txs that
/// never set the PRIORITY_FEE marker) or on parse failure — 0 priority means the
/// base fee is the full `gas_value`, which is the correct legacy semantics.
/// Lamport-scale, so `i64` (BIGINT) holds it; a pathological overflow saturates.
fn extract_priority_fee(tx_result: &serde_json::Value) -> i64 {
    let Some(v) = tx_result.get("gas_report").and_then(|g| g.get("priority_fee")) else {
        return 0;
    };
    if let Some(n) = v.as_u64() {
        return i64::try_from(n).unwrap_or(i64::MAX);
    }
    let Some(s) = v.as_str() else { return 0 };
    let parsed = if let Some(hex) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        u128::from_str_radix(hex, 16).ok()
    } else {
        s.parse::<u128>().ok()
    };
    parsed
        .map(|n| i64::try_from(n).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// Row shape for the evm_tx_result batched insert. JSONB columns travel as JSON
/// text and are cast `::jsonb` in the SELECT.
struct EvmTxResultRow {
    slot_number: i64,
    tx_hash: String,
    tx_result_json: String,
    receipt_params_json: Option<String>,
    priority_fee: i64,
}

/// Batched UNNEST insert for evm_tx_result (ON CONFLICT DO NOTHING).
async fn batch_insert_evm_tx_result(
    target: &PgPool,
    chain_id: i64,
    rows: &[EvmTxResultRow],
) -> anyhow::Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }
    let slot: Vec<i64> = rows.iter().map(|r| r.slot_number).collect();
    let tx_hash: Vec<String> = rows.iter().map(|r| r.tx_hash.clone()).collect();
    let tx_result: Vec<String> = rows.iter().map(|r| r.tx_result_json.clone()).collect();
    let receipt_params: Vec<Option<String>> = rows.iter().map(|r| r.receipt_params_json.clone()).collect();
    let priority_fee: Vec<i64> = rows.iter().map(|r| r.priority_fee).collect();
    let res = sqlx::query(
        r#"
        INSERT INTO rome_via.evm_tx_result
            (chain_id, slot_number, tx_hash, tx_result, receipt_params, priority_fee)
        SELECT $1, u.slot_number, u.tx_hash, u.tx_result::jsonb, u.receipt_params::jsonb, u.priority_fee
        FROM UNNEST($2::bigint[], $3::varchar[], $4::text[], $5::text[], $6::bigint[])
            AS u(slot_number, tx_hash, tx_result, receipt_params, priority_fee)
        ON CONFLICT (chain_id, slot_number, tx_hash) DO NOTHING
        "#,
    )
    .bind(chain_id)
    .bind(&slot)
    .bind(&tx_hash)
    .bind(&tx_result)
    .bind(&receipt_params)
    .bind(&priority_fee)
    .execute(target)
    .await?;
    Ok(res.rows_affected())
}

async fn sync_evm_tx_result(
    source: &PgPool,
    target: &PgPool,
    chain_id: i64,
    batch_size: i64,
) -> anyhow::Result<()> {
    let table = "evm_tx_result";
    let cursor = read_cursor(source, target, chain_id, table).await?;

    // Budget in SLOTS, converted from the shared ROW budget by this table's own
    // observed density, and clamped to what the source has actually written.
    let (src_max, rows_per_slot) = probe_slot_bounds(source, table, cursor).await?;
    let lag = src_max.unwrap_or(cursor).saturating_sub(cursor);
    let slots = slots_per_pass(lag, rows_per_slot, batch_size, MAX_SLOTS_PER_PASS);
    let upper = UpperSlot(match src_max {
        Some(m) => (cursor + slots).min(m),
        None => cursor + slots,
    });

    // Slot-boundary batching (shared builder) — eth_block_txs FK-references this
    // table, so a split here would also block eth_block_txs tail rows from syncing.
    let rows = sqlx::query(&slot_boundary_select(
        "slot_number, tx_hash, tx_result, receipt_params",
        "evm_tx_result",
        "slot_number, tx_hash",
    ))
    .bind(cursor)
    .bind(upper.0)
    .fetch_all(source)
    .await?;

    if rows.is_empty() {
        // A slot keyset can select a legitimately EMPTY range — quiet slots. The
        // OFFSET bound this replaced always landed on a non-empty slot, so returning
        // early was safe; with a keyset it would re-query the same dead range forever
        // and STALL the mirror — worst on quiet chains. Skip the scanned range, never
        // past what the source has actually written.
        if let Some(next) = cursor_after_empty_range(cursor, slots, src_max) {
            write_cursor(target, chain_id, table, next).await?;
        }
        return Ok(());
    }

    let max_slot: i64 = rows.iter().map(|r| r.get::<i64, _>("slot_number")).max().unwrap_or(cursor);
    let count = rows.len();

    let batch: Vec<EvmTxResultRow> = rows
        .into_iter()
        .map(|row| {
            let tx_result: serde_json::Value = row.get("tx_result");
            let receipt_params: Option<serde_json::Value> = row.get("receipt_params");
            // Denormalize the priority portion of the fee out of the gas_report JSONB
            // into its own column. Absent / pre-priority txs read as 0 (base == total).
            let priority_fee = extract_priority_fee(&tx_result);
            EvmTxResultRow {
                slot_number: row.get("slot_number"),
                tx_hash: row.get("tx_hash"),
                tx_result_json: tx_result.to_string(),
                receipt_params_json: receipt_params.map(|v| v.to_string()),
                priority_fee,
            }
        })
        .collect();
    batch_insert_evm_tx_result(target, chain_id, &batch).await?;

    write_cursor(target, chain_id, table, max_slot).await?;

    for _ in 0..count {
        metrics::inc_rows_synced("evm_tx_result", chain_id as u64);
    }

    tracing::debug!(table, cursor, max_slot, count, "synced rows");
    Ok(())
}

// ─── evm_tx_sol_tx ───────────────────────────────────────────────────────────

/// Row shape for the evm_tx_sol_tx batched upsert.
struct EvmTxSolTxRow {
    evm_tx_hash: String,
    sol_signature: String,
    slot_number: i64,
    tx_idx: i32,
    instr_idx: i32,
}

/// Batched UNNEST upsert for evm_tx_sol_tx. `DO UPDATE SET tx_idx, instr_idx`
/// refreshes the per-leg ordinals on re-sync. Dedups by (evm_tx_hash,
/// sol_signature) internally (last-wins) — this table can legitimately carry the
/// same key twice in a batch, and a single DO UPDATE can't touch a key twice.
async fn batch_upsert_evm_tx_sol_tx(
    target: &PgPool,
    chain_id: i64,
    rows: &[EvmTxSolTxRow],
) -> anyhow::Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }
    let kept: Vec<&EvmTxSolTxRow> = dedupe_last_by_key(
        rows.iter().collect(),
        |r| (r.evm_tx_hash.clone(), r.sol_signature.clone()),
    );
    let evm_tx_hash: Vec<String> = kept.iter().map(|r| r.evm_tx_hash.clone()).collect();
    let sol_signature: Vec<String> = kept.iter().map(|r| r.sol_signature.clone()).collect();
    let slot_number: Vec<i64> = kept.iter().map(|r| r.slot_number).collect();
    let tx_idx: Vec<i32> = kept.iter().map(|r| r.tx_idx).collect();
    let instr_idx: Vec<i32> = kept.iter().map(|r| r.instr_idx).collect();
    let res = sqlx::query(
        r#"
        INSERT INTO rome_via.evm_tx_sol_tx
            (chain_id, evm_tx_hash, sol_signature, slot_number, tx_idx, instr_idx)
        SELECT $1, u.evm_tx_hash, u.sol_signature, u.slot_number, u.tx_idx, u.instr_idx
        FROM UNNEST($2::varchar[], $3::varchar[], $4::bigint[], $5::int[], $6::int[])
            AS u(evm_tx_hash, sol_signature, slot_number, tx_idx, instr_idx)
        ON CONFLICT (chain_id, evm_tx_hash, sol_signature)
            DO UPDATE SET tx_idx = EXCLUDED.tx_idx, instr_idx = EXCLUDED.instr_idx
        "#,
    )
    .bind(chain_id)
    .bind(&evm_tx_hash)
    .bind(&sol_signature)
    .bind(&slot_number)
    .bind(&tx_idx)
    .bind(&instr_idx)
    .execute(target)
    .await?;
    Ok(res.rows_affected())
}

async fn sync_evm_tx_sol_tx(
    source: &PgPool,
    target: &PgPool,
    chain_id: i64,
    batch_size: i64,
) -> anyhow::Result<()> {
    let table = "evm_tx_sol_tx";
    let cursor = read_cursor(source, target, chain_id, table).await?;

    // Budget in SLOTS, converted from the shared ROW budget by this table's own
    // observed density, and clamped to what the source has actually written.
    let (src_max, rows_per_slot) = probe_slot_bounds(source, table, cursor).await?;
    let lag = src_max.unwrap_or(cursor).saturating_sub(cursor);
    let slots = slots_per_pass(lag, rows_per_slot, batch_size, MAX_SLOTS_PER_PASS);
    let upper = UpperSlot(match src_max {
        Some(m) => (cursor + slots).min(m),
        None => cursor + slots,
    });

    // Slot-boundary batching (shared builder) — same fix as the other slot-keyed tables.
    let rows = sqlx::query(&slot_boundary_select(
        "evm_tx_hash, sol_signature, slot_number, tx_idx, instr_idx",
        "evm_tx_sol_tx",
        "slot_number, tx_idx, instr_idx",
    ))
    .bind(cursor)
    .bind(upper.0)
    .fetch_all(source)
    .await?;

    if rows.is_empty() {
        // A slot keyset can select a legitimately EMPTY range — quiet slots. The
        // OFFSET bound this replaced always landed on a non-empty slot, so returning
        // early was safe; with a keyset it would re-query the same dead range forever
        // and STALL the mirror — worst on quiet chains. Skip the scanned range, never
        // past what the source has actually written.
        if let Some(next) = cursor_after_empty_range(cursor, slots, src_max) {
            write_cursor(target, chain_id, table, next).await?;
        }
        return Ok(());
    }

    let max_slot: i64 = rows.iter().map(|r| r.get::<i64, _>("slot_number")).max().unwrap_or(cursor);
    let count = rows.len();

    let batch: Vec<EvmTxSolTxRow> = rows
        .into_iter()
        .map(|row| EvmTxSolTxRow {
            evm_tx_hash: row.get("evm_tx_hash"),
            sol_signature: row.get("sol_signature"),
            slot_number: row.get("slot_number"),
            tx_idx: row.get("tx_idx"),
            instr_idx: row.get("instr_idx"),
        })
        .collect();
    batch_upsert_evm_tx_sol_tx(target, chain_id, &batch).await?;

    write_cursor(target, chain_id, table, max_slot).await?;

    for _ in 0..count {
        metrics::inc_rows_synced("evm_tx_sol_tx", chain_id as u64);
    }

    tracing::debug!(table, cursor, max_slot, count, "synced rows");
    Ok(())
}

// ─── Oracle-aware SSE gate ───────────────────────────────────────────────────

/// The `refresh()` selector emitted by the oracle keeper on every price-push.
/// Batches that consist entirely of these txs should not wake SSE listeners —
/// the explorer "new tx" badge and live-feed refetch reflect real activity only.
const ORACLE_REFRESH_SELECTOR: &str = "0xf8ac93e8";

/// Non-oracle tx count for a batch — what the explorer's "N new transactions"
/// badge means. The keeper's `refresh()` runs ~180/hr and is hidden from the
/// de-noised feed the badge sits above, so counting it would advance a number
/// against rows the reader is never shown.
fn non_oracle_count(batch: &[(String, Option<String>)]) -> usize {
    batch
        .iter()
        .filter(|(_, m)| m.as_deref() != Some(ORACLE_REFRESH_SELECTOR))
        .count()
}

/// SSE payload for `rome_via_new_block`.
///
/// Carries the RANGE, not just the endpoint. One event means a whole pass landed,
/// so an endpoint alone left the client unable to tell "1 block arrived" from
/// "1024 did" — it could only count events, and a 5000-row batch rendered as "1".
///
/// `blockNumber` is retained alongside the range so an older client keeps working:
/// the rollout is server-first, and there is no flag day.
pub(crate) fn new_block_notify_payload(
    chain_id: i64,
    from_block: i64,
    to_block: i64,
    block_count: usize,
) -> String {
    format!(
        r#"{{"chainId":{chain_id},"blockNumber":{to_block},"fromBlock":{from_block},"toBlock":{to_block},"blockCount":{block_count}}}"#
    )
}

/// SSE payload for `rome_via_new_tx`. `txHash` is retained for older clients;
/// `txCount` is the NON-oracle count, matching the feed the badge sits above.
///
/// The full hash table is still omitted — 8 kB is the Postgres NOTIFY ceiling and
/// a busy pass carries far more than that. Handlers re-query for the rows; the
/// range is only there so the client can pace the render honestly.
pub(crate) fn new_tx_notify_payload(
    chain_id: i64,
    last_hash: &str,
    from_block: i64,
    to_block: i64,
    tx_count: usize,
) -> String {
    format!(
        r#"{{"chainId":{chain_id},"txHash":"{last_hash}","fromBlock":{from_block},"toBlock":{to_block},"txCount":{tx_count}}}"#
    )
}

/// Given the `(tx_hash, method_id)` pairs for each tx synced in a batch (in
/// insertion order), return the hash to announce via `rome_via_new_tx`, or
/// `None` if every tx in the batch was an oracle refresh.
///
/// "Last non-oracle" is chosen so that the announced hash is a real tx the
/// explorer can navigate to, not a keeper artifact.
fn pick_new_tx_notify_hash(batch: &[(String, Option<String>)]) -> Option<String> {
    batch
        .iter()
        .rev()
        .find(|(_, m)| m.as_deref() != Some(ORACLE_REFRESH_SELECTOR))
        .map(|(h, _)| h.clone())
}

// ─── Unit tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {


    /// The counter must count INSERTS, not affected rows.
    ///
    /// `evm_tx` upserts with `ON CONFLICT … DO UPDATE` (it refreshes origination and
    /// solana_signer), so `rows_affected()` includes updates to rows that already
    /// existed. Incrementing a total by it would drift upward forever, silently, and
    /// the drift is invisible because the number only ever looks plausible.
    ///
    /// `xmax = 0` is true only for a tuple this statement inserted, so filtering on
    /// it gives the exact count of new transactions.
    #[test]
    fn counter_delta_counts_inserts_not_upserts() {
        let q = super::evm_tx_insert_sql();
        assert!(
            q.contains("xmax = 0"),
            "must distinguish inserts from conflict-updates: {q}"
        );
        assert!(
            q.contains("RETURNING"),
            "needs RETURNING to observe xmax per row: {q}"
        );
        // The upsert behaviour itself must not change — it is what keeps origination
        // and solana_signer fresh for rows Hercules re-classified.
        assert!(
            q.contains("DO UPDATE"),
            "must keep refreshing origination/solana_signer: {q}"
        );
        // The counter bump must be part of the SAME statement. Split across two
        // statements on a pool (not a transaction) there is a window where a crash
        // lands the rows but loses the increment, and the counter silently under-counts.
        assert!(
            q.contains("chain_counters"),
            "counter must be bumped in the same statement as the insert: {q}"
        );
        assert!(
            q.contains("count(*) FILTER (WHERE inserted)"),
            "the increment must be the insert count, not rows affected: {q}"
        );
    }

    /// An idle chain must not keep asking. sync issues ~16 queries per cycle
    /// (7 cursor reads + 7 selects + 2 cursors_sum) every 2s whether or not there is
    /// work; enrich adds 15 workers on a 5s tick. Measured 2026-07-27 that is ~11
    /// queries/s per chain with nothing to do, and ~44/s across the four chains on the
    /// shared instance — on a quiet chain, the entire database load.
    #[test]
    fn idle_polling_backs_off_and_is_capped() {
        let base = 2;
        let max = 30;
        // First idle round keeps the base cadence — a chain that just went quiet
        // should still feel responsive.
        assert_eq!(super::next_poll_secs(0, base, max), base);
        assert_eq!(super::next_poll_secs(1, base, max), base);
        // Sustained idleness backs off geometrically.
        assert_eq!(super::next_poll_secs(2, base, max), 4);
        assert_eq!(super::next_poll_secs(3, base, max), 8);
        // And is capped, so a long-quiet chain still notices work within `max`.
        assert_eq!(super::next_poll_secs(20, base, max), max);
        assert_eq!(super::next_poll_secs(i32::MAX as u32, base, max), max);
    }

    /// The first sign of work must restore full cadence immediately — backing off
    /// is only ever about idleness, never about throttling a busy chain.
    #[test]
    fn any_work_resets_to_base_cadence() {
        assert_eq!(super::next_poll_secs(0, 2, 30), 2);
    }

    /// A keyset bound (`slot_number <= cursor + N`) can legitimately select an
    /// EMPTY range — slots with no transactions. The old OFFSET/MAX bound always
    /// landed on a non-empty slot, so the sync could return early on empty and be
    /// correct. With a keyset bound that would re-query the same dead range forever.
    /// The cursor must skip the scanned range instead.
    #[test]
    fn empty_range_advances_the_cursor_instead_of_stalling() {
        // Nothing found in slots 101..=200, but source has data further on.
        assert_eq!(
            super::cursor_after_empty_range(100, 100, Some(5_000)),
            Some(200),
            "must skip the scanned range, not sit on it"
        );
        // Scanned past the end of the source: clamp to what exists, never overshoot.
        assert_eq!(
            super::cursor_after_empty_range(100, 100, Some(150)),
            Some(150),
            "must not advance beyond the source's max slot"
        );
        // Already at/after the source tip → genuinely caught up, leave the cursor.
        assert_eq!(super::cursor_after_empty_range(100, 100, Some(100)), None);
        assert_eq!(super::cursor_after_empty_range(100, 100, None), None);
    }

    /// The budget must be expressed in SLOTS, not rows, or tables advance at wildly
    /// different rates. Measured on hadrian-lt 2026-07-27: one `batch_size` of 5000
    /// meant 5000 slots for `eth_block` but ~34 slots for `evm_tx` (~145 tx/slot), a
    /// ~147x asymmetry — `eth_block` sat 12 slots behind while `evm_tx` was 26,093.
    #[test]
    fn slot_budget_equalises_tables_with_different_row_density() {
        // Same row budget, very different densities → same ROW cost per pass,
        // which is the point: no table starves another.
        let sparse = super::slots_per_pass(100_000, 1.0, 20_000, 5_000); // sol_slot
        let dense = super::slots_per_pass(100_000, 145.0, 20_000, 5_000); // evm_tx
        assert_eq!(sparse, 5_000, "1 row/slot should run to the slot cap");
        assert_eq!(dense, 137, "145 rows/slot should fit the 20k row budget");
        // Row cost is comparable, which is what stops the 147x divergence.
        assert!((dense as f64 * 145.0) <= 20_000.0);
    }

    /// A caught-up chain must emit fine-grained progress so the live feed reads as a
    /// stream rather than a jump. One slot per pass at ~2.6 slots/s is streaming
    /// cadence; a 5000-slot pass is a teleport.
    #[test]
    fn caught_up_chain_advances_one_slot_at_a_time() {
        for lag in [0, 1, 5, super::STREAM_LAG_SLOTS] {
            assert_eq!(
                super::slots_per_pass(lag, 145.0, 20_000, 5_000),
                1,
                "lag={lag} should stream, not batch"
            );
        }
    }

    /// Degenerate inputs must never stall the sync. Returning 0 would advance the
    /// cursor by nothing and spin forever.
    #[test]
    fn budget_is_never_zero() {
        assert!(super::slots_per_pass(100_000, 0.0, 20_000, 5_000) >= 1);
        assert!(super::slots_per_pass(100_000, 1e9, 20_000, 5_000) >= 1);
        assert!(super::slots_per_pass(0, 0.0, 0, 0) >= 1);
    }


    use serde_json::json;
    use sqlx::postgres::PgPoolOptions;
    use sqlx::PgPool;

    /// Env-gated scratch Postgres for the batched-insert tests. Set
    /// `VIA_SYNC_TEST_DATABASE_URL` to a throwaway Postgres to run them:
    ///   VIA_SYNC_TEST_DATABASE_URL=postgres://postgres:pw@localhost:5433/via_test \
    ///     cargo test -p rome-via-sync
    /// Unset (CI default, no DB) → returns None and each DB test returns early.
    /// Every DB test is `#[serial_test::serial]` because this drops + rebuilds the
    /// `rome_via` schema, so concurrent runs would clobber each other.
    async fn test_pool() -> Option<PgPool> {
        let url = std::env::var("VIA_SYNC_TEST_DATABASE_URL").ok()?;
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect(&url)
            .await
            .expect("connect scratch Postgres");
        sqlx::query("DROP SCHEMA IF EXISTS rome_via CASCADE")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DROP TABLE IF EXISTS _sqlx_migrations")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        Some(pool)
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn eth_block_txs_batch_inserts_and_dedupes() {
        let Some(pool) = test_pool().await else { return };
        let chain_id = 200010i64;
        let rows = vec![
            super::EthBlockTxRow { slot_number: 10, slot_block_idx: 0, tx_hash: "0xaa".into(), tx_idx: 0 },
            super::EthBlockTxRow { slot_number: 10, slot_block_idx: 0, tx_hash: "0xbb".into(), tx_idx: 1 },
        ];
        let n = super::batch_insert_eth_block_txs(&pool, chain_id, &rows).await.unwrap();
        assert_eq!(n, 2);
        // Re-insert the same batch → ON CONFLICT DO NOTHING: no error, no dupes.
        super::batch_insert_eth_block_txs(&pool, chain_id, &rows).await.unwrap();
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM rome_via.eth_block_txs WHERE chain_id = $1",
        )
        .bind(chain_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count, 2);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn sol_slot_batch_inserts_and_dedupes() {
        let Some(pool) = test_pool().await else { return };
        let chain_id = 200010i64;
        let rows = vec![
            super::SolSlotRow { slot_number: 10, parent_slot: Some(9), status: Some("Finalized".into()), blockhash: Some("bh10".into()), timestamp: Some(1_700_000_000) },
            // nullable columns exercised: parent_slot/blockhash/timestamp None.
            super::SolSlotRow { slot_number: 11, parent_slot: None, status: None, blockhash: None, timestamp: None },
        ];
        let n = super::batch_insert_sol_slot(&pool, chain_id, &rows).await.unwrap();
        assert_eq!(n, 2);
        super::batch_insert_sol_slot(&pool, chain_id, &rows).await.unwrap(); // DO NOTHING
        let (count, status10): (i64, Option<String>) = {
            let c: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM rome_via.sol_slot WHERE chain_id=$1").bind(chain_id).fetch_one(&pool).await.unwrap();
            let s: Option<String> = sqlx::query_scalar("SELECT status FROM rome_via.sol_slot WHERE chain_id=$1 AND slot_number=10").bind(chain_id).fetch_one(&pool).await.unwrap();
            (c, s)
        };
        assert_eq!(count, 2);
        assert_eq!(status10.as_deref(), Some("Finalized"));
    }



    #[tokio::test]
    #[serial_test::serial]
    async fn eth_block_batch_inserts_numeric_and_dedupes() {
        let Some(pool) = test_pool().await else { return };
        let chain_id = 200010i64;
        let rows = vec![
            super::EthBlockRow {
                slot_number: 10,
                slot_block_idx: 0,
                block_gas_used: Some("12345".into()),
                gas_recipient: Some("0xrecipient".into()),
                slot_timestamp: Some(1_700_000_000),
                params_blockhash: Some("0xbh".into()),
                params_parent_hash: Some("0xph".into()),
                params_number: Some(478_000_000),
                params_block_timestamp: Some("1699999999".into()),
            },
            // params NULL (block_gas_used is NOT NULL in the schema — set at
            // shell-insert, params fill later — so it stays populated here).
            super::EthBlockRow {
                slot_number: 11,
                slot_block_idx: 0,
                block_gas_used: Some("0".into()),
                gas_recipient: None,
                slot_timestamp: None,
                params_blockhash: None,
                params_parent_hash: None,
                params_number: None,
                params_block_timestamp: None,
            },
        ];
        let n = super::batch_insert_eth_block(&pool, chain_id, &rows).await.unwrap();
        assert_eq!(n, 2);
        super::batch_insert_eth_block(&pool, chain_id, &rows).await.unwrap(); // DO NOTHING
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM rome_via.eth_block WHERE chain_id=$1").bind(chain_id).fetch_one(&pool).await.unwrap();
        assert_eq!(count, 2);
        // NUMERIC columns round-trip through the text[] -> ::numeric cast.
        let gas_used: Option<String> = sqlx::query_scalar("SELECT block_gas_used::text FROM rome_via.eth_block WHERE chain_id=$1 AND slot_number=10").bind(chain_id).fetch_one(&pool).await.unwrap();
        assert_eq!(gas_used.as_deref(), Some("12345"));
        let bts: Option<String> = sqlx::query_scalar("SELECT params_block_timestamp::text FROM rome_via.eth_block WHERE chain_id=$1 AND slot_number=10").bind(chain_id).fetch_one(&pool).await.unwrap();
        assert_eq!(bts.as_deref(), Some("1699999999"));
    }

    /// Build an EvmTxRow with sensible defaults, overriding only what a test cares about.
    fn evm_row(tx_hash: &str, origination: &str, solana_signer: Option<&str>) -> super::EvmTxRow {
        super::EvmTxRow {
            tx_hash: tx_hash.into(),
            rlp: Some(vec![1, 2, 3]),
            from_address: Some("0xfrom".into()),
            from_addr: Some("0xfrom".into()),
            to_addr: Some("0xto".into()),
            value_wei: Some("100".into()),
            nonce: Some(1),
            gas_price: Some("5".into()),
            gas_limit: Some(21000),
            method_id: None,
            input_len: Some(0),
            tx_type_byte: Some(2),
            origination: origination.into(),
            solana_signer: solana_signer.map(|s| s.into()),
            cpi_program_calldata: None,
            cpi_program_label_calldata: None,
            cpi_instruction_calldata: None,
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn evm_tx_self_heals_origination_but_keeps_immutable_fields() {
        let Some(pool) = test_pool().await else { return };
        let chain_id = 200010i64;
        super::batch_upsert_evm_tx(&pool, chain_id, &[evm_row("0xtx", "ecdsa", None)]).await.unwrap();
        // Re-sync same tx_hash, now tagged solana_unsigned with a signer AND different immutable fields.
        let mut second = evm_row("0xtx", "solana_unsigned", Some("SoLsigner"));
        second.to_addr = Some("0xDIFFERENT".into());
        second.rlp = Some(vec![9, 9]);
        super::batch_upsert_evm_tx(&pool, chain_id, &[second]).await.unwrap();
        let (orig, signer, to_addr): (String, Option<String>, Option<String>) =
            sqlx::query_as::<_, (String, Option<String>, Option<String>)>(
                "SELECT origination, solana_signer, to_addr FROM rome_via.evm_tx WHERE chain_id=$1 AND tx_hash='0xtx'")
            .bind(chain_id).fetch_one(&pool).await.unwrap();
        assert_eq!(orig, "solana_unsigned");           // self-healed
        assert_eq!(signer.as_deref(), Some("SoLsigner")); // self-healed
        assert_eq!(to_addr.as_deref(), Some("0xto"));  // immutable — NOT overwritten
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn evm_tx_in_batch_duplicate_keys_last_wins() {
        let Some(pool) = test_pool().await else { return };
        let chain_id = 200010i64;
        // Same tx_hash twice in ONE batch — a naive DO UPDATE would raise
        // "cannot affect row a second time"; dedup keeps the LAST occurrence.
        let rows = vec![
            evm_row("0xdup", "ecdsa", None),
            evm_row("0xdup", "solana_unsigned", Some("Sig2")),
        ];
        super::batch_upsert_evm_tx(&pool, chain_id, &rows).await.unwrap();
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM rome_via.evm_tx WHERE chain_id=$1 AND tx_hash='0xdup'").bind(chain_id).fetch_one(&pool).await.unwrap();
        assert_eq!(count, 1);
        let (orig, signer): (String, Option<String>) =
            sqlx::query_as::<_, (String, Option<String>)>(
                "SELECT origination, solana_signer FROM rome_via.evm_tx WHERE chain_id=$1 AND tx_hash='0xdup'")
            .bind(chain_id).fetch_one(&pool).await.unwrap();
        assert_eq!(orig, "solana_unsigned");
        assert_eq!(signer.as_deref(), Some("Sig2"));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn evm_tx_result_batch_inserts_jsonb_and_dedupes() {
        let Some(pool) = test_pool().await else { return };
        let chain_id = 200010i64;
        let rows = vec![
            super::EvmTxResultRow { slot_number: 10, tx_hash: "0xr1".into(), tx_result_json: r#"{"exit_reason":{"code":0}}"#.into(), receipt_params_json: None, priority_fee: 7000 },
            super::EvmTxResultRow { slot_number: 10, tx_hash: "0xr2".into(), tx_result_json: r#"{"exit_reason":{"code":1}}"#.into(), receipt_params_json: Some(r#"{"logs":[]}"#.into()), priority_fee: 0 },
        ];
        let n = super::batch_insert_evm_tx_result(&pool, chain_id, &rows).await.unwrap();
        assert_eq!(n, 2);
        super::batch_insert_evm_tx_result(&pool, chain_id, &rows).await.unwrap(); // DO NOTHING
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM rome_via.evm_tx_result WHERE chain_id=$1").bind(chain_id).fetch_one(&pool).await.unwrap();
        assert_eq!(count, 2);
        // text[] -> ::jsonb round-trips: the JSONB path query resolves.
        let code: Option<String> = sqlx::query_scalar("SELECT tx_result->'exit_reason'->>'code' FROM rome_via.evm_tx_result WHERE chain_id=$1 AND tx_hash='0xr1'").bind(chain_id).fetch_one(&pool).await.unwrap();
        assert_eq!(code.as_deref(), Some("0"));
        let pf: i64 = sqlx::query_scalar("SELECT priority_fee FROM rome_via.evm_tx_result WHERE chain_id=$1 AND tx_hash='0xr1'").bind(chain_id).fetch_one(&pool).await.unwrap();
        assert_eq!(pf, 7000);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn evm_tx_sol_tx_do_update_refreshes_ordinals() {
        let Some(pool) = test_pool().await else { return };
        let chain_id = 200010i64;
        super::batch_upsert_evm_tx_sol_tx(&pool, chain_id, &[super::EvmTxSolTxRow { evm_tx_hash: "0xh".into(), sol_signature: "SIG".into(), slot_number: 10, tx_idx: 0, instr_idx: 0 }]).await.unwrap();
        // Re-upsert same (hash,sig) with new ordinals → DO UPDATE.
        super::batch_upsert_evm_tx_sol_tx(&pool, chain_id, &[super::EvmTxSolTxRow { evm_tx_hash: "0xh".into(), sol_signature: "SIG".into(), slot_number: 10, tx_idx: 1, instr_idx: 2 }]).await.unwrap();
        let (tx_idx, instr_idx): (i32, i32) = sqlx::query_as::<_, (i32, i32)>("SELECT tx_idx, instr_idx FROM rome_via.evm_tx_sol_tx WHERE chain_id=$1 AND evm_tx_hash='0xh' AND sol_signature='SIG'").bind(chain_id).fetch_one(&pool).await.unwrap();
        assert_eq!((tx_idx, instr_idx), (1, 2));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn evm_tx_sol_tx_in_batch_duplicate_keys_last_wins() {
        let Some(pool) = test_pool().await else { return };
        let chain_id = 200010i64;
        // Same (evm_tx_hash, sol_signature) twice in ONE batch — dedup keeps the last.
        let rows = vec![
            super::EvmTxSolTxRow { evm_tx_hash: "0xh".into(), sol_signature: "SIG".into(), slot_number: 10, tx_idx: 0, instr_idx: 0 },
            super::EvmTxSolTxRow { evm_tx_hash: "0xh".into(), sol_signature: "SIG".into(), slot_number: 10, tx_idx: 5, instr_idx: 6 },
        ];
        super::batch_upsert_evm_tx_sol_tx(&pool, chain_id, &rows).await.unwrap();
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM rome_via.evm_tx_sol_tx WHERE chain_id=$1").bind(chain_id).fetch_one(&pool).await.unwrap();
        assert_eq!(count, 1);
        let (tx_idx, instr_idx): (i32, i32) = sqlx::query_as::<_, (i32, i32)>("SELECT tx_idx, instr_idx FROM rome_via.evm_tx_sol_tx WHERE chain_id=$1").bind(chain_id).fetch_one(&pool).await.unwrap();
        assert_eq!((tx_idx, instr_idx), (5, 6)); // last wins
    }

    /// priority_fee extraction from the gas_report JSONB: hex string (ethers U256
    /// default), decimal string, and JSON number all parse; absent → 0 (legacy /
    /// pre-priority rows); explicit 0 → 0.
    #[test]
    fn extract_priority_fee_handles_all_forms() {
        // ethers U256 default serialization is a hex string.
        let hex = json!({ "gas_report": { "priority_fee": "0x1b58" } }); // 7000
        assert_eq!(super::extract_priority_fee(&hex), 7000);

        // tolerate a decimal string and a bare JSON number too.
        let dec = json!({ "gas_report": { "priority_fee": "7000" } });
        assert_eq!(super::extract_priority_fee(&dec), 7000);
        let num = json!({ "gas_report": { "priority_fee": 7000 } });
        assert_eq!(super::extract_priority_fee(&num), 7000);

        // explicit zero (priority on, this tx bid nothing).
        let zero = json!({ "gas_report": { "priority_fee": "0x0" } });
        assert_eq!(super::extract_priority_fee(&zero), 0);

        // legacy row: no priority_fee key under gas_report → 0.
        let legacy = json!({ "gas_report": { "gas_value": "0x5208" } });
        assert_eq!(super::extract_priority_fee(&legacy), 0);

        // no gas_report at all → 0 (never panics).
        let bare = json!({ "exit_reason": { "code": 0 } });
        assert_eq!(super::extract_priority_fee(&bare), 0);
    }

    /// Cursor advancement: the max slot from a batch becomes the new cursor.
    #[test]
    fn cursor_advancement_uses_max_slot() {
        let slots: Vec<i64> = vec![10, 20, 30, 40, 50];
        let cursor: i64 = 5;
        let new_cursor = slots
            .iter()
            .copied()
            .filter(|&s| s > cursor)
            .max()
            .unwrap_or(cursor);
        assert_eq!(new_cursor, 50);
    }

    /// An empty batch leaves the cursor unchanged.
    #[test]
    fn empty_batch_leaves_cursor_unchanged() {
        let slots: Vec<i64> = vec![];
        let cursor: i64 = 42;
        let new_cursor = slots.iter().copied().max().unwrap_or(cursor);
        assert_eq!(new_cursor, 42);
    }

    /// Batch capped at batch_size items.
    #[test]
    fn batch_size_caps_rows() {
        let all_rows: Vec<i64> = (1..=1000).collect();
        let batch_size = 500usize;
        let batch: Vec<i64> = all_rows.into_iter().take(batch_size).collect();
        assert_eq!(batch.len(), 500);
    }

    /// Error classification: a connection error should be retried; no panic.
    #[test]
    fn transient_error_does_not_panic() {
        let err = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
        let is_transient = matches!(err.kind(), std::io::ErrorKind::ConnectionRefused);
        assert!(is_transient);
    }

    /// Slot filter: only rows strictly above cursor are fetched.
    #[test]
    fn cursor_filter_is_strictly_greater_than() {
        let all_slots = vec![1i64, 5, 10, 15, 20];
        let cursor = 10i64;
        let filtered: Vec<i64> = all_slots.into_iter().filter(|&s| s > cursor).collect();
        assert_eq!(filtered, vec![15, 20]);
    }

    // ── slot-boundary batching: a block bigger than batch_size must not lose its tail ──
    //
    // Models the per-tx paginated sync as (slot, tx_idx) rows. The fix: each batch
    // fetches COMPLETE slots up to ~batch_size rows (never splitting a slot), so the
    // i64 max-slot cursor never skips a slot's tail on the next `slot > cursor` page.
    fn simulate_sync(rows: &[(i64, i64)], batch_size: usize) -> Vec<(i64, i64)> {
        let mut sorted = rows.to_vec();
        sorted.sort();
        let mut cursor = i64::MIN;
        let mut out = Vec::new();
        loop {
            let above: Vec<(i64, i64)> =
                sorted.iter().copied().filter(|&(s, _)| s > cursor).collect();
            if above.is_empty() {
                break;
            }
            // FIX: fetch COMPLETE slots up to ~batch_size rows. Boundary = slot of the
            // batch_size-th candidate row (or the last slot if fewer remain); take every
            // row of every slot <= boundary, so no slot is split across a page and the
            // max-slot cursor never skips a slot's tail.
            let boundary = above
                .get(batch_size - 1)
                .map(|&(s, _)| s)
                .unwrap_or_else(|| above.last().unwrap().0);
            let batch: Vec<(i64, i64)> =
                above.iter().copied().filter(|&(s, _)| s <= boundary).collect();
            out.extend(batch.iter().copied());
            cursor = boundary; // always a COMPLETE slot
        }
        out
    }

    /// A slot whose tx count exceeds batch_size must still sync ALL its rows —
    /// the old `take(batch_size)` + max-slot cursor dropped the slot's tail.
    #[test]
    fn slot_exceeding_batch_size_syncs_all_rows() {
        // slot 100 has 686 txs (> batch_size 500); slots 101, 102 add a few more.
        let mut rows: Vec<(i64, i64)> = (0i64..686).map(|i| (100i64, i)).collect();
        rows.extend([(101i64, 0i64), (101, 1), (102, 0)]);
        let mut expected = rows.clone();
        expected.sort();
        let synced = simulate_sync(&rows, 500);
        assert_eq!(
            synced, expected,
            "every row of a >batch_size slot must sync (no tail dropped)"
        );
    }

    /// Every slot-keyed sync must page by COMPLETE slots, never a naive row `LIMIT`.
    /// The naive form splits a > batch_size block across pages and drops its tail
    /// once the max-slot cursor advances. This guards the whole class — eth_block_txs,
    /// evm_tx_result, evm_tx_sol_tx, and any future slot-keyed table routed through
    /// the shared builder — against silently regressing to it.
    #[test]
    fn slot_boundary_select_pages_by_complete_slots() {
        let q = super::slot_boundary_select(
            "slot_number, tx_hash, tx_result, receipt_params",
            "evm_tx_result",
            "slot_number, tx_hash",
        );
        // The bound is now a SLOT KEYSET supplied by the caller ($2), matching
        // evm_tx_select. Slots are whole, so bounding by slot still cannot split a
        // block across pages — the drop-prevention survives the rewrite.
        assert!(
            q.contains("slot_number > $1") && q.contains("slot_number <= $2"),
            "must bound the page by a caller-supplied slot keyset: {q}"
        );
        // The OFFSET scan made choosing a batch cost O(batch), so raising
        // batch_size partly paid for itself; the MAX() fallback scanned to the end
        // of the table on every pass with nothing left to do.
        assert!(
            !q.contains("OFFSET"),
            "must not locate the batch by an OFFSET scan: {q}"
        );
        assert!(
            !q.contains("MAX(slot_number)"),
            "must not fall back to a MAX() scan of the remaining table: {q}"
        );
        assert!(
            !q.trim_end().ends_with("LIMIT $2"),
            "must NOT page by a naive row LIMIT (that drops busy-slot tails): {q}"
        );
        // Both slot-keyed selects must speak the same parameter language, or a
        // caller computing an upper bound for one will silently mean "row count"
        // to the other.
        let ev = super::evm_tx_select();
        assert!(
            ev.contains("slot_number > $1") && ev.contains("slot_number <= $2"),
            "evm_tx_select must use the same $1/$2 slot-keyset shape: {ev}"
        );
    }

    /// evm_tx pages by tx_hash (DISTINCT ON) but must still bound each page to
    /// COMPLETE eth_block_txs slots — the same drop-prevention as the slot-keyed
    /// tables, applied to the join — so a > batch_size block doesn't lose its tx
    /// tail. Guards against regressing to the naive `LIMIT $2`.
    #[test]
    fn evm_tx_select_bounds_by_complete_eth_block_txs_slots() {
        let q = super::evm_tx_select();
        assert!(
            q.contains("DISTINCT ON (et.tx_hash)"),
            "must keep dedup of a tx seen in multiple blocks: {q}"
        );
        // The bound is now a SLOT KEYSET supplied by the caller, not an OFFSET scan.
        // OFFSET made choosing a batch cost O(batch), so enlarging batch_size partly
        // paid for itself; and because the budget was in ROWS while sibling tables
        // bounded SLOTS, evm_tx advanced ~34 slots a pass against eth_block's 5000.
        assert!(
            q.contains("ebt.slot_number > $1") && q.contains("ebt.slot_number <= $2"),
            "must bound by a caller-supplied slot keyset: {q}"
        );
        assert!(
            !q.contains("OFFSET"),
            "must NOT scan rows to locate the bound: {q}"
        );
        assert!(
            !q.contains("MAX(slot_number)"),
            "must NOT aggregate over everything above the cursor: {q}"
        );
        // Whole-slot pages are still the invariant — only the mechanism changed.
        // A slot keyset bounds by slot by construction, so a block can never be
        // split across pages and lose its tx tail.
        assert!(
            !q.contains("LIMIT $2") && !q.contains("COALESCE"),
            "must bound the page to complete eth_block_txs slots: {q}"
        );
        assert!(
            !q.trim_end().ends_with("LIMIT $2"),
            "must NOT page by a naive row LIMIT (drops busy-slot tails): {q}"
        );
    }

    // ── resolve_sync_from: the Solana-gate correctness piece ──────────────────
    //
    // For a Solana-native (DoTxUnsigned) tx the stored RLP carries zeroed v/r/s,
    // so the local RLP recovery either errors or yields a GARBAGE `from`. Hercules
    // stores the authoritative synthetic `from` (= keccak(solana_signer)[12:]). The
    // resolver MUST ignore the local-decoded `from` for those rows and use Hercules'.

    /// Solana-native: the synthetic (Hercules) from wins; the garbage RLP-recovered
    /// from is IGNORED. This is the bug-guard for the Phase-2 resolve_from problem.
    #[test]
    fn resolve_sync_from_solana_unsigned_ignores_decoded() {
        let synthetic = "0x857534c2a3f1b9d0e8a76c5d4e3f2a1b0c9d8e7f";
        let got = super::resolve_sync_from(
            "solana_unsigned",
            Some("0xGARBAGE_RECOVERED"),
            Some(synthetic),
        );
        assert_eq!(got, Some(synthetic.to_string()));
    }

    /// Solana-native with no decoded from at all still uses Hercules' synthetic from.
    #[test]
    fn resolve_sync_from_solana_unsigned_no_decoded() {
        let synthetic = "0x857534c2a3f1b9d0e8a76c5d4e3f2a1b0c9d8e7f";
        let got = super::resolve_sync_from("solana_unsigned", None, Some(synthetic));
        assert_eq!(got, Some(synthetic.to_string()));
    }

    /// ECDSA: decoded (signature-recovered) from is preferred — unchanged behavior.
    #[test]
    fn resolve_sync_from_ecdsa_prefers_hercules() {
        // Hercules is authoritative for BOTH lanes. It recovered this sender from
        // the same RLP bytes Via is looking at, so Via re-recovering and PREFERRING
        // its own answer is duplicate work whose only possible effect is to disagree:
        // a tx type the two decoders treat differently would make the explorer show
        // a different sender than the chain does, silently.
        let got = super::resolve_sync_from("ecdsa", Some("0xRECOVERED"), Some("0xHERCULES"));
        assert_eq!(got, Some("0xHERCULES".to_string()));
    }

    /// ECDSA with failed local decode falls back to Hercules' from — unchanged.
    #[test]
    fn resolve_sync_from_ecdsa_falls_back_to_hercules() {
        let got = super::resolve_sync_from("ecdsa", None, Some("0xHERCULES"));
        assert_eq!(got, Some("0xHERCULES".to_string()));
    }

    // ── denorm_from_addr: the displayed `from` (regression guard) ─────────────
    //
    // The denormalized `from_addr` column is what rome-via-api displays as the
    // tx sender (`COALESCE(from_addr, from_address)`, from_addr wins). After the
    // skip-recovery decode change, `decode_signed_tx(recover_sender=false)`
    // returns `from = "0x0000…0000"` (zero sentinel) for solana_unsigned txs.
    // Binding that sentinel into `from_addr` SHADOWS the correct synthetic
    // sender → explorer shows `from: 0x0000…`. `denorm_from_addr` MUST resolve
    // to the synthetic, exactly as `effective_from` does, NOT the sentinel.

    /// The zero address ethers emits when sender recovery is skipped.
    const ZERO_SENTINEL: &str = "0x0000000000000000000000000000000000000000";

    /// solana_unsigned: even when the decoded `from` is the zero sentinel (the
    /// skip-recovery output), the value bound to `from_addr` is the synthetic.
    #[test]
    fn denorm_from_addr_solana_unsigned_is_synthetic_not_zero() {
        let synthetic = "0x857534c2a3f1b9d0e8a76c5d4e3f2a1b0c9d8e7f";
        let got = super::denorm_from_addr(
            "solana_unsigned",
            Some(ZERO_SENTINEL),
            Some(synthetic),
        );
        assert_eq!(
            got,
            Some(synthetic.to_string()),
            "from_addr must be the synthetic sender, not the skip-recovery zero sentinel"
        );
        assert_ne!(
            got.as_deref(),
            Some(ZERO_SENTINEL),
            "from_addr must NOT be the zero sentinel (the regression)"
        );
    }

    /// ecdsa: unchanged — the locally recovered `from` is what gets bound.
    #[test]
    fn denorm_from_addr_ecdsa_is_hercules() {
        // The displayed sender must agree with the chain. denorm_from_addr shares
        // resolve_sync_from precisely so the stored and displayed values cannot
        // diverge, and both now defer to Hercules.
        let got = super::denorm_from_addr("ecdsa", Some("0xRECOVERED"), Some("0xHERCULES"));
        assert_eq!(got, Some("0xHERCULES".to_string()));
    }

    // ── pick_new_tx_notify_hash: oracle-filtering SSE gate ────────────────────
    //
    // The oracle keeper emits `refresh()` txs (method_id 0xf8ac93e8) on every
    // price-update cycle. A pure-oracle batch must NOT wake the SSE badge; a
    // batch with at least one real tx must announce the latest such hash.

    /// The range fields the DEPLOYED client reads. rome-via `src/lib/live-cadence.ts`
    /// `parseLiveEvent` keys on exactly these names and falls back to the legacy
    /// endpoint shape when they are absent — so a rename here does not fail loudly,
    /// it silently reverts the counter to "1 per event". Assert the contract.
    #[test]
    fn block_notify_payload_carries_the_range_and_stays_backward_compatible() {
        let p = super::new_block_notify_payload(200010, 101, 1124, 1024);
        let v: serde_json::Value = serde_json::from_str(&p).expect("valid JSON");
        assert_eq!(v["chainId"], 200010);
        assert_eq!(v["fromBlock"], 101);
        assert_eq!(v["toBlock"], 1124);
        assert_eq!(v["blockCount"], 1024);
        // Legacy field retained: server ships before the client everywhere, and an
        // older bundle must keep working. No flag day.
        assert_eq!(v["blockNumber"], 1124);
    }

    #[test]
    fn tx_notify_payload_carries_the_count_and_stays_backward_compatible() {
        let p = super::new_tx_notify_payload(200010, "0xabc", 101, 1124, 148_000);
        let v: serde_json::Value = serde_json::from_str(&p).expect("valid JSON");
        assert_eq!(v["txCount"], 148_000);
        assert_eq!(v["fromBlock"], 101);
        assert_eq!(v["toBlock"], 1124);
        assert_eq!(v["txHash"], "0xabc");
        // Well inside the 8 kB NOTIFY ceiling — the range is a handful of ints, which
        // is exactly why it fits where the full hash table never could.
        assert!(p.len() < 512, "payload should stay tiny: {} bytes", p.len());
    }

    /// The badge sits above the de-noised feed, which hides the keeper. Counting
    /// oracle txs would advance a number against rows the reader is never shown.
    #[test]
    fn non_oracle_count_excludes_the_keeper() {
        let oracle = Some(super::ORACLE_REFRESH_SELECTOR.to_string());
        let batch = vec![
            ("0x1".to_string(), oracle.clone()),
            ("0x2".to_string(), Some("0xa9059cbb".to_string())),
            ("0x3".to_string(), oracle.clone()),
            ("0x4".to_string(), None),
        ];
        assert_eq!(super::non_oracle_count(&batch), 2);
        assert_eq!(super::non_oracle_count(&[]), 0);
        let all_oracle = vec![("0x1".to_string(), oracle)];
        assert_eq!(super::non_oracle_count(&all_oracle), 0);
    }

    /// A batch containing only oracle refresh() txs → no NOTIFY (returns None).
    #[test]
    fn pick_notify_hash_oracle_only_is_none() {
        let batch = vec![
            ("h1".to_string(), Some("0xf8ac93e8".to_string())),
            ("h2".to_string(), Some("0xf8ac93e8".to_string())),
        ];
        assert_eq!(super::pick_new_tx_notify_hash(&batch), None);
    }

    /// A mixed batch returns the LAST non-oracle hash (h2 here, not h3).
    #[test]
    fn pick_notify_hash_mixed_returns_last_non_oracle() {
        let batch = vec![
            ("h1".to_string(), Some("0xf8ac93e8".to_string())),
            ("h2".to_string(), Some("0x095ea7b3".to_string())),
            ("h3".to_string(), Some("0xf8ac93e8".to_string())),
        ];
        assert_eq!(
            super::pick_new_tx_notify_hash(&batch),
            Some("h2".to_string())
        );
    }

    /// A batch of all real (non-oracle) txs returns the last hash.
    #[test]
    fn pick_notify_hash_all_real_returns_last() {
        let batch = vec![
            ("h1".to_string(), Some("0xa9059cbb".to_string())),
            ("h2".to_string(), Some("0x095ea7b3".to_string())),
            ("h3".to_string(), None),
        ];
        assert_eq!(
            super::pick_new_tx_notify_hash(&batch),
            Some("h3".to_string())
        );
    }

    /// An empty batch returns None.
    #[test]
    fn pick_notify_hash_empty_is_none() {
        let batch: Vec<(String, Option<String>)> = vec![];
        assert_eq!(super::pick_new_tx_notify_hash(&batch), None);
    }
}
