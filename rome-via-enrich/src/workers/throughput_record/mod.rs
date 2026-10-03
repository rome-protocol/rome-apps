pub mod calc;

use self::calc::{merge_record, trim_full_page, BlockRec, Hist, PagedRebuild, Point, Record, WindowRec, HIST_LABELS};
use rome_via_classify::ORACLE_SELECTORS;
use sqlx::{PgPool, Row};
use std::time::{Duration, Instant};

const MIN_ELAPSED: i64 = 1;
const W: usize = 10;

/// Max blocks one incremental poll loads. A stale cursor after a long outage catches up
/// a page per poll instead of loading the whole gap into memory.
const INCREMENTAL_PAGE: i64 = 50_000;

/// Max blocks one page of the Seed/Backstop rebuild loads; the rebuild pages through the whole
/// chain at this size instead of holding every block in memory.
const REBUILD_PAGE: i64 = 50_000;

// The SQL body of the paged fetch; `$tail` is appended.
macro_rules! points_sql {
    ($tail:literal) => {
        concat!(
            r#"
    SELECT eb.params_number AS block, eb.slot_number AS slot,
           eb.params_block_timestamp::BIGINT AS ts,
           COUNT(ebt.tx_hash)::BIGINT AS total_txs,
           COUNT(ebt.tx_hash) FILTER (WHERE NOT COALESCE(et.method_id = ANY($3), false))::BIGINT AS app_txs,
           COALESCE(eb.block_gas_used, 0)::BIGINT AS gas_used
    FROM rome_via.eth_block eb
    LEFT JOIN rome_via.eth_block_txs ebt
      ON ebt.slot_number = eb.slot_number AND ebt.slot_block_idx = eb.slot_block_idx AND ebt.chain_id = eb.chain_id
    LEFT JOIN rome_via.evm_tx et ON et.tx_hash = ebt.tx_hash AND et.chain_id = eb.chain_id
    WHERE eb.chain_id = $1 AND eb.params_number > $2
    GROUP BY eb.params_number, eb.slot_number, eb.slot_block_idx, eb.params_block_timestamp, eb.block_gas_used
    ORDER BY eb.params_number ASC, eb.slot_number ASC, eb.slot_block_idx ASC
    "#,
            $tail
        )
    };
}

/// Per-block points (total + app tx counts) for blocks with number > $2, ascending, at most the
/// lowest `$4` rows above the cursor, ordered by (params_number, slot_number, slot_block_idx) so
/// a LIMIT cut is deterministic.
/// $1 = chain_id, $2 = after_block (i64::MIN for everything), $3 = oracle selectors (text[]).
fn incremental_points_query() -> &'static str {
    points_sql!("LIMIT $4")
}

fn rows_to_points(rows: &[sqlx::postgres::PgRow]) -> Vec<Point> {
    rows.iter().map(|r| Point {
        block: r.get::<i64, _>("block"), slot: r.get::<i64, _>("slot"),
        ts: r.try_get::<Option<i64>, _>("ts").ok().flatten().unwrap_or(0),
        total_txs: r.get::<i64, _>("total_txs"), app_txs: r.get::<i64, _>("app_txs"),
        gas_used: r.try_get::<Option<i64>, _>("gas_used").ok().flatten().unwrap_or(0),
    }).collect()
}

/// One bounded page of rows above `after_block` (at most `page_size`). Callers pass the page
/// through `trim_full_page` so it ends on a complete block number.
async fn fetch_points_page(pool: &PgPool, chain_id: i64, after_block: i64, page_size: i64) -> anyhow::Result<Vec<Point>> {
    let rows = sqlx::query(incremental_points_query())
        .bind(chain_id).bind(after_block).bind(ORACLE_SELECTORS).bind(page_size)
        .fetch_all(pool).await?;
    Ok(rows_to_points(&rows))
}

/// Full rebuild for Seed/Backstop: page through the chain, folding each page into a fixed-size
/// record, so memory stays at one page rather than every block. Nothing is written here; the
/// caller writes the finished record and cursor once, atomically.
async fn rebuild_record(pool: &PgPool, chain_id: i64) -> anyhow::Result<(Record, Option<i64>)> {
    let mut rebuild = PagedRebuild::new(W, MIN_ELAPSED);
    loop {
        let page = fetch_points_page(pool, chain_id, rebuild.cursor(), REBUILD_PAGE).await?;
        if !rebuild.push_page(page, REBUILD_PAGE as usize) {
            return Ok(rebuild.finish());
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PollMode {
    /// No usable saved state: full scan.
    Seed,
    /// Resume from the saved cursor, one bounded page.
    Incremental,
    /// Periodic full recompute that self-heals after a backfill.
    Backstop,
}

/// Decide the poll mode. A saved cursor plus a non-empty saved record resumes incrementally,
/// including on the first poll after a process start. `since_last_recompute` is the time since
/// the later of process start and the last full recompute, so the backstop never fires at
/// startup; `None` (no baseline) never triggers it.
fn plan_poll(
    cursor: Option<i64>,
    record_is_empty: bool,
    since_last_recompute: Option<Duration>,
    recompute_interval: Duration,
) -> PollMode {
    if cursor.is_none() || record_is_empty {
        return PollMode::Seed;
    }
    match since_last_recompute {
        Some(d) if d >= recompute_interval => PollMode::Backstop,
        _ => PollMode::Incremental,
    }
}

/// True when read_record found nothing saved (never seeded).
fn record_is_empty(rec: &Record) -> bool {
    rec.windows.is_empty() && rec.blocks.is_empty() && rec.hist.iter().all(|&n| n == 0)
}

// Cursor state lives in rome_via.enrich_cursors (worker = "throughput_record").
// Real column names verified against 0100_enrich_tables.up.sql and address_stats.rs:
// (chain_id, worker, last_processed, last_processed_at) — PK (chain_id, worker).
async fn read_cursor(pool: &PgPool, chain_id: i64) -> anyhow::Result<Option<i64>> {
    let row = sqlx::query(
        "SELECT last_processed FROM rome_via.enrich_cursors WHERE chain_id=$1 AND worker=$2",
    )
    .bind(chain_id).bind("throughput_record").fetch_optional(pool).await?;
    Ok(row.map(|r| r.get::<i64, _>("last_processed")))
}

async fn read_record(pool: &PgPool, chain_id: i64) -> anyhow::Result<Record> {
    let wr = sqlx::query(
        "SELECT from_block,to_block,from_slot,to_slot,elapsed_seconds,total_txs,app_txs,total_tps,app_tps
         FROM rome_via.throughput_peak_windows WHERE chain_id=$1 ORDER BY rank",
    )
    .bind(chain_id).fetch_all(pool).await?;
    let windows = wr.iter().map(|r| WindowRec {
        from_block: r.get("from_block"), to_block: r.get("to_block"),
        from_slot: r.get("from_slot"), to_slot: r.get("to_slot"),
        elapsed_seconds: r.get("elapsed_seconds"), total_txs: r.get("total_txs"), app_txs: r.get("app_txs"),
        total_tps: r.get("total_tps"), app_tps: r.get("app_tps"),
    }).collect();
    let br = sqlx::query(
        "SELECT block_number,slot_number,total_txs,app_txs,block_timestamp,gas_used
         FROM rome_via.throughput_busiest_blocks WHERE chain_id=$1 ORDER BY rank",
    )
    .bind(chain_id).fetch_all(pool).await?;
    let blocks = br.iter().map(|r| BlockRec {
        block: r.get("block_number"), slot: r.get("slot_number"),
        total_txs: r.get("total_txs"), app_txs: r.get("app_txs"), ts: r.get("block_timestamp"),
        gas_used: r.try_get::<Option<i64>, _>("gas_used").ok().flatten().unwrap_or(0),
    }).collect();
    // Cumulative histogram; absent rows (never seeded) read as zeros and the next
    // full recompute repopulates them.
    let hr = sqlx::query(
        "SELECT bucket_idx, blocks FROM rome_via.throughput_histogram WHERE chain_id=$1",
    )
    .bind(chain_id).fetch_all(pool).await?;
    let mut hist: Hist = [0; 6];
    for r in &hr {
        let i = r.get::<i32, _>("bucket_idx") as usize;
        if i < hist.len() { hist[i] = r.get::<i64, _>("blocks"); }
    }
    Ok(Record { windows, blocks, hist })
}

/// Overwrite both top-10 tables and (when cursor_block is Some) advance the worker cursor, atomically.
async fn write_record(pool: &PgPool, chain_id: i64, rec: &Record, cursor_block: Option<i64>) -> anyhow::Result<()> {
    let mut tx = pool.begin().await?;
    sqlx::query("DELETE FROM rome_via.throughput_peak_windows WHERE chain_id=$1")
        .bind(chain_id).execute(&mut *tx).await?;
    for (i, w) in rec.windows.iter().enumerate() {
        sqlx::query(
            "INSERT INTO rome_via.throughput_peak_windows
             (chain_id,rank,from_block,to_block,from_slot,to_slot,elapsed_seconds,total_txs,app_txs,total_tps,app_tps)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
        )
        .bind(chain_id).bind((i as i32) + 1)
        .bind(w.from_block).bind(w.to_block).bind(w.from_slot).bind(w.to_slot)
        .bind(w.elapsed_seconds).bind(w.total_txs).bind(w.app_txs).bind(w.total_tps).bind(w.app_tps)
        .execute(&mut *tx).await?;
    }
    sqlx::query("DELETE FROM rome_via.throughput_busiest_blocks WHERE chain_id=$1")
        .bind(chain_id).execute(&mut *tx).await?;
    for (i, b) in rec.blocks.iter().enumerate() {
        sqlx::query(
            "INSERT INTO rome_via.throughput_busiest_blocks
             (chain_id,rank,block_number,slot_number,total_txs,app_txs,block_timestamp,gas_used)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
        )
        .bind(chain_id).bind((i as i32) + 1)
        .bind(b.block).bind(b.slot).bind(b.total_txs).bind(b.app_txs).bind(b.ts).bind(b.gas_used)
        .execute(&mut *tx).await?;
    }
    sqlx::query("DELETE FROM rome_via.throughput_histogram WHERE chain_id=$1")
        .bind(chain_id).execute(&mut *tx).await?;
    for (i, blocks) in rec.hist.iter().enumerate() {
        sqlx::query(
            "INSERT INTO rome_via.throughput_histogram (chain_id,bucket_idx,bucket_label,blocks)
             VALUES ($1,$2,$3,$4)",
        )
        .bind(chain_id).bind(i as i32).bind(HIST_LABELS[i]).bind(*blocks)
        .execute(&mut *tx).await?;
    }
    if let Some(mb) = cursor_block {
        sqlx::query(
            "INSERT INTO rome_via.enrich_cursors (chain_id, worker, last_processed, last_processed_at)
             VALUES ($1, $2, $3, NOW())
             ON CONFLICT (chain_id, worker) DO UPDATE
                 SET last_processed    = EXCLUDED.last_processed,
                     last_processed_at = NOW()",
        )
        .bind(chain_id).bind("throughput_record").bind(mb).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(())
}

pub async fn run(
    pool: PgPool,
    chain_id: i64,
    poll_interval: Duration,
    recompute_interval: Duration,
) -> anyhow::Result<()> {
    // Hybrid (spec §5 + §6): incremental on new blocks, with a SEED when nothing is saved and a
    // periodic full recompute BACKSTOP that self-heals after a backfill. The saved cursor and
    // record survive restarts, so a restart resumes incrementally; the backstop is timed from
    // process start or the last recompute, never immediately at startup.
    let mut last_recompute = Instant::now();
    loop {
        let cursor = read_cursor(&pool, chain_id).await?;
        let existing = if cursor.is_some() { Some(read_record(&pool, chain_id).await?) } else { None };
        let empty = existing.as_ref().is_none_or(record_is_empty);
        let mode = plan_poll(cursor, empty, Some(last_recompute.elapsed()), recompute_interval);
        match (mode, cursor, existing) {
            // INCREMENTAL: fold one bounded page of blocks since the cursor (with W-1 overlap
            // for boundary windows); a long outage catches up a page per poll.
            (PollMode::Incremental, Some(cursor_block), Some(existing)) => {
                let mut page = fetch_points_page(&pool, chain_id, cursor_block - (W as i64 - 1), INCREMENTAL_PAGE).await?;
                trim_full_page(&mut page, INCREMENTAL_PAGE as usize);
                if let Some(max_block) = page.iter().map(|p| p.block).max() {
                    if max_block > cursor_block {
                        let rec = merge_record(&existing, &page, cursor_block, W, MIN_ELAPSED);
                        write_record(&pool, chain_id, &rec, Some(max_block)).await?;
                        tracing::debug!(chain_id, mode = "incremental", new_to = max_block, "throughput record");
                    }
                }
            }
            // SEED (nothing saved) or BACKSTOP (periodic): paged full rebuild, then one atomic
            // overwrite of both tables and the cursor.
            _ => {
                let (rec, max_block) = rebuild_record(&pool, chain_id).await?;
                write_record(&pool, chain_id, &rec, max_block).await?;
                last_recompute = Instant::now();
                tracing::debug!(chain_id, mode = "recompute", windows = rec.windows.len(), blocks = rec.blocks.len(), "throughput record");
            }
        }
        tokio::time::sleep(poll_interval).await;
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn points_query_bounds_by_block_cursor_and_excludes_oracle_for_app() {
        let q = super::incremental_points_query();
        assert!(q.contains("params_number > $2"), "must bound by cursor: {q}");
        assert!(
            q.contains("FILTER (WHERE NOT COALESCE(et.method_id = ANY($3), false))"),
            "app must exclude every oracle selector (NULL-safe): {q}"
        );
        assert!(q.contains("ORDER BY") && q.contains("ASC"), "must be ascending for the window scan: {q}");
        assert!(
            q.contains("ORDER BY eb.params_number ASC, eb.slot_number ASC, eb.slot_block_idx ASC"),
            "a LIMIT cut must be deterministic across rows of one block number: {q}"
        );
        assert!(
            q.contains("GROUP BY eb.params_number, eb.slot_number, eb.slot_block_idx"),
            "rows sharing a block number must stay separate points: {q}"
        );
    }

    use super::calc::{BlockRec, Record};
    use super::{incremental_points_query, plan_poll, record_is_empty, PollMode, INCREMENTAL_PAGE};
    use std::time::Duration;

    const INTERVAL: Duration = Duration::from_secs(3600);

    #[test]
    fn startup_with_cursor_and_saved_record_resumes_incrementally() {
        assert_eq!(plan_poll(Some(100), false, Some(Duration::ZERO), INTERVAL), PollMode::Incremental);
    }

    #[test]
    fn no_cursor_seeds() {
        assert_eq!(plan_poll(None, false, Some(Duration::ZERO), INTERVAL), PollMode::Seed);
        assert_eq!(plan_poll(None, true, Some(Duration::ZERO), INTERVAL), PollMode::Seed);
    }

    #[test]
    fn cursor_with_empty_record_seeds() {
        assert_eq!(plan_poll(Some(100), true, Some(Duration::ZERO), INTERVAL), PollMode::Seed);
    }

    #[test]
    fn interval_not_elapsed_stays_incremental() {
        let just_under = INTERVAL - Duration::from_secs(1);
        assert_eq!(plan_poll(Some(100), false, Some(just_under), INTERVAL), PollMode::Incremental);
    }

    #[test]
    fn interval_elapsed_runs_backstop() {
        assert_eq!(plan_poll(Some(100), false, Some(INTERVAL), INTERVAL), PollMode::Backstop);
        assert_eq!(plan_poll(Some(100), false, Some(INTERVAL * 2), INTERVAL), PollMode::Backstop);
    }

    #[test]
    fn record_emptiness_needs_no_windows_no_blocks_and_zero_histogram() {
        let empty = Record { windows: vec![], blocks: vec![], hist: [0; 6] };
        assert!(record_is_empty(&empty));
        let hist_only = Record { windows: vec![], blocks: vec![], hist: [0, 0, 3, 0, 0, 0] };
        assert!(!record_is_empty(&hist_only));
        let blk = BlockRec { block: 1, slot: 1, total_txs: 1, app_txs: 1, ts: 1, gas_used: 0 };
        let blocks_only = Record { windows: vec![], blocks: vec![blk], hist: [0; 6] };
        assert!(!record_is_empty(&blocks_only));
    }

    #[test]
    fn incremental_query_is_bounded_by_a_page_limit() {
        let q = incremental_points_query();
        assert!(q.contains("LIMIT $4"), "incremental fetch must be paged: {q}");
        assert!(q.contains("params_number > $2"), "must still bound by cursor: {q}");
        assert!(q.contains("ORDER BY eb.params_number ASC"), "page must be the lowest blocks above the cursor: {q}");
        const { assert!(INCREMENTAL_PAGE > 0 && INCREMENTAL_PAGE <= 100_000) };
    }
}
