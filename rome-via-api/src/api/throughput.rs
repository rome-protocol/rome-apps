use axum::{extract::{Query, State}, response::IntoResponse, Json};
use serde::Deserialize;
use sqlx::Row;

use crate::{
    api::models::{Cadence, CurrentTps, Histogram, HistogramBucket, PeakTps, PeakWindowJson, TimeseriesPoint, TopBlock, TopBlockRow, TopWindowRow, ThroughputRecord},
    error::AppError,
    state::AppState,
    throughput_calc::{peak_tps, BlockPoint},
};
use rome_via_classify::ORACLE_SELECTORS;

const MIN_ELAPSED_SECONDS: i64 = 1;

fn range_to_seconds(range: &str) -> i64 {
    match range { "1h" => 3600, "6h" => 21600, "24h" => 86400, "all" => i64::MAX, _ => 86400 }
}

/// Cache TTL for a range: all-time aggregates barely move as blocks land, so they
/// tolerate a full minute; bounded windows slide continuously and get a short TTL.
/// These are chart tiles — the dashboard fires all of them on every load.
pub(crate) fn cache_ttl(range_seconds: i64) -> u64 {
    if range_seconds == i64::MAX { 60 } else { 10 }
}

#[derive(Deserialize)]
pub struct CurrentTpsQuery { pub window_seconds: Option<i64> }

/// All-time peak with the default window is served O(1) from the persisted record table;
/// any other range, or a non-default window, keeps the live sliding-window scan.
pub(crate) fn serves_from_record(range: &str, window: usize) -> bool {
    range == "all" && window == 10
}

/// GET /api/v1/throughput/current-tps — total (incl oracle) + application (excl) TPS over a window.
#[utoipa::path(
    get,
    path = "/api/v1/throughput/current-tps",
    responses(
        (status = 200, description = "Current TPS statistics", body = CurrentTps),
        (status = 500, description = "Internal error", body = crate::error::ProblemJson)
    ),
    tag = "throughput"
)]
pub async fn current_tps(State(state): State<AppState>, Query(q): Query<CurrentTpsQuery>) -> Result<impl IntoResponse, AppError> {
    let window = q.window_seconds.unwrap_or(3600).clamp(1, 86400);
    let chain_id = state.chain_id;
    let db = state.db.clone();
    // Home-page TPS tile: same windowed JOIN+COUNT as stats/overview. Memoize
    // briefly so refreshes + SSE polls don't re-run it against the DB every time.
    let body = crate::cache::cached_json(
        &state,
        &format!("rome_via:current_tps:{chain_id}:{window}"),
        5,
        move || async move { current_tps_compute(&db, chain_id, window).await },
    )
    .await?;
    Ok(Json(body))
}

async fn current_tps_compute(db: &sqlx::PgPool, chain_id: i64, window: i64) -> Result<serde_json::Value, AppError> {
    let row = sqlx::query(
        r#"
        SELECT
          COUNT(et.tx_hash)::BIGINT AS total_txs,
          COUNT(et.tx_hash) FILTER (WHERE et.method_id = ANY($3))::BIGINT AS oracle_txs
        FROM rome_via.evm_tx et
        JOIN rome_via.eth_block_txs ebt ON ebt.tx_hash = et.tx_hash AND ebt.chain_id = et.chain_id
        JOIN rome_via.eth_block eb ON eb.slot_number = ebt.slot_number
          AND eb.slot_block_idx = ebt.slot_block_idx AND eb.chain_id = ebt.chain_id
        WHERE et.chain_id = $1
          AND eb.params_block_timestamp > (EXTRACT(EPOCH FROM NOW()) - $2)::NUMERIC
        "#,
    )
    .bind(chain_id).bind(window).bind(ORACLE_SELECTORS)
    .fetch_one(db).await?;

    let total: i64 = row.try_get::<Option<i64>, _>("total_txs")?.unwrap_or(0);
    let oracle: i64 = row.try_get::<Option<i64>, _>("oracle_txs")?.unwrap_or(0);
    let ct = CurrentTps {
        window_seconds: window,
        total_tps: total as f64 / window as f64,
        application_tps: (total - oracle) as f64 / window as f64,
        total_txs: total,
        oracle_txs: oracle,
    };
    serde_json::to_value(ct).map_err(|e| AppError::Internal(e.to_string()))
}

#[derive(Deserialize)]
pub struct PeakTpsQuery { pub window: Option<usize>, pub range: Option<String> }

/// GET /api/v1/throughput/peak-tps — record sustained TPS over `window` consecutive blocks.
#[utoipa::path(
    get,
    path = "/api/v1/throughput/peak-tps",
    responses(
        (status = 200, description = "Peak TPS over a sliding block window", body = PeakTps),
        (status = 500, description = "Internal error", body = crate::error::ProblemJson)
    ),
    tag = "throughput"
)]
pub async fn peak_tps_handler(State(state): State<AppState>, Query(q): Query<PeakTpsQuery>) -> Result<impl IntoResponse, AppError> {
    let w = q.window.unwrap_or(10).clamp(2, 100);
    // Default to the all-time record rather than a 24h live scan. "Peak TPS" with no
    // arguments means the chain's record, and the ThroughputDashboard calls this with
    // no parameters — under the old 24h default that request missed the O(1) record
    // path entirely and paid a 3.3-4.9s scan on every dashboard load. An explicit
    // ?range=24h still scans, so no caller loses the windowed behaviour.
    let range_str = q.range.clone().unwrap_or_else(|| "all".into());
    if serves_from_record(&range_str, w) {
        if let Some(row) = sqlx::query(
            "SELECT from_block, to_block, elapsed_seconds, total_txs, total_tps
             FROM rome_via.throughput_peak_windows WHERE chain_id = $1 ORDER BY rank LIMIT 1",
        )
        .bind(state.chain_id)
        .fetch_optional(&state.db)
        .await?
        {
            return Ok(Json(PeakTps {
                tps: row.try_get::<f64, _>("total_tps").unwrap_or(0.0),
                window: Some(PeakWindowJson {
                    from_block: row.try_get::<i64, _>("from_block").unwrap_or(0),
                    to_block: row.try_get::<i64, _>("to_block").unwrap_or(0),
                    blocks: 10, // the record is computed over W=10-block windows
                    total_txs: row.try_get::<i64, _>("total_txs").unwrap_or(0),
                    elapsed_seconds: row.try_get::<i64, _>("elapsed_seconds").unwrap_or(0),
                }),
                min_elapsed_seconds: MIN_ELAPSED_SECONDS,
            }));
        }
        // record not seeded yet → fall through to the live scan below.
    }
    let secs = range_to_seconds(&range_str);
    let rows = sqlx::query(
        r#"
        SELECT eb.params_number AS number,
               eb.params_block_timestamp::BIGINT AS ts,
               COUNT(ebt.tx_hash)::BIGINT AS tx_count
        FROM rome_via.eth_block eb
        LEFT JOIN rome_via.eth_block_txs ebt
          ON ebt.slot_number = eb.slot_number AND ebt.slot_block_idx = eb.slot_block_idx AND ebt.chain_id = eb.chain_id
        WHERE eb.chain_id = $1
          AND ($2 = 9223372036854775807 OR eb.params_block_timestamp > (EXTRACT(EPOCH FROM NOW()) - $2)::NUMERIC)
        GROUP BY eb.params_number, eb.params_block_timestamp
        ORDER BY eb.params_number ASC
        "#,
    )
    .bind(state.chain_id).bind(secs)
    .fetch_all(&state.db).await?;

    let points: Vec<BlockPoint> = rows.iter().map(|r| BlockPoint {
        number: r.try_get::<i64, _>("number").unwrap_or(0),
        ts: r.try_get::<Option<i64>, _>("ts").ok().flatten().unwrap_or(0),
        tx_count: r.try_get::<Option<i64>, _>("tx_count").ok().flatten().unwrap_or(0),
    }).collect();

    let peak = peak_tps(&points, w, MIN_ELAPSED_SECONDS);
    Ok(Json(PeakTps {
        tps: peak.as_ref().map(|p| p.tps).unwrap_or(0.0),
        window: peak.map(|p| PeakWindowJson {
            from_block: p.from_block, to_block: p.to_block, blocks: p.blocks as i64,
            total_txs: p.total_txs, elapsed_seconds: p.elapsed_seconds,
        }),
        min_elapsed_seconds: MIN_ELAPSED_SECONDS,
    }))
}

#[derive(Deserialize)]
pub struct TopBlocksQuery { pub limit: Option<i64>, pub range: Option<String> }

/// GET /api/v1/throughput/top-blocks — busiest blocks by Rome EVM tx count.
#[utoipa::path(
    get,
    path = "/api/v1/throughput/top-blocks",
    responses(
        (status = 200, description = "Busiest blocks ranked by tx count", body = [TopBlock]),
        (status = 500, description = "Internal error", body = crate::error::ProblemJson)
    ),
    tag = "throughput"
)]
pub async fn top_blocks(State(state): State<AppState>, Query(q): Query<TopBlocksQuery>) -> Result<impl IntoResponse, AppError> {
    let limit = q.limit.unwrap_or(25).clamp(1, 100);
    let secs = range_to_seconds(&q.range.unwrap_or_else(|| "all".into()));
    let db = state.db.clone();
    let chain_id = state.chain_id;
    let body = crate::cache::cached_json(
        &state,
        &format!("rome_via:top_blocks:{chain_id}:{secs}:{limit}"),
        cache_ttl(secs),
        move || async move { top_blocks_compute(&db, chain_id, secs, limit).await },
    )
    .await?;
    Ok(Json(body))
}

/// Whether a top-blocks request can be answered from the persisted record.
/// All-time only (the record is all-time), and only within its depth.
pub(crate) fn top_blocks_from_record(range_seconds: i64, limit: i64) -> bool {
    range_seconds == i64::MAX && limit <= TOP_BLOCKS_RECORD_DEPTH
}
/// Mirrors calc::TOP_K in rome-via-enrich — the depth the worker persists.
pub(crate) const TOP_BLOCKS_RECORD_DEPTH: i64 = 100;

async fn top_blocks_compute(db: &sqlx::PgPool, chain_id: i64, secs: i64, limit: i64) -> Result<serde_json::Value, AppError> {
    // All-time is served from the record the throughput_record worker maintains, the
    // same pattern as peak-tps (#415): the live aggregate below measured 5-8s under
    // load. Falls through when the record is unseeded so a fresh chain still renders.
    if top_blocks_from_record(secs, limit) {
        let rows = sqlx::query(
            "SELECT block_number, slot_number, total_txs, block_timestamp, gas_used
             FROM rome_via.throughput_busiest_blocks
             WHERE chain_id = $1 ORDER BY rank LIMIT $2",
        )
        .bind(chain_id).bind(limit)
        .fetch_all(db).await?;
        if !rows.is_empty() {
            let items: Vec<TopBlock> = rows.iter().map(|r| {
                let ts = r.try_get::<i64, _>("block_timestamp").unwrap_or(0);
                TopBlock {
                    number: r.try_get::<i64, _>("block_number").unwrap_or(0),
                    slot: r.try_get::<i64, _>("slot_number").unwrap_or(0),
                    tx_count: r.try_get::<i64, _>("total_txs").unwrap_or(0),
                    gas_used: r.try_get::<i64, _>("gas_used").unwrap_or(0).to_string(),
                    timestamp: chrono::DateTime::from_timestamp(ts, 0).map(|d| d.to_rfc3339()).unwrap_or_default(),
                }
            }).collect();
            return serde_json::to_value(items).map_err(|e| AppError::Internal(e.to_string()));
        }
    }
    // INNER JOIN on purpose: a block with zero txs is never "busiest", so exclude empties.
    let rows = sqlx::query(
        r#"
        SELECT eb.params_number AS number, eb.slot_number AS slot,
               COUNT(ebt.tx_hash)::BIGINT AS tx_count,
               eb.block_gas_used::TEXT AS gas_used,
               eb.params_block_timestamp::BIGINT AS ts
        FROM rome_via.eth_block eb
        JOIN rome_via.eth_block_txs ebt
          ON ebt.slot_number = eb.slot_number AND ebt.slot_block_idx = eb.slot_block_idx AND ebt.chain_id = eb.chain_id
        WHERE eb.chain_id = $1
          AND ($2 = 9223372036854775807 OR eb.params_block_timestamp > (EXTRACT(EPOCH FROM NOW()) - $2)::NUMERIC)
        GROUP BY eb.params_number, eb.slot_number, eb.block_gas_used, eb.params_block_timestamp
        ORDER BY tx_count DESC, eb.params_number DESC
        LIMIT $3
        "#,
    )
    .bind(chain_id).bind(secs).bind(limit)
    .fetch_all(db).await?;

    let items: Vec<TopBlock> = rows.iter().map(|r| {
        let ts = r.try_get::<Option<i64>, _>("ts").ok().flatten().unwrap_or(0);
        TopBlock {
            number: r.try_get::<i64, _>("number").unwrap_or(0),
            slot: r.try_get::<i64, _>("slot").unwrap_or(0),
            tx_count: r.try_get::<i64, _>("tx_count").unwrap_or(0),
            gas_used: r.try_get::<Option<String>, _>("gas_used").ok().flatten().unwrap_or_else(|| "0".into()),
            timestamp: chrono::DateTime::from_timestamp(ts, 0).map(|d| d.to_rfc3339()).unwrap_or_default(),
        }
    }).collect();
    serde_json::to_value(items).map_err(|e| AppError::Internal(e.to_string()))
}

#[derive(Deserialize)]
pub struct TimeseriesQuery { pub range: Option<String>, pub bucket_seconds: Option<i64> }

/// GET /api/v1/throughput/timeseries — per-bucket tx count + TPS for the chart.
#[utoipa::path(
    get,
    path = "/api/v1/throughput/timeseries",
    responses(
        (status = 200, description = "TPS timeseries bucketed by time interval", body = [TimeseriesPoint]),
        (status = 500, description = "Internal error", body = crate::error::ProblemJson)
    ),
    tag = "throughput"
)]
pub async fn timeseries(State(state): State<AppState>, Query(q): Query<TimeseriesQuery>) -> Result<impl IntoResponse, AppError> {
    let secs = range_to_seconds(&q.range.unwrap_or_else(|| "1h".into()));
    let secs = if secs == i64::MAX { 86400 } else { secs }; // timeseries is a bounded chart; "all" caps at 24h
    let bucket = q.bucket_seconds.unwrap_or(60).clamp(10, 3600);
    let db = state.db.clone();
    let chain_id = state.chain_id;
    let body = crate::cache::cached_json(
        &state,
        &format!("rome_via:timeseries:{chain_id}:{secs}:{bucket}"),
        cache_ttl(secs),
        move || async move { timeseries_compute(&db, chain_id, secs, bucket).await },
    )
    .await?;
    Ok(Json(body))
}

async fn timeseries_compute(db: &sqlx::PgPool, chain_id: i64, secs: i64, bucket: i64) -> Result<serde_json::Value, AppError> {
    let rows = sqlx::query(
        r#"
        SELECT (FLOOR(eb.params_block_timestamp / $3) * $3)::BIGINT AS t,
               COUNT(ebt.tx_hash)::BIGINT AS txs
        FROM rome_via.eth_block eb
        LEFT JOIN rome_via.eth_block_txs ebt
          ON ebt.slot_number = eb.slot_number AND ebt.slot_block_idx = eb.slot_block_idx AND ebt.chain_id = eb.chain_id
        WHERE eb.chain_id = $1
          AND eb.params_block_timestamp > (EXTRACT(EPOCH FROM NOW()) - $2)::NUMERIC
        GROUP BY t ORDER BY t ASC
        "#,
    )
    .bind(chain_id).bind(secs).bind(bucket)
    .fetch_all(db).await?;
    let items: Vec<TimeseriesPoint> = rows.iter().map(|r| {
        let txs = r.try_get::<Option<i64>, _>("txs").ok().flatten().unwrap_or(0);
        TimeseriesPoint { t: r.try_get::<Option<i64>, _>("t").ok().flatten().unwrap_or(0), txs, tps: txs as f64 / bucket as f64 }
    }).collect();
    serde_json::to_value(items).map_err(|e| AppError::Internal(e.to_string()))
}

#[derive(Deserialize)]
pub struct RangeQuery { pub range: Option<String> }

#[derive(Deserialize)]
pub struct RecordQuery { pub limit: Option<i64> }

/// GET /api/v1/throughput/histogram — distribution of blocks by tx count.
#[utoipa::path(
    get,
    path = "/api/v1/throughput/histogram",
    responses(
        (status = 200, description = "Block tx-count distribution histogram", body = Histogram),
        (status = 500, description = "Internal error", body = crate::error::ProblemJson)
    ),
    tag = "throughput"
)]
pub async fn histogram(State(state): State<AppState>, Query(q): Query<RangeQuery>) -> Result<impl IntoResponse, AppError> {
    let secs = range_to_seconds(&q.range.unwrap_or_else(|| "24h".into()));
    let db = state.db.clone();
    let chain_id = state.chain_id;
    // range=all scans every block on the chain (measured 18.7s under load) — the
    // per-block CTE can't be indexed away. Memoize until the record-backed path lands.
    let body = crate::cache::cached_json(
        &state,
        &format!("rome_via:histogram:{chain_id}:{secs}"),
        cache_ttl(secs),
        move || async move { histogram_compute(&db, chain_id, secs).await },
    )
    .await?;
    Ok(Json(body))
}

async fn histogram_compute(db: &sqlx::PgPool, chain_id: i64, secs: i64) -> Result<serde_json::Value, AppError> {
    // All-time is served from the record the throughput_record worker maintains
    // (migration 0220) — a 6-row read instead of bucketing every block on the chain.
    // Falls through to the live scan when the table has not been seeded yet, so a
    // fresh chain (or a chain whose worker has not run) still renders.
    if secs == i64::MAX {
        let rows = sqlx::query(
            "SELECT bucket_label, blocks FROM rome_via.throughput_histogram
             WHERE chain_id = $1 ORDER BY bucket_idx",
        )
        .bind(chain_id)
        .fetch_all(db)
        .await?;
        if !rows.is_empty() {
            let buckets: Vec<HistogramBucket> = rows.iter().map(|r| HistogramBucket {
                label: r.try_get::<String, _>("bucket_label").unwrap_or_default(),
                blocks: r.try_get::<i64, _>("blocks").unwrap_or(0),
            }).collect();
            return serde_json::to_value(Histogram { buckets })
                .map_err(|e| AppError::Internal(e.to_string()));
        }
    }
    let rows = sqlx::query(
        r#"
        WITH per_block AS (
          SELECT COUNT(ebt.tx_hash)::BIGINT AS n
          FROM rome_via.eth_block eb
          LEFT JOIN rome_via.eth_block_txs ebt
            ON ebt.slot_number = eb.slot_number AND ebt.slot_block_idx = eb.slot_block_idx AND ebt.chain_id = eb.chain_id
          WHERE eb.chain_id = $1
            AND ($2 = 9223372036854775807 OR eb.params_block_timestamp > (EXTRACT(EPOCH FROM NOW()) - $2)::NUMERIC)
          GROUP BY eb.params_number
        )
        SELECT
          COUNT(*) FILTER (WHERE n = 0)::BIGINT AS b0,
          COUNT(*) FILTER (WHERE n = 1)::BIGINT AS b1,
          COUNT(*) FILTER (WHERE n BETWEEN 2 AND 5)::BIGINT AS b2,
          COUNT(*) FILTER (WHERE n BETWEEN 6 AND 20)::BIGINT AS b6,
          COUNT(*) FILTER (WHERE n BETWEEN 21 AND 50)::BIGINT AS b21,
          COUNT(*) FILTER (WHERE n > 50)::BIGINT AS b50
        FROM per_block
        "#,
    )
    .bind(chain_id).bind(secs)
    .fetch_one(db).await?;
    let g = |k: &str| -> i64 { rows.try_get::<Option<i64>, _>(k).ok().flatten().unwrap_or(0) };
    let out = Histogram { buckets: vec![
        HistogramBucket { label: "0".into(), blocks: g("b0") },
        HistogramBucket { label: "1".into(), blocks: g("b1") },
        HistogramBucket { label: "2-5".into(), blocks: g("b2") },
        HistogramBucket { label: "6-20".into(), blocks: g("b6") },
        HistogramBucket { label: "21-50".into(), blocks: g("b21") },
        HistogramBucket { label: "50+".into(), blocks: g("b50") },
    ]};
    serde_json::to_value(out).map_err(|e| AppError::Internal(e.to_string()))
}

/// GET /api/v1/throughput/cadence — slots-filled % over the range.
#[utoipa::path(
    get,
    path = "/api/v1/throughput/cadence",
    responses(
        (status = 200, description = "Slot cadence — slots-filled percentage", body = Cadence),
        (status = 500, description = "Internal error", body = crate::error::ProblemJson)
    ),
    tag = "throughput"
)]
pub async fn cadence(State(state): State<AppState>, Query(q): Query<RangeQuery>) -> Result<impl IntoResponse, AppError> {
    let secs = range_to_seconds(&q.range.unwrap_or_else(|| "24h".into()));
    let db = state.db.clone();
    let chain_id = state.chain_id;
    let body = crate::cache::cached_json(
        &state,
        &format!("rome_via:cadence:{chain_id}:{secs}"),
        cache_ttl(secs),
        move || async move { cadence_compute(&db, chain_id, secs).await },
    )
    .await?;
    Ok(Json(body))
}

async fn cadence_compute(db: &sqlx::PgPool, chain_id: i64, secs: i64) -> Result<serde_json::Value, AppError> {
    let row = sqlx::query(
        r#"
        WITH per_block AS (
          SELECT eb.slot_number AS slot, eb.params_block_timestamp AS ts, COUNT(ebt.tx_hash)::BIGINT AS n
          FROM rome_via.eth_block eb
          LEFT JOIN rome_via.eth_block_txs ebt
            ON ebt.slot_number = eb.slot_number AND ebt.slot_block_idx = eb.slot_block_idx AND ebt.chain_id = eb.chain_id
          WHERE eb.chain_id = $1
            AND ($2 = 9223372036854775807 OR eb.params_block_timestamp > (EXTRACT(EPOCH FROM NOW()) - $2)::NUMERIC)
          GROUP BY eb.params_number, eb.slot_number, eb.params_block_timestamp
        )
        SELECT COUNT(*)::BIGINT AS total,
               COUNT(*) FILTER (WHERE n > 0)::BIGINT AS filled,
               (MAX(slot) - MIN(slot))::BIGINT AS slot_span,
               (MAX(ts) - MIN(ts))::BIGINT AS time_span
        FROM per_block
        "#,
    )
    .bind(chain_id).bind(secs)
    .fetch_one(db).await?;
    let total: i64 = row.try_get::<Option<i64>, _>("total")?.unwrap_or(0);
    let filled: i64 = row.try_get::<Option<i64>, _>("filled")?.unwrap_or(0);
    // Real slot cadence from indexed slot_number/timestamp deltas — same math as the
    // cluster's getRecentPerformanceSamples; chain-agnostic, no Solana-RPC dependency.
    let slot_span: i64 = row.try_get::<Option<i64>, _>("slot_span")?.unwrap_or(0);
    let time_span: i64 = row.try_get::<Option<i64>, _>("time_span")?.unwrap_or(0);
    let (slots_per_sec, avg_slot_ms) = crate::throughput_calc::slot_rate(slot_span, time_span);
    let out = Cadence {
        total_blocks: total,
        filled_blocks: filled,
        slots_filled_pct: if total > 0 { filled as f64 * 100.0 / total as f64 } else { 0.0 },
        slots_per_sec,
        avg_slot_ms,
    };
    serde_json::to_value(out).map_err(|e| AppError::Internal(e.to_string()))
}

/// GET /api/v1/throughput/record — persisted all-time top-10 sustained-TPS windows + busiest blocks (O(1)).
#[utoipa::path(
    get,
    path = "/api/v1/throughput/record",
    responses(
        (status = 200, description = "Persisted all-time throughput record (top-10 windows + blocks)", body = ThroughputRecord),
        (status = 500, description = "Internal error", body = crate::error::ProblemJson)
    ),
    tag = "throughput"
)]
pub async fn record(
    State(state): State<AppState>,
    Query(q): Query<RecordQuery>,
) -> Result<impl IntoResponse, AppError> {
    // The record table is deliberately deep (TOP_K = 100) so /throughput/top-blocks can
    // serve its default page of 25 from it. That depth is storage, not a display choice:
    // returning all 100 windows AND 100 blocks buries the dashboard in rows. Cap the
    // response at a readable default; callers that want more can ask.
    let limit = match q.limit {
        None => 10,
        Some(n) if (1..=100).contains(&n) => n,
        Some(n) => return Err(AppError::BadRequest(format!("limit {n} out of range (1-100)"))),
    };
    let wr = sqlx::query(
        "SELECT rank, from_block, to_block, from_slot, to_slot, elapsed_seconds, total_txs, app_txs, total_tps, app_tps
         FROM rome_via.throughput_peak_windows WHERE chain_id = $1 ORDER BY rank LIMIT $2",
    ).bind(state.chain_id).bind(limit).fetch_all(&state.db).await?;
    let peak_windows: Vec<TopWindowRow> = wr.iter().map(|r| TopWindowRow {
        rank: r.try_get::<i32, _>("rank").unwrap_or(0),
        from_block: r.try_get::<i64, _>("from_block").unwrap_or(0),
        to_block: r.try_get::<i64, _>("to_block").unwrap_or(0),
        from_slot: r.try_get::<i64, _>("from_slot").unwrap_or(0),
        to_slot: r.try_get::<i64, _>("to_slot").unwrap_or(0),
        elapsed_seconds: r.try_get::<i64, _>("elapsed_seconds").unwrap_or(0),
        total_txs: r.try_get::<i64, _>("total_txs").unwrap_or(0),
        app_txs: r.try_get::<i64, _>("app_txs").unwrap_or(0),
        total_tps: r.try_get::<f64, _>("total_tps").unwrap_or(0.0),
        app_tps: r.try_get::<f64, _>("app_tps").unwrap_or(0.0),
    }).collect();

    let br = sqlx::query(
        "SELECT rank, block_number, slot_number, total_txs, app_txs, block_timestamp
         FROM rome_via.throughput_busiest_blocks WHERE chain_id = $1 ORDER BY rank LIMIT $2",
    ).bind(state.chain_id).bind(limit).fetch_all(&state.db).await?;
    let busiest_blocks: Vec<TopBlockRow> = br.iter().map(|r| TopBlockRow {
        rank: r.try_get::<i32, _>("rank").unwrap_or(0),
        block_number: r.try_get::<i64, _>("block_number").unwrap_or(0),
        slot_number: r.try_get::<i64, _>("slot_number").unwrap_or(0),
        total_txs: r.try_get::<i64, _>("total_txs").unwrap_or(0),
        app_txs: r.try_get::<i64, _>("app_txs").unwrap_or(0),
        block_timestamp: r.try_get::<i64, _>("block_timestamp").unwrap_or(0),
    }).collect();

    Ok(Json(ThroughputRecord { peak_windows, busiest_blocks }))
}

#[cfg(test)]
mod tests {
    use super::serves_from_record;

    #[test]
    fn peak_all_default_window_reads_record_others_scan() {
        assert!(serves_from_record("all", 10));   // all-time + default window → table
        assert!(!serves_from_record("24h", 10));  // ranged → live scan
        assert!(!serves_from_record("all", 20));  // non-default window → live scan
    }

    use super::{top_blocks_from_record, TOP_BLOCKS_RECORD_DEPTH};

    #[test]
    fn top_blocks_serves_the_default_page_from_the_record() {
        // The endpoint's default (range=all, limit=25) MUST hit the record — that is the
        // request the dashboard makes, and the live scan measured 5-8s under load.
        assert!(top_blocks_from_record(i64::MAX, 25));
        assert!(top_blocks_from_record(i64::MAX, TOP_BLOCKS_RECORD_DEPTH));
    }

    #[test]
    fn top_blocks_falls_back_for_windowed_or_over_deep_requests() {
        assert!(!top_blocks_from_record(86_400, 25), "a 24h window is not the all-time record");
        assert!(!top_blocks_from_record(i64::MAX, TOP_BLOCKS_RECORD_DEPTH + 1), "beyond persisted depth");
    }

    #[test]
    fn peak_tps_default_range_hits_the_record_path() {
        // serves_from_record requires range=="all" && window==10; the handler's defaults
        // must therefore BE those values or the dashboard's bare call pays a live scan.
        assert!(serves_from_record("all", 10), "defaults must land on the record path");
        assert!(!serves_from_record("24h", 10), "an explicit window still scans");
    }
}
