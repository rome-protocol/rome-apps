//! "Audit" tab backend — a public, read-only browse API over the PII-free
//! Tier-1 record `audit.chain_event` (in the shared `rome_via_db`).
//!
//! These endpoints are public like every other rome-via-api GET endpoint: the
//! data they expose is decoded on-chain events that Via already surfaces in its
//! transaction views. No auth gate.
//!
//! # Tier boundary
//! Every query here touches ONLY `audit.chain_event`. No Tier-2 (correlations),
//! Tier-3 (overlay/identity/evidence), or quarantine relation is joined — the
//! Tier-1 boundary is PII-free by construction. A unit test pins this by
//! scanning the SQL for any `audit.<relation>` other than `chain_event`.

use axum::{
    extract::{Query, State},
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use utoipa::ToSchema;

use crate::cursor::AuditCursor;
use crate::error::AppError;
use crate::state::AppState;

// ─────────────────────────────────────────────────────────────────────────────
// Router
// ─────────────────────────────────────────────────────────────────────────────

/// The two public audit routes, merged into the `/api/v1` nest by
/// [`crate::api::router`]. Plain public GET routes like every other endpoint;
/// handler state is supplied by the caller's `.with_state(...)`.
pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/audit/events", get(list_events))
        .route("/audit/event-counts", get(event_counts))
}

// ─────────────────────────────────────────────────────────────────────────────
// Response DTOs
// ─────────────────────────────────────────────────────────────────────────────

/// One row of the Tier-1 `audit.chain_event` record. Flat single-table
/// projection — bytea columns surface as 0x-hex, `args` passes the JSONB
/// through verbatim (decoded once, never re-parsed).
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AuditEvent {
    /// DB surrogate id (insertion-order identity on this instance). Ordering /
    /// display only — NEVER a stable cross-reindex identifier.
    pub event_id: i64,
    pub block_number: i64,
    /// Block time as unix epoch seconds (raw Tier-1 column, faithful passthrough).
    pub block_timestamp: i64,
    /// Transaction hash (0x-hex from bytea).
    pub tx_hash: String,
    pub tx_index: i32,
    pub log_index: i32,
    pub event_name: String,
    /// Emitting contract (0x-hex from bytea).
    pub source_contract: String,
    pub source_kind: String,
    pub projection_tag: String,
    /// Tx signer (0x-hex from bytea). Nullable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_signer: Option<String>,
    /// Decoded event args — JSONB passed through as-is.
    pub args: serde_json::Value,
}

/// One `(source_kind, event_name, projection_tag)` group's count.
///
/// Grouped rather than by `event_name` alone because `event_name` is ambiguous
/// across source kinds ("Transfer" exists under ArcToken / UV2 / YieldToken /
/// PurchaseToken).
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AuditEventCount {
    pub source_kind: String,
    pub event_name: String,
    pub projection_tag: String,
    pub count: i64,
}

/// Global per-type counts for the filter chips.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AuditEventCounts {
    pub counts: Vec<AuditEventCount>,
    pub total: i64,
    /// Whether the audit trail exists on this deployment at all. `false` means
    /// the chain runs no rome-audit stack (the `audit` schema is absent) — the
    /// UI renders a designed "not enabled" state instead of an empty ledger.
    pub enabled: bool,
}

// ─────────────────────────────────────────────────────────────────────────────
// SQL (kept as named items so the tier-boundary test can scan them)
// ─────────────────────────────────────────────────────────────────────────────

const COUNTS_SQL: &str = r#"
    SELECT source_kind, event_name, projection_tag, COUNT(*)::BIGINT AS count
    FROM audit.chain_event
    WHERE chain_id = $1
    GROUP BY source_kind, event_name, projection_tag
    ORDER BY count DESC, source_kind, event_name
"#;

const EVENT_NAME_EXISTS_SQL: &str =
    "SELECT EXISTS (SELECT 1 FROM audit.chain_event WHERE chain_id = $1 AND event_name = $2) AS exists";

const SOURCE_KIND_EXISTS_SQL: &str =
    "SELECT EXISTS (SELECT 1 FROM audit.chain_event WHERE chain_id = $1 AND source_kind = $2) AS exists";

/// Catalog probe for whether the Tier-1 record exists on this deployment at
/// all. The `audit` schema is created by the rome-audit stack's migrator, and
/// chains that don't run rome-audit never have it — the browse API must present
/// that as a designed disabled state (empty page / `enabled: false`), not a
/// 42P01-driven 500.
const AUDIT_ENABLED_SQL: &str = "SELECT to_regclass('audit.chain_event') IS NOT NULL AS enabled";

async fn audit_trail_enabled(db: &sqlx::PgPool) -> Result<bool, AppError> {
    let row = sqlx::query(AUDIT_ENABLED_SQL).fetch_one(db).await?;
    Ok(row.try_get::<bool, _>("enabled").unwrap_or(false))
}

/// Build the keyset browse query. Param plan is fixed so binds line up:
/// `$1`=chain_id, `$2,$3,$4`=keyset boundary `(block, tx_idx, log_idx)`,
/// `$5`=limit+1; then `$6` (and `$7`) for the optional `event_name` /
/// `source_kind` filters, appended in that order.
///
/// Uses the **row-constructor** boundary `(block_number, tx_index, log_index) <
/// ($2,$3,$4)` — a true backward index-scan start (rides `ce_total_order`
/// unfiltered, `ce_name_order` when `event_name` is pinned), not the
/// `a<$ OR (a=$ AND …)` OR-form (a filter, not a range boundary).
fn events_sql(has_event_name: bool, has_source_kind: bool) -> String {
    let mut sql = String::from(
        "SELECT event_id, block_number, block_timestamp, tx_hash, tx_index, log_index, \
                event_name, source_contract, source_kind, projection_tag, tx_signer, args \
         FROM audit.chain_event \
         WHERE chain_id = $1 \
           AND (block_number, tx_index, log_index) < ($2, $3, $4)",
    );
    let mut next = 6;
    if has_event_name {
        sql.push_str(&format!(" AND event_name = ${next}"));
        next += 1;
    }
    if has_source_kind {
        sql.push_str(&format!(" AND source_kind = ${next}"));
    }
    sql.push_str(" ORDER BY block_number DESC, tx_index DESC, log_index DESC LIMIT $5");
    sql
}

// ─────────────────────────────────────────────────────────────────────────────
// Handlers
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct EventsQuery {
    /// Exact-match filter on `event_name`. Validated against the chain's known
    /// set — an unknown value is a 400, not a silently-empty page.
    pub event_name: Option<String>,
    /// Optional residual exact-match filter on `source_kind`. Same validation.
    pub source_kind: Option<String>,
    /// Opaque keyset cursor from a previous page's `nextCursor`.
    pub cursor: Option<String>,
    /// Items per page (default 25, max 100). Out of range ⇒ 400 (no clamp).
    pub limit: Option<i64>,
}

/// GET /api/v1/audit/events — Tier-1 event browse, newest first.
#[utoipa::path(
    get,
    path = "/api/v1/audit/events",
    params(
        ("event_name" = Option<String>, Query, description = "Exact-match filter on event_name; an unknown value is a 400"),
        ("source_kind" = Option<String>, Query, description = "Exact-match filter on source_kind; an unknown value is a 400"),
        ("cursor" = Option<String>, Query, description = "Pagination cursor from previous response's nextCursor"),
        ("limit" = Option<i64>, Query, description = "Items per page (default 25, max 100)")
    ),
    responses(
        (status = 200, description = "Paginated Tier-1 audit event list", body = inline(crate::api::models::Page<AuditEvent>)),
        (status = 400, description = "Bad request / invalid cursor / unknown filter", body = crate::error::ProblemJson),
        (status = 500, description = "Internal error", body = crate::error::ProblemJson)
    ),
    tag = "audit"
)]
pub async fn list_events(
    State(state): State<AppState>,
    Query(params): Query<EventsQuery>,
) -> Result<Json<crate::api::models::Page<AuditEvent>>, AppError> {
    let chain_id = state.chain_id;

    let limit = match params.limit {
        None => 25,
        Some(n) if (1..=100).contains(&n) => n,
        Some(n) => return Err(AppError::BadRequest(format!("limit {n} out of range (1-100)"))),
    };

    // No `audit` schema on this deployment ⇒ the designed empty page. Must run
    // before the filter validators — they query audit.chain_event too.
    if !audit_trail_enabled(&state.db).await? {
        return Ok(Json(crate::api::models::Page {
            items: vec![],
            next_cursor: None,
            has_more: false,
        }));
    }

    // Reject unknown filter values up front (mirrors txs::validate_seam): an
    // empty page for a typo'd filter is indistinguishable from "no such events".
    if let Some(ref name) = params.event_name {
        if !value_exists(&state.db, chain_id, EVENT_NAME_EXISTS_SQL, name).await? {
            return Err(AppError::BadRequest(format!("unknown event_name '{name}'")));
        }
    }
    if let Some(ref kind) = params.source_kind {
        if !value_exists(&state.db, chain_id, SOURCE_KIND_EXISTS_SQL, kind).await? {
            return Err(AppError::BadRequest(format!("unknown source_kind '{kind}'")));
        }
    }

    // First page uses sentinels above any real value, so the same `< (...)`
    // predicate serves page 1.
    let (last_block, last_tx_idx, last_log_idx) = if let Some(tok) = params.cursor.as_deref() {
        let cur: AuditCursor = crate::cursor::decode(tok, &state.cursor_secret)?;
        if cur.chain_id != chain_id {
            return Err(AppError::ChainNotSupported(format!(
                "cursor is for chain {} but this API serves {}",
                cur.chain_id, chain_id
            )));
        }
        (cur.last_block, cur.last_tx_idx, cur.last_log_idx)
    } else {
        (i64::MAX, i32::MAX, i32::MAX)
    };

    let sql = events_sql(params.event_name.is_some(), params.source_kind.is_some());
    let mut q = sqlx::query(&sql)
        .bind(chain_id)
        .bind(last_block)
        .bind(last_tx_idx)
        .bind(last_log_idx)
        .bind(limit + 1);
    if let Some(ref name) = params.event_name {
        q = q.bind(name.as_str());
    }
    if let Some(ref kind) = params.source_kind {
        q = q.bind(kind.as_str());
    }
    let raw_rows = q.fetch_all(&state.db).await?;

    let has_more = raw_rows.len() as i64 > limit;
    let items: Vec<AuditEvent> = raw_rows
        .into_iter()
        .take(limit as usize)
        .map(row_to_event)
        .collect();

    let next_cursor = if has_more {
        items.last().and_then(|last| {
            let cur = AuditCursor {
                chain_id,
                last_block: last.block_number,
                last_tx_idx: last.tx_index,
                last_log_idx: last.log_index,
            };
            crate::cursor::encode(&cur, &state.cursor_secret).ok()
        })
    } else {
        None
    };

    Ok(Json(crate::api::models::Page {
        items,
        next_cursor,
        has_more,
    }))
}

/// GET /api/v1/audit/event-counts — global per-type counts for the filter chips.
/// Served via SWR cache (30s); counts are global, not filter-conditioned.
#[utoipa::path(
    get,
    path = "/api/v1/audit/event-counts",
    responses(
        (status = 200, description = "Global per-(source_kind, event_name, projection_tag) counts", body = AuditEventCounts),
        (status = 500, description = "Internal error", body = crate::error::ProblemJson)
    ),
    tag = "audit"
)]
pub async fn event_counts(State(state): State<AppState>) -> Result<impl IntoResponse, AppError> {
    let chain_id = state.chain_id;
    let db = state.db.clone();
    let body = crate::cache::cached_json(
        &state,
        &format!("rome_via:audit_event_counts:{chain_id}"),
        30,
        move || async move { event_counts_compute(&db, chain_id).await },
    )
    .await?;
    Ok(Json(body))
}

/// The real counts aggregate (called directly by the cache and by tests, so the
/// two can't drift).
pub async fn event_counts_compute(
    db: &sqlx::PgPool,
    chain_id: i64,
) -> Result<serde_json::Value, AppError> {
    if !audit_trail_enabled(db).await? {
        return serde_json::to_value(AuditEventCounts { counts: vec![], total: 0, enabled: false })
            .map_err(|e| AppError::Internal(format!("serialize audit counts: {e}")));
    }
    let rows = sqlx::query(COUNTS_SQL).bind(chain_id).fetch_all(db).await?;
    let counts: Vec<AuditEventCount> = rows
        .into_iter()
        .map(|r| AuditEventCount {
            source_kind: r.try_get("source_kind").unwrap_or_default(),
            event_name: r.try_get("event_name").unwrap_or_default(),
            projection_tag: r.try_get("projection_tag").unwrap_or_default(),
            count: r.try_get("count").unwrap_or_default(),
        })
        .collect();
    let total: i64 = counts.iter().map(|c| c.count).sum();
    serde_json::to_value(AuditEventCounts { counts, total, enabled: true })
        .map_err(|e| AppError::Internal(format!("serialize audit counts: {e}")))
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

async fn value_exists(
    db: &sqlx::PgPool,
    chain_id: i64,
    sql: &str,
    value: &str,
) -> Result<bool, AppError> {
    let row = sqlx::query(sql).bind(chain_id).bind(value).fetch_one(db).await?;
    Ok(row.try_get::<bool, _>("exists").unwrap_or(false))
}

fn to_hex(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

fn row_to_event(r: sqlx::postgres::PgRow) -> AuditEvent {
    let tx_hash: Vec<u8> = r.try_get("tx_hash").unwrap_or_default();
    let source_contract: Vec<u8> = r.try_get("source_contract").unwrap_or_default();
    let tx_signer: Option<Vec<u8>> = r.try_get("tx_signer").ok().flatten();
    AuditEvent {
        event_id: r.try_get("event_id").unwrap_or_default(),
        block_number: r.try_get("block_number").unwrap_or_default(),
        block_timestamp: r.try_get("block_timestamp").unwrap_or_default(),
        tx_hash: to_hex(&tx_hash),
        tx_index: r.try_get("tx_index").unwrap_or_default(),
        log_index: r.try_get("log_index").unwrap_or_default(),
        event_name: r.try_get("event_name").unwrap_or_default(),
        source_contract: to_hex(&source_contract),
        source_kind: r.try_get("source_kind").unwrap_or_default(),
        projection_tag: r.try_get("projection_tag").unwrap_or_default(),
        tx_signer: tx_signer.map(|b| to_hex(&b)),
        args: r.try_get("args").unwrap_or(serde_json::Value::Null),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Unit tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Assert a query references no `audit.<relation>` other than `chain_event`
    /// (the Tier-1 PII-free boundary) and that it does touch that table.
    fn assert_only_chain_event(sql: &str) {
        for (i, _) in sql.match_indices("audit.") {
            let rest = &sql[i + "audit.".len()..];
            let rel: String = rest
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            assert_eq!(
                rel, "chain_event",
                "audit handler SQL touches audit.{rel}, must be only audit.chain_event:\n{sql}"
            );
        }
        assert!(
            sql.contains("audit.chain_event"),
            "SQL must reference audit.chain_event:\n{sql}"
        );
    }

    #[test]
    fn all_audit_sql_touches_only_chain_event() {
        for (a, b) in [(false, false), (true, false), (false, true), (true, true)] {
            assert_only_chain_event(&events_sql(a, b));
        }
        assert_only_chain_event(COUNTS_SQL);
        assert_only_chain_event(EVENT_NAME_EXISTS_SQL);
        assert_only_chain_event(SOURCE_KIND_EXISTS_SQL);
        assert_only_chain_event(AUDIT_ENABLED_SQL);
    }

    #[test]
    fn events_sql_param_positions_shift_with_filters() {
        let none = events_sql(false, false);
        assert!(none.contains("LIMIT $5"));
        assert!(!none.contains("$6"), "no filter ⇒ no $6: {none}");

        let en = events_sql(true, false);
        assert!(en.contains("event_name = $6"), "{en}");
        assert!(!en.contains("$7"));

        let sk = events_sql(false, true);
        assert!(sk.contains("source_kind = $6"), "single filter lands at $6: {sk}");

        let both = events_sql(true, true);
        assert!(both.contains("event_name = $6"), "{both}");
        assert!(both.contains("source_kind = $7"), "{both}");
    }

    #[test]
    fn events_sql_uses_row_constructor_boundary() {
        // The keyset boundary must be the row-constructor form, not the OR-form.
        let sql = events_sql(false, false);
        assert!(
            sql.contains("(block_number, tx_index, log_index) < ($2, $3, $4)"),
            "must use the row-constructor index boundary: {sql}"
        );
        assert!(sql.contains("ORDER BY block_number DESC, tx_index DESC, log_index DESC"));
    }

    #[test]
    fn to_hex_is_0x_prefixed() {
        assert_eq!(to_hex(&[0xde, 0xad, 0xbe, 0xef]), "0xdeadbeef");
        assert_eq!(to_hex(&[]), "0x");
    }
}
