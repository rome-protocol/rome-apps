//! Shared query layer — the single source of SQL truth for catalog reads.
//!
//! Both REST handlers and MCP tools call into this module. Keeping the SQL
//! here eliminates a whole class of drift bugs (REST and MCP returning
//! different shapes for the same data).
//!
//! Three functions, one per catalog read:
//!
//! - [`list_apps`] — paginated + filtered listing. Returns the full
//!   `AppListResponse` shape (apps array + total + limit + offset).
//! - [`get_app`] — raw manifest JSON for a single app id, `None` if missing.
//! - [`get_metrics`] — metrics cache sliced out of the manifest. `None` if
//!   the app is missing.
//!
//! # Why `Result<Option<T>>` and not `Option<Result<T>>`
//!
//! The outer `Result` is for DB / driver errors — `sqlx::Error` wrapped via
//! `anyhow`. The inner `Option` is the business-level "row exists?" signal.
//! Callers that want to return a typed "not found" error (MCP's
//! `AppNotFound`, REST's 404) match on the inner `None`.

use anyhow::Result;
use serde_json::{json, Value};
use sqlx::{PgPool, Row};

use super::types::{AppListQuery, AppListResponse, AppSummary, MetricsResponse};

/// Page through `apps` with optional filters. Mirrors REST `GET /apps`.
///
/// Filters: `tier`, `status`, `category` (membership test), `q` (substring
/// match on name OR description). Pagination clamped to 1..=200 on `limit`.
pub async fn list_apps(pool: &PgPool, q: &AppListQuery) -> Result<AppListResponse> {
    let limit = q.limit.clamp(1, 200);
    let offset = q.offset;

    let mut builder = sqlx::QueryBuilder::new(
        "SELECT id, name, description, tier, uniqueness, status, categories, \
         manifest_url, icon_url, COUNT(*) OVER() AS total \
         FROM apps WHERE 1=1",
    );
    if let Some(t) = &q.tier {
        builder.push(" AND tier = ").push_bind(t.clone());
    }
    if let Some(s) = &q.status {
        builder.push(" AND status = ").push_bind(s.clone());
    }
    if let Some(c) = &q.category {
        builder
            .push(" AND ")
            .push_bind(c.clone())
            .push(" = ANY(categories)");
    }
    if let Some(qs) = &q.q {
        let pat = format!("%{}%", qs);
        builder.push(" AND (name ILIKE ").push_bind(pat.clone());
        builder
            .push(" OR description ILIKE ")
            .push_bind(pat)
            .push(")");
    }
    builder
        .push(" ORDER BY tier, id LIMIT ")
        .push_bind(limit as i64);
    builder.push(" OFFSET ").push_bind(offset as i64);

    let rows = builder
        .build()
        .fetch_all(pool)
        .await
        .map_err(|e| anyhow::anyhow!("query apps: {e}"))?;

    let total = rows
        .first()
        .map(|r| r.get::<i64, _>("total") as u64)
        .unwrap_or(0);

    let apps: Vec<AppSummary> = rows
        .into_iter()
        .map(|r| AppSummary {
            id: r.get("id"),
            name: r.get("name"),
            description: r.get("description"),
            tier: r.get("tier"),
            uniqueness: r.get("uniqueness"),
            status: r.get("status"),
            categories: r.get::<Vec<String>, _>("categories"),
            manifest_url: r.get("manifest_url"),
            icon_url: r.get::<Option<String>, _>("icon_url"),
        })
        .collect();

    Ok(AppListResponse {
        apps,
        total,
        limit,
        offset,
    })
}

/// Look up a single app's full manifest JSON. `Ok(None)` means the app id
/// isn't in the catalog.
pub async fn get_app(pool: &PgPool, id: &str) -> Result<Option<Value>> {
    let row = sqlx::query("SELECT manifest_raw FROM apps WHERE id = $1")
        .bind(id)
        .fetch_optional(pool)
        .await
        .map_err(|e| anyhow::anyhow!("get_app: {e}"))?;
    Ok(row.map(|r| r.get::<Value, _>("manifest_raw")))
}

/// Slice the `metrics_cache` sub-object out of an app's manifest. Missing
/// fields default to zeros so the response shape is stable; M3D will add a
/// dedicated metrics table that replaces this read.
pub async fn get_metrics(pool: &PgPool, id: &str) -> Result<Option<MetricsResponse>> {
    let row = sqlx::query("SELECT manifest_raw FROM apps WHERE id = $1")
        .bind(id)
        .fetch_optional(pool)
        .await
        .map_err(|e| anyhow::anyhow!("get_metrics: {e}"))?;
    let Some(row) = row else { return Ok(None) };
    let raw: Value = row.get("manifest_raw");
    let mc = raw.get("metrics_cache").cloned().unwrap_or(json!({}));
    Ok(Some(MetricsResponse {
        tx_7d: mc.get("tx_7d").and_then(|v| v.as_u64()).unwrap_or(0),
        unique_callers_7d: mc
            .get("unique_callers_7d")
            .and_then(|v| v.as_u64())
            .unwrap_or(0),
        agent_share_7d: mc
            .get("agent_share_7d")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0),
        gas_usd_7d: mc
            .get("gas_usd_7d")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0),
        as_of: mc
            .get("checked_at")
            .and_then(|v| v.as_str())
            .unwrap_or("1970-01-01T00:00:00Z")
            .to_string(),
    }))
}
