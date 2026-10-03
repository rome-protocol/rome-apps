/// Search endpoint.
///
/// GET /api/v1/search?q=<query>
///
/// Uses pg_trgm similarity on the `search_index` table.
/// Returns up to 20 results ordered by (similarity DESC, weight DESC) so the
/// most relevant (highest-similarity) hits win, with the per-type `weight` only
/// breaking ties between equally-similar matches.
/// `partial: true` is set on a query timeout OR when the result count hits the
/// LIMIT (more matches may exist beyond the cap).
use axum::{
    extract::{Query, State},
    Json,
};
use serde::Deserialize;
use sqlx::Row;

use crate::{
    api::models::{SearchHit, SearchResults},
    error::AppError,
    state::AppState,
};

/// Maximum query length (characters) — prevents expensive trgm on huge inputs.
const MAX_QUERY_LEN: usize = 80;

/// Maximum number of search results returned. When the query returns exactly
/// this many rows the result set is (silently) truncated by the SQL `LIMIT`,
/// so `partial` is set to flag that more matches may exist.
const SEARCH_LIMIT: usize = 20;

/// Decide whether the result set was truncated by the `LIMIT`.
///
/// The search query caps rows at `SEARCH_LIMIT`. If the DB returned exactly
/// that many, more matching rows may exist beyond the cap — flag the response
/// as `partial` so the UI can hint "refine your query". A short result set
/// (`count < limit`) is complete. Pure so it can be unit-tested without a DB.
fn is_partial(count: usize, limit: usize) -> bool {
    count >= limit
}

#[derive(Debug, Deserialize)]
pub struct SearchQuery {
    pub q: String,
}

/// GET /api/v1/search?q= — fuzzy text search over the search_index table.
///
/// Returns up to 20 results. Sets `partial: true` if results may be incomplete.
#[utoipa::path(
    get,
    path = "/api/v1/search",
    params(("q" = String, Query, description = "Search query (max 80 chars)")),
    responses(
        (status = 200, description = "Search results"),
        (status = 400, description = "Bad request (query too long)", body = crate::error::ProblemJson),
        (status = 500, description = "Internal error", body = crate::error::ProblemJson),
    )
)]
pub async fn search(
    State(state): State<AppState>,
    Query(params): Query<SearchQuery>,
) -> Result<Json<SearchResults>, AppError> {
    let q = params.q.trim().to_string();

    if q.is_empty() {
        return Ok(Json(SearchResults {
            results: vec![],
            partial: false,
        }));
    }

    if q.len() > MAX_QUERY_LEN {
        return Err(AppError::BadRequest(format!(
            "search query too long (max {MAX_QUERY_LEN} chars)"
        )));
    }

    // Use pg_trgm similarity. Minimum similarity threshold of 0.05 to avoid noise.
    //
    // Ordering: relevance (trigram similarity) is PRIMARY; entity `weight`
    // (the per-type prior: tx 100 / block 90 / token 80 / contract-label 60 /
    // address 50 — see search_indexer) is only the tiebreaker. The old
    // `weight DESC, sim DESC` let a weak fuzzy tx/block hit (weight 100,
    // sim ~0.06) outrank an exact token/address hit (weight 80, sim 1.0). A
    // multiplicative blend (`sim * weight`) wouldn't fix it — a 0.7-sim weight-100
    // row still beats a 1.0-sim weight-50 row. `sim DESC, weight DESC` puts
    // exact / high-similarity matches on top unconditionally, then uses weight
    // to break ties between equally-similar hits.
    //
    // Note: statement_timeout is applied via a tx wrapping the query so sqlx's
    // prepared-statement requirement (single SQL command) is respected.
    let mut tx = state
        .db
        .begin()
        .await
        .map_err(|e| AppError::InternalError(anyhow::anyhow!(e)))?;
    sqlx::query("SET LOCAL statement_timeout = '2000ms'")
        .execute(&mut *tx)
        .await
        .map_err(|e| AppError::InternalError(anyhow::anyhow!(e)))?;
    // LEFT JOIN token_metadata to surface `kind` for token-type hits so the
    // dropdown can relabel (e.g. SPL → "Wrapped SPL"). Only token entities have
    // a matching row — `si.entity_id` for a token is its (lowercased) address,
    // which equals `token_metadata.address` (stored lowercased; see tokens.rs
    // lookup + metadata worker). Non-token hits get NULL kind. `lower()` guards
    // against any case drift between the search index and the metadata table.
    let rows = sqlx::query(
        "SELECT si.entity_type, si.entity_id, si.display_label, si.weight,
                tm.kind AS token_kind,
                similarity(si.display_label, $2) AS sim
         FROM rome_via.search_index si
         LEFT JOIN rome_via.token_metadata tm
           ON tm.chain_id = $1 AND tm.address = lower(si.entity_id)
         WHERE si.chain_id = $1
           AND similarity(si.display_label, $2) > 0.05
         ORDER BY sim DESC, si.weight DESC
         LIMIT 20",
    )
    .bind(state.chain_id)
    .bind(&q)
    .fetch_all(&mut *tx)
    .await;

    let (rows, partial) = match rows {
        // A full page (len == LIMIT) means the result set was truncated by the
        // SQL `LIMIT` — flag partial so callers know more matches may exist.
        Ok(r) => {
            let partial = is_partial(r.len(), SEARCH_LIMIT);
            (r, partial)
        }
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("statement timeout") || msg.contains("canceling statement") {
                // Timeout — return empty partial result rather than 500.
                return Ok(Json(SearchResults {
                    results: vec![],
                    partial: true,
                }));
            }
            return Err(AppError::InternalError(anyhow::anyhow!(msg)));
        }
    };

    let results: Vec<SearchHit> = rows
        .iter()
        .map(|row| {
            let entity_type: String = row.get("entity_type");
            let entity_id: String = row.get("entity_id");
            let url = build_url(&entity_type, &entity_id);
            // NULL for every non-token hit (no matching token_metadata row), so
            // we don't fabricate a kind for blocks / addresses / txs.
            let kind: Option<String> = row.get("token_kind");
            SearchHit {
                entity_type,
                entity_id,
                display_label: row.get("display_label"),
                url,
                kind,
            }
        })
        .collect();

    Ok(Json(SearchResults { results, partial }))
}

/// Build the UI navigation URL for a search hit.
fn build_url(entity_type: &str, entity_id: &str) -> String {
    match entity_type {
        "tx" => format!("/tx/{entity_id}"),
        "block" => format!("/block/{entity_id}"),
        "address" => format!("/address/{entity_id}"),
        "token" => format!("/token/{entity_id}"),
        _ => format!("/{entity_type}/{entity_id}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_url_tx() {
        assert_eq!(
            build_url("tx", "0xabcd"),
            "/tx/0xabcd"
        );
    }

    #[test]
    fn build_url_block() {
        assert_eq!(build_url("block", "12345"), "/block/12345");
    }

    #[test]
    fn build_url_address() {
        assert_eq!(
            build_url("address", "0x1234"),
            "/address/0x1234"
        );
    }

    #[test]
    fn build_url_token() {
        assert_eq!(
            build_url("token", "0xusdc"),
            "/token/0xusdc"
        );
    }

    // ── partial-truncation flag ─────────────────────────────────────────────
    // `partial` was only ever set on a statement-timeout; a full page of
    // results (count == LIMIT) silently dropped any further matches. Flag
    // partial whenever the result count hits the cap.
    #[test]
    fn is_partial_true_when_count_hits_limit() {
        assert!(is_partial(SEARCH_LIMIT, SEARCH_LIMIT));
        // Defensive: an over-count (shouldn't happen with LIMIT, but the
        // predicate is >=) is also partial.
        assert!(is_partial(SEARCH_LIMIT + 1, SEARCH_LIMIT));
    }

    #[test]
    fn is_partial_false_when_count_below_limit() {
        assert!(!is_partial(SEARCH_LIMIT - 1, SEARCH_LIMIT));
        assert!(!is_partial(0, SEARCH_LIMIT));
    }
}
