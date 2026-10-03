/// Token endpoints.
///
/// - GET /api/v1/tokens?cursor=&limit=
/// - GET /api/v1/tokens/:address
/// - GET /api/v1/tokens/:address/holders?cursor=&limit=
/// - GET /api/v1/tokens/:address/transfers?cursor=&limit=
use axum::{
    extract::{Path, Query, State},
    Json,
};
use bigdecimal::{BigDecimal, ToPrimitive};
use serde::Deserialize;
use sqlx::Row;
use std::str::FromStr;

use crate::{
    api::models::{GateEvent, Page, TokenDetail, TokenHolder, TokenSummary, TokenTransfer},
    error::AppError,
    state::AppState,
};

const DEFAULT_LIMIT: i64 = 50;
const MAX_LIMIT: i64 = 100;

/// Token kinds that are SPL-backed cached wrappers, for which the on-chain
/// `total_supply()` is decimal-misaligned or zero. For these we derive
/// circulating supply + holder share from the SUM of holder balances rather
/// than trusting `total_supply`.
const WRAPPER_KINDS: [&str; 2] = ["SPL", "Token-2022"];

/// Is this token kind an SPL-backed cached wrapper (`SPL` / `Token-2022`)?
/// `None` (unclassified) is treated as not-a-wrapper — we don't fabricate a
/// derived supply for a token we haven't classified.
fn is_wrapper_kind(kind: Option<&str>) -> bool {
    kind.is_some_and(|k| WRAPPER_KINDS.contains(&k))
}

/// Holder share = `balance / total`, as a fraction in `[0.0, 1.0]`.
///
/// `total` is the SUM of all holder balances (the derived circulating supply),
/// NOT the on-chain `total_supply` — both sides are therefore the same base-unit
/// scale, so the ratio is decimal-agnostic and never collapses to ~0% from a
/// 18-vs-6 decimal mismatch. Returns `None` when `total <= 0` (no holders /
/// empty token) so we never divide by zero. Uses `BigDecimal` because a uint256
/// balance can exceed `u128`; the final ratio is small and converts to `f64`.
fn holder_share(balance: &BigDecimal, total: &BigDecimal) -> Option<f64> {
    use bigdecimal::Zero;
    if total <= &BigDecimal::zero() {
        return None;
    }
    (balance / total).to_f64()
}

#[derive(Debug, Deserialize)]
pub struct PaginationQuery {
    pub cursor: Option<i64>,
    pub limit: Option<i64>,
    /// Optional `factory=0x…` filter — the tokens a given factory deployed.
    /// Drives the factory address page's "tokens created" feed.
    pub factory: Option<String>,
    /// Optional `kind=` filter — one of `TOKEN_KINDS`. Drives the Tokens-header
    /// tile links (`/tokens?kind=SPL` etc.) so the filtered list's row count
    /// matches the tile it was clicked from (`stats.rs::overview_compute`'s
    /// `token_erc20` / `token_spl` / `token_token2022` FILTER counts).
    pub kind: Option<String>,
}

/// Validate a `factory=` filter into a lowercased address, or reject it.
///
/// A malformed value must NOT degrade to "unfiltered": returning every token
/// on the chain would render as though one factory had created all of them,
/// which reads as data rather than as an error. Absent is the only way to ask
/// for unfiltered.
fn parse_factory_filter(raw: Option<&str>) -> Result<Option<String>, AppError> {
    let Some(v) = raw else { return Ok(None) };
    let ok = v.len() == 42
        && v.starts_with("0x")
        && v[2..].bytes().all(|b| b.is_ascii_hexdigit());
    if !ok {
        return Err(AppError::BadRequest(format!(
            "factory must be a 0x-prefixed 20-byte hex address (got {v:?})"
        )));
    }
    Ok(Some(v.to_lowercase()))
}

/// The literal `token_metadata.kind` values `stats.rs::overview_compute` FILTERs
/// on. Kept in sync with that query by construction: both this list filter and
/// the header tile must agree on exactly these three strings.
const TOKEN_KINDS: [&str; 3] = ["SPL", "ERC-20", "Token-2022"];

/// Validate a `kind=` filter against `TOKEN_KINDS`, or reject it.
///
/// Same shape as `parse_factory_filter`: a typo'd kind must NOT degrade to
/// "unfiltered" (that would render as though every token had the queried
/// kind). Absent is the only way to ask for unfiltered.
fn parse_kind_filter(raw: Option<&str>) -> Result<Option<String>, AppError> {
    let Some(v) = raw else { return Ok(None) };
    if !TOKEN_KINDS.contains(&v) {
        return Err(AppError::BadRequest(format!(
            "kind must be one of {TOKEN_KINDS:?} (got {v:?})"
        )));
    }
    Ok(Some(v.to_string()))
}

/// Resolve a user-supplied `limit` into a usable value. Returns
/// `400 Bad Request` when the caller exceeds `MAX_LIMIT` — never silently
/// truncate. Mirrors `addresses::resolve_limit`. Closes rome-via#66.
fn resolve_limit(limit: Option<i64>) -> Result<i64, AppError> {
    let Some(l) = limit else { return Ok(DEFAULT_LIMIT) };
    if l < 1 {
        return Err(AppError::BadRequest(format!(
            "limit must be >= 1 (got {l})"
        )));
    }
    if l > MAX_LIMIT {
        return Err(AppError::BadRequest(format!(
            "limit exceeds max of {MAX_LIMIT} (got {l})"
        )));
    }
    Ok(l)
}

/// GET /api/v1/tokens — paginated list of tokens ordered by holder count DESC.
#[utoipa::path(
    get,
    path = "/api/v1/tokens",
    params(
        ("cursor" = Option<i64>, Query, description = "Offset cursor"),
        ("limit" = Option<i64>, Query, description = "Items per page (default 50, max 100)"),
        ("factory" = Option<String>, Query, description = "Only tokens deployed by this factory address (0x-prefixed)"),
        ("kind" = Option<String>, Query, description = "Only tokens of this kind: SPL | ERC-20 | Token-2022"),
    ),
    responses(
        (status = 200, description = "Paginated token list"),
        (status = 400, description = "Malformed factory or kind filter", body = crate::error::ProblemJson),
        (status = 500, description = "Internal error", body = crate::error::ProblemJson),
    )
)]
pub async fn list_tokens(
    State(state): State<AppState>,
    Query(params): Query<PaginationQuery>,
) -> Result<Json<Page<TokenSummary>>, AppError> {
    let limit = resolve_limit(params.limit)?;
    let offset = params.cursor.unwrap_or(0).max(0);
    // Optional factory filter. `token_metadata` is chain-token-cardinality
    // (hundreds of rows), so no index is needed for the added predicate.
    let factory = parse_factory_filter(params.factory.as_deref())?;
    // Optional kind filter — literal `token_metadata.kind` values, same set
    // `stats.rs::overview_compute` FILTERs on, so `?kind=SPL` etc. returns
    // exactly the tokens the Tokens-header tile counted.
    let kind = parse_kind_filter(params.kind.as_deref())?;

    // Per-row `circulating_supply` for wrapper kinds (`SPL` / `Token-2022`):
    // SUM(holder balances WHERE balance > 0) — the SAME decimal-agnostic value
    // the detail endpoint (`get_token`) derives, because a cached wrapper's raw
    // `total_supply` is 18-vs-6 misaligned or literally 0. NULL for plain
    // ERC-20s (whose raw `supply` is self-consistent) so the key is omitted.
    // Correlated subquery is fine at a 50-row page — `token_holders` has the
    // (chain_id, token_address) WHERE balance > 0 partial index.
    let rows = sqlx::query(
        "SELECT m.address, m.symbol, m.name, m.kind, m.decimals,
                m.gated, m.restriction_module,
                m.total_supply::TEXT AS total_supply,
                COALESCE(c.holder_count, 0) AS holders,
                CASE WHEN m.kind IN ('SPL', 'Token-2022')
                     THEN (SELECT COALESCE(SUM(th.balance), 0)::TEXT
                           FROM rome_via.token_holders th
                           WHERE th.chain_id = m.chain_id
                             AND th.token_address = m.address
                             AND th.balance > 0)
                     ELSE NULL
                END AS circulating_supply
         FROM rome_via.token_metadata m
         LEFT JOIN rome_via.token_holder_counts c
             ON c.chain_id = m.chain_id AND c.token_address = m.address
         WHERE m.chain_id = $1
           AND ($4::TEXT IS NULL OR LOWER(m.factory) = $4)
           AND ($5::TEXT IS NULL OR m.kind = $5)
         ORDER BY holders DESC, m.address ASC
         LIMIT $2 OFFSET $3",
    )
    .bind(state.chain_id)
    .bind(limit + 1)
    .bind(offset)
    .bind(factory.as_deref())
    .bind(kind.as_deref())
    .fetch_all(&state.db)
    .await
    .map_err(|e| AppError::InternalError(anyhow::anyhow!(e.to_string())))?;

    let has_more = rows.len() as i64 > limit;
    let items: Vec<TokenSummary> = rows
        .into_iter()
        .take(limit as usize)
        .map(|row: sqlx::postgres::PgRow| TokenSummary {
            address: row.get("address"),
            symbol: row.get("symbol"),
            name: row.get("name"),
            // `kind` is nullable (NULL = not-yet-classified sentinel, migration
            // 0213). A plain `row.get::<String,_>` would fail to decode a NULL
            // and 500 the whole list the moment any unclassified token exists —
            // read as Option. Matches `code_hash` read in addresses.rs.
            kind: row.get::<Option<String>, _>("kind"),
            holders: row.get("holders"),
            supply: row.get("total_supply"),
            // SMALLINT → Option<i16> (matches TokenDetail.decimals). Nullable
            // when the metadata worker has not filled it yet.
            decimals: row.get::<Option<i16>, _>("decimals"),
            // TEXT-encoded SUM for wrapper kinds, NULL (→ omitted) for ERC-20s.
            circulating_supply: row.get::<Option<String>, _>("circulating_supply"),
            gated: row.get::<Option<bool>, _>("gated"),
            restriction_module: row.get::<Option<String>, _>("restriction_module"),
        })
        .collect();

    let next_cursor = if has_more {
        Some((offset + limit).to_string())
    } else {
        None
    };

    Ok(Json(Page {
        items,
        next_cursor,
        has_more,
    }))
}

/// GET /api/v1/tokens/:address — full token detail.
#[utoipa::path(
    get,
    path = "/api/v1/tokens/{address}",
    params(("address" = String, Path, description = "Token contract address (0x-prefixed)")),
    responses(
        (status = 200, description = "Token detail"),
        (status = 404, description = "Token not found", body = crate::error::ProblemJson),
        (status = 500, description = "Internal error", body = crate::error::ProblemJson),
    )
)]
pub async fn get_token(
    State(state): State<AppState>,
    Path(address): Path<String>,
) -> Result<Json<TokenDetail>, AppError> {
    let addr = address.to_lowercase();

    let row = sqlx::query(
        "SELECT m.address, m.symbol, m.name, m.kind, m.decimals,
                m.total_supply::TEXT AS total_supply, m.updated_at,
                m.mint, m.factory, m.creator,
                m.gated, m.restriction_module,
                COALESCE(c.holder_count, 0) AS holders
         FROM rome_via.token_metadata m
         LEFT JOIN rome_via.token_holder_counts c
             ON c.chain_id = m.chain_id AND c.token_address = m.address
         WHERE m.chain_id = $1 AND m.address = $2
         LIMIT 1",
    )
    .bind(state.chain_id)
    .bind(&addr)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| AppError::InternalError(anyhow::anyhow!(e.to_string())))?
    .ok_or_else(|| AppError::NotFound(format!("token {addr}")))?;

    let updated_at: chrono::DateTime<chrono::Utc> = row.get("updated_at");
    let kind: Option<String> = row.get("kind");

    // For SPL-backed cached wrappers the on-chain `total_supply()` is
    // decimal-misaligned (18-dec-scaled against 6-dec balances) or literally 0
    // (e.g. wSOL with live holders). Derive a decimal-agnostic circulating
    // supply = SUM(holder balances WHERE balance > 0) — same base-unit scale as
    // balances, consistent with holder `share` by construction. Summed in SQL
    // and returned as TEXT (a uint256 sum never fits i64/u128 reliably). Left
    // None for plain ERC-20s, whose raw `supply` is already self-consistent.
    let circulating_supply: Option<String> = if is_wrapper_kind(kind.as_deref()) {
        sqlx::query_scalar(
            "SELECT COALESCE(SUM(balance), 0)::TEXT
             FROM rome_via.token_holders
             WHERE chain_id = $1 AND token_address = $2 AND balance > 0",
        )
        .bind(state.chain_id)
        .bind(&addr)
        .fetch_optional(&state.db)
        .await
        .map_err(|e| AppError::InternalError(anyhow::anyhow!(e.to_string())))?
        .flatten()
    } else {
        None
    };

    Ok(Json(TokenDetail {
        address: row.get("address"),
        symbol: row.get("symbol"),
        name: row.get("name"),
        // Nullable (NULL = not-yet-classified, migration 0213) — read as Option
        // so a NULL doesn't fail decode and 500 the detail response.
        kind,
        holders: row.get("holders"),
        supply: row.get("total_supply"),
        circulating_supply,
        decimals: row.get("decimals"),
        updated_at: updated_at.to_rfc3339(),
        mint: row.get("mint"),
        factory: row.get("factory"),
        creator: row.get("creator"),
        gated: row.get::<Option<bool>, _>("gated"),
        restriction_module: row.get::<Option<String>, _>("restriction_module"),
    }))
}

/// GET /api/v1/tokens/:address/holders — paginated holder list.
#[utoipa::path(
    get,
    path = "/api/v1/tokens/{address}/holders",
    params(
        ("address" = String, Path, description = "Token contract address"),
        ("cursor" = Option<i64>, Query, description = "Offset cursor"),
        ("limit" = Option<i64>, Query, description = "Items per page (default 50, max 100)"),
    ),
    responses(
        (status = 200, description = "Token holders"),
        (status = 500, description = "Internal error", body = crate::error::ProblemJson),
    )
)]
pub async fn list_token_holders(
    State(state): State<AppState>,
    Path(address): Path<String>,
    Query(params): Query<PaginationQuery>,
) -> Result<Json<Page<TokenHolder>>, AppError> {
    let addr = address.to_lowercase();
    let limit = resolve_limit(params.limit)?;
    let offset = params.cursor.unwrap_or(0).max(0);

    // Share denominator = SUM of ALL holder balances for this token (balance>0),
    // i.e. the same derived circulating supply used on the detail endpoint — NOT
    // the on-chain `total_supply`, which is decimal-misaligned / zero for cached
    // wrappers and would collapse share to ~0% or null. Summed across the whole
    // holder set (not just the current page), as NUMERIC so a uint256 sum never
    // overflows. `holder_share` guards the zero-total case.
    let total: Option<BigDecimal> = sqlx::query_scalar(
        "SELECT COALESCE(SUM(balance), 0)
         FROM rome_via.token_holders
         WHERE chain_id = $1 AND token_address = $2 AND balance > 0",
    )
    .bind(state.chain_id)
    .bind(&addr)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| AppError::InternalError(anyhow::anyhow!(e.to_string())))?
    .flatten();

    let rows = sqlx::query(
        "SELECT holder_address, balance::TEXT AS balance
         FROM rome_via.token_holders
         WHERE chain_id = $1 AND token_address = $2 AND balance > 0
         ORDER BY balance DESC
         LIMIT $3 OFFSET $4",
    )
    .bind(state.chain_id)
    .bind(&addr)
    .bind(limit + 1)
    .bind(offset)
    .fetch_all(&state.db)
    .await
    .map_err(|e| AppError::InternalError(anyhow::anyhow!(e.to_string())))?;

    let has_more = rows.len() as i64 > limit;
    let items: Vec<TokenHolder> = rows
        .into_iter()
        .take(limit as usize)
        .map(|row: sqlx::postgres::PgRow| {
            let balance: String = row.get("balance");
            let share = total.as_ref().and_then(|t| {
                BigDecimal::from_str(&balance)
                    .ok()
                    .and_then(|b| holder_share(&b, t))
            });
            TokenHolder {
                address: row.get("holder_address"),
                balance,
                share,
            }
        })
        .collect();

    let next_cursor = if has_more {
        Some((offset + limit).to_string())
    } else {
        None
    };

    Ok(Json(Page {
        items,
        next_cursor,
        has_more,
    }))
}

/// GET /api/v1/tokens/:address/transfers — paginated transfer list.
#[utoipa::path(
    get,
    path = "/api/v1/tokens/{address}/transfers",
    params(
        ("address" = String, Path, description = "Token contract address"),
        ("cursor" = Option<i64>, Query, description = "Offset cursor"),
        ("limit" = Option<i64>, Query, description = "Items per page (default 50, max 100)"),
    ),
    responses(
        (status = 200, description = "Token transfers"),
        (status = 500, description = "Internal error", body = crate::error::ProblemJson),
    )
)]
pub async fn list_token_transfers(
    State(state): State<AppState>,
    Path(address): Path<String>,
    Query(params): Query<PaginationQuery>,
) -> Result<Json<Page<TokenTransfer>>, AppError> {
    let addr = address.to_lowercase();
    let limit = resolve_limit(params.limit)?;
    let offset = params.cursor.unwrap_or(0).max(0);

    let rows = sqlx::query(
        "SELECT tx_hash, log_index, token_address, from_addr, to_addr,
                amount::TEXT AS amount, block_number,
                timestamp
         FROM rome_via.token_transfers
         WHERE chain_id = $1 AND token_address = $2
         ORDER BY block_number DESC, log_index DESC
         LIMIT $3 OFFSET $4",
    )
    .bind(state.chain_id)
    .bind(&addr)
    .bind(limit + 1)
    .bind(offset)
    .fetch_all(&state.db)
    .await
    .map_err(|e| AppError::InternalError(anyhow::anyhow!(e.to_string())))?;

    let has_more = rows.len() as i64 > limit;
    let items: Vec<TokenTransfer> = rows
        .into_iter()
        .take(limit as usize)
        .map(|row: sqlx::postgres::PgRow| {
            let ts: Option<chrono::DateTime<chrono::Utc>> = row.get("timestamp");
            TokenTransfer {
                tx_hash: row.get("tx_hash"),
                log_index: row.get("log_index"),
                token_address: row.get("token_address"),
                from: row.get("from_addr"),
                to: row.get("to_addr"),
                amount: row.get("amount"),
                block_number: row.get("block_number"),
                timestamp: ts.map(|t| t.to_rfc3339()),
            }
        })
        .collect();

    let next_cursor = if has_more {
        Some((offset + limit).to_string())
    } else {
        None
    };

    Ok(Json(Page {
        items,
        next_cursor,
        has_more,
    }))
}

/// `GET /api/v1/tokens/:address/gate-events` — the token's gate-change history.
///
/// Serves rows from `token_gate_events` (written by the enrich `gate_events`
/// worker), newest-first, joined to `eth_block` for the on-chain block time.
/// Empty for an ungated token. Powers the "Gate history" timeline on the token
/// page. The LEFT JOIN keeps an event whose block isn't yet indexed (timestamp
/// null) rather than dropping it.
#[utoipa::path(
    get,
    path = "/api/v1/tokens/{address}/gate-events",
    params(
        ("address" = String, Path, description = "Token contract address (0x-prefixed)"),
        ("cursor" = Option<i64>, Query, description = "Offset cursor from a previous response"),
        ("limit" = Option<i64>, Query, description = "Items per page (default 20, max 100)"),
    ),
    responses(
        (status = 200, description = "Gate-change history, newest first"),
        (status = 500, description = "Internal error", body = crate::error::ProblemJson),
    )
)]
pub async fn list_token_gate_events(
    State(state): State<AppState>,
    Path(address): Path<String>,
    Query(params): Query<PaginationQuery>,
) -> Result<Json<Page<GateEvent>>, AppError> {
    let addr = address.to_lowercase();
    let limit = resolve_limit(params.limit)?;
    let offset = params.cursor.unwrap_or(0).max(0);

    let rows = sqlx::query(
        "SELECT ge.event_type, ge.module_address, ge.transfers_allowed,
                ge.slot_number, ge.tx_hash, ge.log_index,
                eb.params_block_timestamp::TEXT AS timestamp_raw
         FROM rome_via.token_gate_events ge
         LEFT JOIN rome_via.eth_block_txs ebt
             ON ebt.tx_hash = ge.tx_hash AND ebt.chain_id = ge.chain_id
         LEFT JOIN rome_via.eth_block eb
             ON eb.slot_number = ebt.slot_number
             AND eb.slot_block_idx = ebt.slot_block_idx
             AND eb.chain_id = ebt.chain_id
         WHERE ge.chain_id = $1 AND ge.token_address = $2
         ORDER BY ge.slot_number DESC, ge.log_index DESC
         LIMIT $3 OFFSET $4",
    )
    .bind(state.chain_id)
    .bind(&addr)
    .bind(limit + 1)
    .bind(offset)
    .fetch_all(&state.db)
    .await
    .map_err(|e| AppError::InternalError(anyhow::anyhow!(e.to_string())))?;

    let has_more = rows.len() as i64 > limit;
    let items: Vec<GateEvent> = rows
        .into_iter()
        .take(limit as usize)
        .map(|row: sqlx::postgres::PgRow| {
            let ts_raw: Option<String> = row.get("timestamp_raw");
            let timestamp = ts_raw.and_then(|s| s.parse::<f64>().ok()).map(|epoch| {
                chrono::DateTime::from_timestamp(epoch as i64, 0)
                    .unwrap_or_default()
                    .format("%Y-%m-%dT%H:%M:%SZ")
                    .to_string()
            });
            GateEvent {
                event_type: row.get("event_type"),
                module_address: row.get("module_address"),
                transfers_allowed: row.get("transfers_allowed"),
                slot_number: row.get("slot_number"),
                tx_hash: row.get("tx_hash"),
                log_index: row.get("log_index"),
                timestamp,
            }
        })
        .collect();

    let next_cursor = if has_more {
        Some((offset + limit).to_string())
    } else {
        None
    };

    Ok(Json(Page {
        items,
        next_cursor,
        has_more,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bd(s: &str) -> BigDecimal {
        BigDecimal::from_str(s).unwrap()
    }

    // ── holder_share: decimal-agnostic share from SUM(balances) ─────────────
    // Share is balance / SUM(all holder balances), NOT balance / on-chain
    // total_supply. So it never collapses to ~0% from a 18-vs-6 decimal mismatch
    // and never divides by a zero on-chain supply.
    #[test]
    fn holder_share_normal_case() {
        // 25 of a 100-unit pool → 0.25.
        assert_eq!(holder_share(&bd("25"), &bd("100")), Some(0.25));
    }

    #[test]
    fn holder_share_zero_total_is_none() {
        // Empty token (no positive balances) → no share, never a divide-by-zero.
        assert_eq!(holder_share(&bd("0"), &bd("0")), None);
        // A stray balance against a zero total is still guarded.
        assert_eq!(holder_share(&bd("5"), &bd("0")), None);
    }

    #[test]
    fn holder_share_balance_equals_total_is_one() {
        // Sole holder owns the whole circulating supply → 1.0.
        assert_eq!(holder_share(&bd("100"), &bd("100")), Some(1.0));
    }

    #[test]
    fn holder_share_handles_large_uint256_balances() {
        // Balances beyond u128 (an 18-dec-scaled wSOL-style amount) must not
        // overflow — BigDecimal handles it; the ratio is still ~0.5.
        let total = bd("200000000000000000000000000000000000000");
        let bal = bd("100000000000000000000000000000000000000");
        let share = holder_share(&bal, &total).unwrap();
        assert!((share - 0.5).abs() < 1e-9, "expected ~0.5, got {share}");
    }

    // ── is_wrapper_kind: only SPL-backed cached wrappers get derived supply ──
    #[test]
    fn is_wrapper_kind_classifies_spl_kinds() {
        assert!(is_wrapper_kind(Some("SPL")));
        assert!(is_wrapper_kind(Some("Token-2022")));
        // Plain ERC-20 keeps its self-consistent on-chain supply.
        assert!(!is_wrapper_kind(Some("ERC-20")));
        // Unclassified (NULL kind) is NOT treated as a wrapper — no fabrication.
        assert!(!is_wrapper_kind(None));
    }
}

#[cfg(test)]
mod factory_filter_tests {
    use super::*;

    #[test]
    fn absent_filter_is_explicitly_unfiltered() {
        assert_eq!(parse_factory_filter(None).unwrap(), None);
    }

    #[test]
    fn valid_address_is_lowercased() {
        let got = parse_factory_filter(Some("0xAbCdEf0123456789012345678901234567890123"))
            .unwrap()
            .unwrap();
        assert_eq!(got, "0xabcdef0123456789012345678901234567890123");
    }

    // The dangerous failure mode: a typo'd or truncated factory param that
    // silently falls through to "no filter" would render the WHOLE token list
    // as if one factory had created every token. Reject instead.
    #[test]
    fn malformed_address_is_rejected_not_ignored() {
        for bad in [
            "",
            "0x",
            "not-an-address",
            "0x123",                                          // too short
            "0xabcdef01234567890123456789012345678901234",    // too long
            "abcdef0123456789012345678901234567890123",       // missing 0x
            "0xzzcdef0123456789012345678901234567890123",      // non-hex
        ] {
            let res = parse_factory_filter(Some(bad));
            assert!(
                res.is_err(),
                "expected {bad:?} to be rejected, got {:?}",
                res.ok()
            );
        }
    }

    #[test]
    fn rejection_is_a_bad_request_not_a_500() {
        match parse_factory_filter(Some("nope")) {
            Err(AppError::BadRequest(msg)) => assert!(
                msg.to_lowercase().contains("factory"),
                "message should name the offending param, got: {msg}"
            ),
            other => panic!("expected BadRequest, got {other:?}"),
        }
    }
}

#[cfg(test)]
mod kind_filter_tests {
    use super::*;

    #[test]
    fn absent_filter_is_explicitly_unfiltered() {
        assert_eq!(parse_kind_filter(None).unwrap(), None);
    }

    #[test]
    fn accepts_every_literal_kind() {
        for k in TOKEN_KINDS {
            assert_eq!(parse_kind_filter(Some(k)).unwrap(), Some(k.to_string()));
        }
    }

    // A typo'd kind (e.g. "spl" lowercase, or "erc20" without the hyphen) must
    // NOT degrade to "unfiltered" — that would render the whole token list as
    // though every token matched the queried kind.
    #[test]
    fn unrecognized_kind_is_rejected_not_ignored() {
        for bad in ["spl", "erc20", "token2022", "", "NFT", " SPL"] {
            let res = parse_kind_filter(Some(bad));
            assert!(
                res.is_err(),
                "expected {bad:?} to be rejected, got {:?}",
                res.ok()
            );
        }
    }

    #[test]
    fn rejection_is_a_bad_request_not_a_500() {
        match parse_kind_filter(Some("nope")) {
            Err(AppError::BadRequest(msg)) => assert!(
                msg.to_lowercase().contains("kind"),
                "message should name the offending param, got: {msg}"
            ),
            other => panic!("expected BadRequest, got {other:?}"),
        }
    }
}
