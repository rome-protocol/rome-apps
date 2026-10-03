/// Address endpoints.
///
/// - GET /api/v1/addresses — paginated list of top addresses by tx_count
/// - GET /api/v1/addresses/:address — address detail + live ETH balance
/// - GET /api/v1/addresses/:address/txs?cursor=&limit= — paginated tx list for address
use axum::{
    extract::{Path, Query, State},
    Json,
};
use serde::Deserialize;
use serde::Serialize;
use sqlx::Row;
use utoipa::ToSchema;

use crate::{
    api::{
        address_kind::classify_address_kind,
        models::{AddressDetail, AddressTokenHolding, Page, Tx, TxType},
        txs::{derive_status, extract_gas_report, parse_solana_legs},
    },
    classify::{classify, ClassifyInput},
    error::AppError,
    state::AppState,
};

const DEFAULT_LIMIT: i64 = 50;
const MAX_LIMIT: i64 = 100;

#[derive(Debug, Deserialize)]
pub struct PaginationQuery {
    pub cursor: Option<i64>,
    pub limit: Option<i64>,
    /// Optional `kind=` filter — one of `ADDRESS_KINDS`. Drives the
    /// Addresses-header tile links (`/addresses?kind=contract` etc.) so the
    /// filtered list's row count matches the tile it was clicked from
    /// (`stats.rs::overview_compute`'s `contracts` / `synthetics` counts and
    /// the derived `eoas` residual).
    pub kind: Option<String>,
    /// Optional free-text search term: matches addresses by PREFIX and
    /// contract labels by SUBSTRING (both case-insensitive). Absent/empty →
    /// unfiltered. Drives the Addresses page search box (search by name,
    /// e.g. "ARC", or by address, e.g. "0x9cde").
    pub q: Option<String>,
}

/// The three address kinds `stats.rs::overview_compute` breaks `active_addresses`
/// into: `contracts` (is_contract), `synthetics` (Solana-controlled sender),
/// `eoas` (residual). Kept in sync with that query by construction.
const ADDRESS_KINDS: [&str; 3] = ["contract", "eoa", "synthetic"];

/// Validate a `kind=` filter against `ADDRESS_KINDS`, or reject it.
///
/// Same shape as `tokens::parse_kind_filter` — a typo'd kind must NOT degrade
/// to "unfiltered" (that would render the whole address list as though every
/// address matched the queried kind). Absent is the only way to ask for
/// unfiltered.
fn parse_kind_filter(raw: Option<&str>) -> Result<Option<String>, AppError> {
    let Some(v) = raw else { return Ok(None) };
    if !ADDRESS_KINDS.contains(&v) {
        return Err(AppError::BadRequest(format!(
            "kind must be one of {ADDRESS_KINDS:?} (got {v:?})"
        )));
    }
    Ok(Some(v.to_string()))
}

/// Escape SQL `LIKE`/`ILIKE` metacharacters (`%`, `_`, and the escape
/// character itself) in a user-supplied search term, so it's matched as
/// literal text — a user typing "%" or "_" must not get wildcard behavior.
fn escape_like(raw: &str) -> String {
    raw.replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

/// Address tx feed pagination. `cursor` is an OPAQUE signed keyset token, not an
/// offset: a busy address takes new transactions continuously, and offset paging on a
/// moving feed silently skips and repeats rows (measured: page 1, page 2 and offset=5
/// returned three disjoint sets on hadrian-lt). Same scheme `/txs` uses.
#[derive(Debug, Deserialize)]
pub struct AddressTxsQuery {
    pub cursor: Option<String>,
    pub limit: Option<i64>,
}

/// Resolve a user-supplied `limit` into a usable value. Returns
/// `400 Bad Request` when the caller exceeds `MAX_LIMIT` — never silently
/// truncate, since callers can't tell pagination is incomplete except via
/// `hasMore` (and that's easy to overlook). Closes rome-via#66.
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

/// GET /api/v1/addresses/:address — address stats + live ETH balance.
#[utoipa::path(
    get,
    path = "/api/v1/addresses/{address}",
    params(("address" = String, Path, description = "EVM address (0x-prefixed)")),
    responses(
        (status = 200, description = "Address detail"),
        (status = 500, description = "Internal error", body = crate::error::ProblemJson),
    )
)]
pub async fn get_address(
    State(state): State<AppState>,
    Path(address): Path<String>,
) -> Result<Json<AddressDetail>, AppError> {
    let addr = address.to_lowercase();

    // Fetch from address_stats (may not exist for a fresh address). LEFT JOIN
    // contract_labels for the clean protocol/token label — same source +
    // join shape the sibling `list_address_txs` uses for `toLabel`
    // (`cl.chain_id = $1 AND cl.address = <lowercase addr>`). The label row may
    // not exist even when address_stats does, hence LEFT JOIN; and the address
    // may have a label with no stats row yet (fresh contract), so the label is
    // fetched in its own query keyed on the same lowercased `$2` rather than
    // hanging off the stats row.
    let stats_row = sqlx::query(
        "SELECT tx_count, first_seen, last_seen, is_contract, code_hash
         FROM rome_via.address_stats
         WHERE chain_id = $1 AND address = $2",
    )
    .bind(state.chain_id)
    .bind(&addr)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| AppError::InternalError(anyhow::anyhow!(e.to_string())))?;

    let (tx_count, first_seen, last_seen, is_contract, code_hash) =
        if let Some(row) = stats_row {
            let first: Option<chrono::DateTime<chrono::Utc>> = row.get("first_seen");
            let last: Option<chrono::DateTime<chrono::Utc>> = row.get("last_seen");
            (
                row.get::<i64, _>("tx_count"),
                first.map(|t| t.to_rfc3339()),
                last.map(|t| t.to_rfc3339()),
                row.get::<bool, _>("is_contract"),
                row.get::<Option<String>, _>("code_hash"),
            )
        } else {
            (0i64, None, None, false, None)
        };

    // Clean protocol/token label for this address, if it's a known contract.
    // Mirrors `list_address_txs`'s `contract_labels` join (cl.display_label /
    // cl.display_label_detail) — addresses are stored lowercase on both sides,
    // so the lowercased `$2` (= `addr`) matches without any casing change.
    let label_row = sqlx::query(
        "SELECT cl.display_label, cl.display_label_detail, cl.creator, cl.creation_tx
         FROM rome_via.contract_labels cl
         WHERE cl.chain_id = $1 AND cl.address = $2",
    )
    .bind(state.chain_id)
    .bind(&addr)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| AppError::InternalError(anyhow::anyhow!(e.to_string())))?;

    let (label, label_detail, creator, creation_tx) = match label_row {
        Some(row) => (
            row.try_get::<Option<String>, _>("display_label").ok().flatten(),
            row.try_get::<Option<String>, _>("display_label_detail").ok().flatten(),
            row.try_get::<Option<String>, _>("creator").ok().flatten(),
            row.try_get::<Option<String>, _>("creation_tx").ok().flatten(),
        ),
        None => (None, None, None, None),
    };

    // Fetch live ETH balance via eth_getBalance JSON-RPC.
    let balance = fetch_eth_balance(&state.proxy_url, &addr).await;

    // Is this address controlled by a Solana account? Derived directly from
    // evm_tx — a Solana-originated tx carries its controlling signer in
    // `solana_signer` and the synthetic `from` (= keccak(signer)[12:]) in
    // from_addr/from_address. No separate reverse-map table: the columns the
    // indexer already populates ARE the reverse map. Path-agnostic — any
    // origination != 'ecdsa' (today solana_unsigned; solana_ed25519 later)
    // surfaces here automatically once the indexer tags it.
    let sol = sqlx::query(
        "SELECT solana_signer FROM rome_via.evm_tx
         WHERE chain_id = $1
           AND origination <> 'ecdsa'
           AND solana_signer IS NOT NULL
           AND lower(COALESCE(from_addr, from_address)) = $2
         LIMIT 1",
    )
    .bind(state.chain_id)
    .bind(&addr)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| AppError::InternalError(anyhow::anyhow!(e.to_string())))?;
    let solana_pubkey: Option<String> = sol.and_then(|r| r.try_get::<String, _>("solana_signer").ok());
    let controlled_by_solana = solana_pubkey.is_some();

    // Is this address an indexed token? Feeds the kind classifier so the
    // client can dispatch (token pages redirect to /token/:address) without a
    // second probe request.
    let is_token: bool = sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1 FROM rome_via.token_metadata
             WHERE chain_id = $1 AND address = $2
         )",
    )
    .bind(state.chain_id)
    .bind(&addr)
    .fetch_one(&state.db)
    .await
    .map_err(|e| AppError::InternalError(anyhow::anyhow!(e.to_string())))?;

    let kind = classify_address_kind(&addr, is_token, is_contract, controlled_by_solana);

    Ok(Json(AddressDetail {
        address: addr,
        balance,
        tx_count,
        is_contract,
        code_hash,
        first_seen,
        last_seen,
        controlled_by_solana,
        solana_pubkey,
        label,
        label_detail,
        creator,
        creation_tx,
        kind,
    }))
}

/// GET /api/v1/addresses/:address/tokens — token holdings of an address.
#[utoipa::path(
    get,
    path = "/api/v1/addresses/{address}/tokens",
    params(
        ("address" = String, Path, description = "EVM address (0x-prefixed)"),
        ("cursor" = Option<i64>, Query, description = "Offset cursor"),
        ("limit" = Option<i64>, Query, description = "Items per page (default 50, max 100)"),
    ),
    responses(
        (status = 200, description = "Token holdings for the address"),
        (status = 400, description = "Invalid pagination", body = crate::error::ProblemJson),
        (status = 500, description = "Internal error", body = crate::error::ProblemJson),
    )
)]
pub async fn list_address_tokens(
    State(state): State<AppState>,
    Path(address): Path<String>,
    Query(params): Query<PaginationQuery>,
) -> Result<Json<Page<AddressTokenHolding>>, AppError> {
    let addr = address.to_lowercase();
    let limit = resolve_limit(params.limit)?;
    let offset = params.cursor.unwrap_or(0).max(0);

    // Inverted token_holders lookup (holder-first), joined to token_metadata
    // for identity. Ordered by balance DESC as a stable, index-friendly
    // default; cross-token base-unit balances aren't value-comparable, but a
    // deterministic order is what pagination needs. Backed by
    // ix_token_holders_holder (migration 0229).
    //
    // decimals is SMALLINT (INT2) in token_metadata — cast to INT4 in SQL so
    // the i32 decode can't panic (hit live: ColumnDecode 502s on hadrian-lt).
    // symbol/name are NULLable while the metadata worker hasn't resolved the
    // token yet — COALESCE so the String decode can't panic either.
    let rows = sqlx::query(
        "SELECT th.token_address, th.balance::TEXT AS balance,
                COALESCE(m.symbol, '') AS symbol,
                COALESCE(m.name, '') AS name,
                m.kind, m.decimals::INT4 AS decimals
         FROM rome_via.token_holders th
         JOIN rome_via.token_metadata m
           ON m.chain_id = th.chain_id AND m.address = th.token_address
         WHERE th.chain_id = $1 AND th.holder_address = $2 AND th.balance > 0
         ORDER BY th.balance DESC, th.token_address
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
    let items: Vec<AddressTokenHolding> = rows
        .into_iter()
        .take(limit as usize)
        .map(|row: sqlx::postgres::PgRow| AddressTokenHolding {
            token_address: row.get("token_address"),
            symbol: row.get("symbol"),
            name: row.get("name"),
            kind: row.get("kind"),
            decimals: row.get("decimals"),
            balance: row.get("balance"),
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

/// GET /api/v1/addresses/:address/txs — paginated transactions for an address.
#[utoipa::path(
    get,
    path = "/api/v1/addresses/{address}/txs",
    params(
        ("address" = String, Path, description = "EVM address (0x-prefixed)"),
        ("cursor" = Option<i64>, Query, description = "Offset cursor"),
        ("limit" = Option<i64>, Query, description = "Items per page (default 50, max 100)"),
    ),
    responses(
        (status = 200, description = "Address transactions"),
        (status = 500, description = "Internal error", body = crate::error::ProblemJson),
    )
)]
pub async fn list_address_txs(
    State(state): State<AppState>,
    Path(address): Path<String>,
    Query(params): Query<AddressTxsQuery>,
) -> Result<Json<Page<Tx>>, AppError> {
    let addr = address.to_lowercase();
    let limit = resolve_limit(params.limit)?;
    let (last_slot, last_tx_idx) = if let Some(tok) = params.cursor.as_deref() {
        let cur: crate::cursor::TxCursor = crate::cursor::decode(tok, &state.cursor_secret)?;
        if cur.chain_id != state.chain_id {
            return Err(AppError::ChainNotSupported(format!(
                "cursor is for chain {} but this API serves {}", cur.chain_id, state.chain_id
            )));
        }
        (cur.last_slot, cur.last_tx_idx)
    } else {
        (i64::MAX, i32::MAX)
    };

    // Query mirrors `/api/v1/txs` for the joined data — adds
    // `evm_tx_result` (with `chain_id` predicate),
    // `cross_chain_correlations` (for Romulus/Remus classification + solana
    // legs), and `tx_type_byte`. Without these joins:
    //   - `tx_type` always returned Rhea (closes rome-via#63 first half)
    //   - status was computed by a divergent local `derive_tx_status` that
    //     read top-level `code` instead of `exit_reason.code`, so every
    //     Hercules-shape result fell through to Failed (#63 second half)
    //   - `actionTags` was always empty (closes rome-via#64)
    let rows = sqlx::query(
        // PAGINATE FIRST, DECORATE SECOND. Two materialized CTEs:
        //   `hit`  — the address's tx_hashes, via three OWN-indexed lookups
        //            (from / to / fee-recipient). ORing them in one predicate
        //            made the query unservable (fee-recipient reads JSONB on the
        //            joined result table, forcing a full materialise of
        //            evm_tx_result). As a UNION each branch uses its index.
        //   `page` — resolves those hits' (slot, tx_idx) via a nested-loop into
        //            eth_block_txs, applies the keyset + ORDER BY + LIMIT, and
        //            returns only the <=limit+1 rows of THIS page.
        // Then the decoration joins run against `page` (<=16 rows), so the
        // planner nested-loops every one — no whole-table scan can creep in.
        //
        // Why both CTEs are needed: `hit` alone (MATERIALIZED) fixes the
        // eth_block_txs join, but the planner still ESTIMATES it at ~371k
        // (coarse from/to stats over a low-frequency address) and, believing it
        // is decorating 371k rows, seq-scans cross_chain_correlations (~14M) and
        // eth_block (~9M) via hash joins. `page`'s LIMIT caps the estimate at
        // 16, so those become nested-loop index lookups too. Measured on
        // hadrian: 13,469 ms -> 9 ms typical (321k-tx outlier address: 4.6 s,
        // bounded by its OWN tx count). #460's 4.4s regressed because the old
        // plan scaled with chain size (now ~19.7M eth_block_txs); this does not.
        "WITH hit AS MATERIALIZED (
                  SELECT tx_hash FROM rome_via.evm_tx
                   WHERE chain_id = $1 AND from_addr = $2
                  UNION
                  SELECT tx_hash FROM rome_via.evm_tx
                   WHERE chain_id = $1 AND to_addr = $2
                  UNION
                  SELECT tx_hash FROM rome_via.evm_tx_result
                   WHERE chain_id = $1
                     AND lower((tx_result -> 'gas_report') ->> 'gas_recipient') = $2
              ),
              page AS MATERIALIZED (
                  SELECT ebt.tx_hash, ebt.slot_number, ebt.slot_block_idx, ebt.tx_idx
                    FROM hit
                    JOIN rome_via.eth_block_txs ebt
                      ON ebt.tx_hash = hit.tx_hash AND ebt.chain_id = $1
                   WHERE (ebt.slot_number < $4 OR (ebt.slot_number = $4 AND ebt.tx_idx < $5))
                   ORDER BY ebt.slot_number DESC, ebt.tx_idx DESC
                   LIMIT $3
              )
         SELECT tx.tx_hash,
                COALESCE(tx.from_addr, tx.from_address) AS from_addr,
                tx.to_addr,
                COALESCE(tx.value_wei::TEXT, '0') AS value_wei,
                COALESCE(tx.method_id, '0x')      AS method_id,
                COALESCE(ms.signature, tx.method_id, '0x') AS method_decoded,
                tx.gas_limit,
                tx.tx_type_byte,
                tx.input_len,
                r.tx_result,
                ccc.rome_tx_type                  AS cross_chain_type,
                ccc.solana_legs                   AS solana_legs_json,
                -- CPI target: calldata primary for program id (deterministic,
                -- supersedes the cross_chain log-scraper's plumbing-filter blind
                -- spot); ccc primary for label/instruction (broader coverage today).
                COALESCE(tx.cpi_program_calldata, ccc.cpi_program)             AS cpi_program,
                COALESCE(ccc.cpi_program_label, tx.cpi_program_label_calldata) AS cpi_program_label,
                COALESCE(ccc.cpi_instruction, tx.cpi_instruction_calldata)     AS cpi_instruction,
                page.slot_number                  AS slot_number,
                page.tx_idx                       AS tx_idx,
                b.params_number                   AS block_number,
                b.params_block_timestamp::TEXT    AS block_ts,
                COALESCE(tx.origination, 'ecdsa') AS origination,
                tx.solana_signer,
                cl.display_label                  AS to_label,
                cl.display_label_detail           AS to_label_detail
         FROM page
         JOIN rome_via.evm_tx tx
             ON tx.tx_hash = page.tx_hash AND tx.chain_id = $1
         LEFT JOIN rome_via.evm_tx_result r
             ON r.tx_hash = page.tx_hash AND r.chain_id = $1
         LEFT JOIN rome_via.eth_block b
             ON b.slot_number = page.slot_number
             AND b.slot_block_idx = page.slot_block_idx
             AND b.chain_id = $1
         LEFT JOIN rome_via.method_signatures ms
             ON ms.selector = tx.method_id
         LEFT JOIN rome_via.cross_chain_correlations ccc
             ON ccc.tx_hash = page.tx_hash AND ccc.chain_id = $1
         LEFT JOIN rome_via.contract_labels cl
             ON cl.chain_id = $1 AND cl.address = tx.to_addr
         ORDER BY page.slot_number DESC, page.tx_idx DESC",
    )
    .bind(state.chain_id)
    .bind(&addr)
    .bind(limit + 1)
    .bind(last_slot)
    .bind(last_tx_idx)
    .fetch_all(&state.db)
    .await
    .map_err(|e| AppError::InternalError(anyhow::anyhow!(e.to_string())))?;

    let has_more = rows.len() as i64 > limit;
    // Keyset position of the LAST row we actually return (not the limit+1 probe row),
    // so the next page resumes exactly where this one ended.
    let last_keyset: Option<(i64, i32)> = rows
        .iter()
        .take(limit as usize)
        .last()
        .map(|r| (r.get::<i64, _>("slot_number"), r.get::<i32, _>("tx_idx")));
    let items: Vec<Tx> = rows
        .into_iter()
        .take(limit as usize)
        .map(|row: sqlx::postgres::PgRow| {
            let tx_result = row.get::<Option<serde_json::Value>, _>("tx_result");
            let status = derive_status(tx_result.as_ref());
            // Convert raw Unix timestamp (NUMERIC cast to TEXT) → ISO-8601 UTC.
            // Matches the /api/v1/txs response format per SPEC §Conformance.
            let ts: Option<String> = row.get::<Option<String>, _>("block_ts")
                .as_deref()
                .and_then(|s: &str| s.parse::<f64>().ok())
                .map(|epoch| {
                    chrono::DateTime::from_timestamp(epoch as i64, 0)
                        .unwrap_or_default()
                        .format("%Y-%m-%dT%H:%M:%SZ")
                        .to_string()
                });
            let (gas_used, gas_price, gas_recipient, priority_fee, base) =
                extract_gas_report(tx_result.as_ref());
            let tx_type = match row
                .get::<Option<String>, _>("cross_chain_type")
                .as_deref()
            {
                Some("Romulus") => TxType::Romulus,
                Some("Remus") => TxType::Remus,
                _ => TxType::Rhea,
            };
            let solana_legs_json = row.get::<Option<serde_json::Value>, _>("solana_legs_json");
            let solana_legs = parse_solana_legs(solana_legs_json.as_ref());
            let method = row
                .get::<Option<String>, _>("method_decoded")
                .unwrap_or_else(|| "0x".to_string());
            let value: String = row.get("value_wei");
            let to_addr: Option<String> = row.get("to_addr");
            let to_lower = to_addr.as_ref().map(|s| s.to_ascii_lowercase());
            let tx_type_byte: Option<i16> = row.get::<Option<i16>, _>("tx_type_byte");
            let input_len: Option<i32> = row.get::<Option<i32>, _>("input_len");
            let logs_json = tx_result.as_ref().and_then(|v| v.get("logs"));
            let logs_count = logs_json
                .and_then(|v| v.as_array())
                .map(|a| a.len() as i32);
            let action_tags = classify(&ClassifyInput {
                tx_type: tx_type.as_str(),
                method: &method,
                to: to_lower.as_deref(),
                value_wei: &value,
                tx_type_byte,
                solana_leg_count: solana_legs.len(),
                logs: logs_json,
                input_len,
            });
            Tx {
                hash: row.get("tx_hash"),
                status,
                tx_type,
                method,
                from: row
                    .get::<Option<String>, _>("from_addr")
                    .unwrap_or_default(),
                to: to_addr,
                value,
                gas_used,
                gas_price,
                gas_recipient,
                priority_fee,
                base,
                gas_limit: row.get::<Option<i64>, _>("gas_limit"),
                tx_type_byte,
                timestamp: ts,
                block_number: row.get("block_number"),
                hook_executions: vec![],
                logs: vec![],
                logs_count,
                action_tags,
                solana_legs,
                origination: row
                    .get::<Option<String>, _>("origination")
                    .unwrap_or_else(|| "ecdsa".into()),
                solana_signer: row.get::<Option<String>, _>("solana_signer"),
                to_label: row.get::<Option<String>, _>("to_label"),
                to_label_detail: row.get::<Option<String>, _>("to_label_detail"),
                // Detail-endpoint-only; the address tx list leaves this empty.
                transfers: vec![],
                // Settling Solana signature is detail-endpoint-only.
                solana_settlement_sig: None,
                cpi_program: row.get::<Option<String>, _>("cpi_program"),
                cpi_program_label: row.get::<Option<String>, _>("cpi_program_label"),
                cpi_instruction: row.get::<Option<String>, _>("cpi_instruction"),
                // This list query doesn't SELECT `nonce`; derivation is scoped
                // to rows_to_txs / get_tx_internal for now.
                contract_address: None,
                input_len: None,
                // Detail-endpoint-only, like `transfers`.
                revert_reason: None,
            }
        })
        .collect();

    let next_cursor = if has_more {
        last_keyset.and_then(|(slot, idx)| {
            crate::cursor::encode(
                &crate::cursor::TxCursor { chain_id: state.chain_id, last_slot: slot, last_tx_idx: idx },
                &state.cursor_secret,
            ).ok()
        })
    } else {
        None
    };

    Ok(Json(Page {
        items,
        next_cursor,
        has_more,
    }))
}

/// GET /api/v1/addresses — paginated list of addresses ordered by tx_count DESC.
///
/// Offset-based cursor so the client can page deeper into the list. Contract
/// vs EOA is inferred from the `is_contract` column on `address_stats`.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AddressSummary {
    pub address: String,
    pub tx_count: i64,
    pub is_contract: bool,
    /// True if this is a synthetic address controlled by a Solana account —
    /// it originates Solana-native txs and has NO private key, so it is neither
    /// a normal EOA nor a contract (keep `is_contract` independent). Derived
    /// directly from `evm_tx`: any origination != 'ecdsa' carrying a
    /// `solana_signer`. Mirrors the address-detail endpoint's derivation; the
    /// indexer-populated columns ARE the reverse map (no separate table).
    /// Path-agnostic — surfaces solana_unsigned today and solana_ed25519 later
    /// the moment the indexer tags it.
    pub controlled_by_solana: bool,
    /// The controlling Solana pubkey (base58), if controlledBySolana.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub solana_pubkey: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_seen: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_seen: Option<String>,
    /// Clean protocol/token label for this address, if it's a known contract.
    /// Same source + join shape as `get_address`'s `contract_labels` LEFT JOIN
    /// (`cl.chain_id = $1 AND cl.address = <lowercased address>`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label_detail: Option<String>,
}

#[utoipa::path(
    get,
    path = "/api/v1/addresses",
    params(
        ("cursor" = Option<i64>, Query, description = "Offset cursor from previous response"),
        ("limit" = Option<i64>, Query, description = "Items per page (default 50, max 100)"),
        ("kind" = Option<String>, Query, description = "Only addresses of this kind: contract | eoa | synthetic"),
        ("q" = Option<String>, Query, description = "Search term: matches address by prefix or contract label by substring (case-insensitive)"),
    ),
    responses(
        (status = 200, description = "Page of addresses", body = inline(Page<AddressSummary>)),
        (status = 400, description = "Malformed kind filter", body = crate::error::ProblemJson),
        (status = 500, description = "Internal error", body = crate::error::ProblemJson),
    )
)]
pub async fn list_addresses(
    State(state): State<AppState>,
    Query(params): Query<PaginationQuery>,
) -> Result<Json<Page<AddressSummary>>, AppError> {
    let limit = resolve_limit(params.limit)?;
    let offset = params.cursor.unwrap_or(0).max(0);
    // Optional kind filter — mirrors stats.rs::overview_compute's breakdown
    // of `active_addresses` into contracts / synthetics / eoas(residual).
    let kind = parse_kind_filter(params.kind.as_deref())?;
    // Optional search term: NULL means "no predicate" (identical SQL to
    // today); empty string is normalized to NULL too, since a user clearing
    // the search box should get the unfiltered list, not a zero-length
    // prefix/substring match. `%`/`_` are escaped so a literal percent sign
    // typed by a user can't act as a SQL LIKE wildcard.
    let q = params
        .q
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        // Clamp length so a multi-KB term can't make the ILIKE pattern
        // pathologically expensive (addresses are 42 chars, labels short —
        // 128 is ample). Clamp before escaping so the cap is on user input.
        .map(|s| escape_like(&s.chars().take(128).collect::<String>()));

    // `sol.solana_signer` derives the Solana-controlled / synthetic kind per
    // row, mirroring the address-detail endpoint's proven logic: a Solana-
    // originated tx carries its controlling signer in `solana_signer` and the
    // synthetic `from` (= keccak(signer)[12:]) in from_addr/from_address. The
    // LATERAL subquery picks one such signer (LIMIT 1) for any origination !=
    // 'ecdsa' whose synthetic `from` matches this address — no separate
    // reverse-map table. Path-agnostic: surfaces solana_unsigned today and
    // solana_ed25519 later automatically once the indexer tags it. Returns
    // NULL (→ controlledBySolana=false, solanaPubkey omitted) for normal EOAs
    // and contracts.
    let rows = sqlx::query(
        "SELECT a.address, a.tx_count, a.is_contract,
                sol.solana_signer AS solana_signer,
                to_char(a.first_seen AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS first_seen_str,
                to_char(a.last_seen  AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS last_seen_str,
                cl.display_label AS label,
                cl.display_label_detail AS label_detail
         FROM rome_via.address_stats a
         LEFT JOIN LATERAL (
             SELECT t.solana_signer
             FROM rome_via.evm_tx t
             WHERE t.chain_id = a.chain_id
               AND t.origination <> 'ecdsa'
               AND t.solana_signer IS NOT NULL
               AND lower(COALESCE(t.from_addr, t.from_address)) = a.address
             LIMIT 1
         ) sol ON TRUE
         -- Mirrors get_address's contract_labels join exactly (cl.chain_id = $1
         -- AND cl.address = <lowercased addr>) — a.address is already stored
         -- lowercase, so no cast is needed on this side.
         LEFT JOIN rome_via.contract_labels cl
             ON cl.chain_id = $1 AND cl.address = a.address
         WHERE a.chain_id = $1
           -- `sol.solana_signer IS NOT NULL` is exactly the LATERAL predicate
           -- above materialized per row — the same non-ecdsa/solana-controlled
           -- condition stats.rs's `synthetics` DISTINCT-count filters on, so
           -- kind=synthetic's row count matches that tile by construction.
           -- kind=contract mirrors stats.rs's `contracts` (is_contract=true)
           -- verbatim; kind=eoa is the same residual stats.rs derives
           -- (not contract AND not synthetic).
           AND (
                 $4::TEXT IS NULL
              OR ($4 = 'contract'  AND a.is_contract = true)
              OR ($4 = 'eoa'       AND a.is_contract = false AND sol.solana_signer IS NULL)
              OR ($4 = 'synthetic' AND sol.solana_signer IS NOT NULL)
           )
           -- Optional `q=` search: address PREFIX match (case-insensitive) OR
           -- label SUBSTRING match (case-insensitive). $5 is NULL when q is
           -- absent/empty, so this whole clause is a no-op — identical SQL
           -- to the unfiltered case. The user's term arrives pre-escaped
           -- (escape_like) so literal %/_ can't act as wildcards.
           AND (
                 $5::TEXT IS NULL
              OR a.address ILIKE $5 || '%' ESCAPE '\\'
              OR cl.display_label ILIKE '%' || $5 || '%' ESCAPE '\\'
           )
         ORDER BY a.tx_count DESC, a.address ASC
         LIMIT $2 OFFSET $3",
    )
    .bind(state.chain_id)
    .bind(limit + 1)
    .bind(offset)
    .bind(kind.as_deref())
    .bind(q.as_deref())
    .fetch_all(&state.db)
    .await
    .map_err(|e| AppError::InternalError(anyhow::anyhow!(e.to_string())))?;

    let has_more = rows.len() as i64 > limit;
    let items: Vec<AddressSummary> = rows
        .into_iter()
        .take(limit as usize)
        .map(|r| {
            let solana_pubkey = r
                .try_get::<Option<String>, _>("solana_signer")
                .ok()
                .flatten();
            AddressSummary {
                address: r.try_get("address").unwrap_or_default(),
                tx_count: r.try_get("tx_count").unwrap_or(0),
                is_contract: r.try_get("is_contract").unwrap_or(false),
                controlled_by_solana: solana_pubkey.is_some(),
                solana_pubkey,
                first_seen: r.try_get::<Option<String>, _>("first_seen_str").ok().flatten(),
                last_seen: r.try_get::<Option<String>, _>("last_seen_str").ok().flatten(),
                label: r.try_get::<Option<String>, _>("label").ok().flatten(),
                label_detail: r.try_get::<Option<String>, _>("label_detail").ok().flatten(),
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

/// Fetch ETH balance via eth_getBalance JSON-RPC call to the Proxy.
/// Returns "0" on any error (balance unknown, not an error in the API response).
async fn fetch_eth_balance(proxy_url: &str, address: &str) -> String {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .unwrap_or_default();

    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getBalance",
        "params": [address, "latest"]
    });

    let resp = client
        .post(proxy_url)
        .json(&body)
        .send()
        .await;

    match resp {
        Ok(r) => {
            match r.json::<serde_json::Value>().await {
                Ok(json) => {
                    // Result is a hex-encoded Wei value, e.g. "0x1bc16d674ec80000"
                    let hex = json
                        .get("result")
                        .and_then(|v| v.as_str())
                        .unwrap_or("0x0");
                    // Convert hex to decimal string.
                    let hex_clean = hex.trim_start_matches("0x");
                    u128::from_str_radix(hex_clean, 16)
                        .map(|n| n.to_string())
                        .unwrap_or_else(|_| "0".to_string())
                }
                Err(_) => "0".to_string(),
            }
        }
        Err(_) => "0".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_limit_defaults_when_absent() {
        assert_eq!(resolve_limit(None).unwrap(), DEFAULT_LIMIT);
    }

    #[test]
    fn resolve_limit_accepts_valid_values() {
        assert_eq!(resolve_limit(Some(1)).unwrap(), 1);
        assert_eq!(resolve_limit(Some(MAX_LIMIT)).unwrap(), MAX_LIMIT);
        assert_eq!(resolve_limit(Some(42)).unwrap(), 42);
    }

    #[test]
    fn resolve_limit_rejects_over_max() {
        let err = resolve_limit(Some(MAX_LIMIT + 1)).expect_err("should reject");
        match err {
            AppError::BadRequest(msg) => assert!(msg.contains("exceeds max")),
            other => panic!("expected BadRequest, got {other:?}"),
        }
    }

    #[test]
    fn kind_filter_absent_is_explicitly_unfiltered() {
        assert_eq!(parse_kind_filter(None).unwrap(), None);
    }

    #[test]
    fn kind_filter_accepts_every_literal_kind() {
        for k in ADDRESS_KINDS {
            assert_eq!(parse_kind_filter(Some(k)).unwrap(), Some(k.to_string()));
        }
    }

    // A typo'd kind (e.g. "Contract" capitalized, or "eoas" plural) must NOT
    // degrade to "unfiltered" — that would render the whole address list as
    // though every address matched the queried kind.
    #[test]
    fn kind_filter_unrecognized_is_rejected_not_ignored() {
        for bad in ["Contract", "eoas", "synthetics", "", " contract"] {
            let res = parse_kind_filter(Some(bad));
            assert!(
                res.is_err(),
                "expected {bad:?} to be rejected, got {:?}",
                res.ok()
            );
        }
    }

    #[test]
    fn kind_filter_rejection_is_a_bad_request_not_a_500() {
        match parse_kind_filter(Some("nope")) {
            Err(AppError::BadRequest(msg)) => assert!(
                msg.to_lowercase().contains("kind"),
                "message should name the offending param, got: {msg}"
            ),
            other => panic!("expected BadRequest, got {other:?}"),
        }
    }

    #[test]
    fn resolve_limit_rejects_non_positive() {
        assert!(matches!(
            resolve_limit(Some(0)),
            Err(AppError::BadRequest(_))
        ));
        assert!(matches!(
            resolve_limit(Some(-1)),
            Err(AppError::BadRequest(_))
        ));
    }

    /// A synthetic (Solana-controlled) address on the list surfaces
    /// `controlledBySolana: true` plus its `solanaPubkey`. Mirrors the
    /// detail-endpoint contract so a client never has to special-case the
    /// list shape vs the detail shape. camelCase per `serde(rename_all)`.
    #[test]
    fn address_summary_serializes_solana_controlled() {
        let s = AddressSummary {
            address: "0xabc0000000000000000000000000000000000000".into(),
            tx_count: 7,
            is_contract: false,
            controlled_by_solana: true,
            solana_pubkey: Some("So1anaPubKey11111111111111111111111111111111".into()),
            first_seen: None,
            last_seen: None,
            label: None,
            label_detail: None,
        };
        let v = serde_json::to_value(&s).expect("serialize");
        assert_eq!(v["controlledBySolana"], serde_json::json!(true));
        assert_eq!(
            v["solanaPubkey"],
            serde_json::json!("So1anaPubKey11111111111111111111111111111111")
        );
        // A synthetic is neither a normal EOA nor a contract — isContract stays
        // independent of controlledBySolana.
        assert_eq!(v["isContract"], serde_json::json!(false));
    }

    /// A normal externally-owned address (ecdsa origination) is NOT
    /// Solana-controlled: `controlledBySolana` is `false` and `solanaPubkey`
    /// is omitted entirely (skip_serializing_if on the None).
    #[test]
    fn address_summary_omits_solana_pubkey_for_normal_eoa() {
        let s = AddressSummary {
            address: "0xdef0000000000000000000000000000000000000".into(),
            tx_count: 3,
            is_contract: false,
            controlled_by_solana: false,
            solana_pubkey: None,
            first_seen: None,
            last_seen: None,
            label: None,
            label_detail: None,
        };
        let v = serde_json::to_value(&s).expect("serialize");
        assert_eq!(v["controlledBySolana"], serde_json::json!(false));
        assert!(
            v.get("solanaPubkey").is_none(),
            "solanaPubkey must be omitted for a non-synthetic address, got {v}"
        );
    }
}

#[cfg(test)]
mod address_paging_tests {
    /// Regression: the address feed must page by a KEYSET, not an offset.
    ///
    /// `cursor` was `Option<i64>` used directly as SQL OFFSET, with
    /// `next_cursor = offset + limit`. On a live chain that silently corrupts paging:
    /// a busy address takes new transactions continuously, so by the time the reader
    /// clicks "next", rows have been inserted ABOVE the offset — page 2 then shows
    /// transactions that were never on page 1 and skips others entirely. Measured on
    /// hadrian-lt: page 1, page 2 via cursor, and offset=5 returned three disjoint
    /// sets of hashes.
    ///
    /// A keyset cursor is immune: it says "everything strictly older than this exact
    /// position", so inserts at the head cannot shift the window. /txs already does
    /// this; the address feed must match.
    #[test]
    fn address_cursor_is_a_keyset_not_an_offset() {
        // The keyset predicate: strictly older than (slot, tx_idx).
        let older = |slot: i64, idx: i32, last_slot: i64, last_idx: i32| {
            slot < last_slot || (slot == last_slot && idx < last_idx)
        };
        // Same block, earlier position in it -> included.
        assert!(older(100, 4, 100, 5));
        // Same position -> excluded (no duplicate across the page boundary).
        assert!(!older(100, 5, 100, 5));
        // Newer rows arriving at the head are excluded no matter how many appear,
        // which is exactly what offset paging failed to do.
        assert!(!older(101, 0, 100, 5));
        assert!(!older(9_999, 0, 100, 5));
        // Older block -> included regardless of index.
        assert!(older(99, 999, 100, 5));
    }
}
