use axum::{extract::State, response::IntoResponse, Json};
use sqlx::Row;

use crate::{
    api::models::{AddressTypeCounts, StatsOverview, TokenKindCounts},
    error::AppError,
    state::AppState,
};

/// EOA count = active addresses − contracts − synthetics, clamped to ≥ 0.
///
/// The three sub-counts come from independent queries (`address_stats` total,
/// `address_stats.is_contract`, DISTINCT Solana-controlled `from` addresses),
/// so the subtraction can over-shoot on tiny / racing datasets (e.g. a synthetic
/// that has not yet landed in `address_stats`, or classifier lag). Clamp at 0 so
/// the Addresses header never shows a negative "EOAs" tile. Pure — unit-tested.
fn eoa_count(active_addresses: i64, contracts: i64, synthetics: i64) -> i64 {
    (active_addresses - contracts - synthetics).max(0)
}

/// GET /api/v1/stats/overview — chain-level statistics for the explorer home page.
#[utoipa::path(
    get,
    path = "/api/v1/stats/overview",
    responses(
        (status = 200, description = "Chain statistics", body = StatsOverview),
        (status = 500, description = "Internal error", body = crate::error::ProblemJson)
    ),
    tag = "stats"
)]
pub async fn overview(State(state): State<AppState>) -> Result<impl IntoResponse, AppError> {
    let chain_id = state.chain_id;

    // Home-page tile: memoize the whole aggregate set for a few seconds. Every
    // refresh + SSE invalidation hits this; the underlying queries are chain-wide
    // COUNTs that grow with data, so without the cache they pile up on the DB pool
    // under load. 5s staleness is imperceptible for these estimate tiles.
    let db = state.db.clone();
    let mut body = crate::cache::cached_json(
        &state,
        &format!("rome_via:stats_overview:{chain_id}"),
        5,
        move || async move { overview_compute(&db, chain_id).await },
    )
    .await?;

    // Overlay the LIVE head fresh, outside the cache.
    //
    // The TTL above exists for the chain-wide COUNT(*) aggregates below, which are
    // genuinely expensive and barely move. The head fields are not: totalTxs is a
    // maintained counter (one buffer) and the rest are indexed lookups. Bundling
    // them under one TTL made the headline counter change at most once every 5s —
    // it STEPPED rather than ticked, which no amount of client-side animation can
    // undo, because the value simply is not there to animate towards.
    //
    // Degrades to the cached values on any error: a stale head beats a 500.
    if let Ok(head) = live_head(&state.db, chain_id).await {
        if let (Some(obj), Some(h)) = (body.as_object_mut(), head.as_object()) {
            for (k, v) in h {
                obj.insert(k.clone(), v.clone());
            }
        }
    }
    Ok(Json(body))
}

/// The cheap, always-fresh part of the overview: chain head plus the maintained
/// transaction counter. Deliberately a separate query so it can be served outside
/// the aggregate cache — see `overview`.
async fn live_head(db: &sqlx::PgPool, chain_id: i64) -> Result<serde_json::Value, AppError> {
    let row = sqlx::query(
        r#"
        SELECT
            MAX(eb.params_number) AS latest_block,
            MAX(eb.slot_number)   AS latest_slot,
            (SELECT sc.source_max_slot FROM rome_via.sync_cursors sc
              WHERE sc.chain_id = $1 AND sc.table_name = 'eth_block') AS source_max_slot,
            (SELECT COALESCE(
                 (SELECT cc.total_txs FROM rome_via.chain_counters cc
                   WHERE cc.chain_id = $1), 0)::BIGINT) AS total_txs
        FROM rome_via.eth_block eb
        WHERE eb.chain_id = $1
        "#,
    )
    .bind(chain_id)
    .fetch_one(db)
    .await?;

    let total_txs: i64 = row.try_get::<Option<i64>, _>("total_txs")?.unwrap_or(0);
    Ok(serde_json::json!({
        "latestBlockNumber": row.try_get::<Option<i64>, _>("latest_block")?.unwrap_or(0),
        "latestSlot": row.try_get::<Option<i64>, _>("latest_slot")?.unwrap_or(0),
        "sourceMaxSlot": row.try_get::<Option<i64>, _>("source_max_slot")?,
        "totalTxs": total_txs,
        "txCountTotal": total_txs,
    }))
}

/// Computes the full stats/overview payload as a JSON value (cache-miss path).
///
/// `pub` (not private) so integration tests can call the exact production
/// aggregate query directly — e.g. to assert a `kind=` list filter's row count
/// equals the header tile it corresponds to — without re-typing its SQL as a
/// second copy that could silently drift from this one.
pub async fn overview_compute(db: &sqlx::PgPool, chain_id: i64) -> Result<serde_json::Value, AppError> {
    // Main stats: latest block number, latest slot, total txs.
    let row = sqlx::query(
        r#"
        SELECT
            MAX(eb.params_number)   AS latest_block,
            MAX(eb.slot_number)     AS latest_slot,
            -- Recorded once per cycle by rome-via-sync (see sync::record_source_max).
            -- NULL until it has run at least once; the UI renders nothing for NULL.
            (SELECT sc.source_max_slot FROM rome_via.sync_cursors sc
              WHERE sc.chain_id = $1 AND sc.table_name = 'eth_block') AS source_max_slot,
            -- Maintained by rome-via-sync in the same statement as the evm_tx
            -- insert (see sync::evm_tx_insert_sql). Previously COUNT(*) over
            -- evm_tx — 33M rows and growing ~1.5M/hour — on every cache miss, so
            -- the headline number cost more as the chain aged and, behind a short
            -- TTL, STEPPED rather than ticked. Falls back to 0 only before the
            -- seeding migration has run.
            (SELECT COALESCE(
                 (SELECT cc.total_txs FROM rome_via.chain_counters cc
                   WHERE cc.chain_id = $1), 0)::BIGINT) AS total_txs
        FROM rome_via.eth_block eb
        WHERE eb.chain_id = $1
        "#,
    )
    .bind(chain_id)
    .fetch_one(db)
    .await?;

    // TPS estimate: count txs in last 60 seconds.
    let tps_row = sqlx::query(
        r#"
        SELECT COUNT(et.tx_hash)::BIGINT AS recent_txs
        FROM rome_via.evm_tx et
        JOIN rome_via.eth_block_txs ebt ON ebt.tx_hash = et.tx_hash AND ebt.chain_id = et.chain_id
        JOIN rome_via.eth_block eb ON eb.slot_number = ebt.slot_number
            AND eb.slot_block_idx = ebt.slot_block_idx
            AND eb.chain_id = ebt.chain_id
        WHERE et.chain_id = $1
          AND eb.params_block_timestamp > (EXTRACT(EPOCH FROM NOW()) - 60)::NUMERIC
        "#,
    )
    .bind(chain_id)
    .fetch_one(db)
    .await?;

    // Chain-wide aggregates for the Home dashboard + Tokens header. Cheap
    // COUNTs scoped to chain_id; each returns 0 on an empty chain. `address_stats`
    // (one row per address with activity) → active addresses; `token_metadata`
    // (one row per token) → token count; `cross_chain_correlations` (one row per
    // tx with its rome_tx_type) → per-kind tx counts via FILTER. Combined into a
    // single round-trip so we don't fan out three more queries.
    // `token_kind_*` FILTER counts over `token_metadata.kind` (nullable — NULL /
    // unclassified counts toward none) feed the Tokens-header breakdown.
    // `contracts` over `address_stats.is_contract` feeds the Addresses-header
    // breakdown. `synthetics` is computed separately below (DISTINCT on evm_tx).
    let agg_row = sqlx::query(
        r#"
        SELECT
            (SELECT COUNT(*)::BIGINT FROM rome_via.address_stats   a  WHERE a.chain_id  = $1) AS active_addresses,
            (SELECT COUNT(*)::BIGINT FROM rome_via.token_metadata  tm WHERE tm.chain_id = $1) AS token_count_total,
            (SELECT COUNT(*) FILTER (WHERE tm.kind = 'ERC-20')::BIGINT     FROM rome_via.token_metadata tm WHERE tm.chain_id = $1) AS token_erc20,
            (SELECT COUNT(*) FILTER (WHERE tm.kind = 'SPL')::BIGINT        FROM rome_via.token_metadata tm WHERE tm.chain_id = $1) AS token_spl,
            (SELECT COUNT(*) FILTER (WHERE tm.kind = 'Token-2022')::BIGINT FROM rome_via.token_metadata tm WHERE tm.chain_id = $1) AS token_token2022,
            (SELECT COUNT(*)::BIGINT FROM rome_via.address_stats a WHERE a.chain_id = $1 AND a.is_contract = true) AS contracts
        "#,
    )
    .bind(chain_id)
    .fetch_one(db)
    .await?;

    // Synthetics = DISTINCT addresses that are Solana-controlled. Mirrors the
    // per-row `controlled_by_solana` derivation in addresses.rs: an address is
    // synthetic if it is the `from` of any non-ecdsa tx that carries a
    // `solana_signer`. The proven query there COALESCEs both possible column
    // spellings (`from_addr` / `from_address`); mirror it exactly so this stays
    // robust to the Hercules-mirror column naming.
    let synthetics: i64 = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(DISTINCT COALESCE(from_addr, from_address))::BIGINT
        FROM rome_via.evm_tx
        WHERE chain_id = $1
          AND origination <> 'ecdsa'
          AND solana_signer IS NOT NULL
          AND COALESCE(from_addr, from_address) IS NOT NULL
        "#,
    )
    .bind(chain_id)
    .fetch_one(db)
    .await?;

    let latest_block_number: i64 = row.try_get::<Option<i64>, _>("latest_block")?.unwrap_or(0);
    let latest_slot: i64 = row.try_get::<Option<i64>, _>("latest_slot")?.unwrap_or(0);
    // Deliberately NOT defaulted to 0 or to latest_slot. Either would assert a lag
    // the sync has not actually reported — 0 reads as "the chain is empty" and
    // latest_slot reads as "perfectly caught up". Absent means unknown.
    let source_max_slot: Option<i64> = row.try_get::<Option<i64>, _>("source_max_slot")?;
    let total_txs: i64 = row.try_get::<Option<i64>, _>("total_txs")?.unwrap_or(0);
    let recent_txs: i64 = tps_row.try_get::<Option<i64>, _>("recent_txs")?.unwrap_or(0);
    let tps_60s_estimate = recent_txs as f64 / 60.0;

    let active_addresses: i64 = agg_row.try_get::<Option<i64>, _>("active_addresses")?.unwrap_or(0);
    let token_count_total: i64 = agg_row.try_get::<Option<i64>, _>("token_count_total")?.unwrap_or(0);

    let token_kind_counts = TokenKindCounts {
        erc20: agg_row.try_get::<Option<i64>, _>("token_erc20")?.unwrap_or(0),
        spl: agg_row.try_get::<Option<i64>, _>("token_spl")?.unwrap_or(0),
        token2022: agg_row
            .try_get::<Option<i64>, _>("token_token2022")?
            .unwrap_or(0),
    };

    let contracts: i64 = agg_row.try_get::<Option<i64>, _>("contracts")?.unwrap_or(0);
    let address_type_counts = AddressTypeCounts {
        contracts,
        // synthetic > contract > EOA priority is enforced by deriving EOAs as the
        // residual (active − contracts − synthetics), clamped ≥ 0.
        eoas: eoa_count(active_addresses, contracts, synthetics),
        synthetics,
    };

    let overview = StatsOverview {
        latest_block_number,
        latest_slot,
        source_max_slot,
        total_txs,
        tps_60s_estimate,
        // Chain-wide COUNT(*) over evm_tx — identical to the value computed for
        // `total_txs` above. Exposed under an explicit name the UI can rely on.
        tx_count_total: total_txs,
        active_addresses,
        token_count_total,
        token_kind_counts,
        address_type_counts,
    };
    serde_json::to_value(overview).map_err(|e| AppError::Internal(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The live-head overlay writes into the cached payload BY KEY. If a key here
    /// stops matching the serialized model — a field rename, a serde attribute
    /// change — the overlay silently inserts a phantom key and the STALE cached
    /// value survives untouched. Nothing errors; the counter just quietly goes back
    /// to stepping every 5s. Pin the names.
    #[test]
    fn live_head_keys_exist_on_the_serialized_overview() {
        use crate::api::models::{AddressTypeCounts, StatsOverview, TokenKindCounts, TypeCounts};
        let model = StatsOverview {
            latest_block_number: 1,
            latest_slot: 2,
            source_max_slot: Some(3),
            total_txs: 4,
            tps_60s_estimate: 0.0,
            tx_count_total: 4,
            active_addresses: 0,
            token_count_total: 0,
            token_kind_counts: TokenKindCounts::default(),
            address_type_counts: AddressTypeCounts::default(),
        };
        let v = serde_json::to_value(&model).expect("serializes");
        let obj = v.as_object().expect("object");
        for k in ["latestBlockNumber", "latestSlot", "sourceMaxSlot", "totalTxs", "txCountTotal"] {
            assert!(
                obj.contains_key(k),
                "live_head writes `{k}`, which no longer exists on StatsOverview — the \
                 overlay would insert a phantom key and leave the cached value stale"
            );
        }
    }

    // ── eoa_count: active − contracts − synthetics, clamped ≥ 0 ──────────────
    #[test]
    fn eoa_count_normal_subtraction() {
        // 37 active − 3 contracts − 4 synthetics = 30 EOAs.
        assert_eq!(eoa_count(37, 3, 4), 30);
    }

    #[test]
    fn eoa_count_clamps_over_subtraction_to_zero() {
        // Over-subtraction (counts from independent queries can race) must never
        // yield a negative tile.
        assert_eq!(eoa_count(5, 4, 4), 0);
        assert_eq!(eoa_count(0, 0, 0), 0);
        assert_eq!(eoa_count(0, 1, 1), 0);
    }

    #[test]
    fn eoa_count_all_eoas() {
        // No contracts, no synthetics → every active address is an EOA.
        assert_eq!(eoa_count(42, 0, 0), 42);
    }
}
