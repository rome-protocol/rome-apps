//! `asset_event` (capture §1.2 C1 / IMPL-PLAN §1.4): the many-to-many
//! junction that maps each captured `audit.chain_event` row to the asset(s)
//! whose resolved graph it belongs to. A shared-source event (router,
//! factory, storefront) maps into MANY assets' rows — this is exactly why
//! `chain_event` itself carries no `asset_id` column (C1).
//!
//! **REBUILDABLE, Tier-2-style, NOT Tier-1** (per the task): this table is a
//! pure function of `audit.chain_event` + the resolved graphs (persisted as
//! `capture_manifest`), never a hand-written fact. [`rebuild_asset_event`]
//! deletes and recomputes it, mirroring `tier2::rebuild::rebuild_tier2`'s
//! doctrine (one transaction, so no reader ever sees a half-rebuilt table).
//!
//! **Chain-scoped, never a global `TRUNCATE` (P3c M3).** `audit` lives in
//! the SHARED `rome_via_db` (every chain's audit worker writes the same
//! schema) — a `TRUNCATE audit.asset_event` from one chain's rebuild would
//! wipe every OTHER chain's rows too. The rebuild is a `DELETE ... WHERE
//! chain_id = $1`, scoped to exactly the chain being rebuilt.

use sqlx::PgConnection;
use sqlx::PgPool;

use super::graph::{ResolvedGraph, ScopeFilter};

/// One asset's rebuild input: its `asset_id`, the exact `manifest_hash` this
/// membership was resolved under (recorded on every row so a later report
/// can trace which manifest produced it — capture §1.2 point 4's
/// self-recording requirement), and its resolved graph.
#[derive(Debug, Clone)]
pub struct AssetManifest {
    pub asset_id: String,
    pub manifest_hash: [u8; 32],
    pub graph: ResolvedGraph,
}

/// Deletes THIS CHAIN's `audit.asset_event` rows and recomputes them for
/// every asset in `assets`, atomically, self-contained (opens and commits
/// its own transaction). Callers rebuilding a chain's WHOLE table must pass
/// every asset that should have rows afterward (same contract as
/// `tier2::rebuild::rebuild_tier2` — a partial `assets` list on a full
/// rebuild silently drops the assets left out, by design: this function
/// doesn't know about assets it wasn't told about).
pub async fn rebuild_asset_event(
    pool: &PgPool,
    chain_id: i64,
    assets: &[AssetManifest],
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    rebuild_asset_event_tx(&mut tx, chain_id, assets).await?;
    tx.commit().await?;
    Ok(())
}

/// The same rebuild, against an ALREADY-OPEN transaction — used by
/// `resolve::pass::run_resolve_pass` (P3c H2) so the manifest inserts, this
/// rebuild, and any gap-detection insert land as ONE atomic unit. Never
/// commits or rolls back itself; the caller owns the transaction's lifetime.
pub(crate) async fn rebuild_asset_event_tx(
    tx: &mut PgConnection,
    chain_id: i64,
    assets: &[AssetManifest],
) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM audit.asset_event WHERE chain_id = $1")
        .bind(chain_id)
        .execute(&mut *tx)
        .await?;

    for asset in assets {
        for interval in &asset.graph.intervals {
            match &interval.scope_filter {
                None => {
                    sqlx::query(
                        r#"
                        INSERT INTO audit.asset_event (chain_id, asset_id, event_id, manifest_hash)
                        SELECT $1, $2, event_id, $3
                        FROM audit.chain_event
                        WHERE chain_id = $1
                          AND source_contract = $4
                          AND block_number >= $5
                          AND ($6::BIGINT IS NULL OR block_number < $6)
                        ON CONFLICT DO NOTHING
                        "#,
                    )
                    .bind(chain_id)
                    .bind(&asset.asset_id)
                    .bind(asset.manifest_hash.as_slice())
                    .bind(interval.address.as_slice())
                    .bind(interval.from_block)
                    .bind(interval.to_block)
                    .execute(&mut *tx)
                    .await?;
                }
                // Yield-token leg (capture §1.2): only Transfer events where
                // `from` is this ArcToken — THE landmine-defusing predicate
                // (see graph.rs's `ScopeFilter` doc) — never the yield
                // token's unrelated chain-wide transfer history.
                Some(ScopeFilter::TransferFrom { from }) => {
                    sqlx::query(
                        r#"
                        INSERT INTO audit.asset_event (chain_id, asset_id, event_id, manifest_hash)
                        SELECT $1, $2, event_id, $3
                        FROM audit.chain_event
                        WHERE chain_id = $1
                          AND source_contract = $4
                          AND block_number >= $5
                          AND ($6::BIGINT IS NULL OR block_number < $6)
                          AND args ->> 'from' = $7
                        ON CONFLICT DO NOTHING
                        "#,
                    )
                    .bind(chain_id)
                    .bind(&asset.asset_id)
                    .bind(asset.manifest_hash.as_slice())
                    .bind(interval.address.as_slice())
                    .bind(interval.from_block)
                    .bind(interval.to_block)
                    .bind(format!("0x{}", hex::encode(from)))
                    .execute(&mut *tx)
                    .await?;
                }
                // Purchase-token leg (capture §1.2): only Transfer events
                // touching the storefront on either leg.
                Some(ScopeFilter::TransferTouches { party }) => {
                    sqlx::query(
                        r#"
                        INSERT INTO audit.asset_event (chain_id, asset_id, event_id, manifest_hash)
                        SELECT $1, $2, event_id, $3
                        FROM audit.chain_event
                        WHERE chain_id = $1
                          AND source_contract = $4
                          AND block_number >= $5
                          AND ($6::BIGINT IS NULL OR block_number < $6)
                          AND (args ->> 'from' = $7 OR args ->> 'to' = $7)
                        ON CONFLICT DO NOTHING
                        "#,
                    )
                    .bind(chain_id)
                    .bind(&asset.asset_id)
                    .bind(asset.manifest_hash.as_slice())
                    .bind(interval.address.as_slice())
                    .bind(interval.from_block)
                    .bind(interval.to_block)
                    .bind(format!("0x{}", hex::encode(party)))
                    .execute(&mut *tx)
                    .await?;
                }
                // Morpho market leg (capture §2.9): market-keyed events
                // scoped to this asset's matched market ids; market-less
                // governance events (no `id` arg at all) stay in scope
                // unconditionally — chain-global supporting facts, not
                // market-specific.
                Some(ScopeFilter::MorphoMarkets { market_ids }) => {
                    let ids: Vec<String> = market_ids
                        .iter()
                        .map(|id| format!("0x{}", hex::encode(id)))
                        .collect();
                    sqlx::query(
                        r#"
                        INSERT INTO audit.asset_event (chain_id, asset_id, event_id, manifest_hash)
                        SELECT $1, $2, event_id, $3
                        FROM audit.chain_event
                        WHERE chain_id = $1
                          AND source_contract = $4
                          AND block_number >= $5
                          AND ($6::BIGINT IS NULL OR block_number < $6)
                          AND (args ->> 'id' = ANY($7::text[]) OR NOT (args ? 'id'))
                        ON CONFLICT DO NOTHING
                        "#,
                    )
                    .bind(chain_id)
                    .bind(&asset.asset_id)
                    .bind(asset.manifest_hash.as_slice())
                    .bind(interval.address.as_slice())
                    .bind(interval.from_block)
                    .bind(interval.to_block)
                    .bind(&ids)
                    .execute(&mut *tx)
                    .await?;
                }
            }
        }
    }

    Ok(())
}
