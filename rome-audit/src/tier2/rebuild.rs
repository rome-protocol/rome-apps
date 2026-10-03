//! `rebuild_tier2` — the §13.1 rebuild-determinism entry point: delete
//! (chain-scoped, P4a M3 — see below) every Tier-2 row for this chain, then
//! recompute every row purely from `audit.chain_event` (read via
//! [`super::fetch::fetch_events`], which orders by `(block_number,
//! tx_index, log_index)` — never `event_id` insertion order). Calling this
//! twice against the same `chain_event` content — from a completely fresh
//! set of Tier-2 tables, regardless of what order `chain_event` itself was
//! populated in — must always leave every Tier-2 table byte-identical.
//!
//! **Chain-scoped, never a global `TRUNCATE` (P4a M3).** `audit` lives in
//! the SHARED `rome_via_db` (every chain's audit worker writes the same
//! schema) — a bare `TRUNCATE` from one chain's rebuild would wipe every
//! OTHER chain's Tier-2 rows too. The original implementation truncated
//! unconditionally (fine for a single-chain deployment, wrong for the real
//! shared-DB topology); this is now `DELETE ... WHERE chain_id = $1` per
//! table, same fix `resolve::asset_event::rebuild_asset_event_tx` already
//! had for its own M3.
//!
//! **Wrapped in ONE transaction (H3, P2 review).** The whole
//! delete + recompute runs inside a single `sqlx::Transaction`, committed
//! once at the end — not the crate's usual "no explicit transaction,
//! `ON CONFLICT DO NOTHING` makes a retry idempotent" style
//! (`ingest::pipeline`'s justification, which does NOT apply here: a
//! chain-scoped delete is destructive, not append-only-idempotent).
//! Without a transaction, a
//! crash mid-rebuild — or any concurrent reader, and P3's projections WILL
//! read these tables — could observe a half-truncated, half-rebuilt table
//! that LOOKS complete (e.g. an asset's `gate_interval` truncated but not
//! yet re-populated reads as "never gated", a false negative on an
//! audit-critical fact). One transaction makes every reader see either the
//! fully-old state or the fully-new state, never anything in between.
//! Matches this workspace's existing multi-statement-transaction idiom
//! (e.g. `rome-via-enrich`'s `throughput_record::write_record`:
//! `pool.begin()` → `&mut *tx` per query → `tx.commit()`).

use std::collections::{BTreeSet, HashMap};

use sqlx::PgPool;

use super::allowlist::{build_allowlist_intervals, AllowlistIntervalRow};
use super::code_change::{
    build_module_linked_changes, build_module_set_changes, build_router_global_changes,
    build_storefront_upgrade_changes, build_upgrade_changes, ChainedChange, CodeChangeRow,
};
use super::denyset::{build_denyset_intervals, DenysetIntervalRow};
use super::exposure::{build_exposure_windows, ExposureWindowRow};
use super::fetch::{fetch_events, merge_events_by_position, ChainEventRow, Tier2Config};
use super::gate::{build_gate_intervals, GateIntervalRow};
use super::holder_balance::{build_holder_balances, HolderBalanceRow};
use super::role_interval::{build_role_intervals, RoleIntervalRow};
use super::router_epoch::{build_router_epochs, RouterEpochRow};
use super::sale::{build_sales, SaleRow};
use super::screening::{build_transfer_screening_and_gaps, ScreeningGapRow, TransferScreeningRow};
use super::yield_blacklist::{build_yield_blacklist_intervals, YieldBlacklistIntervalRow};
use super::yield_run::{build_yield_runs, YieldCreditRow, YieldRunRow};

/// Deletes (chain-scoped, P4a M3) and rebuilds every Tier-2 table for `config.chain_id` from
/// `audit.chain_event`, atomically. `config.assets` drives
/// `allowlist_interval` / `gate_interval` / `exposure_window` (per-asset);
/// `config.sanctions_modules` / `config.routers` drive
/// `sanction_denyset_interval` / `router_sanctions_epoch` (chain-global,
/// not per-asset).
pub async fn rebuild_tier2(pool: &PgPool, config: &Tier2Config) -> Result<(), sqlx::Error> {
    validate_config(config)?;

    let mut tx = pool.begin().await?;

    // Chain-SCOPED delete (P4a M3 — matches the `asset_event` M3 fix), NOT
    // a global TRUNCATE: `audit` lives in the SHARED `rome_via_db` (every
    // chain's audit worker writes the same schema) — a bare TRUNCATE from
    // one chain's rebuild would wipe every OTHER chain's Tier-2 rows too.
    // Order (child-before-parent FK) is preserved from the original
    // TRUNCATE sequence — exposure_window/gate/allowlist all FK into
    // audit.chain_event only, not into each other, so any order among the
    // five is actually fine; this order is just readable.
    sqlx::query("DELETE FROM audit.exposure_window WHERE chain_id = $1")
        .bind(config.chain_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM audit.gate_interval WHERE chain_id = $1")
        .bind(config.chain_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM audit.allowlist_interval WHERE chain_id = $1")
        .bind(config.chain_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM audit.sanction_denyset_interval WHERE chain_id = $1")
        .bind(config.chain_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM audit.router_sanctions_epoch WHERE chain_id = $1")
        .bind(config.chain_id)
        .execute(&mut *tx)
        .await?;
    // P4b-i additions — same chain-scoped discipline, never TRUNCATE.
    sqlx::query("DELETE FROM audit.yield_credit WHERE chain_id = $1")
        .bind(config.chain_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM audit.yield_run WHERE chain_id = $1")
        .bind(config.chain_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM audit.screening_gap WHERE chain_id = $1")
        .bind(config.chain_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM audit.transfer_screening WHERE chain_id = $1")
        .bind(config.chain_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM audit.yield_blacklist_interval WHERE chain_id = $1")
        .bind(config.chain_id)
        .execute(&mut *tx)
        .await?;
    // P4b-ii additions — same chain-scoped discipline, never TRUNCATE.
    sqlx::query("DELETE FROM audit.holder_balance WHERE chain_id = $1")
        .bind(config.chain_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM audit.sale WHERE chain_id = $1")
        .bind(config.chain_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM audit.code_change WHERE chain_id = $1")
        .bind(config.chain_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM audit.role_interval WHERE chain_id = $1")
        .bind(config.chain_id)
        .execute(&mut *tx)
        .await?;

    // ---- Chain-global sources FIRST (P4b-i): both the sanctions denyset
    // loop and the router loop below produce data the per-asset loop needs
    // (screening_events / router_epochs_by_router) — fetched+built once,
    // reused by every asset, never per-asset-refetched.
    let mut screening_events: Vec<([u8; 20], ChainEventRow)> = Vec::new();

    for module in &config.sanctions_modules {
        let events = fetch_events(
            &mut tx,
            config.chain_id,
            module,
            &["Sanctioned", "Unsanctioned"],
        )
        .await?;
        let rows = build_denyset_intervals(&events);
        for row in &rows {
            insert_denyset_row(&mut tx, config.chain_id, module, row).await?;
        }

        let screened = fetch_events(&mut tx, config.chain_id, module, &["TransferScreened"]).await?;
        screening_events.extend(screened.into_iter().map(|ev| (*module, ev)));
    }

    let mut router_epochs_by_router: HashMap<[u8; 20], Vec<RouterEpochRow>> = HashMap::new();
    // P4b-ii: ROUTER_GLOBAL fans out to every asset on a router — built
    // ONCE per router, reusing the SAME `events` fetch as `router_epochs`
    // above (never a second query for the same rows). Unlike
    // `router_epochs` (filtered to `GLOBAL_SANCTIONS_TYPE`), this is every
    // typeId the router has ever registered.
    let mut router_global_by_router: HashMap<[u8; 20], Vec<ChainedChange>> = HashMap::new();

    for router in &config.routers {
        let events = fetch_events(
            &mut tx,
            config.chain_id,
            router,
            &[
                "ModuleTypeRegistered",
                "ModuleTypeRemoved",
                "GlobalImplementationUpdated",
            ],
        )
        .await?;
        let rows = build_router_epochs(&events);
        for row in &rows {
            insert_router_epoch_row(&mut tx, config.chain_id, router, row).await?;
        }
        router_epochs_by_router.insert(*router, rows);
        router_global_by_router.insert(*router, build_router_global_changes(&events));
    }

    // ---- P4b-ii: role_interval — built ONCE per DISTINCT role-bearing
    // source across every asset (module doc on `role_interval` — the PK is
    // the tripwire against a double-build for a source two assets share).
    let mut role_source_union: BTreeSet<[u8; 20]> = BTreeSet::new();
    for asset in &config.assets {
        role_source_union.insert(asset.token_address);
        role_source_union.insert(asset.axis1_module);
        role_source_union.extend(asset.yield_blacklist_modules.iter().copied());
        if let Some(s) = asset.storefront {
            role_source_union.insert(s);
        }
        if let Some(f) = asset.factory {
            role_source_union.insert(f);
        }
    }
    role_source_union.extend(config.routers.iter().copied());

    let mut role_intervals_by_source: HashMap<[u8; 20], Vec<RoleIntervalRow>> = HashMap::new();
    for source in &role_source_union {
        let events = fetch_events(&mut tx, config.chain_id, source, &["RoleGranted", "RoleRevoked"]).await?;
        let rows = build_role_intervals(&events);
        for row in &rows {
            insert_role_interval_row(&mut tx, config.chain_id, source, row).await?;
        }
        role_intervals_by_source.insert(*source, rows);
    }

    // ---- P4b-ii: per-distinct-factory event caches (UPGRADE-via-factory +
    // MODULE_LINKED) — fetched ONCE per distinct factory address, reused by
    // every asset that shares it.
    let mut factory_addrs: BTreeSet<[u8; 20]> = BTreeSet::new();
    for asset in &config.assets {
        if let Some(f) = asset.factory {
            factory_addrs.insert(f);
        }
    }
    let mut factory_token_upgraded: HashMap<[u8; 20], Vec<ChainEventRow>> = HashMap::new();
    let mut factory_module_linked: HashMap<[u8; 20], Vec<ChainEventRow>> = HashMap::new();
    for factory in &factory_addrs {
        factory_token_upgraded.insert(
            *factory,
            fetch_events(&mut tx, config.chain_id, factory, &["TokenUpgraded"]).await?,
        );
        factory_module_linked.insert(
            *factory,
            fetch_events(&mut tx, config.chain_id, factory, &["ModuleLinked"]).await?,
        );
    }

    // ---- P4b-ii: per-distinct-storefront event caches (STOREFRONT_UPGRADE
    // fan-out chain + `sale`'s PurchaseMade anchor) — fetched ONCE per
    // distinct storefront address.
    let mut storefront_addrs: BTreeSet<[u8; 20]> = BTreeSet::new();
    for asset in &config.assets {
        if let Some(s) = asset.storefront {
            storefront_addrs.insert(s);
        }
    }
    let mut storefront_upgrade_by_storefront: HashMap<[u8; 20], Vec<ChainedChange>> = HashMap::new();
    let mut purchases_by_storefront: HashMap<[u8; 20], Vec<ChainEventRow>> = HashMap::new();
    for storefront in &storefront_addrs {
        let upgraded = fetch_events(&mut tx, config.chain_id, storefront, &["Upgraded"]).await?;
        storefront_upgrade_by_storefront
            .insert(*storefront, build_storefront_upgrade_changes(&upgraded, *storefront));
        purchases_by_storefront.insert(
            *storefront,
            fetch_events(&mut tx, config.chain_id, storefront, &["PurchaseMade"]).await?,
        );
    }

    // ---- P4b-ii: per-distinct-purchase-token Transfer cache — `sale`'s
    // payment leg. Fetched ONCE per distinct address across the WHOLE
    // config (never per-asset) so two assets sharing one purchase token
    // (test 29's shared-pool scenario) draw from the SAME event list,
    // never a duplicated one.
    let mut purchase_token_addrs: BTreeSet<[u8; 20]> = BTreeSet::new();
    for asset in &config.assets {
        purchase_token_addrs.extend(asset.purchase_tokens.iter().copied());
    }
    let mut purchase_token_transfers: HashMap<[u8; 20], Vec<ChainEventRow>> = HashMap::new();
    for token in &purchase_token_addrs {
        purchase_token_transfers.insert(
            *token,
            fetch_events(&mut tx, config.chain_id, token, &["Transfer"]).await?,
        );
    }
    // storefront -> deduped set of purchase-token addresses in scope for it
    // (accumulated during the asset loop below, consumed after it).
    let mut purchase_tokens_by_storefront: HashMap<[u8; 20], BTreeSet<[u8; 20]>> = HashMap::new();
    let mut sale_assets_by_storefront: HashMap<[u8; 20], Vec<(String, [u8; 20])>> = HashMap::new();
    let mut sale_token_legs_by_storefront: HashMap<[u8; 20], Vec<(String, ChainEventRow)>> =
        HashMap::new();

    // HIGH-1 (P4b-i review): screening pairing is ONE chain-global
    // walk, never per-asset — collected here across the loop, consumed
    // ONCE after it (see `screening` module doc for why a per-asset walk
    // corrupts evidence: a screening consumed by asset B's transfer must be
    // REMOVED from the shared candidate pool before asset A's transfer in
    // the same tx is ever considered).
    let mut all_asset_transfers: Vec<(String, ChainEventRow)> = Vec::new();
    let mut router_epochs_by_asset: HashMap<String, Vec<RouterEpochRow>> = HashMap::new();

    for asset in &config.assets {
        let allowlist_events = fetch_events(
            &mut tx,
            config.chain_id,
            &asset.axis1_module,
            &["WhitelistStatusChanged"],
        )
        .await?;
        let allowlist_rows = build_allowlist_intervals(&allowlist_events);
        for row in &allowlist_rows {
            insert_allowlist_row(&mut tx, config.chain_id, &asset.asset_id, row).await?;
        }

        let gate_events = fetch_events(
            &mut tx,
            config.chain_id,
            &asset.axis1_module,
            &["TransfersRestrictionToggled"],
        )
        .await?;
        let gate_rows = build_gate_intervals(&gate_events);
        for row in &gate_rows {
            insert_gate_row(&mut tx, config.chain_id, &asset.asset_id, row).await?;
        }

        let transfer_events = fetch_events(
            &mut tx,
            config.chain_id,
            &asset.token_address,
            &["Transfer"],
        )
        .await?;
        let exposure_rows = build_exposure_windows(&gate_rows, &transfer_events, &allowlist_rows);
        for row in &exposure_rows {
            insert_exposure_row(&mut tx, config.chain_id, &asset.asset_id, row).await?;
        }

        // ---- P4b-i: yield_blacklist_interval — merge every configured
        // module's history by position (an asset can swap modules).
        let mut yield_blacklist_sources = Vec::new();
        for module in &asset.yield_blacklist_modules {
            yield_blacklist_sources.push(
                fetch_events(&mut tx, config.chain_id, module, &["YieldBlacklistUpdated"]).await?,
            );
        }
        let yield_blacklist_events = merge_events_by_position(yield_blacklist_sources);
        let yield_blacklist_rows = build_yield_blacklist_intervals(&yield_blacklist_events);
        for row in &yield_blacklist_rows {
            insert_yield_blacklist_row(&mut tx, config.chain_id, &asset.asset_id, row).await?;
        }

        // HIGH-1: collect this asset's Transfers + its own router epochs for
        // the deferred, chain-global screening walk below — never call the
        // screening builder per-asset.
        for ev in &transfer_events {
            all_asset_transfers.push((asset.asset_id.clone(), ev.clone()));
        }
        let this_asset_epochs = asset
            .router_address
            .and_then(|r| router_epochs_by_router.get(&r))
            .cloned()
            .unwrap_or_default();
        router_epochs_by_asset.insert(asset.asset_id.clone(), this_asset_epochs);

        // ---- P4b-i: yield_run + yield_credit — one series per yield token
        // this asset has ever pointed at. `distributed_events` is the same
        // query for every yield_token this asset has (fetched once, not
        // per-token).
        if !asset.yield_tokens.is_empty() {
            let distributed_events = fetch_events(
                &mut tx,
                config.chain_id,
                &asset.token_address,
                &["YieldDistributed"],
            )
            .await?;
            for yield_token in &asset.yield_tokens {
                let yield_transfer_events =
                    fetch_events(&mut tx, config.chain_id, yield_token, &["Transfer"]).await?;
                let (run_rows, credit_rows) = build_yield_runs(
                    &yield_transfer_events,
                    &distributed_events,
                    *yield_token,
                    asset.token_address,
                );
                for row in &run_rows {
                    insert_yield_run_row(&mut tx, config.chain_id, &asset.asset_id, row).await?;
                }
                for row in &credit_rows {
                    insert_yield_credit_row(&mut tx, config.chain_id, &asset.asset_id, row).await?;
                }
            }
        }

        // ---- P4b-ii: code_change — this asset's own role sources
        // (multisig is a separate, chain-global config field, checked
        // FIRST in `signer_attribution` regardless).
        let mut role_sources: Vec<[u8; 20]> = vec![asset.token_address, asset.axis1_module];
        role_sources.extend(asset.yield_blacklist_modules.iter().copied());
        if let Some(s) = asset.storefront {
            role_sources.push(s);
        }
        if let Some(f) = asset.factory {
            role_sources.push(f);
        }
        if let Some(r) = asset.router_address {
            role_sources.push(r);
        }

        let mut code_change_rows: Vec<CodeChangeRow> = Vec::new();

        let own_upgraded = fetch_events(&mut tx, config.chain_id, &asset.token_address, &["Upgraded"]).await?;
        let factory_upgraded = asset
            .factory
            .and_then(|f| factory_token_upgraded.get(&f))
            .cloned()
            .unwrap_or_default();
        for c in build_upgrade_changes(&own_upgraded, &factory_upgraded, asset.token_address) {
            code_change_rows.push(c.attribute(&role_sources, &role_intervals_by_source, config.multisig));
        }

        let module_set_events = fetch_events(
            &mut tx,
            config.chain_id,
            &asset.token_address,
            &["SpecificRestrictionModuleSet"],
        )
        .await?;
        for c in build_module_set_changes(&module_set_events) {
            code_change_rows.push(c.attribute(&role_sources, &role_intervals_by_source, config.multisig));
        }

        if let Some(f) = asset.factory {
            let module_linked_events = factory_module_linked.get(&f).cloned().unwrap_or_default();
            for c in build_module_linked_changes(&module_linked_events, asset.token_address) {
                code_change_rows.push(c.attribute(&role_sources, &role_intervals_by_source, config.multisig));
            }
        }

        // ROUTER_GLOBAL / STOREFRONT_UPGRADE — FAN-OUT: the SAME chain
        // (built once above) re-attributed per asset, never re-chained.
        if let Some(r) = asset.router_address {
            if let Some(chain) = router_global_by_router.get(&r) {
                for c in chain {
                    code_change_rows.push(c.attribute(&role_sources, &role_intervals_by_source, config.multisig));
                }
            }
        }
        if let Some(s) = asset.storefront {
            if let Some(chain) = storefront_upgrade_by_storefront.get(&s) {
                for c in chain {
                    code_change_rows.push(c.attribute(&role_sources, &role_intervals_by_source, config.multisig));
                }
            }
        }

        for row in &code_change_rows {
            insert_code_change_row(&mut tx, config.chain_id, &asset.asset_id, row).await?;
        }

        // ---- P4b-ii: holder_balance — reuses `transfer_events` (already
        // fetched above for gate/exposure), no refetch.
        let holder_balance_rows = build_holder_balances(&transfer_events);
        for row in &holder_balance_rows {
            insert_holder_balance_row(&mut tx, config.chain_id, &asset.asset_id, row).await?;
        }

        // ---- P4b-ii: `sale` prep — deferred to ONE walk per storefront
        // after this loop (same HIGH-1 pattern as `screening`): accumulate
        // this asset's contribution to its storefront's shared pools.
        if let Some(storefront) = asset.storefront {
            sale_assets_by_storefront
                .entry(storefront)
                .or_default()
                .push((asset.asset_id.clone(), asset.token_address));
            let legs = sale_token_legs_by_storefront.entry(storefront).or_default();
            for ev in &transfer_events {
                legs.push((asset.asset_id.clone(), ev.clone()));
            }
            purchase_tokens_by_storefront
                .entry(storefront)
                .or_default()
                .extend(asset.purchase_tokens.iter().copied());
        }
    }

    // ---- P4b-ii: `sale` — the ONE walk per storefront across ALL assets
    // sharing it (the HIGH-1 pattern): the payment-leg pool is deduped by
    // distinct purchase-token address BEFORE this walk (see
    // `purchase_tokens_by_storefront`/`purchase_token_transfers` above), so
    // two assets listing the SAME purchase token draw from ONE shared
    // event list, never a duplicated one.
    for storefront in &storefront_addrs {
        let assets_for_storefront = sale_assets_by_storefront.remove(storefront).unwrap_or_default();
        let token_legs = sale_token_legs_by_storefront.remove(storefront).unwrap_or_default();
        let mut payment_legs = Vec::new();
        if let Some(tokens) = purchase_tokens_by_storefront.get(storefront) {
            for token in tokens {
                if let Some(events) = purchase_token_transfers.get(token) {
                    payment_legs.extend(events.iter().cloned());
                }
            }
        }
        let purchases = purchases_by_storefront.get(storefront).cloned().unwrap_or_default();
        let sale_rows = build_sales(&purchases, &assets_for_storefront, &token_legs, &payment_legs, *storefront);
        for row in &sale_rows {
            insert_sale_row(&mut tx, config.chain_id, row).await?;
        }
    }

    // ---- P4b-i / HIGH-1: transfer_screening + screening_gap — the ONE
    // chain-global pairing walk, over every configured asset's transfers at
    // once, against the chain-global screening_events, each result
    // attributed to its own asset_id and scoped to that asset's own router
    // epochs (see `screening` module doc).
    let (screening_rows, gap_rows) = build_transfer_screening_and_gaps(
        &all_asset_transfers,
        &screening_events,
        &router_epochs_by_asset,
    );
    for row in &screening_rows {
        insert_transfer_screening_row(&mut tx, config.chain_id, row).await?;
    }
    for row in &gap_rows {
        insert_screening_gap_row(&mut tx, config.chain_id, row).await?;
    }

    tx.commit().await?;
    Ok(())
}

/// MED-3 (P4b-i review): an `AssetSources.router_address` that isn't
/// also in `config.routers` would silently starve `screening_gap` forever —
/// `router_epochs_by_router.get(&r)` finds nothing, so
/// `router_epochs_by_asset` records an empty epoch list, which the
/// screening builder correctly (and silently) reads as "never a gap" for
/// that asset. That silence is indistinguishable from "this asset really
/// has no router" from inside the builder, so the guard belongs here, at
/// the config boundary, loud and up front — before any delete or fetch runs.
fn validate_config(config: &Tier2Config) -> Result<(), sqlx::Error> {
    for asset in &config.assets {
        if let Some(router) = asset.router_address {
            if !config.routers.contains(&router) {
                return Err(sqlx::Error::Configuration(
                    format!(
                        "asset {:?}'s router_address 0x{} is not in config.routers — \
                         screening_gap would silently report zero gaps for this asset forever",
                        asset.asset_id,
                        hex::encode(router)
                    )
                    .into(),
                ));
            }
        }
    }
    Ok(())
}

async fn insert_allowlist_row(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    chain_id: i64,
    asset_id: &str,
    row: &AllowlistIntervalRow,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO audit.allowlist_interval
            (chain_id, asset_id, address, from_block, to_block, opened_by_event, closed_by_event)
        VALUES ($1,$2,$3,$4,$5,$6,$7)
        "#,
    )
    .bind(chain_id)
    .bind(asset_id)
    .bind(row.address.as_slice())
    .bind(row.from_block)
    .bind(row.to_block)
    .bind(row.opened_by_event)
    .bind(row.closed_by_event)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn insert_gate_row(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    chain_id: i64,
    asset_id: &str,
    row: &GateIntervalRow,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO audit.gate_interval
            (chain_id, asset_id, gated, from_block, to_block, opened_by_event, closed_by_event)
        VALUES ($1,$2,$3,$4,$5,$6,$7)
        "#,
    )
    .bind(chain_id)
    .bind(asset_id)
    .bind(row.gated)
    .bind(row.from_block)
    .bind(row.to_block)
    .bind(row.opened_by_event)
    .bind(row.closed_by_event)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn insert_denyset_row(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    chain_id: i64,
    module_address: &[u8; 20],
    row: &DenysetIntervalRow,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO audit.sanction_denyset_interval
            (chain_id, module_address, address, from_block, to_block, opened_by_event, closed_by_event)
        VALUES ($1,$2,$3,$4,$5,$6,$7)
        "#,
    )
    .bind(chain_id)
    .bind(module_address.as_slice())
    .bind(row.address.as_slice())
    .bind(row.from_block)
    .bind(row.to_block)
    .bind(row.opened_by_event)
    .bind(row.closed_by_event)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn insert_router_epoch_row(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    chain_id: i64,
    router_address: &[u8; 20],
    row: &RouterEpochRow,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO audit.router_sanctions_epoch
            (chain_id, router_address, module_address, from_block, to_block, opened_by_event, closed_by_event)
        VALUES ($1,$2,$3,$4,$5,$6,$7)
        "#,
    )
    .bind(chain_id)
    .bind(router_address.as_slice())
    .bind(row.module_address.as_slice())
    .bind(row.from_block)
    .bind(row.to_block)
    .bind(row.opened_by_event)
    .bind(row.closed_by_event)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn insert_yield_blacklist_row(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    chain_id: i64,
    asset_id: &str,
    row: &YieldBlacklistIntervalRow,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO audit.yield_blacklist_interval
            (chain_id, asset_id, address, from_block, to_block, opened_by_event, closed_by_event)
        VALUES ($1,$2,$3,$4,$5,$6,$7)
        "#,
    )
    .bind(chain_id)
    .bind(asset_id)
    .bind(row.address.as_slice())
    .bind(row.from_block)
    .bind(row.to_block)
    .bind(row.opened_by_event)
    .bind(row.closed_by_event)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn insert_transfer_screening_row(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    chain_id: i64,
    row: &TransferScreeningRow,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO audit.transfer_screening
            (chain_id, asset_id, block_number, tx_index, screening_log_index, transfer_log_index,
             module_address, screening_event, transfer_event)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        "#,
    )
    .bind(chain_id)
    .bind(&row.asset_id)
    .bind(row.block_number)
    .bind(row.tx_index)
    .bind(row.screening_log_index)
    .bind(row.transfer_log_index)
    .bind(row.module_address.as_slice())
    .bind(row.screening_event)
    .bind(row.transfer_event)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn insert_screening_gap_row(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    chain_id: i64,
    row: &ScreeningGapRow,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO audit.screening_gap
            (chain_id, asset_id, block_number, tx_index, log_index, transfer_event, epoch_module)
        VALUES ($1,$2,$3,$4,$5,$6,$7)
        "#,
    )
    .bind(chain_id)
    .bind(&row.asset_id)
    .bind(row.block_number)
    .bind(row.tx_index)
    .bind(row.log_index)
    .bind(row.transfer_event)
    .bind(row.epoch_module.as_slice())
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn insert_yield_run_row(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    chain_id: i64,
    asset_id: &str,
    row: &YieldRunRow,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO audit.yield_run
            (chain_id, asset_id, yield_token, run_seq, total_amount, credited_total,
             withheld_remainder, over_credit, closed_by_event)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        "#,
    )
    .bind(chain_id)
    .bind(asset_id)
    .bind(row.yield_token.as_slice())
    .bind(row.run_seq)
    .bind(&row.total_amount)
    .bind(&row.credited_total)
    .bind(&row.withheld_remainder)
    .bind(row.over_credit)
    .bind(row.closed_by_event)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn insert_yield_credit_row(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    chain_id: i64,
    asset_id: &str,
    row: &YieldCreditRow,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO audit.yield_credit
            (chain_id, asset_id, yield_token, run_seq, block_number, tx_index, log_index,
             holder, share, credit_event)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
        "#,
    )
    .bind(chain_id)
    .bind(asset_id)
    .bind(row.yield_token.as_slice())
    .bind(row.run_seq)
    .bind(row.block_number)
    .bind(row.tx_index)
    .bind(row.log_index)
    .bind(row.holder.as_slice())
    .bind(&row.share)
    .bind(row.credit_event)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn insert_exposure_row(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    chain_id: i64,
    asset_id: &str,
    row: &ExposureWindowRow,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO audit.exposure_window
            (chain_id, asset_id, open_block, close_block, pattern, flags, opened_by_event, closed_by_event)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
        "#,
    )
    .bind(chain_id)
    .bind(asset_id)
    .bind(row.open_block)
    .bind(row.close_block)
    .bind(row.pattern.as_db_str())
    .bind(&row.flags)
    .bind(row.opened_by_event)
    .bind(row.closed_by_event)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn insert_role_interval_row(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    chain_id: i64,
    source_contract: &[u8; 20],
    row: &RoleIntervalRow,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO audit.role_interval
            (chain_id, source_contract, role, account, from_block, to_block, opened_by_event, closed_by_event)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
        "#,
    )
    .bind(chain_id)
    .bind(source_contract.as_slice())
    .bind(row.role.as_slice())
    .bind(row.account.as_slice())
    .bind(row.from_block)
    .bind(row.to_block)
    .bind(row.opened_by_event)
    .bind(row.closed_by_event)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn insert_code_change_row(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    chain_id: i64,
    asset_id: &str,
    row: &CodeChangeRow,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO audit.code_change
            (chain_id, asset_id, kind, subject, block_number, tx_index, log_index,
             before, after, signer_attribution, event_id)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
        "#,
    )
    .bind(chain_id)
    .bind(asset_id)
    .bind(row.kind.as_db_str())
    .bind(&row.subject)
    .bind(row.block_number)
    .bind(row.tx_index)
    .bind(row.log_index)
    .bind(row.before.as_ref().map(|b| b.as_slice()))
    .bind(row.after.as_ref().map(|a| a.as_slice()))
    .bind(row.signer_attribution.as_db_str())
    .bind(row.event_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn insert_sale_row(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    chain_id: i64,
    row: &SaleRow,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO audit.sale
            (chain_id, asset_id, block_number, tx_index, purchase_log_index, buyer, amount,
             price_paid, purchase_event, token_transfer_event, payment_transfer_event)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
        "#,
    )
    .bind(chain_id)
    .bind(&row.asset_id)
    .bind(row.block_number)
    .bind(row.tx_index)
    .bind(row.purchase_log_index)
    .bind(row.buyer.as_slice())
    .bind(&row.amount)
    .bind(&row.price_paid)
    .bind(row.purchase_event)
    .bind(row.token_transfer_event)
    .bind(row.payment_transfer_event)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn insert_holder_balance_row(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    chain_id: i64,
    asset_id: &str,
    row: &HolderBalanceRow,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO audit.holder_balance
            (chain_id, asset_id, address, block_number, balance, integrity_alarm)
        VALUES ($1,$2,$3,$4,$5,$6)
        "#,
    )
    .bind(chain_id)
    .bind(asset_id)
    .bind(row.address.as_slice())
    .bind(row.block_number)
    .bind(&row.balance)
    .bind(row.integrity_alarm)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tier2::fetch::AssetSources;

    fn asset(id: &str, router: Option<[u8; 20]>) -> AssetSources {
        AssetSources {
            asset_id: id.to_string(),
            axis1_module: [0x11; 20],
            token_address: [0x22; 20],
            router_address: router,
            yield_tokens: vec![],
            yield_blacklist_modules: vec![],
            storefront: None,
            purchase_tokens: vec![],
            factory: None,
        }
    }

    /// MED-3 (P4b-i review): an asset's `router_address` that isn't
    /// in `config.routers` must be a LOUD config error, never a silent
    /// zero-gaps-forever false negative.
    #[test]
    fn router_address_not_in_routers_is_a_loud_config_error() {
        let config = Tier2Config {
            chain_id: 1,
            assets: vec![asset("A", Some([0x44; 20]))],
            sanctions_modules: vec![],
            routers: vec![], // [0x44;20] is NOT here
            multisig: None,
        };
        let err = validate_config(&config).unwrap_err();
        assert!(matches!(err, sqlx::Error::Configuration(_)));
    }

    #[test]
    fn router_address_present_in_routers_passes() {
        let config = Tier2Config {
            chain_id: 1,
            assets: vec![asset("A", Some([0x44; 20]))],
            sanctions_modules: vec![],
            routers: vec![[0x44; 20]],
            multisig: None,
        };
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn no_router_address_configured_always_passes() {
        let config = Tier2Config {
            chain_id: 1,
            assets: vec![asset("A", None)],
            sanctions_modules: vec![],
            routers: vec![],
            multisig: None,
        };
        assert!(validate_config(&config).is_ok());
    }
}
