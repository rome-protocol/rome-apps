//! Tier-2 DB tests — against a REAL local Postgres (same disposable
//! `rome-audit-test-pg` container `tests/ingest_db.rs` uses; one fresh
//! database per test). Unlike `ingest_db.rs`, these tests don't drive the
//! full Hercules-ingest simulation — Tier-2 derivations are a pure function
//! of `audit.chain_event` alone, so fixtures seed that table directly via
//! plain INSERTs (append-only doesn't forbid INSERT, only UPDATE/DELETE).
//!
//! **The load-bearing test is `rebuild_determinism_*`** (IMPL-PLAN §13.1):
//! Tier-2 tables carry `opened_by_event`/`closed_by_event` FK columns into
//! `audit.chain_event.event_id` — legitimate for Tier-2 (drill-down aid,
//! spec §9.0-e), unlike `ReportContent` (C1: NEVER a surrogate key in
//! hashed content, a later phase). So:
//! - **within one DB**, re-truncating and rebuilding Tier-2 must reproduce
//!   the exact same rows, FK ids included (chain_event itself is untouched,
//!   so its event_ids don't move) — `rebuild_determinism_same_db_retruncate`.
//! - **across two independently-seeded DBs** with the same chain_event
//!   *content* inserted in a *different order*, `event_id` values
//!   themselves legitimately differ (assigned by insertion order on each
//!   instance) — so that comparison is over VALUE columns only (never the
//!   FK id columns), proving the derived FACTS (from_block/to_block/gated/
//!   pattern/flags) depend on `(block_number, tx_index, log_index)`, never
//!   on insertion order or surrogate ids — `rebuild_determinism_cross_db_different_insert_order`.

use std::str::FromStr;
use std::sync::atomic::{AtomicI64, Ordering};

use sqlx::PgPool;

mod common;
use common::fresh_audit_db as fresh_test_db;
use rome_audit::tier2::{rebuild_tier2, AssetSources, Tier2Config};

/// (module_or_router_address, address, from_block, to_block) — the
/// denyset/router-epoch snapshot row shape, factored out to silence
/// clippy::type_complexity on the repeated inline tuple type.
type AddrAddrBlockRange = (Vec<u8>, Vec<u8>, i64, Option<i64>);

/// (asset_id, yield_token, run_seq, total_amount, credited_total,
/// withheld_remainder, over_credit) — the `yield_run` snapshot row shape,
/// used by both `snapshot_all_tier2` and `value_snapshot` below.
type YieldRunSnapshotRow = (
    String,
    Vec<u8>,
    i64,
    Option<bigdecimal::BigDecimal>,
    bigdecimal::BigDecimal,
    Option<bigdecimal::BigDecimal>,
    bool,
);

/// (asset_id, yield_token, run_seq, block_number, tx_index, log_index,
/// holder, share) — the `yield_credit` snapshot row shape.
type YieldCreditSnapshotRow = (String, Vec<u8>, i64, i64, i32, i32, Vec<u8>, bigdecimal::BigDecimal);

/// (asset_id, kind, subject, block_number, tx_index, log_index, before,
/// after, signer_attribution) — the `code_change` snapshot row shape
/// (P4b-ii). Never `event_id` (FK, excluded by the same discipline as
/// every other snapshot column above).
type CodeChangeSnapshotRow = (
    String,
    String,
    Vec<u8>,
    i64,
    i32,
    i32,
    Option<Vec<u8>>,
    Option<Vec<u8>>,
    String,
);

/// (asset_id, block_number, tx_index, purchase_log_index, buyer, amount,
/// price_paid) — the `sale` snapshot row shape (P4b-ii). Never
/// `purchase_event`/`token_transfer_event`/`payment_transfer_event` (FKs).
type SaleSnapshotRow = (
    String,
    i64,
    i32,
    i32,
    Vec<u8>,
    bigdecimal::BigDecimal,
    bigdecimal::BigDecimal,
);

/// (source_contract, role, account, from_block, to_block) — the
/// `role_interval` snapshot row shape (P4b-ii).
type RoleIntervalSnapshotRow = (Vec<u8>, Vec<u8>, Vec<u8>, i64, Option<i64>);

// Test-DB provisioning lives in `tests/common/mod.rs` (this file used to
// carry its own duplicate `CREATE DATABASE` — no matching `DROP DATABASE`
// anywhere — one of three leak sites fixed together; see `common::TestDb`).

// ---- chain_event seeding (direct INSERT — Tier-2 doesn't care how the
// row got there, only its columns) ---------------------------------------

static EVENT_COUNTER: AtomicI64 = AtomicI64::new(1);

fn addr(b: u8) -> [u8; 20] {
    [b; 20]
}
fn addr_hex(b: u8) -> String {
    format!("0x{}", hex::encode(addr(b)))
}
/// Same as `addr_hex`, but for an already-built `[u8; 20]` (P4b-i fixtures
/// pass real addresses like `token`/`yield_token` around as values, not
/// bare bytes).
fn hex_of(a: [u8; 20]) -> String {
    format!("0x{}", hex::encode(a))
}

/// Inserts one `audit.chain_event` row, hardcoding `tx_signer = 0x99` —
/// delegates to [`seed_event_signed`]. Every P4a/P4b-i fixture in this file
/// predates P4b-ii's `signer_attribution` and doesn't care WHO signed, only
/// that a signer is present; `seed_event_signed` exists for the P4b-ii
/// fixtures that need a SPECIFIC signer.
#[allow(clippy::too_many_arguments)]
async fn seed_event(
    pool: &PgPool,
    chain_id: i64,
    source_contract: [u8; 20],
    event_name: &str,
    block_number: i64,
    tx_index: i32,
    log_index: i32,
    args: serde_json::Value,
) -> i64 {
    seed_event_signed(
        pool,
        chain_id,
        source_contract,
        event_name,
        block_number,
        tx_index,
        log_index,
        args,
        addr(0x99),
    )
    .await
}

/// Same as [`seed_event`], with an explicit `signer` param (P4b-ii). Each
/// call gets a fresh, globally unique `tx_hash` (via the counter) so
/// `UNIQUE(chain_id, tx_hash, log_index, block_hash)` never collides
/// regardless of what `block_number`/`tx_index`/`log_index` the caller
/// passes — callers control exactly the three ordering columns that matter
/// (`block_number`, `tx_index`, `log_index`) and nothing else about row
/// identity.
#[allow(clippy::too_many_arguments)]
async fn seed_event_signed(
    pool: &PgPool,
    chain_id: i64,
    source_contract: [u8; 20],
    event_name: &str,
    block_number: i64,
    tx_index: i32,
    log_index: i32,
    args: serde_json::Value,
    signer: [u8; 20],
) -> i64 {
    let n = EVENT_COUNTER.fetch_add(1, Ordering::SeqCst);
    let tx_hash = format!("0x{:064x}", n);
    let block_hash = format!("0x{:064x}", block_number * 1_000_000 + n);

    let row: (i64,) = sqlx::query_as(
        r#"
        INSERT INTO audit.chain_event
            (chain_id, source_contract, source_kind, event_name, projection_tag, topic0,
             block_number, block_hash, block_timestamp, tx_hash, tx_index, log_index, tx_signer, args)
        VALUES ($1,$2,'TEST_SOURCE',$3,'primary',$4,$5,$6,$7,$8,$9,$10,$11,$12)
        RETURNING event_id
        "#,
    )
    .bind(chain_id)
    .bind(source_contract.as_slice())
    .bind(event_name)
    .bind(vec![0u8; 32]) // topic0 — irrelevant to Tier-2, which never reads it
    .bind(block_number)
    .bind(hex::decode(block_hash.trim_start_matches("0x")).unwrap())
    .bind(1_700_000_000i64 + block_number)
    .bind(hex::decode(tx_hash.trim_start_matches("0x")).unwrap())
    .bind(tx_index)
    .bind(log_index)
    .bind(signer.as_slice())
    .bind(&args)
    .fetch_one(pool)
    .await
    .unwrap();
    row.0
}

async fn seed_whitelist_toggle(
    pool: &PgPool,
    chain_id: i64,
    module: [u8; 20],
    block: i64,
    account: u8,
    is_whitelisted: bool,
) -> i64 {
    seed_event(
        pool,
        chain_id,
        module,
        "WhitelistStatusChanged",
        block,
        0,
        0,
        serde_json::json!({ "account": addr_hex(account), "isWhitelisted": is_whitelisted }),
    )
    .await
}

async fn seed_gate_toggle(
    pool: &PgPool,
    chain_id: i64,
    module: [u8; 20],
    block: i64,
    transfers_allowed: bool,
) -> i64 {
    seed_event(
        pool,
        chain_id,
        module,
        "TransfersRestrictionToggled",
        block,
        0,
        0,
        serde_json::json!({ "transfersAllowed": transfers_allowed }),
    )
    .await
}

async fn seed_transfer(
    pool: &PgPool,
    chain_id: i64,
    token: [u8; 20],
    block: i64,
    from: u8,
    to: u8,
) -> i64 {
    seed_event(
        pool,
        chain_id,
        token,
        "Transfer",
        block,
        0,
        0,
        serde_json::json!({ "from": addr_hex(from), "to": addr_hex(to), "value": "1000" }),
    )
    .await
}

async fn seed_sanction(
    pool: &PgPool,
    chain_id: i64,
    module: [u8; 20],
    block: i64,
    account: u8,
    sanctioned: bool,
) -> i64 {
    seed_event(
        pool,
        chain_id,
        module,
        if sanctioned {
            "Sanctioned"
        } else {
            "Unsanctioned"
        },
        block,
        0,
        0,
        serde_json::json!({ "account": addr_hex(account) }),
    )
    .await
}

async fn seed_module_type_registered(
    pool: &PgPool,
    chain_id: i64,
    router: [u8; 20],
    block: i64,
    module: u8,
) -> i64 {
    let type_id = format!(
        "0x{}",
        hex::encode(rome_audit::abi::router::GLOBAL_SANCTIONS_TYPE)
    );
    seed_event(
        pool,
        chain_id,
        router,
        "ModuleTypeRegistered",
        block,
        0,
        0,
        serde_json::json!({ "typeId": type_id, "isGlobal": true, "globalImplementation": addr_hex(module) }),
    )
    .await
}

/// Same event as [`seed_module_type_registered`], but with an EXPLICIT
/// `typeId` (P4b-ii `code_change`'s `ROUTER_GLOBAL` is NOT filtered to
/// `GLOBAL_SANCTIONS_TYPE` — every registered type contributes).
#[allow(clippy::too_many_arguments)]
async fn seed_module_type_registered_typed(
    pool: &PgPool,
    chain_id: i64,
    router: [u8; 20],
    block: i64,
    type_id: u8,
    is_global: bool,
    module: u8,
) -> i64 {
    seed_event(
        pool,
        chain_id,
        router,
        "ModuleTypeRegistered",
        block,
        0,
        0,
        serde_json::json!({
            "typeId": format!("0x{}", hex::encode([type_id; 32])),
            "isGlobal": is_global,
            "globalImplementation": addr_hex(module),
        }),
    )
    .await
}

// ---- P4b-i seed helpers ---------------------------------------------------

async fn seed_yield_blacklist_toggle(
    pool: &PgPool,
    chain_id: i64,
    module: [u8; 20],
    block: i64,
    account: u8,
    is_blacklisted: bool,
) -> i64 {
    seed_event(
        pool,
        chain_id,
        module,
        "YieldBlacklistUpdated",
        block,
        0,
        0,
        serde_json::json!({ "account": addr_hex(account), "isBlacklisted": is_blacklisted }),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn seed_transfer_screened(
    pool: &PgPool,
    chain_id: i64,
    module: [u8; 20],
    block: i64,
    tx_index: i32,
    log_index: i32,
    from: u8,
    to: u8,
) -> i64 {
    seed_event(
        pool,
        chain_id,
        module,
        "TransferScreened",
        block,
        tx_index,
        log_index,
        serde_json::json!({ "from": addr_hex(from), "to": addr_hex(to), "amount": "500" }),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn seed_transfer_at(
    pool: &PgPool,
    chain_id: i64,
    token: [u8; 20],
    block: i64,
    tx_index: i32,
    log_index: i32,
    from: u8,
    to: u8,
    value: &str,
) -> i64 {
    seed_event(
        pool,
        chain_id,
        token,
        "Transfer",
        block,
        tx_index,
        log_index,
        serde_json::json!({ "from": addr_hex(from), "to": addr_hex(to), "value": value }),
    )
    .await
}

/// A yield-token credit: `Transfer(from=arc_token, to=holder, value)` on
/// the yield token's own contract.
async fn seed_yield_credit(
    pool: &PgPool,
    chain_id: i64,
    yield_token: [u8; 20],
    block: i64,
    arc_token: [u8; 20],
    holder: u8,
    value: &str,
) -> i64 {
    seed_event(
        pool,
        chain_id,
        yield_token,
        "Transfer",
        block,
        0,
        0,
        serde_json::json!({ "from": hex_of(arc_token), "to": addr_hex(holder), "value": value }),
    )
    .await
}

/// `YieldDistributed(amount, token)` on the ArcToken itself — closes the run.
async fn seed_yield_distributed(
    pool: &PgPool,
    chain_id: i64,
    arc_token: [u8; 20],
    block: i64,
    yield_token: [u8; 20],
    amount: &str,
) -> i64 {
    seed_event(
        pool,
        chain_id,
        arc_token,
        "YieldDistributed",
        block,
        0,
        0,
        serde_json::json!({ "amount": amount, "token": hex_of(yield_token) }),
    )
    .await
}

// ---- P4b-ii seed helpers --------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn seed_role_granted(
    pool: &PgPool,
    chain_id: i64,
    source: [u8; 20],
    block: i64,
    tx_index: i32,
    log_index: i32,
    role: u8,
    account: u8,
) -> i64 {
    seed_event(
        pool,
        chain_id,
        source,
        "RoleGranted",
        block,
        tx_index,
        log_index,
        serde_json::json!({
            "role": format!("0x{}", hex::encode([role; 32])),
            "account": addr_hex(account),
            "sender": addr_hex(0x99),
        }),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn seed_role_revoked(
    pool: &PgPool,
    chain_id: i64,
    source: [u8; 20],
    block: i64,
    tx_index: i32,
    log_index: i32,
    role: u8,
    account: u8,
) -> i64 {
    seed_event(
        pool,
        chain_id,
        source,
        "RoleRevoked",
        block,
        tx_index,
        log_index,
        serde_json::json!({
            "role": format!("0x{}", hex::encode([role; 32])),
            "account": addr_hex(account),
            "sender": addr_hex(0x99),
        }),
    )
    .await
}

async fn seed_upgraded(pool: &PgPool, chain_id: i64, source: [u8; 20], block: i64, implementation: u8) -> i64 {
    seed_event(
        pool,
        chain_id,
        source,
        "Upgraded",
        block,
        0,
        0,
        serde_json::json!({ "implementation": addr_hex(implementation) }),
    )
    .await
}

async fn seed_token_upgraded(
    pool: &PgPool,
    chain_id: i64,
    factory: [u8; 20],
    block: i64,
    token: [u8; 20],
    new_implementation: u8,
) -> i64 {
    seed_event(
        pool,
        chain_id,
        factory,
        "TokenUpgraded",
        block,
        0,
        1,
        serde_json::json!({ "token": hex_of(token), "newImplementation": addr_hex(new_implementation) }),
    )
    .await
}

async fn seed_module_set(
    pool: &PgPool,
    chain_id: i64,
    token: [u8; 20],
    block: i64,
    type_id: u8,
    module: u8,
) -> i64 {
    seed_event(
        pool,
        chain_id,
        token,
        "SpecificRestrictionModuleSet",
        block,
        0,
        0,
        serde_json::json!({
            "typeId": format!("0x{}", hex::encode([type_id; 32])),
            "moduleAddress": addr_hex(module),
        }),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn seed_module_linked(
    pool: &PgPool,
    chain_id: i64,
    factory: [u8; 20],
    block: i64,
    token: [u8; 20],
    module: u8,
    module_type: u8,
) -> i64 {
    seed_event(
        pool,
        chain_id,
        factory,
        "ModuleLinked",
        block,
        0,
        0,
        serde_json::json!({
            "tokenAddress": hex_of(token),
            "moduleAddress": addr_hex(module),
            "moduleType": format!("0x{}", hex::encode([module_type; 32])),
        }),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn seed_purchase_made(
    pool: &PgPool,
    chain_id: i64,
    storefront: [u8; 20],
    block: i64,
    tx_index: i32,
    log_index: i32,
    token: [u8; 20],
    buyer: u8,
    amount: &str,
    price_paid: &str,
) -> i64 {
    seed_event(
        pool,
        chain_id,
        storefront,
        "PurchaseMade",
        block,
        tx_index,
        log_index,
        serde_json::json!({
            "buyer": addr_hex(buyer),
            "tokenContract": hex_of(token),
            "amount": amount,
            "pricePaid": price_paid,
        }),
    )
    .await
}

fn config_one_asset(chain_id: i64, module: [u8; 20], token: [u8; 20]) -> Tier2Config {
    Tier2Config {
        chain_id,
        assets: vec![AssetSources {
            asset_id: format!("{chain_id}:{}", addr_hex(0xAB)),
            axis1_module: module,
            token_address: token,
            router_address: None,
            yield_tokens: vec![],
            yield_blacklist_modules: vec![],
            storefront: None,
            purchase_tokens: vec![],
            factory: None,
        }],
        sanctions_modules: vec![],
        routers: vec![],
        multisig: None,
    }
}

// ---- (1) allowlist_interval: add/remove via a real rebuild ---------------

#[tokio::test]
async fn allowlist_interval_add_then_remove_via_rebuild() {
    let pool = fresh_test_db().await;
    let chain_id = 5001i64;
    let module = addr(0x11);
    let token = addr(0x22);

    seed_whitelist_toggle(&pool, chain_id, module, 100, 0xAA, true).await;
    seed_whitelist_toggle(&pool, chain_id, module, 200, 0xAA, false).await;

    rebuild_tier2(&pool, &config_one_asset(chain_id, module, token))
        .await
        .unwrap();

    let rows: Vec<(Vec<u8>, i64, Option<i64>)> = sqlx::query_as(
        "SELECT address, from_block, to_block FROM audit.allowlist_interval WHERE chain_id = $1 ORDER BY address, from_block",
    )
    .bind(chain_id)
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(rows, vec![(addr(0xAA).to_vec(), 100, Some(200))]);
}

#[tokio::test]
async fn allowlist_interval_open_with_no_remove_via_rebuild() {
    let pool = fresh_test_db().await;
    let chain_id = 5002i64;
    let module = addr(0x11);
    let token = addr(0x22);

    seed_whitelist_toggle(&pool, chain_id, module, 100, 0xAA, true).await;

    rebuild_tier2(&pool, &config_one_asset(chain_id, module, token))
        .await
        .unwrap();

    let rows: Vec<(Vec<u8>, i64, Option<i64>)> = sqlx::query_as(
        "SELECT address, from_block, to_block FROM audit.allowlist_interval WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows, vec![(addr(0xAA).to_vec(), 100, None)]);
}

// ---- (2) gate coalescing via a real rebuild ------------------------------

#[tokio::test]
async fn gate_interval_coalesces_redundant_toggles_via_rebuild() {
    let pool = fresh_test_db().await;
    let chain_id = 5003i64;
    let module = addr(0x11);
    let token = addr(0x22);

    // false,false,true → gated(coalesced),ungated
    seed_gate_toggle(&pool, chain_id, module, 100, false).await;
    seed_gate_toggle(&pool, chain_id, module, 150, false).await;
    seed_gate_toggle(&pool, chain_id, module, 200, true).await;

    rebuild_tier2(&pool, &config_one_asset(chain_id, module, token))
        .await
        .unwrap();

    let rows: Vec<(bool, i64, Option<i64>)> = sqlx::query_as(
        "SELECT gated, from_block, to_block FROM audit.gate_interval WHERE chain_id = $1 ORDER BY from_block",
    )
    .bind(chain_id)
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(
        rows,
        vec![(true, 100, Some(200)), (false, 200, None)],
        "the two redundant false-toggles must collapse into ONE gated interval, not two"
    );
}

// ---- (3) exposure-window classification via a real rebuild --------------

#[tokio::test]
async fn exposure_window_issuance_vs_other_via_rebuild() {
    let pool = fresh_test_db().await;
    let chain_id = 5004i64;

    // Asset A: un-gate → mint → re-gate ⇒ ISSUANCE
    let module_a = addr(0x11);
    let token_a = addr(0x22);
    seed_gate_toggle(&pool, chain_id, module_a, 50, false).await; // gated (transfersAllowed=false)
    seed_gate_toggle(&pool, chain_id, module_a, 100, true).await; // un-gate
    seed_transfer(&pool, chain_id, token_a, 150, 0x00, 0xCC).await; // mint
    seed_gate_toggle(&pool, chain_id, module_a, 200, false).await; // re-gate

    // Asset B: un-gate → transfer-to-stranger → re-gate ⇒ OTHER
    let module_b = addr(0x33);
    let token_b = addr(0x44);
    seed_gate_toggle(&pool, chain_id, module_b, 50, false).await;
    seed_gate_toggle(&pool, chain_id, module_b, 100, true).await;
    seed_transfer(&pool, chain_id, token_b, 150, 0x01, 0xFF).await; // stranger transfer
    seed_gate_toggle(&pool, chain_id, module_b, 200, false).await;

    let config = Tier2Config {
        chain_id,
        assets: vec![
            AssetSources {
                asset_id: "A".to_string(),
                axis1_module: module_a,
                token_address: token_a,
                router_address: None,
                yield_tokens: vec![],
                yield_blacklist_modules: vec![],
                storefront: None,
                purchase_tokens: vec![],
                factory: None,
            },
            AssetSources {
                asset_id: "B".to_string(),
                axis1_module: module_b,
                token_address: token_b,
                router_address: None,
                yield_tokens: vec![],
                yield_blacklist_modules: vec![],
                storefront: None,
                purchase_tokens: vec![],
                factory: None,
            },
        ],
        sanctions_modules: vec![],
        routers: vec![],
        multisig: None,
    };
    rebuild_tier2(&pool, &config).await.unwrap();

    let rows: Vec<(String, String, serde_json::Value)> = sqlx::query_as(
        "SELECT asset_id, pattern, flags FROM audit.exposure_window WHERE chain_id = $1 ORDER BY asset_id",
    )
    .bind(chain_id)
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0],
        (
            "A".to_string(),
            "ISSUANCE".to_string(),
            serde_json::json!({
                "third_party_transfer_events": [],
                "never_allowlisted_recipients": [],
            })
        )
    );
    assert_eq!(rows[1].0, "B");
    assert_eq!(rows[1].1, "OTHER");
    let flagged = rows[1].2["never_allowlisted_recipients"]
        .as_array()
        .unwrap();
    assert_eq!(flagged.len(), 1);
    assert_eq!(flagged[0].as_str().unwrap(), addr_hex(0xFF));
}

// ---- (4) THE LOAD-BEARING TEST: rebuild determinism, same DB ------------

/// Snapshots every Tier-2 table's VALUE columns (never the `opened_by_event`/
/// `closed_by_event` FK ids for the cross-DB variant below — but here it's
/// the SAME DB re-truncated, so including the FK ids is valid too; this
/// helper is shared by both tests and always drops them, which is a
/// strictly stronger assertion in the same-DB case).
async fn snapshot_all_tier2(pool: &PgPool, chain_id: i64) -> String {
    let allowlist: Vec<(String, Vec<u8>, i64, Option<i64>)> = sqlx::query_as(
        "SELECT asset_id, address, from_block, to_block FROM audit.allowlist_interval WHERE chain_id = $1 ORDER BY asset_id, address, from_block",
    ).bind(chain_id).fetch_all(pool).await.unwrap();

    let gate: Vec<(String, bool, i64, Option<i64>)> = sqlx::query_as(
        "SELECT asset_id, gated, from_block, to_block FROM audit.gate_interval WHERE chain_id = $1 ORDER BY asset_id, from_block",
    ).bind(chain_id).fetch_all(pool).await.unwrap();

    let denyset: Vec<AddrAddrBlockRange> = sqlx::query_as(
        "SELECT module_address, address, from_block, to_block FROM audit.sanction_denyset_interval WHERE chain_id = $1 ORDER BY module_address, address, from_block",
    ).bind(chain_id).fetch_all(pool).await.unwrap();

    let router_epoch: Vec<AddrAddrBlockRange> = sqlx::query_as(
        "SELECT router_address, module_address, from_block, to_block FROM audit.router_sanctions_epoch WHERE chain_id = $1 ORDER BY router_address, from_block",
    ).bind(chain_id).fetch_all(pool).await.unwrap();

    let exposure: Vec<(String, i64, Option<i64>, String, serde_json::Value)> = sqlx::query_as(
        "SELECT asset_id, open_block, close_block, pattern, flags FROM audit.exposure_window WHERE chain_id = $1 ORDER BY asset_id, open_block",
    ).bind(chain_id).fetch_all(pool).await.unwrap();

    // P4b-i tables — VALUE-only (no event_id FK columns selected here
    // either; unlike the five ORIGINAL tables above, none of these five
    // SELECTs picked up `opened_by_event`/`closed_by_event`/`screening_event`/
    // `transfer_event`/`credit_event` in the first place, so this projection
    // is already identical between the same-DB and cross-DB snapshot fns —
    // see `value_snapshot` below, which literally copies these five blocks).
    let yield_blacklist: Vec<(String, Vec<u8>, i64, Option<i64>)> = sqlx::query_as(
        "SELECT asset_id, address, from_block, to_block FROM audit.yield_blacklist_interval WHERE chain_id = $1 ORDER BY asset_id, address, from_block",
    ).bind(chain_id).fetch_all(pool).await.unwrap();

    let screening: Vec<(String, i64, i32, i32, i32, Vec<u8>)> = sqlx::query_as(
        "SELECT asset_id, block_number, tx_index, screening_log_index, transfer_log_index, module_address FROM audit.transfer_screening WHERE chain_id = $1 ORDER BY asset_id, block_number, tx_index, screening_log_index",
    ).bind(chain_id).fetch_all(pool).await.unwrap();

    let gaps: Vec<(String, i64, i32, i32, Vec<u8>)> = sqlx::query_as(
        "SELECT asset_id, block_number, tx_index, log_index, epoch_module FROM audit.screening_gap WHERE chain_id = $1 ORDER BY asset_id, block_number, tx_index, log_index",
    ).bind(chain_id).fetch_all(pool).await.unwrap();

    let yield_runs: Vec<YieldRunSnapshotRow> = sqlx::query_as(
        "SELECT asset_id, yield_token, run_seq, total_amount, credited_total, withheld_remainder, over_credit FROM audit.yield_run WHERE chain_id = $1 ORDER BY asset_id, yield_token, run_seq",
    ).bind(chain_id).fetch_all(pool).await.unwrap();

    let yield_credits: Vec<YieldCreditSnapshotRow> = sqlx::query_as(
        "SELECT asset_id, yield_token, run_seq, block_number, tx_index, log_index, holder, share FROM audit.yield_credit WHERE chain_id = $1 ORDER BY asset_id, yield_token, run_seq, block_number, tx_index, log_index",
    ).bind(chain_id).fetch_all(pool).await.unwrap();

    // P4b-ii tables — SAME VALUE-only discipline (never an event_id/FK column).
    let role_intervals: Vec<RoleIntervalSnapshotRow> = sqlx::query_as(
        "SELECT source_contract, role, account, from_block, to_block FROM audit.role_interval WHERE chain_id = $1 ORDER BY source_contract, role, account, from_block",
    ).bind(chain_id).fetch_all(pool).await.unwrap();

    let code_changes: Vec<CodeChangeSnapshotRow> = sqlx::query_as(
        "SELECT asset_id, kind, subject, block_number, tx_index, log_index, before, after, signer_attribution FROM audit.code_change WHERE chain_id = $1 ORDER BY asset_id, kind, subject, block_number, tx_index, log_index",
    ).bind(chain_id).fetch_all(pool).await.unwrap();

    let sales: Vec<SaleSnapshotRow> = sqlx::query_as(
        "SELECT asset_id, block_number, tx_index, purchase_log_index, buyer, amount, price_paid FROM audit.sale WHERE chain_id = $1 ORDER BY asset_id, block_number, tx_index, purchase_log_index",
    ).bind(chain_id).fetch_all(pool).await.unwrap();

    let holder_balances: Vec<(String, Vec<u8>, i64, bigdecimal::BigDecimal, bool)> = sqlx::query_as(
        "SELECT asset_id, address, block_number, balance, integrity_alarm FROM audit.holder_balance WHERE chain_id = $1 ORDER BY asset_id, address, block_number",
    ).bind(chain_id).fetch_all(pool).await.unwrap();

    format!(
        "{allowlist:?}|{gate:?}|{denyset:?}|{router_epoch:?}|{exposure:?}|{yield_blacklist:?}|{screening:?}|{gaps:?}|{yield_runs:?}|{yield_credits:?}|\
         {role_intervals:?}|{code_changes:?}|{sales:?}|{holder_balances:?}"
    )
}

/// Asset A: un-gate → mint (ISSUANCE, empty flags) → re-gate. Asset B: a
/// SEPARATE gate/token pair, un-gate → stranger transfer (OTHER,
/// NON-EMPTY flags) → re-gate — added per P2 review H2: the
/// determinism tests must exercise a non-empty `flags` value (natural-key
/// positions), not just the empty-flags ISSUANCE case, or the H2 fix's
/// determinism guarantee is unproven ("green by luck").
#[allow(clippy::too_many_arguments)]
async fn seed_full_scenario(
    pool: &PgPool,
    chain_id: i64,
    module: [u8; 20],
    token: [u8; 20],
    module_b: [u8; 20],
    token_b: [u8; 20],
    sanctions_module: [u8; 20],
    router: [u8; 20],
    yield_blacklist_module: [u8; 20],
    yield_token: [u8; 20],
    storefront: [u8; 20],
    factory: [u8; 20],
    purchase_token: [u8; 20],
) {
    // allowlist: two addresses, one closed, one still open
    seed_whitelist_toggle(pool, chain_id, module, 10, 0xA1, true).await;
    seed_whitelist_toggle(pool, chain_id, module, 20, 0xA1, false).await;
    seed_whitelist_toggle(pool, chain_id, module, 15, 0xA2, true).await;

    // gate (asset A): gated → ungated (mint, ISSUANCE) → gated
    seed_gate_toggle(pool, chain_id, module, 5, false).await;
    seed_gate_toggle(pool, chain_id, module, 30, true).await;
    seed_transfer(pool, chain_id, token, 32, 0x00, 0xA2).await;
    seed_gate_toggle(pool, chain_id, module, 40, false).await;

    // gate (asset B): gated → ungated (stranger transfer, OTHER, non-empty
    // flags) → gated.
    seed_gate_toggle(pool, chain_id, module_b, 6, false).await;
    seed_gate_toggle(pool, chain_id, module_b, 33, true).await;
    seed_transfer(pool, chain_id, token_b, 35, 0x01, 0xFE).await;
    seed_gate_toggle(pool, chain_id, module_b, 41, false).await;

    // sanctions denyset: one closed
    seed_sanction(pool, chain_id, sanctions_module, 12, 0xB1, true).await;
    seed_sanction(pool, chain_id, sanctions_module, 22, 0xB1, false).await;

    // router epoch: open, no close — asset A wires router_address to this
    // router (two_asset_config), so any of asset A's unscreened transfers
    // from block 1 onward (including the block-32 mint above) fall inside
    // this epoch and are real screening_gap candidates.
    seed_module_type_registered(pool, chain_id, router, 1, 0xC1).await;

    // P4b-i: yield_blacklist_interval — one closed interval.
    seed_yield_blacklist_toggle(pool, chain_id, yield_blacklist_module, 3, 0xD1, true).await;
    seed_yield_blacklist_toggle(pool, chain_id, yield_blacklist_module, 4, 0xD1, false).await;

    // P4b-i: transfer_screening — a paired screening+transfer in one tx
    // (asset A's own token), plus a foreign leftover TransferScreened in a
    // SEPARATE tx (no matching Transfer at all) that must produce zero rows.
    seed_transfer_screened(pool, chain_id, sanctions_module, 60, 0, 0, 0xF1, 0xF2).await;
    seed_transfer_at(pool, chain_id, token, 60, 0, 1, 0xF1, 0xF2, "777").await;
    seed_transfer_screened(pool, chain_id, sanctions_module, 61, 0, 0, 0xF3, 0xF4).await;

    // P4b-i: a 2-tx yield walk with a withheld remainder (30 credited
    // against a declared 40 ⇒ remainder 10, never clamped, over_credit=false).
    seed_yield_credit(pool, chain_id, yield_token, 70, token, 0xE1, "15").await;
    seed_yield_credit(pool, chain_id, yield_token, 71, token, 0xE2, "15").await;
    seed_yield_distributed(pool, chain_id, token, 72, yield_token, "40").await;

    // ---- P4b-ii ------------------------------------------------------
    // NEW event names / NEW addresses only from here down — none of these
    // are ever picked up by an EXISTING P4a/P4b-i query (all filter on a
    // specific event_name list), so nothing above is perturbed.

    // role_interval: grant then revoke, on the router (an existing address
    // — RoleGranted/RoleRevoked are new event names on it, safe reuse).
    seed_role_granted(pool, chain_id, router, 80, 0, 0, 0x01, 0xE9).await;
    seed_role_revoked(pool, chain_id, router, 85, 0, 0, 0x01, 0xE9).await;

    // code_change UPGRADE: token's own Upgraded, THEN factory-mediated
    // (two rows, chained: X->Y, Y->Y self-loop is NOT triggered here since
    // these are on separate blocks/implementations, not the same-tx case —
    // that shape is covered by code_change's own unit test 15).
    seed_upgraded(pool, chain_id, token, 90, 0x01).await;
    seed_token_upgraded(pool, chain_id, factory, 91, token, 0x02).await;

    // code_change MODULE_SET: token's own SpecificRestrictionModuleSet.
    seed_module_set(pool, chain_id, token, 92, 0x50, 0x51).await;

    // code_change MODULE_LINKED: factory's ModuleLinked, filtered to `token`.
    seed_module_linked(pool, chain_id, factory, 93, token, 0x52, 0x53).await;

    // code_change ROUTER_GLOBAL: a DIFFERENT typeId than GLOBAL_SANCTIONS_TYPE
    // (0x54 != the router_epoch's own type) — must not perturb
    // router_epoch/screening_gap at all.
    seed_module_type_registered_typed(pool, chain_id, router, 94, 0x54, true, 0x55).await;

    // code_change STOREFRONT_UPGRADE: storefront's own Upgraded.
    seed_upgraded(pool, chain_id, storefront, 95, 0x60).await;

    // sale: a purchase on the storefront for `token`, with a REAL payment
    // leg on `purchase_token` (a brand-new address — never touches
    // `token`'s own Transfer stream, so exposure_window/screening_gap stay
    // untouched). No token leg on purpose — a legitimate, tested NULL state.
    seed_event(
        pool,
        chain_id,
        purchase_token,
        "Transfer",
        96,
        0,
        0,
        serde_json::json!({ "from": addr_hex(0x70), "to": hex_of(storefront), "value": "100" }),
    )
    .await;
    seed_purchase_made(pool, chain_id, storefront, 96, 0, 1, token, 0x70, "20", "100").await;
}

#[allow(clippy::too_many_arguments)]
fn two_asset_config(
    chain_id: i64,
    module: [u8; 20],
    token: [u8; 20],
    module_b: [u8; 20],
    token_b: [u8; 20],
    sanctions_module: [u8; 20],
    router: [u8; 20],
    yield_blacklist_module: [u8; 20],
    yield_token: [u8; 20],
    storefront: [u8; 20],
    factory: [u8; 20],
    purchase_token: [u8; 20],
) -> Tier2Config {
    Tier2Config {
        chain_id,
        assets: vec![
            AssetSources {
                asset_id: "ASSET".to_string(),
                axis1_module: module,
                token_address: token,
                // P4b-i wiring — asset A only (keeps asset B's fixture, and
                // its existing OTHER-classification exposure window, untouched).
                router_address: Some(router),
                yield_tokens: vec![yield_token],
                yield_blacklist_modules: vec![yield_blacklist_module],
                // P4b-ii wiring — asset A only, same discipline.
                storefront: Some(storefront),
                purchase_tokens: vec![purchase_token],
                factory: Some(factory),
            },
            AssetSources {
                asset_id: "ASSET_B".to_string(),
                axis1_module: module_b,
                token_address: token_b,
                router_address: None,
                yield_tokens: vec![],
                yield_blacklist_modules: vec![],
                storefront: None,
                purchase_tokens: vec![],
                factory: None,
            },
        ],
        sanctions_modules: vec![sanctions_module],
        routers: vec![router],
        multisig: None,
    }
}

#[tokio::test]
async fn rebuild_determinism_same_db_retruncate() {
    let pool = fresh_test_db().await;
    let chain_id = 6001i64;
    let module = addr(0x11);
    let token = addr(0x22);
    let module_b = addr(0x55);
    let token_b = addr(0x66);
    let sanctions_module = addr(0x33);
    let router = addr(0x44);
    let yield_blacklist_module = addr(0x88);
    let yield_token = addr(0x77);
    let storefront = addr(0xAF);
    let factory = addr(0xEE);
    let purchase_token = addr(0xDD);

    seed_full_scenario(
        &pool,
        chain_id,
        module,
        token,
        module_b,
        token_b,
        sanctions_module,
        router,
        yield_blacklist_module,
        yield_token,
        storefront,
        factory,
        purchase_token,
    )
    .await;

    let config = two_asset_config(
        chain_id,
        module,
        token,
        module_b,
        token_b,
        sanctions_module,
        router,
        yield_blacklist_module,
        yield_token,
        storefront,
        factory,
        purchase_token,
    );

    rebuild_tier2(&pool, &config).await.unwrap();
    let first = snapshot_all_tier2(&pool, chain_id).await;
    assert!(
        first.contains("ISSUANCE"),
        "setup sanity: asset A's mint-only window must have classified ISSUANCE — got {first}"
    );
    assert!(
        first.contains("OTHER") && first.contains(&addr_hex(0xFE)),
        "setup sanity: asset B's stranger-transfer window must have classified OTHER with a \
         non-empty never_allowlisted_recipients flag — got {first}"
    );

    // P4b-i setup sanity — direct counts, not string-matching (the snapshot
    // itself is asserted for byte-identical re-rebuild below, not content).
    let yield_blacklist_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM audit.yield_blacklist_interval WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(yield_blacklist_count.0, 1, "one closed yield_blacklist_interval");

    let screening_count: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM audit.transfer_screening WHERE chain_id = $1")
            .bind(chain_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(screening_count.0, 1, "exactly one paired transfer_screening row");

    let run: (Option<bigdecimal::BigDecimal>, bigdecimal::BigDecimal, Option<bigdecimal::BigDecimal>, bool) =
        sqlx::query_as(
            "SELECT total_amount, credited_total, withheld_remainder, over_credit FROM audit.yield_run WHERE chain_id = $1",
        )
        .bind(chain_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(run.0, Some(bigdecimal::BigDecimal::from(40)));
    assert_eq!(run.1, bigdecimal::BigDecimal::from(30));
    assert_eq!(run.2, Some(bigdecimal::BigDecimal::from(10)));
    assert!(!run.3);

    // MED-2 (P4b-i review): screening_gap and yield_credit had ZERO
    // non-vacuous content assertions — the fixture PRODUCES both (the
    // block-32 mint sits unscreened inside the open router epoch; the
    // 2-tx yield walk produces two real credits), but nothing was checking
    // it. Real content, not just counts.
    let gap: (i64, Vec<u8>) = sqlx::query_as(
        "SELECT block_number, epoch_module FROM audit.screening_gap WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        gap.0, 32,
        "the block-32 mint (unscreened, inside the open router epoch) must be the gap"
    );
    assert_eq!(
        gap.1,
        addr(0xC1).to_vec(),
        "the gap's epoch_module must be the router's registered module"
    );

    let mut credit_rows: Vec<(i64, i64, Vec<u8>, bigdecimal::BigDecimal)> = sqlx::query_as(
        "SELECT run_seq, block_number, holder, share FROM audit.yield_credit WHERE chain_id = $1 ORDER BY block_number",
    )
    .bind(chain_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(credit_rows.len(), 2, "the 2-tx yield walk must produce exactly two credits");
    let (run_seq_1, block_1, holder_1, share_1) = credit_rows.remove(0);
    assert_eq!((run_seq_1, block_1), (0, 70));
    assert_eq!(holder_1, addr(0xE1).to_vec());
    assert_eq!(share_1, bigdecimal::BigDecimal::from(15));
    let (run_seq_2, block_2, holder_2, share_2) = credit_rows.remove(0);
    assert_eq!((run_seq_2, block_2), (0, 71));
    assert_eq!(holder_2, addr(0xE2).to_vec());
    assert_eq!(share_2, bigdecimal::BigDecimal::from(15));

    // P4b-ii — real content assertions, not just counts.
    let role_interval_count: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM audit.role_interval WHERE chain_id = $1")
            .bind(chain_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(role_interval_count.0, 1, "one closed role_interval (grant@80, revoke@85)");

    let code_change_count: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM audit.code_change WHERE chain_id = $1 AND asset_id = 'ASSET'")
            .bind(chain_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        code_change_count.0, 7,
        "UPGRADE (own@90 + factory-mediated@91, chained = 2 rows) + MODULE_SET (1) + \
         MODULE_LINKED (1) + ROUTER_GLOBAL (2 — the fixture's PRE-EXISTING \
         GLOBAL_SANCTIONS_TYPE registration@block1 PLUS this test's own typeId 0x54, \
         both fanned to ASSET via its router) + STOREFRONT_UPGRADE (1, fanned to ASSET \
         via its storefront) = 7"
    );

    let sale_row: (Vec<u8>, bigdecimal::BigDecimal, bigdecimal::BigDecimal, Option<i64>, Option<i64>) =
        sqlx::query_as(
            "SELECT buyer, amount, price_paid, payment_transfer_event, token_transfer_event FROM audit.sale WHERE chain_id = $1",
        )
        .bind(chain_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(sale_row.0, addr(0x70).to_vec());
    assert_eq!(sale_row.1, bigdecimal::BigDecimal::from(20));
    assert_eq!(sale_row.2, bigdecimal::BigDecimal::from(100));
    assert!(sale_row.3.is_some(), "the payment leg on `purchase_token` must pair");
    // NIT-2 (P4b-ii review): pin the deliberate NULL state, not just
    // the payment leg's presence — `seed_full_scenario` intentionally never
    // seeds a token-leg Transfer on `token` (would perturb screening_gap/
    // exposure_window, see that fn's own comment), so this MUST stay NULL.
    assert_eq!(
        sale_row.4, None,
        "no token-leg Transfer was ever seeded for this sale on purpose — must stay NULL"
    );

    let holder_balance_count: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM audit.holder_balance WHERE chain_id = $1 AND asset_id = 'ASSET'")
            .bind(chain_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(holder_balance_count.0 > 0, "holder_balance must derive non-trivial content from the existing Transfer fixture");

    // Re-run: TRUNCATE + recompute from the SAME chain_event content.
    rebuild_tier2(&pool, &config).await.unwrap();
    let second = snapshot_all_tier2(&pool, chain_id).await;

    assert_eq!(
        first, second,
        "TRUNCATE + rebuild from the same chain_event content must be byte-identical"
    );

    // And a third time, for good measure — proves it's not "stable across
    // exactly two runs by accident".
    rebuild_tier2(&pool, &config).await.unwrap();
    let third = snapshot_all_tier2(&pool, chain_id).await;
    assert_eq!(second, third);
}

// ---- (5) rebuild determinism ACROSS two independently-seeded DBs, with
// chain_event content inserted in a DIFFERENT order ------------------------

#[tokio::test]
async fn rebuild_determinism_cross_db_different_insert_order() {
    let chain_id = 6002i64;
    let module = addr(0x11);
    let token = addr(0x22);
    let module_b = addr(0x55);
    let token_b = addr(0x66);
    let sanctions_module = addr(0x33);
    let router = addr(0x44);
    let yield_blacklist_module = addr(0x88);
    let yield_token = addr(0x77);
    let storefront = addr(0xAF);
    let factory = addr(0xEE);
    let purchase_token = addr(0xDD);

    // DB #1: seed in the "natural" order used elsewhere in this file.
    let pool_a = fresh_test_db().await;
    seed_full_scenario(
        &pool_a,
        chain_id,
        module,
        token,
        module_b,
        token_b,
        sanctions_module,
        router,
        yield_blacklist_module,
        yield_token,
        storefront,
        factory,
        purchase_token,
    )
    .await;

    // DB #2: seed the exact same logical events, but in REVERSED order —
    // event_ids land completely differently (DB #2's first-inserted row is
    // DB #1's LAST-inserted row), which is exactly what an independent
    // from-genesis re-index looks like (IMPL-PLAN C1). Includes asset B's
    // events (H2, P2 review: the fixture must exercise a non-empty
    // `flags` value across independent DBs, not just the empty-flags
    // ISSUANCE case — otherwise this test is green by luck).
    let pool_b = fresh_test_db().await;
    // P4b-ii additions, reversed (these were appended LAST in
    // `seed_full_scenario`'s natural order, so they go FIRST here).
    seed_purchase_made(&pool_b, chain_id, storefront, 96, 0, 1, token, 0x70, "20", "100").await;
    seed_event(
        &pool_b,
        chain_id,
        purchase_token,
        "Transfer",
        96,
        0,
        0,
        serde_json::json!({ "from": addr_hex(0x70), "to": hex_of(storefront), "value": "100" }),
    )
    .await;
    seed_upgraded(&pool_b, chain_id, storefront, 95, 0x60).await;
    seed_module_type_registered_typed(&pool_b, chain_id, router, 94, 0x54, true, 0x55).await;
    seed_module_linked(&pool_b, chain_id, factory, 93, token, 0x52, 0x53).await;
    seed_module_set(&pool_b, chain_id, token, 92, 0x50, 0x51).await;
    seed_token_upgraded(&pool_b, chain_id, factory, 91, token, 0x02).await;
    seed_upgraded(&pool_b, chain_id, token, 90, 0x01).await;
    seed_role_revoked(&pool_b, chain_id, router, 85, 0, 0, 0x01, 0xE9).await;
    seed_role_granted(&pool_b, chain_id, router, 80, 0, 0, 0x01, 0xE9).await;
    seed_yield_distributed(&pool_b, chain_id, token, 72, yield_token, "40").await;
    seed_yield_credit(&pool_b, chain_id, yield_token, 71, token, 0xE2, "15").await;
    seed_yield_credit(&pool_b, chain_id, yield_token, 70, token, 0xE1, "15").await;
    seed_transfer_screened(&pool_b, chain_id, sanctions_module, 61, 0, 0, 0xF3, 0xF4).await;
    seed_transfer_at(&pool_b, chain_id, token, 60, 0, 1, 0xF1, 0xF2, "777").await;
    seed_transfer_screened(&pool_b, chain_id, sanctions_module, 60, 0, 0, 0xF1, 0xF2).await;
    seed_yield_blacklist_toggle(&pool_b, chain_id, yield_blacklist_module, 4, 0xD1, false).await;
    seed_yield_blacklist_toggle(&pool_b, chain_id, yield_blacklist_module, 3, 0xD1, true).await;
    seed_gate_toggle(&pool_b, chain_id, module_b, 41, false).await;
    seed_transfer(&pool_b, chain_id, token_b, 35, 0x01, 0xFE).await;
    seed_gate_toggle(&pool_b, chain_id, module_b, 33, true).await;
    seed_gate_toggle(&pool_b, chain_id, module_b, 6, false).await;
    seed_gate_toggle(&pool_b, chain_id, module, 40, false).await;
    seed_transfer(&pool_b, chain_id, token, 32, 0x00, 0xA2).await;
    seed_gate_toggle(&pool_b, chain_id, module, 30, true).await;
    seed_gate_toggle(&pool_b, chain_id, module, 5, false).await;
    seed_whitelist_toggle(&pool_b, chain_id, module, 15, 0xA2, true).await;
    seed_whitelist_toggle(&pool_b, chain_id, module, 20, 0xA1, false).await;
    seed_whitelist_toggle(&pool_b, chain_id, module, 10, 0xA1, true).await;
    seed_sanction(&pool_b, chain_id, sanctions_module, 22, 0xB1, false).await;
    seed_sanction(&pool_b, chain_id, sanctions_module, 12, 0xB1, true).await;
    seed_module_type_registered(&pool_b, chain_id, router, 1, 0xC1).await;

    let config = two_asset_config(
        chain_id,
        module,
        token,
        module_b,
        token_b,
        sanctions_module,
        router,
        yield_blacklist_module,
        yield_token,
        storefront,
        factory,
        purchase_token,
    );

    rebuild_tier2(&pool_a, &config).await.unwrap();
    rebuild_tier2(&pool_b, &config).await.unwrap();

    // VALUE-ONLY comparison (never opened_by_event/closed_by_event — those
    // are surrogate event_ids, legitimately different across two
    // independently-seeded databases; C1 discipline).
    async fn value_snapshot(pool: &PgPool, chain_id: i64) -> String {
        let allowlist: Vec<(String, Vec<u8>, i64, Option<i64>)> = sqlx::query_as(
            "SELECT asset_id, address, from_block, to_block FROM audit.allowlist_interval WHERE chain_id = $1 ORDER BY asset_id, address, from_block",
        ).bind(chain_id).fetch_all(pool).await.unwrap();
        let gate: Vec<(String, bool, i64, Option<i64>)> = sqlx::query_as(
            "SELECT asset_id, gated, from_block, to_block FROM audit.gate_interval WHERE chain_id = $1 ORDER BY asset_id, from_block",
        ).bind(chain_id).fetch_all(pool).await.unwrap();
        let denyset: Vec<AddrAddrBlockRange> = sqlx::query_as(
            "SELECT module_address, address, from_block, to_block FROM audit.sanction_denyset_interval WHERE chain_id = $1 ORDER BY module_address, address, from_block",
        ).bind(chain_id).fetch_all(pool).await.unwrap();
        let router_epoch: Vec<AddrAddrBlockRange> = sqlx::query_as(
            "SELECT router_address, module_address, from_block, to_block FROM audit.router_sanctions_epoch WHERE chain_id = $1 ORDER BY router_address, from_block",
        ).bind(chain_id).fetch_all(pool).await.unwrap();
        let exposure: Vec<(String, i64, Option<i64>, String, serde_json::Value)> = sqlx::query_as(
            "SELECT asset_id, open_block, close_block, pattern, flags FROM audit.exposure_window WHERE chain_id = $1 ORDER BY asset_id, open_block",
        ).bind(chain_id).fetch_all(pool).await.unwrap();

        // P4b-i tables — VALUE-only, same discipline (never an event_id FK column).
        let yield_blacklist: Vec<(String, Vec<u8>, i64, Option<i64>)> = sqlx::query_as(
            "SELECT asset_id, address, from_block, to_block FROM audit.yield_blacklist_interval WHERE chain_id = $1 ORDER BY asset_id, address, from_block",
        ).bind(chain_id).fetch_all(pool).await.unwrap();
        let screening: Vec<(String, i64, i32, i32, i32, Vec<u8>)> = sqlx::query_as(
            "SELECT asset_id, block_number, tx_index, screening_log_index, transfer_log_index, module_address FROM audit.transfer_screening WHERE chain_id = $1 ORDER BY asset_id, block_number, tx_index, screening_log_index",
        ).bind(chain_id).fetch_all(pool).await.unwrap();
        let gaps: Vec<(String, i64, i32, i32, Vec<u8>)> = sqlx::query_as(
            "SELECT asset_id, block_number, tx_index, log_index, epoch_module FROM audit.screening_gap WHERE chain_id = $1 ORDER BY asset_id, block_number, tx_index, log_index",
        ).bind(chain_id).fetch_all(pool).await.unwrap();
        let yield_runs: Vec<YieldRunSnapshotRow> = sqlx::query_as(
            "SELECT asset_id, yield_token, run_seq, total_amount, credited_total, withheld_remainder, over_credit FROM audit.yield_run WHERE chain_id = $1 ORDER BY asset_id, yield_token, run_seq",
        ).bind(chain_id).fetch_all(pool).await.unwrap();
        let yield_credits: Vec<YieldCreditSnapshotRow> = sqlx::query_as(
            "SELECT asset_id, yield_token, run_seq, block_number, tx_index, log_index, holder, share FROM audit.yield_credit WHERE chain_id = $1 ORDER BY asset_id, yield_token, run_seq, block_number, tx_index, log_index",
        ).bind(chain_id).fetch_all(pool).await.unwrap();

        // P4b-ii tables — VALUE-only, same discipline (never an event_id FK column).
        let role_intervals: Vec<RoleIntervalSnapshotRow> = sqlx::query_as(
            "SELECT source_contract, role, account, from_block, to_block FROM audit.role_interval WHERE chain_id = $1 ORDER BY source_contract, role, account, from_block",
        ).bind(chain_id).fetch_all(pool).await.unwrap();
        let code_changes: Vec<CodeChangeSnapshotRow> = sqlx::query_as(
            "SELECT asset_id, kind, subject, block_number, tx_index, log_index, before, after, signer_attribution FROM audit.code_change WHERE chain_id = $1 ORDER BY asset_id, kind, subject, block_number, tx_index, log_index",
        ).bind(chain_id).fetch_all(pool).await.unwrap();
        let sales: Vec<SaleSnapshotRow> = sqlx::query_as(
            "SELECT asset_id, block_number, tx_index, purchase_log_index, buyer, amount, price_paid FROM audit.sale WHERE chain_id = $1 ORDER BY asset_id, block_number, tx_index, purchase_log_index",
        ).bind(chain_id).fetch_all(pool).await.unwrap();
        let holder_balances: Vec<(String, Vec<u8>, i64, bigdecimal::BigDecimal, bool)> = sqlx::query_as(
            "SELECT asset_id, address, block_number, balance, integrity_alarm FROM audit.holder_balance WHERE chain_id = $1 ORDER BY asset_id, address, block_number",
        ).bind(chain_id).fetch_all(pool).await.unwrap();

        format!(
            "{allowlist:?}|{gate:?}|{denyset:?}|{router_epoch:?}|{exposure:?}|{yield_blacklist:?}|{screening:?}|{gaps:?}|{yield_runs:?}|{yield_credits:?}|\
             {role_intervals:?}|{code_changes:?}|{sales:?}|{holder_balances:?}"
        )
    }

    let snap_a = value_snapshot(&pool_a, chain_id).await;
    let snap_b = value_snapshot(&pool_b, chain_id).await;

    assert!(
        snap_a.contains("OTHER"),
        "setup sanity: asset B's third-party window must be present with a non-empty flags \
         value — got {snap_a}"
    );
    assert!(
        snap_a.contains("60, 0, 0, 1"),
        "setup sanity: the paired transfer_screening row (block 60, screening@log0, transfer@log1) \
         must be present — got {snap_a}"
    );
    assert!(
        snap_a.contains("UPGRADE") && snap_a.contains("UNKNOWN"),
        "setup sanity: the UPGRADE code_change chain (unattributed — no role/multisig \
         configured in this fixture) must be present — got {snap_a}"
    );

    assert_eq!(
        snap_a, snap_b,
        "the same logical chain_event content, seeded in a different order into an \
         independent DB (different event_ids throughout), must rebuild to identical \
         Tier-2 VALUES — proves determinism keys on (block_number, tx_index, log_index), \
         never on insertion order or surrogate event_id. This now covers a NON-EMPTY flags \
         value (asset B's OTHER window) across the two independent DBs (H2)."
    );
}

// ---- L2 (P4b-i review): a full uint256 share round-trips through
// NUMERIC(78,0) via a REAL rebuild against REAL Postgres — the in-memory
// unit test (`yield_run::tests::uint256_round_trips_through_numeric_78_0`)
// proves the builder computes it right; this proves the DB column can
// actually hold it and hand it back byte-for-byte, never truncated by an
// i64/u128 anywhere on the write or read path. -----------------------------

#[tokio::test]
async fn yield_credit_share_round_trips_full_uint256_via_rebuild() {
    let pool = fresh_test_db().await;
    let chain_id = 8001i64;
    let module = addr(0x11);
    let token = addr(0x22);
    let yield_token = addr(0x77);

    // 2^200 — comfortably bigger than u128::MAX (2^128-1), well inside
    // NUMERIC(78,0)'s 78-digit ceiling (2^256-1 is 78 digits).
    let huge_share = "1606938044258990275541962092341162602522202993782792835301376";
    seed_yield_credit(&pool, chain_id, yield_token, 10, token, 0xE9, huge_share).await;

    let config = Tier2Config {
        chain_id,
        assets: vec![AssetSources {
            asset_id: format!("{chain_id}:{}", addr_hex(0xAB)),
            axis1_module: module,
            token_address: token,
            router_address: None,
            yield_tokens: vec![yield_token],
            yield_blacklist_modules: vec![],
            storefront: None,
            purchase_tokens: vec![],
            factory: None,
        }],
        sanctions_modules: vec![],
        routers: vec![],
        multisig: None,
    };
    rebuild_tier2(&pool, &config).await.unwrap();

    let share: (bigdecimal::BigDecimal,) =
        sqlx::query_as("SELECT share FROM audit.yield_credit WHERE chain_id = $1")
            .bind(chain_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        share.0,
        bigdecimal::BigDecimal::from_str(huge_share).unwrap(),
        "a share far past u128::MAX must round-trip through NUMERIC(78,0) byte-for-byte"
    );
}

// ---- P4b-ii test 7: role_interval built ONCE per shared source ----------
//
// Two DIFFERENT assets share ONE storefront. `role_interval`'s PK is
// `(chain_id, source_contract, role, account, from_block)` — a caller that
// (incorrectly) built the role-interval chain once PER ASSET rather than
// once per DISTINCT source would attempt to INSERT the exact same row
// TWICE, violating that PK. This test proves `rebuild_tier2` does the
// right thing: exactly ONE row for the shared storefront, not two.

#[tokio::test]
async fn role_interval_built_once_per_shared_source() {
    let pool = fresh_test_db().await;
    let chain_id = 8101i64;
    let shared_storefront = addr(0x99);
    let module_a = addr(0x11);
    let token_a = addr(0x22);
    let module_b = addr(0x33);
    let token_b = addr(0x44);

    seed_role_granted(&pool, chain_id, shared_storefront, 100, 0, 0, 0x01, 0xAA).await;

    let config = Tier2Config {
        chain_id,
        assets: vec![
            AssetSources {
                asset_id: "A".to_string(),
                axis1_module: module_a,
                token_address: token_a,
                router_address: None,
                yield_tokens: vec![],
                yield_blacklist_modules: vec![],
                storefront: Some(shared_storefront),
                purchase_tokens: vec![],
                factory: None,
            },
            AssetSources {
                asset_id: "B".to_string(),
                axis1_module: module_b,
                token_address: token_b,
                router_address: None,
                yield_tokens: vec![],
                yield_blacklist_modules: vec![],
                storefront: Some(shared_storefront),
                purchase_tokens: vec![],
                factory: None,
            },
        ],
        sanctions_modules: vec![],
        routers: vec![],
        multisig: None,
    };

    // If `rebuild_tier2` (incorrectly) built role_interval per-asset, this
    // would fail with a PK violation instead of succeeding.
    rebuild_tier2(&pool, &config).await.unwrap();

    let rows: Vec<(Vec<u8>, i64, Option<i64>)> = sqlx::query_as(
        "SELECT source_contract, from_block, to_block FROM audit.role_interval WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        rows,
        vec![(shared_storefront.to_vec(), 100, None)],
        "exactly ONE row for the shared storefront — never one per asset that references it"
    );
}

// ---- P4b-ii test 29: `sale`'s shared payment pool never gets stolen ------
//
// Two assets sold through the SAME storefront, paid in the SAME purchase
// token, in ONE tx. Deleting asset B's own legs (leaving B's PurchaseMade
// with NOTHING to pair) must leave B's `payment_transfer_event` NULL — it
// must NEVER silently steal asset A's payment leg.

#[tokio::test]
async fn one_tx_buying_two_assets_shares_one_payment_pool() {
    let pool = fresh_test_db().await;
    let chain_id = 8102i64;
    let storefront = addr(0x99);
    let purchase_token = addr(0xDD);
    let module_a = addr(0x11);
    let token_a = addr(0x22);
    let module_b = addr(0x33);
    let token_b = addr(0x44);
    let buyer = 0x70u8;

    // ONE tx: A's payment leg, A's purchase, B's purchase — B has NO
    // payment leg of its own (deliberately never seeded).
    seed_event(
        &pool,
        chain_id,
        purchase_token,
        "Transfer",
        100,
        0,
        0,
        serde_json::json!({ "from": addr_hex(buyer), "to": hex_of(storefront), "value": "1" }),
    )
    .await;
    seed_purchase_made(&pool, chain_id, storefront, 100, 0, 1, token_a, buyer, "5", "1").await;
    seed_purchase_made(&pool, chain_id, storefront, 100, 0, 2, token_b, buyer, "9", "1").await;

    let config = Tier2Config {
        chain_id,
        assets: vec![
            AssetSources {
                asset_id: "A".to_string(),
                axis1_module: module_a,
                token_address: token_a,
                router_address: None,
                yield_tokens: vec![],
                yield_blacklist_modules: vec![],
                storefront: Some(storefront),
                purchase_tokens: vec![purchase_token],
                factory: None,
            },
            AssetSources {
                asset_id: "B".to_string(),
                axis1_module: module_b,
                token_address: token_b,
                router_address: None,
                yield_tokens: vec![],
                yield_blacklist_modules: vec![],
                storefront: Some(storefront),
                purchase_tokens: vec![purchase_token],
                factory: None,
            },
        ],
        sanctions_modules: vec![],
        routers: vec![],
        multisig: None,
    };
    rebuild_tier2(&pool, &config).await.unwrap();

    let rows: Vec<(String, Option<i64>)> = sqlx::query_as(
        "SELECT asset_id, payment_transfer_event FROM audit.sale WHERE chain_id = $1 ORDER BY asset_id",
    )
    .bind(chain_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, "A");
    assert!(rows[0].1.is_some(), "A's purchase must claim the payment leg — it was FIRST in the tx");
    assert_eq!(rows[1].0, "B");
    assert_eq!(rows[1].1, None, "B's purchase must NEVER steal A's already-consumed payment leg");
}

// ---- P4b-ii test 35: holder_balance round-trips a full uint256 ----------

#[tokio::test]
async fn holder_balance_round_trips_full_uint256() {
    let pool = fresh_test_db().await;
    let chain_id = 8103i64;
    let module = addr(0x11);
    let token = addr(0x22);
    let max_u256 = "115792089237316195423570985008687907853269984665640564039457584007913129639935";

    seed_transfer_at(&pool, chain_id, token, 10, 0, 0, 0x00, 0xE9, max_u256).await;

    let config = config_one_asset(chain_id, module, token);
    rebuild_tier2(&pool, &config).await.unwrap();

    let row: (Vec<u8>, bigdecimal::BigDecimal, bool) = sqlx::query_as(
        "SELECT address, balance, integrity_alarm FROM audit.holder_balance WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.0, addr(0xE9).to_vec());
    assert_eq!(
        row.1,
        bigdecimal::BigDecimal::from_str(max_u256).unwrap(),
        "2^256-1 must round-trip through NUMERIC(78,0) byte-for-byte"
    );
    assert!(!row.2, "a mint's ending balance is positive — no integrity alarm");
}

// ---- HIGH-1 (P4b-ii review): signer_attribution WIRING reaches
// rebuild_tier2 ---------------------------------------------------------
//
// The attribution FUNCTION (`code_change::signer_attribution`) is
// unit-tested in isolation (tests 16-22), but nothing proved
// `rebuild_tier2` actually threads `role_sources` / `role_intervals_by_source`
// / `config.multisig` into it — every existing DB fixture used
// `multisig: None` plus the default seeded signer (0x99), which never
// matches any granted role, so every code_change row landed UNKNOWN
// regardless of whether the wiring was even present. This test uses
// `seed_event_signed` with a REAL signer to prove three things at once:
// (1) a role granted on asset A's own token attributes A's OWN code_change
// row to ISSUER_KEY; (2) the SAME event, signed by the SAME signer, fanned
// to asset B (which shares A's router but has NO role for that signer
// anywhere in ITS OWN sources) stays UNKNOWN — proving per-asset scoping,
// not a chain-global role bag; (3) re-running with `multisig: Some(signer)`
// flips A's attribution to ROME_MULTISIG (checked BEFORE any role lookup).

#[tokio::test]
async fn signer_attribution_wiring_reaches_rebuild_tier2() {
    let pool = fresh_test_db().await;
    let chain_id = 8104i64;
    let module_a = addr(0x11);
    let token_a = addr(0x22);
    let module_b = addr(0x33);
    let token_b = addr(0x44);
    let router = addr(0x55);
    let signer = addr(0xAB);

    // A role for `signer` on asset A's OWN token, open from block 10 —
    // NOT granted anywhere in asset B's own sources.
    seed_role_granted(&pool, chain_id, token_a, 10, 0, 0, 0x01, 0xAB).await;

    // A ROUTER_GLOBAL-triggering event on the SHARED router, signed by
    // `signer`, at block 15 (after the grant) — fans out to BOTH assets.
    seed_event_signed(
        &pool,
        chain_id,
        router,
        "ModuleTypeRegistered",
        15,
        0,
        0,
        serde_json::json!({
            "typeId": format!("0x{}", hex::encode([0x60u8; 32])),
            "isGlobal": true,
            "globalImplementation": addr_hex(0x61),
        }),
        signer,
    )
    .await;

    // Asset A's OWN Upgraded, signed by `signer`, at block 20 (after the grant).
    seed_event_signed(
        &pool,
        chain_id,
        token_a,
        "Upgraded",
        20,
        0,
        0,
        serde_json::json!({ "implementation": addr_hex(0x62) }),
        signer,
    )
    .await;

    fn config(chain_id: i64, module_a: [u8; 20], token_a: [u8; 20], module_b: [u8; 20], token_b: [u8; 20], router: [u8; 20], multisig: Option<[u8; 20]>) -> Tier2Config {
        Tier2Config {
            chain_id,
            assets: vec![
                AssetSources {
                    asset_id: "A".to_string(),
                    axis1_module: module_a,
                    token_address: token_a,
                    router_address: Some(router),
                    yield_tokens: vec![],
                    yield_blacklist_modules: vec![],
                    storefront: None,
                    purchase_tokens: vec![],
                    factory: None,
                },
                AssetSources {
                    asset_id: "B".to_string(),
                    axis1_module: module_b,
                    token_address: token_b,
                    router_address: Some(router),
                    yield_tokens: vec![],
                    yield_blacklist_modules: vec![],
                    storefront: None,
                    purchase_tokens: vec![],
                    factory: None,
                },
            ],
            sanctions_modules: vec![],
            routers: vec![router],
            multisig,
        }
    }

    async fn attribution(pool: &PgPool, chain_id: i64, asset_id: &str, kind: &str) -> String {
        let row: (String,) = sqlx::query_as(
            "SELECT signer_attribution FROM audit.code_change WHERE chain_id = $1 AND asset_id = $2 AND kind = $3",
        )
        .bind(chain_id)
        .bind(asset_id)
        .bind(kind)
        .fetch_one(pool)
        .await
        .unwrap();
        row.0
    }

    // Phase 1: no multisig configured — role-based attribution only.
    rebuild_tier2(&pool, &config(chain_id, module_a, token_a, module_b, token_b, router, None))
        .await
        .unwrap();

    assert_eq!(
        attribution(&pool, chain_id, "A", "UPGRADE").await,
        "ISSUER_KEY",
        "signer holds a role on asset A's OWN token — must attribute ISSUER_KEY"
    );
    assert_eq!(
        attribution(&pool, chain_id, "B", "ROUTER_GLOBAL").await,
        "UNKNOWN",
        "the SAME signer, on the SAME fanned-out router event, but asset B has NO role for \
         this signer anywhere in ITS OWN sources — must stay UNKNOWN, never bleed A's role in"
    );
    assert_eq!(
        attribution(&pool, chain_id, "A", "ROUTER_GLOBAL").await,
        "ISSUER_KEY",
        "asset A's OWN role source (its token) covers the fanned router event too"
    );

    // Phase 2: SAME chain_event content, re-rebuilt with multisig = signer —
    // multisig is checked BEFORE any role lookup, so A's UPGRADE flips.
    rebuild_tier2(
        &pool,
        &config(chain_id, module_a, token_a, module_b, token_b, router, Some(signer)),
    )
    .await
    .unwrap();

    assert_eq!(
        attribution(&pool, chain_id, "A", "UPGRADE").await,
        "ROME_MULTISIG",
        "multisig = signer must override the role-based ISSUER_KEY attribution"
    );
}

// ---- (6) idempotent re-run: two consecutive rebuilds never duplicate ----

#[tokio::test]
async fn rebuild_is_idempotent_no_duplicate_or_double_close() {
    let pool = fresh_test_db().await;
    let chain_id = 7001i64;
    let module = addr(0x11);
    let token = addr(0x22);

    seed_whitelist_toggle(&pool, chain_id, module, 100, 0xAA, true).await;
    seed_whitelist_toggle(&pool, chain_id, module, 200, 0xAA, false).await;

    let config = config_one_asset(chain_id, module, token);
    rebuild_tier2(&pool, &config).await.unwrap();
    rebuild_tier2(&pool, &config).await.unwrap();

    let count: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM audit.allowlist_interval WHERE chain_id = $1")
            .bind(chain_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        count.0, 1,
        "re-running rebuild must never duplicate an interval"
    );
}

// ---- M3 (P4a): rebuild_tier2 is chain-SCOPED, not a global TRUNCATE ------
//
// `audit` lives in the SHARED rome_via_db — a second chain's own Tier-2
// rebuild must never wipe another chain's already-built Tier-2 rows.

#[tokio::test]
async fn rebuild_tier2_is_chain_scoped() {
    let pool = fresh_test_db().await;
    let chain_a = 7001i64;
    let chain_b = 7002i64;

    let module_a = addr(0x11);
    let token_a = addr(0x22);
    let module_b = addr(0x33);
    let token_b = addr(0x44);
    let storefront_b = addr(0x66);

    seed_whitelist_toggle(&pool, chain_a, module_a, 100, 0xAA, true).await;
    seed_whitelist_toggle(&pool, chain_b, module_b, 100, 0xBB, true).await;
    // P4b-i: extend the chain-scoping proof to a NEW table too — a
    // yield_blacklist_interval row on chain B (module_b doubles as the
    // yield-blacklist module here; a test-only convenience, no real
    // semantics attached to address reuse).
    seed_yield_blacklist_toggle(&pool, chain_b, module_b, 100, 0xCC, true).await;
    // P4b-ii: extend the chain-scoping proof to the 4 NEW tables too.
    seed_role_granted(&pool, chain_b, module_b, 100, 0, 0, 0x01, 0xDD).await;
    seed_role_revoked(&pool, chain_b, module_b, 150, 0, 0, 0x01, 0xDD).await;
    seed_upgraded(&pool, chain_b, token_b, 100, 0x01).await;
    seed_transfer(&pool, chain_b, token_b, 100, 0x00, 0xEE).await; // mint -> holder_balance
    seed_purchase_made(&pool, chain_b, storefront_b, 100, 0, 0, token_b, 0x70, "5", "10").await;

    let config_a = config_one_asset(chain_a, module_a, token_a);
    let config_b = Tier2Config {
        chain_id: chain_b,
        assets: vec![AssetSources {
            asset_id: format!("{chain_b}:{}", addr_hex(0xAB)),
            axis1_module: module_b,
            token_address: token_b,
            router_address: None,
            yield_tokens: vec![],
            yield_blacklist_modules: vec![module_b],
            storefront: Some(storefront_b),
            purchase_tokens: vec![],
            factory: None,
        }],
        sanctions_modules: vec![],
        routers: vec![],
        multisig: None,
    };

    rebuild_tier2(&pool, &config_a).await.unwrap();
    rebuild_tier2(&pool, &config_b).await.unwrap();

    let count_b_before: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM audit.allowlist_interval WHERE chain_id = $1")
            .bind(chain_b)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count_b_before.0, 1, "setup: chain B's row landed");
    let yield_blacklist_count_before: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM audit.yield_blacklist_interval WHERE chain_id = $1",
    )
    .bind(chain_b)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(yield_blacklist_count_before.0, 1, "setup: chain B's yield_blacklist_interval row landed");

    async fn p4bii_counts(pool: &PgPool, chain_id: i64) -> (i64, i64, i64, i64) {
        let role: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM audit.role_interval WHERE chain_id = $1")
            .bind(chain_id)
            .fetch_one(pool)
            .await
            .unwrap();
        let code: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM audit.code_change WHERE chain_id = $1")
            .bind(chain_id)
            .fetch_one(pool)
            .await
            .unwrap();
        let sale: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM audit.sale WHERE chain_id = $1")
            .bind(chain_id)
            .fetch_one(pool)
            .await
            .unwrap();
        let holder: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM audit.holder_balance WHERE chain_id = $1")
            .bind(chain_id)
            .fetch_one(pool)
            .await
            .unwrap();
        (role.0, code.0, sale.0, holder.0)
    }

    let p4bii_before = p4bii_counts(&pool, chain_b).await;
    assert_eq!(p4bii_before, (1, 1, 1, 1), "setup: chain B's role_interval/code_change/sale/holder_balance rows landed");

    // A second rebuild of chain A ONLY must not touch chain B's rows.
    rebuild_tier2(&pool, &config_a).await.unwrap();

    let count_b_after: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM audit.allowlist_interval WHERE chain_id = $1")
            .bind(chain_b)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        count_b_after.0, 1,
        "chain B's Tier-2 row must survive chain A's rebuild"
    );
    let yield_blacklist_count_after: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM audit.yield_blacklist_interval WHERE chain_id = $1",
    )
    .bind(chain_b)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        yield_blacklist_count_after.0, 1,
        "chain B's yield_blacklist_interval row must ALSO survive chain A's rebuild — proves the \
         chain-scoped-delete discipline (P4a M3) was applied to the new P4b-i table too"
    );

    let p4bii_after = p4bii_counts(&pool, chain_b).await;
    assert_eq!(
        p4bii_after, (1, 1, 1, 1),
        "chain B's role_interval/code_change/sale/holder_balance rows must ALSO survive chain \
         A's rebuild — proves the chain-scoped-delete discipline (P4a M3) was applied to all \
         four NEW P4b-ii tables too"
    );
}
