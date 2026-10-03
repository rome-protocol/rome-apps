//! Shared integration-test scaffolding (P3c a.8 prerequisite refactor):
//! the disposable-Postgres provisioning + Hercules-fixture seeding helpers
//! that used to live only in `tests/ingest_db.rs`, now also needed by
//! `tests/resolve_ingest_db.rs` (the resolve→ingest worker tests drive the
//! SAME Hercules-shaped fixture schema). `tests/ingest_db.rs` delegates to
//! this module rather than keeping its own copy.
//!
//! Lives at `tests/common/mod.rs` (a directory, not `tests/common.rs`)
//! specifically so cargo does NOT treat it as its own top-level test binary
//! — the standard "shared test helper module" convention.
//!
//! **Test-double placement (P3c verify-item #3):** this module ALSO carries
//! its own `FakeRpc`/`FakeRegistry` for [`rome_audit::resolve::rpc::ResolverRpc`]/
//! [`rome_audit::resolve::registry_source::RegistrySource`], separate from
//! `src/resolve/fixture.rs`'s `#[cfg(test)]`-gated fakes. Chosen over
//! un-gating `fixture.rs` into the shipped binary: nothing here forces
//! single-sourcing (the no-hardcode canary that depends on
//! `fixture.rs::FakeRpc::known_addresses()` is a `resolver.rs` UNIT test —
//! compiled with the crate itself under `--cfg test`, so it never needs this
//! module at all) and integration tests need a genuinely different
//! capability the unit-test fixture doesn't (`&self` + `Mutex`-backed
//! setters plus `set_fail`, so a single already-`Arc`'d `dyn ResolverRpc`
//! can script failure→recovery across a running worker's ticks — a
//! `&mut self` fixture can't do that once shared behind an `Arc`). The
//! trade-off, named honestly: this duplicates a subset of `fixture.rs`'s
//! address-bookkeeping logic; kept deliberately smaller (no UV2/Morpho
//! fixed-point scripting) since the worker tests only need `resolve()` to
//! succeed end-to-end, not to re-prove P3b's own discovery-algorithm tests.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;

use sqlx::PgPool;

use rome_audit::resolve::registry_source::RegistrySource;
use rome_audit::resolve::rpc::{LogEntry, MarketParams, ResolverRpc, RpcError};

// ---- Test-DB provisioning ---------------------------------------------
//
// Points at the disposable `rome-audit-test-pg` Docker container. Override
// with ROME_AUDIT_TEST_PG_ADMIN_URL if needed.

pub fn admin_url() -> String {
    std::env::var("ROME_AUDIT_TEST_PG_ADMIN_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:55432/postgres".to_string())
}

pub fn unique_db_name(prefix: &str) -> String {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}_{}_{}_{}", std::process::id(), nanos, n)
}

/// Owns one disposable per-test Postgres database and `DROP`s it when this
/// guard goes out of scope — including on test panic, since `Drop` still
/// runs on unwind. Every provisioning helper below (`fresh_empty_db`,
/// `fresh_audit_db`, `fresh_hercules_audit_db`) returns one of these instead
/// of a bare `PgPool`, so a leaked database is now a compile-time-impossible
/// outcome rather than something each test file has to remember to clean up
/// (the bug this type exists to fix: every `CREATE DATABASE` in this crate's
/// tests used to have zero matching `DROP DATABASE`, leaking dozens–hundreds
/// of databases per full suite run into the shared CI Postgres until it
/// starved and the `unit-test` CI job hung).
///
/// Derefs to the wrapped `PgPool` so this is a drop-in replacement at every
/// existing call site: `&pool` still coerces to `&PgPool`, and `pool.clone()`
/// / `pool.begin()` still resolve through autoderef to the real `PgPool`
/// methods. The one place deref coercion doesn't reach is a `PgPool`-generic
/// `sqlx::Executor`/`Acquire` bound (`fetch_all`, `execute`, `insert_capture_manifest<E>`,
/// …) — those call sites need an explicit `&*pool`, same idiom sqlx itself
/// documents for `Transaction`/`PoolConnection` not implementing `Executor`
/// directly.
#[derive(Debug)]
pub struct TestDb {
    pool: PgPool,
    db_name: String,
}

impl TestDb {
    /// The database's generated name — lets a caller (e.g. the teardown
    /// anchor test) verify independently, via its own admin connection, that
    /// `Drop` actually removed it.
    pub fn name(&self) -> &str {
        &self.db_name
    }
}

impl std::ops::Deref for TestDb {
    type Target = PgPool;
    fn deref(&self) -> &PgPool {
        &self.pool
    }
}

// `sqlx::Executor`/`Acquire` are implemented for `&PgPool`, not for
// arbitrary `Deref<Target = PgPool>` types (no blanket impl exists) — a
// generic-executor call site (`insert_capture_manifest::<E>`, `.fetch_all`,
// `Migrator::run`, …) needs an ACTUAL trait impl, deref coercion alone only
// reaches concrete `&PgPool`-typed parameters. Both impls below just
// forward to `&self.pool`'s own impl one level in, the same "immediately
// clone the underlying `Pool`, so the returned future doesn't literally
// borrow `self`" trick sqlx's own `&Pool` impls use — which is why this
// compiles for an unconstrained lifetime despite `self` being a short-lived
// reference.
impl<'p> sqlx::Executor<'p> for &'_ TestDb {
    type Database = sqlx::Postgres;

    fn fetch_many<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> futures_core::stream::BoxStream<
        'e,
        Result<sqlx::Either<sqlx::postgres::PgQueryResult, sqlx::postgres::PgRow>, sqlx::Error>,
    >
    where
        E: 'q + sqlx::Execute<'q, Self::Database>,
    {
        (&self.pool).fetch_many(query)
    }

    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> futures_core::future::BoxFuture<'e, Result<Option<sqlx::postgres::PgRow>, sqlx::Error>>
    where
        E: 'q + sqlx::Execute<'q, Self::Database>,
    {
        (&self.pool).fetch_optional(query)
    }

    fn prepare_with<'e, 'q: 'e>(
        self,
        sql: &'q str,
        parameters: &'e [<Self::Database as sqlx::Database>::TypeInfo],
    ) -> futures_core::future::BoxFuture<
        'e,
        Result<<Self::Database as sqlx::Database>::Statement<'q>, sqlx::Error>,
    > {
        (&self.pool).prepare_with(sql, parameters)
    }

    fn describe<'e, 'q: 'e>(
        self,
        sql: &'q str,
    ) -> futures_core::future::BoxFuture<'e, Result<sqlx::Describe<Self::Database>, sqlx::Error>> {
        (&self.pool).describe(sql)
    }
}

impl<'a> sqlx::Acquire<'a> for &'_ TestDb {
    type Database = sqlx::Postgres;
    type Connection = sqlx::pool::PoolConnection<sqlx::Postgres>;

    fn acquire(self) -> futures_core::future::BoxFuture<'a, Result<Self::Connection, sqlx::Error>> {
        // `Pool::acquire` is an INHERENT method (bare `impl Future`, not
        // boxed) — goes through `sqlx::Acquire::acquire` explicitly so we
        // get the already-boxed `'static` future the trait impl for
        // `&PgPool` returns (same shape this method must return).
        sqlx::Acquire::acquire(&self.pool)
    }

    fn begin(
        self,
    ) -> futures_core::future::BoxFuture<'a, Result<sqlx::Transaction<'a, Self::Database>, sqlx::Error>> {
        // Same reasoning as `acquire` above — `Pool` also has an inherent,
        // non-boxed `begin`, so go through the trait explicitly.
        sqlx::Acquire::begin(&self.pool)
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        let db_name = self.db_name.clone();
        let db_name_for_log = db_name.clone();

        // Deliberately NOT `self.pool.clone()` + `pool.close()` here: every
        // `tokio::net::TcpStream` backing this pool's connections is
        // registered with the I/O driver of the runtime that created it —
        // the TEST's own runtime, not the fresh one this `Drop` spins up
        // below. Driving that pool's connections from a DIFFERENT runtime's
        // reactor hangs forever (confirmed while writing this fix — the
        // teardown thread parked indefinitely inside `pool.close().await`).
        // `DROP DATABASE … WITH (FORCE)` alone is sufficient: it terminates
        // every other backend on the target database server-side, so this
        // pool's own (client-side) connections never need to be closed
        // first — they simply become invalid once the database is gone.
        //
        // `Drop` is sync, and we're normally being dropped from inside the
        // test's own (commonly current-thread) tokio runtime — calling
        // `Handle::block_on` here would panic ("cannot start a runtime from
        // within a runtime"). A fresh OS thread with its own tiny runtime has
        // no such conflict, and joining it makes teardown finish before this
        // `drop()` call returns (so a caller checking "is it gone yet"
        // immediately afterward, like the anchor test, sees it gone).
        let joined = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| format!("teardown runtime build failed: {e}"))?;
            rt.block_on(async move {
                let admin_pool = PgPool::connect(&admin_url())
                    .await
                    .map_err(|e| format!("connect to admin DB for teardown: {e}"))?;
                // `WITH (FORCE)` is PG13+ (confirmed on the CI container,
                // 15.17) — terminates any other straggling backend on the
                // target DB too, so one stray extra connection never wedges
                // teardown indefinitely.
                sqlx::query(&format!("DROP DATABASE IF EXISTS {db_name} WITH (FORCE)"))
                    .execute(&admin_pool)
                    .await
                    .map_err(|e| format!("DROP DATABASE {db_name}: {e}"))?;
                Ok::<(), String>(())
            })
        })
        .join();

        // Best-effort but loud: a failed teardown must never fail an
        // otherwise-passing test (Drop can't propagate a Result), but it
        // must not fail silently either — this is exactly the leak this
        // type exists to prevent, so it goes to stderr even though nothing
        // reads it in the success path.
        match joined {
            Ok(Ok(())) => {}
            Ok(Err(e)) => eprintln!("test-db teardown: leaked {db_name_for_log} — {e}"),
            Err(_) => eprintln!("test-db teardown: leaked {db_name_for_log} — teardown thread panicked"),
        }
    }
}

async fn fresh_db_pool(prefix: &str) -> TestDb {
    let admin_pool = PgPool::connect(&admin_url())
        .await
        .expect("connect to the test Postgres admin DB — is the rome-audit-test-pg container running?");
    let db_name = unique_db_name(prefix);
    sqlx::query(&format!("CREATE DATABASE {db_name}"))
        .execute(&admin_pool)
        .await
        .expect("CREATE DATABASE for a fresh isolated test DB");

    // NOT `.replace("/postgres", ...)`: that also corrupts the `postgres`
    // username right after the `//` in the scheme (two non-overlapping
    // matches of the literal "/postgres" substring in
    // "postgres://postgres:...@host/postgres"). Strip only the trailing
    // "/postgres" database-name suffix instead.
    let db_url = format!("{}/{db_name}", admin_url().trim_end_matches("/postgres"));
    let pool = PgPool::connect(&db_url)
        .await
        .expect("connect to freshly created test DB");
    TestDb { pool, db_name }
}

/// A fresh, EMPTY DB — provisioned but with no migrations run yet. Lets a
/// caller seed `public._sqlx_migrations` (e.g. to simulate a co-tenant like
/// rome-via-sync having already claimed low version numbers on the shared
/// `rome_via_db`) BEFORE running rome-audit's own migrator. See
/// `tests/migration_collision_db.rs`.
pub async fn fresh_empty_db() -> TestDb {
    fresh_db_pool("rome_audit_migtest").await
}

/// A fresh DB with just the real `audit` schema migrations applied — what
/// `tests/resolve_db.rs` and `tests/tier2_db.rs` need.
pub async fn fresh_audit_db() -> TestDb {
    let db = fresh_db_pool("rome_audit_test").await;
    // NOTE: `sqlx::migrate!`'s path is resolved relative to
    // `CARGO_MANIFEST_DIR`, not to this file's location — so "./migrations"
    // is correct here despite this module living in `tests/common/`.
    sqlx::migrate!("./migrations")
        .run(&db.pool)
        .await
        .expect("apply audit schema migrations");
    db
}

/// A fresh DB with the real `audit` schema migrations AND the Hercules-shaped
/// fixture schema applied (same DB — different namespaces, `audit.*` vs
/// `public.*`) — what `tests/ingest_db.rs` and `tests/resolve_ingest_db.rs`
/// both need.
pub async fn fresh_hercules_audit_db() -> TestDb {
    let db = fresh_db_pool("rome_audit_test").await;
    sqlx::migrate!("./migrations")
        .run(&db.pool)
        .await
        .expect("apply audit schema migrations");

    let hercules_ddl = include_str!("../fixtures/hercules_schema.sql");
    sqlx::Executor::execute(&db.pool, sqlx::raw_sql(hercules_ddl))
        .await
        .expect("apply Hercules-shaped fixture schema");

    db
}

// ---- Hercules-fixture seeding helpers ----------------------------------

pub async fn seed_sol_slot(
    pool: &PgPool,
    slot: i64,
    parent: i64,
    status: &str,
    blockhash: &str,
    timestamp: i64,
) {
    sqlx::query(
        // `status` is the real `slotstatus` ENUM — a bound TEXT param needs an
        // explicit `::slotstatus` cast (Postgres does not implicitly assign
        // text to an enum column), same shape the production writers use.
        "INSERT INTO sol_slot (slot_number, parent_slot, status, blockhash, timestamp) VALUES ($1,$2,$3::slotstatus,$4,$5)
         ON CONFLICT (slot_number) DO UPDATE SET status = excluded.status, blockhash = excluded.blockhash",
    )
    .bind(slot)
    .bind(parent)
    .bind(status)
    .bind(blockhash)
    .bind(timestamp)
    .execute(pool)
    .await
    .unwrap();
}

/// Marks a slot as having produced an (empty, no-tx) block — enough to
/// advance `max_produced_slot()` past it for head-clearance, without any
/// matched logs.
pub async fn seed_produced_empty_block(
    pool: &PgPool,
    slot: i64,
    eth_block_hash: &str,
    eth_block_number: i64,
) {
    sqlx::query(
        "INSERT INTO eth_block (slot_number, slot_block_idx, block_gas_used, slot_timestamp, params)
         VALUES ($1, 0, 0, $4, ROW($2, $3, $5, 1700000000)::blockparams)
         ON CONFLICT (slot_number, slot_block_idx) DO UPDATE SET params = excluded.params",
    )
    .bind(slot)
    .bind(eth_block_hash)
    .bind(format!("0x{:064x}", 0)) // parent_hash, irrelevant to these tests
    .bind(1700000000i64)
    .bind(eth_block_number)
    .execute(pool)
    .await
    .unwrap();
}

pub async fn delete_produced_block(pool: &PgPool, slot: i64) {
    sqlx::query("DELETE FROM eth_block WHERE slot_number = $1")
        .bind(slot)
        .execute(pool)
        .await
        .unwrap();
}

/// A contiguous run of Finalized, block-produced, empty (no-log) slots.
/// Used both as the "head-clearance for an earlier slot" trailing half AND
/// as the mandatory unbroken prefix from slot 1 (see `tests/ingest_db.rs`'s
/// module doc for why this is mandatory seeding, not optional).
pub async fn seed_contiguous_finalized_chain(pool: &PgPool, from: i64, to_inclusive: i64) {
    for s in from..=to_inclusive {
        seed_sol_slot(
            pool,
            s,
            s - 1,
            "Finalized",
            &format!("0x{:064x}", s),
            1700000000 + s,
        )
        .await;
        seed_produced_empty_block(pool, s, &format!("0x{:064x}", 900_000 + s), s).await;
    }
}

/// One log to seed: `(address, topic0, topic1, topic2, topic3, data_hex_or_none)`.
pub type SeedLog = (
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

/// Seeds one tx at `slot` carrying exactly the logs in `logs`, in
/// log_ordinal order, with a real, block-produced `eth_block` +
/// `receipt_params` + `evm_tx.from_address` — everything `tx_receipt_info`
/// and `log_data` need. Returns the seeded tx's hash.
#[allow(clippy::too_many_arguments)]
pub async fn seed_tx_with_logs(
    pool: &PgPool,
    slot: i64,
    eth_block_hash: &str,
    eth_block_number: i64,
    from_address: &str,
    logs: &[SeedLog],
) -> String {
    seed_produced_empty_block(pool, slot, eth_block_hash, eth_block_number).await;

    let tx_hash = format!("0x{:064x}", slot as u64 * 1000 + 7); // deterministic, unique-enough per test

    sqlx::query("INSERT INTO evm_tx (tx_hash, from_address, origination) VALUES ($1,$2,'ecdsa') ON CONFLICT DO NOTHING")
        .bind(&tx_hash)
        .bind(from_address)
        .execute(pool)
        .await
        .unwrap();

    let logs_json: Vec<serde_json::Value> = logs
        .iter()
        .map(|(_, _, _, _, _, data)| serde_json::json!({ "data": data.clone().unwrap_or_else(|| "0x".to_string()) }))
        .collect();
    let tx_result = serde_json::json!({ "logs": logs_json });
    let receipt_params = serde_json::json!({
        "blockhash": eth_block_hash,
        "block_number": format!("0x{:x}", eth_block_number),
        "tx_index": 0,
        "block_gas_used": "0x5208",
        "first_log_index": "0x0",
    });

    sqlx::query(
        "INSERT INTO evm_tx_result (slot_number, tx_hash, tx_result, receipt_params) VALUES ($1,$2,$3,$4)
         ON CONFLICT (slot_number, tx_hash) DO UPDATE SET tx_result = excluded.tx_result, receipt_params = excluded.receipt_params",
    )
    .bind(slot)
    .bind(&tx_hash)
    .bind(&tx_result)
    .bind(&receipt_params)
    .execute(pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO eth_block_txs (slot_number, slot_block_idx, tx_hash, tx_idx) VALUES ($1,0,$2,0)
         ON CONFLICT DO NOTHING",
    )
    .bind(slot)
    .bind(&tx_hash)
    .execute(pool)
    .await
    .unwrap();

    for (ordinal, (address, topic0, topic1, topic2, topic3, _)) in logs.iter().enumerate() {
        sqlx::query(
            "INSERT INTO evm_log (slot_number, tx_hash, log_ordinal, address, topic0, topic1, topic2, topic3)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT DO NOTHING",
        )
        .bind(slot)
        .bind(&tx_hash)
        .bind(ordinal as i32)
        .bind(address)
        .bind(topic0)
        .bind(topic1)
        .bind(topic2)
        .bind(topic3)
        .execute(pool)
        .await
        .unwrap();
    }

    tx_hash
}

/// Like [`seed_tx_with_logs`], but with full control over the receipt fields
/// `logs_for_address_topic0`'s ordering + `log_index`-offset math reads —
/// `tx_index`, `first_log_index`, and an explicit `tx_hash` (so two txs can
/// live at DIFFERENT `(block_number, tx_index)` positions). The fixed
/// `tx_index=0` / `first_log_index=0` in [`seed_tx_with_logs`] can't
/// distinguish `log_index = log_ordinal + first_log_index` from a bare
/// `log_ordinal`, which is exactly what the logs-history read must get right.
#[allow(clippy::too_many_arguments)]
pub async fn seed_tx_with_logs_indexed(
    pool: &PgPool,
    slot: i64,
    tx_hash: &str,
    eth_block_hash: &str,
    eth_block_number: i64,
    tx_index: i32,
    first_log_index: i64,
    from_address: &str,
    logs: &[SeedLog],
) {
    seed_produced_empty_block(pool, slot, eth_block_hash, eth_block_number).await;

    sqlx::query("INSERT INTO evm_tx (tx_hash, from_address, origination) VALUES ($1,$2,'ecdsa') ON CONFLICT DO NOTHING")
        .bind(tx_hash)
        .bind(from_address)
        .execute(pool)
        .await
        .unwrap();

    let logs_json: Vec<serde_json::Value> = logs
        .iter()
        .map(|(_, _, _, _, _, data)| serde_json::json!({ "data": data.clone().unwrap_or_else(|| "0x".to_string()) }))
        .collect();
    let tx_result = serde_json::json!({ "logs": logs_json });
    let receipt_params = serde_json::json!({
        "blockhash": eth_block_hash,
        "block_number": format!("0x{:x}", eth_block_number),
        "tx_index": tx_index,
        "block_gas_used": "0x5208",
        "first_log_index": format!("0x{:x}", first_log_index),
    });

    sqlx::query(
        "INSERT INTO evm_tx_result (slot_number, tx_hash, tx_result, receipt_params) VALUES ($1,$2,$3,$4)
         ON CONFLICT (slot_number, tx_hash) DO UPDATE SET tx_result = excluded.tx_result, receipt_params = excluded.receipt_params",
    )
    .bind(slot)
    .bind(tx_hash)
    .bind(&tx_result)
    .bind(&receipt_params)
    .execute(pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO eth_block_txs (slot_number, slot_block_idx, tx_hash, tx_idx) VALUES ($1,0,$2,$3)
         ON CONFLICT DO NOTHING",
    )
    .bind(slot)
    .bind(tx_hash)
    .bind(tx_index)
    .execute(pool)
    .await
    .unwrap();

    for (ordinal, (address, topic0, topic1, topic2, topic3, _)) in logs.iter().enumerate() {
        sqlx::query(
            "INSERT INTO evm_log (slot_number, tx_hash, log_ordinal, address, topic0, topic1, topic2, topic3)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT DO NOTHING",
        )
        .bind(slot)
        .bind(tx_hash)
        .bind(ordinal as i32)
        .bind(address)
        .bind(topic0)
        .bind(topic1)
        .bind(topic2)
        .bind(topic3)
        .execute(pool)
        .await
        .unwrap();
    }
}

/// Like [`seed_tx_with_logs`], but writes `tx_result = {"logs": []}` — no
/// data recoverable for ANY log_ordinal, on purpose (drives the "a
/// non-indexed event genuinely cannot decode without it" tests).
pub async fn seed_tx_with_empty_tx_result(
    pool: &PgPool,
    slot: i64,
    eth_block_hash: &str,
    eth_block_number: i64,
    from_address: &str,
    log: &SeedLog,
) -> String {
    seed_produced_empty_block(pool, slot, eth_block_hash, eth_block_number).await;
    let tx_hash = format!("0x{:064x}", slot as u64 * 1000 + 7);

    sqlx::query("INSERT INTO evm_tx (tx_hash, from_address, origination) VALUES ($1,$2,'ecdsa') ON CONFLICT DO NOTHING")
        .bind(&tx_hash)
        .bind(from_address)
        .execute(pool)
        .await
        .unwrap();

    let tx_result = serde_json::json!({ "logs": [] }); // deliberately empty
    let receipt_params = serde_json::json!({
        "blockhash": eth_block_hash,
        "block_number": format!("0x{:x}", eth_block_number),
        "tx_index": 0,
        "block_gas_used": "0x5208",
        "first_log_index": "0x0",
    });
    sqlx::query(
        "INSERT INTO evm_tx_result (slot_number, tx_hash, tx_result, receipt_params) VALUES ($1,$2,$3,$4)
         ON CONFLICT (slot_number, tx_hash) DO UPDATE SET tx_result = excluded.tx_result, receipt_params = excluded.receipt_params",
    )
    .bind(slot)
    .bind(&tx_hash)
    .bind(&tx_result)
    .bind(&receipt_params)
    .execute(pool)
    .await
    .unwrap();

    sqlx::query("INSERT INTO eth_block_txs (slot_number, slot_block_idx, tx_hash, tx_idx) VALUES ($1,0,$2,0) ON CONFLICT DO NOTHING")
        .bind(slot)
        .bind(&tx_hash)
        .execute(pool)
        .await
        .unwrap();

    let (address, topic0, topic1, topic2, topic3, _) = log;
    sqlx::query(
        "INSERT INTO evm_log (slot_number, tx_hash, log_ordinal, address, topic0, topic1, topic2, topic3)
         VALUES ($1,$2,0,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING",
    )
    .bind(slot)
    .bind(&tx_hash)
    .bind(address)
    .bind(topic0)
    .bind(topic1)
    .bind(topic2)
    .bind(topic3)
    .execute(pool)
    .await
    .unwrap();

    tx_hash
}

// ---- small hex helpers --------------------------------------------------

pub fn hex_addr(byte: u8) -> String {
    format!("0x{}", hex::encode([byte; 20]))
}

pub fn hex_topic(bytes: &[u8; 32]) -> String {
    format!("0x{}", hex::encode(bytes))
}

pub fn address_topic(addr_byte: u8) -> String {
    let mut word = [0u8; 32];
    word[12..].copy_from_slice(&[addr_byte; 20]);
    hex_topic(&word)
}

// ---- audit-schema read helpers -------------------------------------------

pub async fn count_chain_event_rows(pool: &PgPool, chain_id: i64) -> i64 {
    let (n,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM audit.chain_event WHERE chain_id = $1")
        .bind(chain_id)
        .fetch_one(pool)
        .await
        .unwrap();
    n
}

pub async fn quarantine_rows(pool: &PgPool, chain_id: i64) -> Vec<(i64, String)> {
    sqlx::query_as("SELECT slot_number, reason FROM audit.quarantine WHERE chain_id = $1 ORDER BY quarantine_id")
        .bind(chain_id)
        .fetch_all(pool)
        .await
        .unwrap()
}

pub async fn watermark(pool: &PgPool, chain_id: i64) -> Option<i64> {
    let row: Option<(i64,)> = sqlx::query_as(
        "SELECT verified_through_slot FROM audit.ingest_watermark WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_optional(pool)
    .await
    .unwrap();
    row.map(|(s,)| s)
}

/// Ticks `run_ingest_once` until `target_slot` shows up in `slots_passed`
/// (or panics after a generous bound) — the "this MUST eventually pass"
/// counterpart to the manual bounded loops the "must never pass" tests use.
pub async fn run_until_slot_passes(
    source: &PgPool,
    target: &PgPool,
    registry: &rome_audit::AbiRegistry,
    tracker: &mut rome_audit::ingest::WatermarkTracker,
    config: &rome_audit::ingest::IngestConfig,
    target_slot: i64,
) -> rome_audit::ingest::IngestOutcome {
    let mut total = rome_audit::ingest::IngestOutcome::default();
    for _ in 0..(target_slot + 50) {
        let out = rome_audit::ingest::run_ingest_once(source, target, registry, tracker, config, 30)
            .await
            .unwrap();
        let reached = out.slots_passed.contains(&target_slot);
        total.events_inserted += out.events_inserted;
        total.slots_passed.extend(out.slots_passed);
        if reached {
            return total;
        }
    }
    panic!("slot {target_slot} never passed within the tick budget — the pipeline is stuck");
}

// ---- Resolve-side test doubles (P3c, integration-test-reachable) --------
//
// See module doc for why these are a separate, smaller double from
// `src/resolve/fixture.rs`'s unit-test fakes rather than an un-gate of that
// module.

#[derive(Debug, Default)]
struct FakeRpcState {
    token_implementations: BTreeMap<([u8; 20], [u8; 20]), [u8; 20]>,
    routers: BTreeMap<[u8; 20], [u8; 20]>,
    restriction_modules: BTreeMap<([u8; 20], [u8; 32]), [u8; 20]>,
    logs: BTreeMap<([u8; 20], [u8; 32]), Vec<LogEntry>>,
    global_modules: BTreeMap<([u8; 20], [u8; 32]), [u8; 20]>,
    pair_tokens: BTreeMap<[u8; 20], ([u8; 20], [u8; 20])>,
    market_params: BTreeMap<[u8; 32], MarketParams>,
}

/// A fully in-memory, caller-scripted [`ResolverRpc`] usable from behind a
/// shared `Arc<dyn ResolverRpc>` — every setter takes `&self` (Mutex-backed),
/// and [`FakeRpc::set_fail`] flips every trait method to return `Err` until
/// unset, so ONE instance can script "resolve succeeds, then fails, then
/// recovers" across a running `AuditWorker`'s ticks (a.8's requirement).
#[derive(Debug, Default)]
pub struct FakeRpc {
    state: Mutex<FakeRpcState>,
    failing: AtomicBool,
}

impl FakeRpc {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_fail(&self, fail: bool) {
        self.failing.store(fail, Ordering::SeqCst);
    }

    pub fn set_token_implementation(&self, factory: [u8; 20], token: [u8; 20], impl_addr: [u8; 20]) {
        self.state
            .lock()
            .unwrap()
            .token_implementations
            .insert((factory, token), impl_addr);
    }

    pub fn set_router(&self, token: [u8; 20], router: [u8; 20]) {
        self.state.lock().unwrap().routers.insert(token, router);
    }

    #[allow(dead_code)]
    pub fn set_restriction_module(&self, token: [u8; 20], type_id: [u8; 32], module: [u8; 20]) {
        self.state
            .lock()
            .unwrap()
            .restriction_modules
            .insert((token, type_id), module);
    }

    pub fn add_log(&self, address: [u8; 20], topic0: [u8; 32], log: LogEntry) {
        self.state
            .lock()
            .unwrap()
            .logs
            .entry((address, topic0))
            .or_default()
            .push(log);
    }

    #[allow(dead_code)]
    pub fn set_global_module(&self, router: [u8; 20], type_id: [u8; 32], module: [u8; 20]) {
        self.state
            .lock()
            .unwrap()
            .global_modules
            .insert((router, type_id), module);
    }

    #[allow(dead_code)]
    pub fn set_pair(&self, pair: [u8; 20], token0: [u8; 20], token1: [u8; 20]) {
        self.state
            .lock()
            .unwrap()
            .pair_tokens
            .insert(pair, (token0, token1));
    }

    #[allow(dead_code)]
    pub fn set_market(&self, market_id: [u8; 32], loan_token: [u8; 20], collateral_token: [u8; 20]) {
        self.state.lock().unwrap().market_params.insert(
            market_id,
            MarketParams {
                loan_token,
                collateral_token,
            },
        );
    }
}

#[async_trait::async_trait]
impl ResolverRpc for FakeRpc {
    async fn get_token_implementation(
        &self,
        factory: [u8; 20],
        token: [u8; 20],
    ) -> Result<[u8; 20], RpcError> {
        if self.failing.load(Ordering::SeqCst) {
            return Err(RpcError::CallFailed("scripted failure".into()));
        }
        Ok(self
            .state
            .lock()
            .unwrap()
            .token_implementations
            .get(&(factory, token))
            .copied()
            .unwrap_or([0u8; 20]))
    }

    async fn restrictions_router(&self, token: [u8; 20]) -> Result<[u8; 20], RpcError> {
        if self.failing.load(Ordering::SeqCst) {
            return Err(RpcError::CallFailed("scripted failure".into()));
        }
        self.state
            .lock()
            .unwrap()
            .routers
            .get(&token)
            .copied()
            .ok_or_else(|| RpcError::CallFailed(format!("no router configured for {token:?}")))
    }

    async fn get_restriction_module(
        &self,
        token: [u8; 20],
        type_id: [u8; 32],
    ) -> Result<[u8; 20], RpcError> {
        if self.failing.load(Ordering::SeqCst) {
            return Err(RpcError::CallFailed("scripted failure".into()));
        }
        Ok(self
            .state
            .lock()
            .unwrap()
            .restriction_modules
            .get(&(token, type_id))
            .copied()
            .unwrap_or([0u8; 20]))
    }

    async fn logs_for(&self, address: [u8; 20], topic0: [u8; 32]) -> Result<Vec<LogEntry>, RpcError> {
        if self.failing.load(Ordering::SeqCst) {
            return Err(RpcError::CallFailed("scripted failure".into()));
        }
        Ok(self
            .state
            .lock()
            .unwrap()
            .logs
            .get(&(address, topic0))
            .cloned()
            .unwrap_or_default())
    }

    async fn get_global_module_address(
        &self,
        router: [u8; 20],
        type_id: [u8; 32],
    ) -> Result<[u8; 20], RpcError> {
        if self.failing.load(Ordering::SeqCst) {
            return Err(RpcError::CallFailed("scripted failure".into()));
        }
        Ok(self
            .state
            .lock()
            .unwrap()
            .global_modules
            .get(&(router, type_id))
            .copied()
            .unwrap_or([0u8; 20]))
    }

    async fn token0(&self, pair: [u8; 20]) -> Result<[u8; 20], RpcError> {
        if self.failing.load(Ordering::SeqCst) {
            return Err(RpcError::CallFailed("scripted failure".into()));
        }
        self.state
            .lock()
            .unwrap()
            .pair_tokens
            .get(&pair)
            .map(|(t0, _)| *t0)
            .ok_or_else(|| RpcError::CallFailed(format!("token0() reverted for {pair:?}")))
    }

    async fn token1(&self, pair: [u8; 20]) -> Result<[u8; 20], RpcError> {
        if self.failing.load(Ordering::SeqCst) {
            return Err(RpcError::CallFailed("scripted failure".into()));
        }
        self.state
            .lock()
            .unwrap()
            .pair_tokens
            .get(&pair)
            .map(|(_, t1)| *t1)
            .ok_or_else(|| RpcError::CallFailed(format!("token1() reverted for {pair:?}")))
    }

    async fn id_to_market_params(
        &self,
        _morpho: [u8; 20],
        market_id: [u8; 32],
    ) -> Result<MarketParams, RpcError> {
        if self.failing.load(Ordering::SeqCst) {
            return Err(RpcError::CallFailed("scripted failure".into()));
        }
        self.state
            .lock()
            .unwrap()
            .market_params
            .get(&market_id)
            .copied()
            .ok_or_else(|| RpcError::CallFailed(format!("no market configured for {market_id:?}")))
    }
}

/// A fully in-memory, caller-scripted [`RegistrySource`] — built once, then
/// wrapped in an `Arc` (its trait is already `&self`-only, so no `Mutex`
/// needed: capture §"D5" — a real implementation reads a pinned,
/// already-fetched snapshot, never mutated in place after resolution).
#[derive(Debug, Clone, Default)]
pub struct FakeRegistry {
    pub commit_sha: String,
    pub listed_assets: std::collections::BTreeSet<[u8; 20]>,
    pub factory: [u8; 20],
    pub storefront: [u8; 20],
    pub candidate_pools: Vec<[u8; 20]>,
    pub morpho: [u8; 20],
    pub global_sanctions_router: Option<[u8; 20]>,
    pub global_sanctions_router_from_block: Option<i64>,
}

impl FakeRegistry {
    pub fn new(commit_sha: &str, factory: [u8; 20], storefront: [u8; 20]) -> Self {
        Self {
            commit_sha: commit_sha.to_string(),
            factory,
            storefront,
            ..Default::default()
        }
    }
}

impl RegistrySource for FakeRegistry {
    fn commit_sha(&self) -> String {
        self.commit_sha.clone()
    }

    fn is_listed_asset(&self, token: [u8; 20]) -> bool {
        self.listed_assets.contains(&token)
    }

    fn factory(&self) -> [u8; 20] {
        self.factory
    }

    fn storefront(&self) -> [u8; 20] {
        self.storefront
    }

    fn candidate_pools(&self) -> Vec<[u8; 20]> {
        self.candidate_pools.clone()
    }

    fn morpho(&self) -> [u8; 20] {
        self.morpho
    }

    fn global_sanctions_router(&self) -> Option<[u8; 20]> {
        self.global_sanctions_router
    }

    fn global_sanctions_router_from_block(&self) -> Option<i64> {
        self.global_sanctions_router_from_block
    }
}
