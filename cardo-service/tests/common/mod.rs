//! Shared test harness used by `mcp_handler_test`, `mcp_http_test`, and the
//! seeded DB tests.
//!
//! Two capability levels:
//!
//! - `headless_harness()` — no network, no DB. For tests that only exercise
//!   dispatch logic, argument parsing, and static registry.
//! - `db_harness()` — connects to a live Postgres via `CARDO_POSTGRES_URL`,
//!   runs migrations, seeds a `test-app` fixture. Gated on the env var;
//!   tests calling this must be `#[ignore]` by default.

#![allow(dead_code)]

use cardo_service::manifest::Manifest;
use cardo_service::store::PostgresStore;
use sqlx::PgPool;

/// Seeded DB harness: returns a live pool + the seeded manifest. Migrations
/// run first; any existing `apps` row for the fixture id is upserted so the
/// test is idempotent across runs.
///
/// # Env
///
/// - `CARDO_POSTGRES_URL` — required; tests that call this must be
///   `#[ignore]` gated so unit-test runs without Postgres still succeed.
pub async fn db_harness() -> (PgPool, Manifest) {
    let url = std::env::var("CARDO_POSTGRES_URL")
        .expect("set CARDO_POSTGRES_URL to run DB-gated MCP tests");
    let store = PostgresStore::connect(&url).await.expect("pg connect");
    store.migrate().await.expect("migrate");

    let raw_str = include_str!("../fixtures/manifests/test-app.json");
    let raw: serde_json::Value = serde_json::from_str(raw_str).expect("fixture json");
    let manifest: Manifest = serde_json::from_str(raw_str).expect("fixture manifest");
    store
        .upsert_manifest(&manifest, &raw)
        .await
        .expect("upsert fixture");

    (store.pool().clone(), manifest)
}
