//! Durability follow-up to the finality-prefix fix (`ingest::watermark`):
//! `/healthz` surfaces `finalized_tip - verified_through_slot` so a wedge is
//! visible on a dashboard/alert instead of discovered 31h later. DB-backed
//! against the real Hercules-shaped fixture schema + a real HTTP server on
//! `127.0.0.1:0`, driven with `reqwest` (same shape as `tests/overlay_db.rs`).

use rome_audit::server::{start_health_server, LagProbe};

mod common;
use common::{fresh_hercules_audit_db, seed_sol_slot};

#[tokio::test]
async fn healthz_reports_null_lag_when_no_probe_is_wired() {
    let (addr, _shutdown) = start_health_server("127.0.0.1:0", None).await.unwrap();

    let body: serde_json::Value = reqwest::get(format!("http://{addr}/healthz"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(body["status"], "ok");
    assert!(
        body["ingest_lag_slots"].is_null(),
        "no probe wired ⇒ lag must be null, byte-identical to every existing deploy: {body}"
    );
}

#[tokio::test]
async fn healthz_reports_the_ingest_lag_in_slots() {
    let db = fresh_hercules_audit_db().await;
    let chain_id = 5001i64;

    seed_sol_slot(&db, 100, 99, "Finalized", "0xtip", 1_700_000_100).await;
    // Persist the watermark directly — a pinned, direct proof of the
    // health-endpoint's own arithmetic, independent of the ingest walk.
    sqlx::query(
        "INSERT INTO audit.ingest_watermark (chain_id, verified_through_slot, updated_at)
         VALUES ($1, $2, 0)
         ON CONFLICT (chain_id) DO UPDATE SET verified_through_slot = excluded.verified_through_slot",
    )
    .bind(chain_id)
    .bind(70i64)
    .execute(&db)
    .await
    .unwrap();

    let probe = LagProbe {
        source: db.clone(),
        target: db.clone(),
        chain_id,
    };
    let (addr, _shutdown) = start_health_server("127.0.0.1:0", Some(probe)).await.unwrap();

    let body: serde_json::Value = reqwest::get(format!("http://{addr}/healthz"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(body["status"], "ok");
    assert_eq!(
        body["ingest_lag_slots"], 30,
        "finalized_tip(100) - verified_through_slot(70) = 30: {body}"
    );
}

#[tokio::test]
async fn healthz_reports_null_lag_when_the_source_read_fails() {
    // A wired probe pointed at a pool whose `sol_slot` table doesn't exist
    // (a plain `audit`-only DB, no Hercules fixture schema) — the source
    // read errors, and `/healthz` must still return 200 with `null`, never
    // fail the request itself (M-non-fatal).
    let db = common::fresh_audit_db().await;
    let probe = LagProbe {
        source: db.clone(),
        target: db.clone(),
        chain_id: 5002,
    };
    let (addr, _shutdown) = start_health_server("127.0.0.1:0", Some(probe)).await.unwrap();

    let resp = reqwest::get(format!("http://{addr}/healthz")).await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["ingest_lag_slots"].is_null(),
        "a failed source read must degrade to null, never a non-200: {body}"
    );
}
