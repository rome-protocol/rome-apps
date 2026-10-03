//! P5a overlay ingest — DB-backed tests against a REAL local Postgres (same
//! disposable `rome-audit-test-pg` container the rest of the suite uses)
//! AND a REAL HTTP server (`start_overlay_server` on `127.0.0.1:0`, driven
//! with `reqwest`, not axum's in-process test client) — so the auth header
//! path is exercised exactly as Bloom would hit it over the wire.
//!
//! Tests 1-19 per the P5a task ordering (test #1, the migration up/down/up
//! cycle, lives in `src/migrations_test.rs` instead — a `--lib` unit test,
//! since it needs `sqlx::migrate::Migrator::undo`, not the HTTP surface).

use std::sync::{Arc, Mutex};

use hmac::{Hmac, Mac};
use serde_json::{json, Value};
use sha2::Sha256;
use sqlx::PgPool;

use rome_audit::overlay::{auth, start_overlay_server, OverlayState};

mod common;
use common::fresh_audit_db;

const SECRET: &[u8] = b"test-overlay-secret-at-least-32-bytes-long";

// ---- test harness --------------------------------------------------------

struct TestServer {
    base: String,
    pool: PgPool,
    clock: Arc<Mutex<i64>>,
    _shutdown: tokio::sync::oneshot::Sender<()>,
    // Only set by `start()` — holds the disposable test DB's teardown guard
    // alive for the whole server's (and so the whole test's) lifetime.
    // `start_against` stays `PgPool`-by-value for its one direct caller
    // (`overlay_boot_touches_nothing`), which already holds its own
    // outer guard for the duration of that test.
    _db_guard: Option<common::TestDb>,
}

impl TestServer {
    async fn start() -> Self {
        let guard = fresh_audit_db().await;
        let mut srv = Self::start_against(guard.clone()).await;
        srv._db_guard = Some(guard);
        srv
    }

    async fn start_against(pool: PgPool) -> Self {
        let clock = Arc::new(Mutex::new(1_700_000_000i64));
        let clock_read = clock.clone();
        let state = OverlayState::with_clock(
            pool.clone(),
            SECRET.to_vec(),
            Arc::new(move || *clock_read.lock().unwrap()),
        );
        let (addr, shutdown) = start_overlay_server("127.0.0.1:0", state)
            .await
            .expect("bind overlay server on an ephemeral port");
        Self {
            base: format!("http://{addr}"),
            pool,
            clock,
            _shutdown: shutdown,
            _db_guard: None,
        }
    }

    fn now(&self) -> i64 {
        *self.clock.lock().unwrap()
    }

    fn set_now(&self, v: i64) {
        *self.clock.lock().unwrap() = v;
    }
}

/// Independently re-derives the signature `auth::verify` expects —
/// deliberately NOT calling into `src/overlay/auth.rs` to compute it, so a
/// bug in the production formula can't hide behind a shared helper.
fn sign(ts: i64, method: &str, path: &str, raw: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(SECRET).unwrap();
    mac.update(ts.to_string().as_bytes());
    mac.update(method.as_bytes());
    mac.update(path.as_bytes());
    mac.update(raw);
    hex::encode(mac.finalize().into_bytes())
}

async fn post_signed(srv: &TestServer, path: &str, ts: i64, sig: &str, body: &[u8]) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{}{}", srv.base, path))
        .header(auth::TS_HEADER, ts.to_string())
        .header(auth::SIG_HEADER, sig)
        .header("content-type", "application/json")
        .body(body.to_vec())
        .send()
        .await
        .expect("HTTP request to the overlay test server must succeed at the transport level")
}

/// Signs correctly and posts — the "everything is fine" happy path helper.
async fn post_ok_signed(srv: &TestServer, path: &str, body: &Value) -> reqwest::Response {
    let raw = serde_json::to_vec(body).unwrap();
    let ts = srv.now();
    let sig = sign(ts, "POST", path, &raw);
    post_signed(srv, path, ts, &sig, &raw).await
}

async fn count_identity(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM audit.identity").fetch_one(pool).await.unwrap()
}
async fn count_identity_key(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM audit.identity_key").fetch_one(pool).await.unwrap()
}
async fn count_evidence(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM audit.evidence_record").fetch_one(pool).await.unwrap()
}

fn evm_key(root_key: &str) -> Value {
    json!({ "root_key_type": "EVM", "root_key": root_key, "derivation_fn_version": "v1" })
}
fn solana_key(root_key: &str, synthetic: &str) -> Value {
    json!({
        "root_key_type": "SOLANA",
        "root_key": root_key,
        "derivation_fn_version": "v1",
        "synthetic_evm_address": synthetic,
    })
}

fn evm_addr(byte: char) -> String {
    format!("0x{}", byte.to_string().repeat(40))
}
fn solana_addr() -> String {
    "1".repeat(32)
}
fn hex32(byte: char) -> String {
    format!("0x{}", byte.to_string().repeat(64))
}

fn evidence_body(source_ref: &str, identity_id: &str, status: &str, valid_through: Option<i64>) -> Value {
    json!({
        "source_ref": source_ref,
        "identity_id": identity_id,
        "kind": "KYC_STATUS",
        "vendor": "sumsub",
        "subject_ref": hex32('a'),
        "subject_ref_version": "v1",
        "status": status,
        "valid_through": valid_through,
        "vendor_timestamp": 1_699_999_000,
    })
}

// ---- 2. unsigned_request_is_401_before_parse -----------------------------

#[tokio::test]
async fn unsigned_request_is_401_before_parse() {
    let srv = TestServer::start().await;

    // Well-formed JSON, deliberately wrong signature.
    let good_json = serde_json::to_vec(&json!({ "identity_id": "x", "keys": [] })).unwrap();
    let ts = srv.now();
    let resp = post_signed(&srv, "/overlay/identity", ts, "00", &good_json).await;
    assert_eq!(resp.status(), 401);

    // Malformed JSON, ALSO a wrong signature — if parsing ever ran before
    // verification this would surface as 400, not 401. Proves the order.
    let garbage = b"{not json at all";
    let resp = post_signed(&srv, "/overlay/identity", ts, "00", garbage).await;
    assert_eq!(
        resp.status(),
        401,
        "a malformed body behind a bad signature must still 401 — auth runs BEFORE parse"
    );

    assert_eq!(count_identity(&srv.pool).await, 0);
}

// ---- 3. stale_ts_is_401 --------------------------------------------------

#[tokio::test]
async fn stale_ts_is_401() {
    let srv = TestServer::start().await;
    let body = serde_json::to_vec(&json!({ "identity_id": "skew-test", "keys": [] })).unwrap();

    let now = srv.now();

    let ts = now - 301;
    let sig = sign(ts, "POST", "/overlay/identity", &body);
    let resp = post_signed(&srv, "/overlay/identity", ts, &sig, &body).await;
    assert_eq!(resp.status(), 401, "now-301 must be rejected");

    let ts = now - 300;
    let sig = sign(ts, "POST", "/overlay/identity", &body);
    let resp = post_signed(&srv, "/overlay/identity", ts, &sig, &body).await;
    assert_eq!(resp.status(), 200, "now-300 (the boundary) must be accepted");
}

// ---- 4. identity_register_inserts_rows -----------------------------------

#[tokio::test]
async fn identity_register_inserts_rows() {
    let srv = TestServer::start().await;
    let now = srv.now();

    let body = json!({
        "identity_id": "id-4",
        "keys": [ evm_key(&evm_addr('1')), solana_key(&solana_addr(), &evm_addr('2')) ],
    });
    let resp = post_ok_signed(&srv, "/overlay/identity", &body).await;
    assert_eq!(resp.status(), 200);
    let out: Value = resp.json().await.unwrap();
    assert_eq!(out["keys_appended"], 2);
    assert_eq!(out["keys_existing"], 0);

    assert_eq!(count_identity(&srv.pool).await, 1);
    assert_eq!(count_identity_key(&srv.pool).await, 2);

    let (created_at,): (i64,) = sqlx::query_as("SELECT created_at FROM audit.identity WHERE identity_id = 'id-4'")
        .fetch_one(&srv.pool)
        .await
        .unwrap();
    assert_eq!(created_at, now);

    let added_ats: Vec<(i64,)> = sqlx::query_as("SELECT added_at FROM audit.identity_key WHERE identity_id = 'id-4'")
        .fetch_all(&srv.pool)
        .await
        .unwrap();
    assert_eq!(added_ats.len(), 2);
    for (a,) in added_ats {
        assert_eq!(a, now);
    }
}

// ---- 5. identity_replay_is_idempotent ------------------------------------

#[tokio::test]
async fn identity_replay_is_idempotent() {
    let srv = TestServer::start().await;
    let body = json!({
        "identity_id": "id-5",
        "keys": [ evm_key(&evm_addr('3')), solana_key(&solana_addr(), &evm_addr('4')) ],
    });

    let resp = post_ok_signed(&srv, "/overlay/identity", &body).await;
    assert_eq!(resp.status(), 200);

    let resp = post_ok_signed(&srv, "/overlay/identity", &body).await;
    assert_eq!(resp.status(), 200);
    let out: Value = resp.json().await.unwrap();
    assert_eq!(out["keys_appended"], 0);
    assert_eq!(out["keys_existing"], 2);

    assert_eq!(count_identity(&srv.pool).await, 1);
    assert_eq!(count_identity_key(&srv.pool).await, 2);
}

// ---- 6. multi_wallet_appends ----------------------------------------------

#[tokio::test]
async fn multi_wallet_appends() {
    let srv = TestServer::start().await;
    let first = json!({ "identity_id": "id-6", "keys": [ evm_key(&evm_addr('5')) ] });
    let resp = post_ok_signed(&srv, "/overlay/identity", &first).await;
    assert_eq!(resp.status(), 200);

    let second = json!({ "identity_id": "id-6", "keys": [ solana_key(&solana_addr(), &evm_addr('6')) ] });
    let resp = post_ok_signed(&srv, "/overlay/identity", &second).await;
    assert_eq!(resp.status(), 200);
    let out: Value = resp.json().await.unwrap();
    assert_eq!(out["keys_appended"], 1);

    assert_eq!(count_identity(&srv.pool).await, 1, "still one identity");
    assert_eq!(count_identity_key(&srv.pool).await, 2, "two distinct wallets under it");
}

// ---- 7. same_key_under_different_identity_is_409 -------------------------

#[tokio::test]
async fn same_key_under_different_identity_is_409() {
    let srv = TestServer::start().await;
    let shared_key = evm_addr('7');

    let a = json!({ "identity_id": "id-A", "keys": [ evm_key(&shared_key) ] });
    let resp = post_ok_signed(&srv, "/overlay/identity", &a).await;
    assert_eq!(resp.status(), 200);

    let before = count_identity_key(&srv.pool).await;

    let b = json!({ "identity_id": "id-B", "keys": [ evm_key(&shared_key) ] });
    let resp = post_ok_signed(&srv, "/overlay/identity", &b).await;
    assert_eq!(resp.status(), 409);

    assert_eq!(count_identity_key(&srv.pool).await, before, "no new rows from the rejected claim");
    assert_eq!(
        count_identity(&srv.pool).await,
        1,
        "one transaction per request (routes.rs doc) — id-B's OWN identity insert rolls back too"
    );
}

// ---- 8. bare_identity_keys_empty_is_legal ---------------------------------

#[tokio::test]
async fn bare_identity_keys_empty_is_legal() {
    let srv = TestServer::start().await;
    let body = json!({ "identity_id": "id-8", "keys": [] });
    let resp = post_ok_signed(&srv, "/overlay/identity", &body).await;
    assert_eq!(resp.status(), 200);
    assert_eq!(count_identity(&srv.pool).await, 1);
    assert_eq!(count_identity_key(&srv.pool).await, 0);

    let ev = evidence_body("src-8", "id-8", "GREEN", Some(1_800_000_000));
    let resp = post_ok_signed(&srv, "/overlay/evidence", &ev).await;
    assert_eq!(resp.status(), 200, "evidence for a bare (keyless) identity must still succeed");
}

// ---- 9. solana_key_requires_synthetic_evm_forbids_it ----------------------

#[tokio::test]
async fn solana_key_requires_synthetic_evm_forbids_it() {
    let srv = TestServer::start().await;

    let missing_synthetic = json!({
        "identity_id": "id-9a",
        "keys": [ { "root_key_type": "SOLANA", "root_key": solana_addr(), "derivation_fn_version": "v1" } ],
    });
    let resp = post_ok_signed(&srv, "/overlay/identity", &missing_synthetic).await;
    assert_eq!(resp.status(), 422, "SOLANA without synthetic_evm_address must be 422");

    let evm_with_synthetic = json!({
        "identity_id": "id-9b",
        "keys": [ {
            "root_key_type": "EVM", "root_key": evm_addr('9'), "derivation_fn_version": "v1",
            "synthetic_evm_address": evm_addr('8'),
        } ],
    });
    let resp = post_ok_signed(&srv, "/overlay/identity", &evm_with_synthetic).await;
    assert_eq!(resp.status(), 422, "EVM WITH a synthetic_evm_address must be 422");

    assert_eq!(count_identity_key(&srv.pool).await, 0);
}

// ---- 10. pii_canary_structural_reject -------------------------------------

#[tokio::test]
async fn pii_canary_structural_reject() {
    let srv = TestServer::start().await;

    let mut identity_with_pii = json!({ "identity_id": "id-10", "keys": [] });
    identity_with_pii["fullName"] = json!("Jane Doe");
    let resp = post_ok_signed(&srv, "/overlay/identity", &identity_with_pii).await;
    assert_eq!(resp.status(), 400, "an unknown top-level field on /overlay/identity must 400");

    let mut identity_with_nested_pii = json!({
        "identity_id": "id-10b",
        "keys": [ evm_key(&evm_addr('a')) ],
    });
    identity_with_nested_pii["keys"][0]["dob"] = json!("1990-01-01");
    let resp = post_ok_signed(&srv, "/overlay/identity", &identity_with_nested_pii).await;
    assert_eq!(resp.status(), 400, "an unknown field NESTED in a key object must also 400");

    let mut evidence_with_pii = evidence_body("src-10", "id-10", "GREEN", Some(1_800_000_000));
    evidence_with_pii["email"] = json!("jane@example.com");
    let resp = post_ok_signed(&srv, "/overlay/evidence", &evidence_with_pii).await;
    assert_eq!(resp.status(), 400, "an unknown field on /overlay/evidence must 400");

    assert_eq!(count_identity(&srv.pool).await, 0);
    assert_eq!(count_identity_key(&srv.pool).await, 0);
    assert_eq!(count_evidence(&srv.pool).await, 0);
}

// ---- 11. evidence_insert_stamps_and_hashes --------------------------------

#[tokio::test]
async fn evidence_insert_stamps_and_hashes() {
    let srv = TestServer::start().await;
    let now = srv.now();

    let identity = json!({ "identity_id": "id-11", "keys": [] });
    post_ok_signed(&srv, "/overlay/identity", &identity).await;

    let ev = evidence_body("src-11", "id-11", "GREEN", Some(1_800_000_000));
    let resp = post_ok_signed(&srv, "/overlay/evidence", &ev).await;
    assert_eq!(resp.status(), 200);
    let out: Value = resp.json().await.unwrap();
    let returned_hash = out["evidence_hash"].as_str().unwrap().to_string();

    let row: (i64, Option<i64>, i64, String, Vec<u8>, String, String, String, Vec<u8>) = sqlx::query_as(
        "SELECT received_at, valid_through, vendor_timestamp, identity_id, subject_ref, subject_ref_version, status, vendor, evidence_hash
         FROM audit.evidence_record WHERE source_ref = 'src-11'",
    )
    .fetch_one(&srv.pool)
    .await
    .unwrap();
    let (received_at, valid_through, vendor_timestamp, identity_id, subject_ref, subject_ref_version, status, vendor, stored_hash) = row;

    assert_eq!(received_at, now, "received_at must be the INJECTED clock, not the wall clock");

    let recomputed = rome_audit::overlay::canonical::evidence_hash(&rome_audit::overlay::canonical::CanonicalEvidence {
        identity_id: &identity_id,
        kind: "KYC_STATUS",
        vendor: &vendor,
        subject_ref: &subject_ref,
        subject_ref_version: &subject_ref_version,
        status: &status,
        valid_through,
        vendor_timestamp,
        received_at,
    });
    assert_eq!(stored_hash, recomputed.to_vec());
    assert_eq!(returned_hash, format!("0x{}", hex::encode(recomputed)));
}

// ---- 12. evidence_hash_reverifiable_from_row ------------------------------

#[tokio::test]
async fn evidence_hash_reverifiable_from_row() {
    let srv = TestServer::start().await;
    let identity = json!({ "identity_id": "id-12", "keys": [] });
    post_ok_signed(&srv, "/overlay/identity", &identity).await;

    let ev = evidence_body("src-12", "id-12", "GREEN", Some(1_800_000_000));
    post_ok_signed(&srv, "/overlay/evidence", &ev).await;

    // Recompute using ONLY what's stored in the row (never the request) —
    // §14.2's "re-verifiable" property.
    let row: (String, String, String, Vec<u8>, String, String, Option<i64>, i64, i64, Vec<u8>) = sqlx::query_as(
        "SELECT identity_id, kind, vendor, subject_ref, subject_ref_version, status, valid_through, vendor_timestamp, received_at, evidence_hash
         FROM audit.evidence_record WHERE source_ref = 'src-12'",
    )
    .fetch_one(&srv.pool)
    .await
    .unwrap();
    let (identity_id, kind, vendor, subject_ref, subject_ref_version, status, valid_through, vendor_timestamp, received_at, stored_hash) = row;

    let recomputed = rome_audit::overlay::canonical::evidence_hash(&rome_audit::overlay::canonical::CanonicalEvidence {
        identity_id: &identity_id,
        kind: &kind,
        vendor: &vendor,
        subject_ref: &subject_ref,
        subject_ref_version: &subject_ref_version,
        status: &status,
        valid_through,
        vendor_timestamp,
        received_at,
    });
    assert_eq!(stored_hash, recomputed.to_vec());
}

// ---- 13. evidence_idempotent_on_source_ref --------------------------------

#[tokio::test]
async fn evidence_idempotent_on_source_ref() {
    let srv = TestServer::start().await;
    let identity = json!({ "identity_id": "id-13", "keys": [] });
    post_ok_signed(&srv, "/overlay/identity", &identity).await;

    let ev = evidence_body("src-13", "id-13", "GREEN", Some(1_800_000_000));
    let resp = post_ok_signed(&srv, "/overlay/evidence", &ev).await;
    assert_eq!(resp.status(), 200);
    let first_out: Value = resp.json().await.unwrap();
    let first_hash = first_out["evidence_hash"].as_str().unwrap().to_string();

    let resp = post_ok_signed(&srv, "/overlay/evidence", &ev).await;
    assert_eq!(resp.status(), 200);
    let out: Value = resp.json().await.unwrap();
    assert_eq!(out["result"], "deduped");
    assert_eq!(
        out["evidence_hash"].as_str().unwrap(),
        first_hash,
        "LOW-5: a redelivering feeder must get the STORED evidence_hash back to reconcile against"
    );

    assert_eq!(count_evidence(&srv.pool).await, 1);
}

// ---- 14. same_source_ref_different_content_is_409 -------------------------

#[tokio::test]
async fn same_source_ref_different_content_is_409() {
    let srv = TestServer::start().await;
    let identity = json!({ "identity_id": "id-14", "keys": [] });
    post_ok_signed(&srv, "/overlay/identity", &identity).await;

    let ev = evidence_body("src-14", "id-14", "GREEN", Some(1_800_000_000));
    let resp = post_ok_signed(&srv, "/overlay/evidence", &ev).await;
    assert_eq!(resp.status(), 200);

    let ev2 = evidence_body("src-14", "id-14", "RED", None);
    let resp = post_ok_signed(&srv, "/overlay/evidence", &ev2).await;
    assert_eq!(resp.status(), 409);

    assert_eq!(count_evidence(&srv.pool).await, 1, "still exactly one row — the conflicting write never landed");
}

// ---- 15. valid_through_requires_green -------------------------------------

#[tokio::test]
async fn valid_through_requires_green() {
    let srv = TestServer::start().await;
    let identity = json!({ "identity_id": "id-15", "keys": [] });
    post_ok_signed(&srv, "/overlay/identity", &identity).await;

    let ev = evidence_body("src-15", "id-15", "RED", Some(1_800_000_000));
    let resp = post_ok_signed(&srv, "/overlay/evidence", &ev).await;
    assert_eq!(resp.status(), 422);
    assert_eq!(count_evidence(&srv.pool).await, 0);
}

// ---- 16. evidence_for_unknown_identity_is_422 ------------------------------

#[tokio::test]
async fn evidence_for_unknown_identity_is_422() {
    let srv = TestServer::start().await;
    let ev = evidence_body("src-16", "no-such-identity", "GREEN", Some(1_800_000_000));
    let resp = post_ok_signed(&srv, "/overlay/evidence", &ev).await;
    assert_eq!(resp.status(), 422);
    assert_eq!(count_evidence(&srv.pool).await, 0);
}

// ---- 17. tier3_append_only_triggers_raise ----------------------------------

#[tokio::test]
async fn tier3_append_only_triggers_raise() {
    let srv = TestServer::start().await;
    let identity = json!({ "identity_id": "id-17", "keys": [ evm_key(&evm_addr('e')) ] });
    post_ok_signed(&srv, "/overlay/identity", &identity).await;
    let ev = evidence_body("src-17", "id-17", "GREEN", Some(1_800_000_000));
    post_ok_signed(&srv, "/overlay/evidence", &ev).await;

    let err = sqlx::query("UPDATE audit.identity SET created_at = 0 WHERE identity_id = 'id-17'")
        .execute(&srv.pool)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("append-only"));

    let err = sqlx::query("DELETE FROM audit.identity WHERE identity_id = 'id-17'")
        .execute(&srv.pool)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("append-only"));

    let err = sqlx::query("UPDATE audit.identity_key SET added_at = 0 WHERE identity_id = 'id-17'")
        .execute(&srv.pool)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("append-only"));

    let err = sqlx::query("DELETE FROM audit.identity_key WHERE identity_id = 'id-17'")
        .execute(&srv.pool)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("append-only"));

    let err = sqlx::query("UPDATE audit.evidence_record SET status = 'RED' WHERE source_ref = 'src-17'")
        .execute(&srv.pool)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("append-only"));

    let err = sqlx::query("DELETE FROM audit.evidence_record WHERE source_ref = 'src-17'")
        .execute(&srv.pool)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("append-only"));
}

// ---- MED-2: evidence status whitelist -------------------------------------

#[tokio::test]
async fn kyc_status_must_be_exactly_green_or_red() {
    let srv = TestServer::start().await;
    let identity = json!({ "identity_id": "id-med2a", "keys": [] });
    post_ok_signed(&srv, "/overlay/identity", &identity).await;

    for (n, bad_status) in ["Red", "green", ""].iter().enumerate() {
        let ev = evidence_body(&format!("src-med2a-{n}"), "id-med2a", bad_status, None);
        let resp = post_ok_signed(&srv, "/overlay/evidence", &ev).await;
        assert_eq!(resp.status(), 422, "status {bad_status:?} must be rejected — case-exact GREEN/RED only");
    }

    let green = evidence_body("src-med2a-green", "id-med2a", "GREEN", Some(1_800_000_000));
    assert_eq!(post_ok_signed(&srv, "/overlay/evidence", &green).await.status(), 200);

    let red = evidence_body("src-med2a-red", "id-med2a", "RED", None);
    assert_eq!(post_ok_signed(&srv, "/overlay/evidence", &red).await.status(), 200);

    assert_eq!(count_evidence(&srv.pool).await, 2, "only the two GREEN/RED posts landed");
}

#[tokio::test]
async fn sanctions_screen_evidence_not_yet_accepted() {
    let srv = TestServer::start().await;
    let identity = json!({ "identity_id": "id-med2b", "keys": [] });
    post_ok_signed(&srv, "/overlay/identity", &identity).await;

    let mut ev = evidence_body("src-med2b", "id-med2b", "GREEN", None);
    ev["kind"] = json!("SANCTIONS_SCREEN");
    let resp = post_ok_signed(&srv, "/overlay/evidence", &ev).await;
    assert_eq!(resp.status(), 422, "SANCTIONS_SCREEN has no feeder yet — forward-only, don't ingest an undesigned vocab");
    assert_eq!(count_evidence(&srv.pool).await, 0);
}

// ---- LOW-2: identity-409 divergence arms (mutation-invisible today) ------

#[tokio::test]
async fn same_key_different_synthetic_address_is_409() {
    let srv = TestServer::start().await;
    let shared_key = solana_addr();

    let first = json!({ "identity_id": "id-low2a", "keys": [ solana_key(&shared_key, &evm_addr('1')) ] });
    assert_eq!(post_ok_signed(&srv, "/overlay/identity", &first).await.status(), 200);

    let second = json!({ "identity_id": "id-low2a", "keys": [ solana_key(&shared_key, &evm_addr('2')) ] });
    let resp = post_ok_signed(&srv, "/overlay/identity", &second).await;
    assert_eq!(
        resp.status(),
        409,
        "same identity + same root_key, DIFFERENT synthetic_evm_address must 409, not silently update"
    );
    assert_eq!(count_identity_key(&srv.pool).await, 1);
}

#[tokio::test]
async fn same_key_different_root_key_type_is_409() {
    let srv = TestServer::start().await;
    let shared_key = solana_addr(); // base58-shaped
    let synthetic = evm_addr('f');

    // Pre-seed DIRECTLY (bypassing the handler's shape check, which would
    // never let an EVM-typed row hold a base58-shaped root_key — the two
    // types' shapes are disjoint through the HTTP surface, see auth.rs
    // MED-4 doc for the analogous "verified disjoint" pattern) so the
    // SAME literal root_key exists under root_key_type = 'EVM'. The
    // synthetic address is seeded to the SAME value the incoming SOLANA
    // request will carry — isolating root_key_type as the ONLY divergent
    // field, so this test cannot pass via the (unrelated) synthetic-address
    // comparison arm.
    sqlx::query("INSERT INTO audit.identity (identity_id, created_at) VALUES ('id-low2b', 0)")
        .execute(&srv.pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO audit.identity_key (identity_id, root_key_type, root_key, derivation_fn_version, synthetic_evm_address, added_at)
         VALUES ('id-low2b', 'EVM', $1, 'v1', decode($2, 'hex'), 0)",
    )
    .bind(&shared_key)
    .bind(synthetic.trim_start_matches("0x"))
    .execute(&srv.pool)
    .await
    .unwrap();

    let body = json!({ "identity_id": "id-low2b", "keys": [ solana_key(&shared_key, &synthetic) ] });
    let resp = post_ok_signed(&srv, "/overlay/identity", &body).await;
    assert_eq!(
        resp.status(),
        409,
        "same identity + same root_key + same synthetic, DIFFERENT root_key_type must 409, not silently accept"
    );
    assert_eq!(count_identity_key(&srv.pool).await, 1);
}

// ---- LOW-3: future-side skew (integration level) --------------------------

#[tokio::test]
async fn future_skew_is_401() {
    let srv = TestServer::start().await;
    let body = serde_json::to_vec(&json!({ "identity_id": "future-skew-test", "keys": [] })).unwrap();
    let now = srv.now();

    let ts = now + 300;
    let sig = sign(ts, "POST", "/overlay/identity", &body);
    let resp = post_signed(&srv, "/overlay/identity", ts, &sig, &body).await;
    assert_eq!(resp.status(), 200, "now+300 (the boundary) must be accepted");

    let ts = now + 301;
    let sig = sign(ts, "POST", "/overlay/identity", &body);
    let resp = post_signed(&srv, "/overlay/identity", ts, &sig, &body).await;
    assert_eq!(resp.status(), 401, "now+301 (feeder clock running fast) must be rejected");
}

// ---- NIT: unsigned-401 on /overlay/evidence too ----------------------------

#[tokio::test]
async fn unsigned_evidence_request_is_401() {
    let srv = TestServer::start().await;
    let ev = evidence_body("src-nit-unsigned", "no-such-identity", "GREEN", None);
    let raw = serde_json::to_vec(&ev).unwrap();
    let ts = srv.now();
    let resp = post_signed(&srv, "/overlay/evidence", ts, "00", &raw).await;
    assert_eq!(resp.status(), 401);
    assert_eq!(count_evidence(&srv.pool).await, 0);
}

// ---- NIT: text-field length cap (an unbounded string is a permanent row) --

#[tokio::test]
async fn oversized_identity_id_is_422() {
    let srv = TestServer::start().await;
    let body = json!({ "identity_id": "x".repeat(300), "keys": [] });
    let resp = post_ok_signed(&srv, "/overlay/identity", &body).await;
    assert_eq!(resp.status(), 422);
    assert_eq!(count_identity(&srv.pool).await, 0);
}

#[tokio::test]
async fn oversized_evidence_field_is_422() {
    let srv = TestServer::start().await;
    let identity = json!({ "identity_id": "id-nit-oversized", "keys": [] });
    post_ok_signed(&srv, "/overlay/identity", &identity).await;

    // Delta review: cover each capped field individually, not just `vendor`
    // — deleting `source_ref` or `subject_ref_version` from the check_len
    // set would otherwise pass every test in this file.
    for field in ["vendor", "source_ref", "subject_ref_version"] {
        let mut ev = evidence_body(&format!("src-nit-oversized-{field}"), "id-nit-oversized", "GREEN", Some(1_800_000_000));
        ev[field] = json!("v".repeat(300));
        let resp = post_ok_signed(&srv, "/overlay/evidence", &ev).await;
        assert_eq!(resp.status(), 422, "field {field:?} must be capped");
    }
    assert_eq!(count_evidence(&srv.pool).await, 0);
}

// ---- Delta-review NIT: derivation_fn_version has the same permanent-bad-row
// exposure as the other TEXT fields (it's part of identity_key's PK,
// append-only) but had no length cap. -----------------------------------

#[tokio::test]
async fn oversized_derivation_fn_version_is_422() {
    let srv = TestServer::start().await;
    let body = json!({
        "identity_id": "id-nit-derivfn",
        "keys": [ {
            "root_key_type": "EVM",
            "root_key": evm_addr('d'),
            "derivation_fn_version": "v".repeat(300),
        } ],
    });
    let resp = post_ok_signed(&srv, "/overlay/identity", &body).await;
    assert_eq!(resp.status(), 422);
    assert_eq!(count_identity(&srv.pool).await, 0);
    assert_eq!(count_identity_key(&srv.pool).await, 0);
}

// ---- MED-3: DB-level CHECK constraints (defense-in-depth, bypass the handler) --

#[tokio::test]
async fn subject_ref_length_check_rejects_bad_bytea() {
    let srv = TestServer::start().await;
    sqlx::query("INSERT INTO audit.identity (identity_id, created_at) VALUES ('id-med3a', 0)")
        .execute(&srv.pool)
        .await
        .unwrap();

    let err = sqlx::query(
        "INSERT INTO audit.evidence_record
            (source_ref, identity_id, kind, vendor, subject_ref, subject_ref_version, status, valid_through, vendor_timestamp, received_at, evidence_hash)
         VALUES ('src-med3a', 'id-med3a', 'KYC_STATUS', 'sumsub', decode('aa', 'hex'), 'v1', 'GREEN', 1, 1, 1, decode('bb', 'hex'))",
    )
    .execute(&srv.pool)
    .await
    .unwrap_err();
    assert!(err.to_string().to_lowercase().contains("check"), "error was: {err}");
}

#[tokio::test]
async fn synthetic_evm_address_length_check_rejects_bad_bytea() {
    let srv = TestServer::start().await;
    sqlx::query("INSERT INTO audit.identity (identity_id, created_at) VALUES ('id-med3b', 0)")
        .execute(&srv.pool)
        .await
        .unwrap();

    let err = sqlx::query(
        "INSERT INTO audit.identity_key (identity_id, root_key_type, root_key, derivation_fn_version, synthetic_evm_address, added_at)
         VALUES ('id-med3b', 'SOLANA', 'somekey', 'v1', decode('aabb', 'hex'), 0)",
    )
    .execute(&srv.pool)
    .await
    .unwrap_err();
    assert!(err.to_string().to_lowercase().contains("check"), "error was: {err}");
}

// ---- 18. overlay_boot_touches_nothing --------------------------------------

#[tokio::test]
async fn overlay_boot_touches_nothing() {
    let pool = fresh_audit_db().await;

    // Pre-seed a Tier-1 row, a Tier-3 identity row — the "existing content"
    // a backfill/reconcile bug would touch.
    sqlx::query(
        "INSERT INTO audit.chain_event (chain_id, source_contract, source_kind, event_name, projection_tag, topic0, block_number, block_hash, block_timestamp, tx_hash, tx_index, log_index, tx_signer, args)
         VALUES (1, decode('00','hex'), 'GLOBAL_SANCTIONS', 'Sanctioned', 'primary', decode('00','hex'), 1, decode('00','hex'), 1, decode('00','hex'), 0, 0, decode('00','hex'), '{}')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO audit.identity (identity_id, created_at) VALUES ('pre-existing', 1)")
        .execute(&pool)
        .await
        .unwrap();

    let before_chain_event: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM audit.chain_event").fetch_one(&pool).await.unwrap();
    let before_identity = count_identity(&pool).await;

    // Boot the overlay server against this pre-seeded DB — no request sent.
    let _srv = TestServer::start_against(pool.clone()).await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let after_chain_event: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM audit.chain_event").fetch_one(&pool).await.unwrap();
    let after_identity = count_identity(&pool).await;

    assert_eq!(before_chain_event, after_chain_event, "boot must not touch audit.chain_event");
    assert_eq!(before_identity, after_identity, "boot must not touch audit.identity");
    assert_eq!(after_identity, 1, "still exactly the one pre-seeded row — forward-only, no backfill");
}
