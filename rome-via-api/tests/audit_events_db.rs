// DB-backed integration tests for the public Audit tab backend.
//
// The endpoints are public like every other rome-via-api GET endpoint (no auth
// gate). Over real HTTP through the fully mounted router, these cover the events
// keyset walk, filters, validation 400s, and the counts shape.
//
// Requires a disposable Postgres — same gate the rome-audit suite uses:
//   ROME_AUDIT_TEST_PG_ADMIN_URL (default postgres://postgres:test@localhost:55432/postgres)
// Runs the rome-audit migrator (so the `audit` schema + the 0913 browse index
// exist), seeds `audit.chain_event` under a throwaway chain_id in a fresh DB,
// and drops the DB at the end. Skips cleanly when the admin URL is unset.

use std::net::SocketAddr;
use std::sync::Arc;

use rome_via_api::api;
use rome_via_api::cursor::{self, AuditCursor};
use rome_via_api::state::AppState;
use sqlx::PgPool;

const THROWAWAY_CHAIN_ID: i64 = 990913;
const SECRET: &[u8] = b"audit-test-cursor-secret-0000000";

fn admin_url() -> Option<String> {
    // Mirror the rome-audit suite's gate; also accept HERCULES_TEST_DATABASE_URL's
    // admin form is NOT assumed — we always create a fresh DB off the admin URL.
    std::env::var("ROME_AUDIT_TEST_PG_ADMIN_URL")
        .ok()
        .or_else(|| {
            // If only the shared hercules URL is set, skip — we need admin (CREATE DB).
            None
        })
}

fn unique_db_name() -> String {
    // (pid, nanos) alone collides: macOS clock granularity lets two concurrent
    // tests draw the same nanosecond → duplicate CREATE DATABASE (23505). The
    // process-global sequence makes the name unique regardless of clock.
    static DB_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let seq = DB_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!("rome_via_audit_test_{}_{}_{}", std::process::id(), nanos, seq)
}

/// Create a fresh DB off the admin URL and run the rome-audit migrator against
/// it. Returns (pool, admin_pool, db_name) so the caller can drop it after.
async fn fresh_migrated_db(admin: &str) -> (PgPool, PgPool, String) {
    let admin_pool = PgPool::connect(admin)
        .await
        .expect("connect to admin Postgres — is the test PG running?");
    let db_name = unique_db_name();
    sqlx::query(&format!("CREATE DATABASE {db_name}"))
        .execute(&admin_pool)
        .await
        .expect("CREATE DATABASE");
    let db_url = format!("{}/{db_name}", admin.trim_end_matches("/postgres"));
    let pool = PgPool::connect(&db_url).await.expect("connect to fresh DB");

    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../rome-audit/migrations");
    let migrator = sqlx::migrate::Migrator::new(std::path::Path::new(dir))
        .await
        .expect("load rome-audit migrator");
    migrator.run(&pool).await.expect("rome-audit migrations (incl. 0913)");

    (pool, admin_pool, db_name)
}

async fn drop_db(admin_pool: &PgPool, pool: PgPool, db_name: &str) {
    pool.close().await;
    let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS {db_name} WITH (FORCE)"))
        .execute(admin_pool)
        .await;
}

/// Seed one audit.chain_event row. bytea columns are derived from the ordering
/// triple so every row is unique.
async fn seed_event(
    pool: &PgPool,
    block: i64,
    tx_idx: i32,
    log_idx: i32,
    event_name: &str,
    source_kind: &str,
    projection_tag: &str,
) {
    let tx_hash: Vec<u8> = {
        let mut v = vec![0u8; 32];
        v[0] = block as u8;
        v[1] = tx_idx as u8;
        v[2] = log_idx as u8;
        v[31] = 0xaa;
        v
    };
    let source_contract = vec![0xc0u8; 20];
    let tx_signer = vec![0x51u8; 20];
    let topic0 = vec![0x70u8; 32];
    let block_hash = vec![0xb1u8; 32];
    let args = serde_json::json!({ "block": block, "amount": "1000" });

    sqlx::query(
        "INSERT INTO audit.chain_event
           (chain_id, source_contract, source_kind, event_name, projection_tag,
            topic0, block_number, block_hash, block_timestamp, tx_hash, tx_index,
            log_index, tx_signer, args)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)",
    )
    .bind(THROWAWAY_CHAIN_ID)
    .bind(source_contract)
    .bind(source_kind)
    .bind(event_name)
    .bind(projection_tag)
    .bind(topic0)
    .bind(block)
    .bind(block_hash)
    .bind(1_700_000_000i64 + block)
    .bind(tx_hash)
    .bind(tx_idx)
    .bind(log_idx)
    .bind(tx_signer)
    .bind(args)
    .execute(pool)
    .await
    .expect("seed audit.chain_event row");
}

fn state_with(pool: PgPool) -> AppState {
    AppState::new(
        pool,
        THROWAWAY_CHAIN_ID,
        SECRET.to_vec(),
        "http://127.0.0.1:1".to_string(),
        None,
        Arc::new(std::collections::HashMap::new()),
        Arc::new(std::collections::HashMap::new()),
    )
}

/// Bind the fully-mounted router on an ephemeral port; returns its base URL.
async fn serve(state: AppState) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let app = api::router(state);
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    format!("http://{addr}")
}

// ── public access: both routes are open (no auth) ──────────────────────────

#[tokio::test]
async fn audit_routes_are_public_200() {
    let Some(admin) = admin_url() else {
        eprintln!("SKIP: ROME_AUDIT_TEST_PG_ADMIN_URL unset");
        return;
    };
    let (pool, admin_pool, db) = fresh_migrated_db(&admin).await;
    seed_event(&pool, 10, 0, 0, "Transfer", "ArcToken", "primary").await;
    let base = serve(state_with(pool.clone())).await;
    let client = reqwest::Client::new();

    // No Authorization header — both endpoints answer 200 like every other GET.
    for path in ["/api/v1/audit/events", "/api/v1/audit/event-counts"] {
        let r = client.get(format!("{base}{path}")).send().await.unwrap();
        assert_eq!(r.status().as_u16(), 200, "public {path} must 200 (no auth)");
    }

    drop_db(&admin_pool, pool, &db).await;
}

// ── events: keyset, filters, validation ────────────────────────────────────

async fn get_json(client: &reqwest::Client, url: &str) -> (u16, serde_json::Value) {
    let r = client.get(url).send().await.unwrap();
    let status = r.status().as_u16();
    let body = r.json::<serde_json::Value>().await.unwrap_or(serde_json::Value::Null);
    (status, body)
}

#[tokio::test]
async fn events_keyset_walk_no_dup_no_gap() {
    let Some(admin) = admin_url() else {
        eprintln!("SKIP: ROME_AUDIT_TEST_PG_ADMIN_URL unset");
        return;
    };
    let (pool, admin_pool, db) = fresh_migrated_db(&admin).await;

    // 9 events across 3 blocks × mixed (tx_idx, log_idx) → ≥3 pages at limit=3.
    let mut expected = Vec::new();
    for block in [100i64, 101, 102] {
        for tx_idx in [0i32, 1] {
            for log_idx in [0i32, 1] {
                // keep it to 9 total: skip the 4th combo on block 102
                if block == 102 && tx_idx == 1 && log_idx == 1 {
                    continue;
                }
                if block == 101 && tx_idx == 1 && log_idx == 1 {
                    continue;
                }
                if block == 100 && tx_idx == 1 && log_idx == 1 {
                    continue;
                }
                seed_event(&pool, block, tx_idx, log_idx, "Transfer", "ArcToken", "primary").await;
                expected.push((block, tx_idx, log_idx));
            }
        }
    }
    // newest-first order
    expected.sort_by(|a, b| b.cmp(a));

    let base = serve(state_with(pool.clone())).await;
    let client = reqwest::Client::new();

    let mut seen: Vec<(i64, i32, i32)> = Vec::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0;
    loop {
        let url = match &cursor {
            Some(c) => format!("{base}/api/v1/audit/events?limit=3&cursor={c}"),
            None => format!("{base}/api/v1/audit/events?limit=3"),
        };
        let (status, body) = get_json(&client, &url).await;
        assert_eq!(status, 200);
        pages += 1;
        for item in body["items"].as_array().unwrap() {
            seen.push((
                item["blockNumber"].as_i64().unwrap(),
                item["txIndex"].as_i64().unwrap() as i32,
                item["logIndex"].as_i64().unwrap() as i32,
            ));
        }
        if body["hasMore"].as_bool().unwrap() {
            cursor = Some(body["nextCursor"].as_str().unwrap().to_string());
        } else {
            assert!(body["nextCursor"].is_null(), "last page has null nextCursor");
            break;
        }
        assert!(pages <= 10, "runaway pagination");
    }

    assert!(pages >= 3, "expected ≥3 pages, got {pages}");
    assert_eq!(seen.len(), expected.len(), "no rows dropped / duplicated");
    assert_eq!(seen, expected, "exact newest-first order, no dup, no gap");
    // uniqueness
    let mut dedup = seen.clone();
    dedup.dedup();
    assert_eq!(dedup.len(), seen.len(), "no duplicate keys across pages");

    drop_db(&admin_pool, pool, &db).await;
}

#[tokio::test]
async fn events_event_name_filter_is_exact_and_row_shape_is_flat() {
    let Some(admin) = admin_url() else {
        eprintln!("SKIP: ROME_AUDIT_TEST_PG_ADMIN_URL unset");
        return;
    };
    let (pool, admin_pool, db) = fresh_migrated_db(&admin).await;
    seed_event(&pool, 5, 0, 0, "Transfer", "ArcToken", "primary").await;
    seed_event(&pool, 5, 0, 1, "Approval", "ArcToken", "supporting").await;
    seed_event(&pool, 6, 0, 0, "Transfer", "UV2", "primary").await;

    let base = serve(state_with(pool.clone())).await;
    let client = reqwest::Client::new();

    let (status, body) =
        get_json(&client, &format!("{base}/api/v1/audit/events?event_name=Transfer")).await;
    assert_eq!(status, 200);
    let items = body["items"].as_array().unwrap();
    assert_eq!(items.len(), 2, "two Transfer rows");
    assert!(items.iter().all(|i| i["eventName"] == "Transfer"));

    // Flat single-table camelCase shape + 0x-hex bytea projection.
    let first = &items[0];
    for key in [
        "eventId",
        "blockNumber",
        "blockTimestamp",
        "txHash",
        "txIndex",
        "logIndex",
        "eventName",
        "sourceContract",
        "sourceKind",
        "projectionTag",
        "txSigner",
        "args",
    ] {
        assert!(first.get(key).is_some(), "missing key {key} in {first}");
    }
    assert!(first["txHash"].as_str().unwrap().starts_with("0x"));
    assert!(first["sourceContract"].as_str().unwrap().starts_with("0x"));
    assert!(first["txSigner"].as_str().unwrap().starts_with("0x"));
    assert!(first["args"].is_object(), "args passed through as JSON");

    // residual source_kind filter narrows further
    let (_, body2) = get_json(
        &client,
        &format!("{base}/api/v1/audit/events?event_name=Transfer&source_kind=UV2"),
    )
    .await;
    let items2 = body2["items"].as_array().unwrap();
    assert_eq!(items2.len(), 1, "Transfer∧UV2 is one row");
    assert_eq!(items2[0]["blockNumber"].as_i64().unwrap(), 6);

    drop_db(&admin_pool, pool, &db).await;
}

#[tokio::test]
async fn events_validation_and_limit_and_cursor_rejections() {
    let Some(admin) = admin_url() else {
        eprintln!("SKIP: ROME_AUDIT_TEST_PG_ADMIN_URL unset");
        return;
    };
    let (pool, admin_pool, db) = fresh_migrated_db(&admin).await;
    seed_event(&pool, 1, 0, 0, "Transfer", "ArcToken", "primary").await;

    let base = serve(state_with(pool.clone())).await;
    let client = reqwest::Client::new();

    // unknown event_name ⇒ 400 (not a silently-empty page)
    let (s1, _) = get_json(&client, &format!("{base}/api/v1/audit/events?event_name=Nope")).await;
    assert_eq!(s1, 400, "unknown event_name must 400");

    // unknown source_kind ⇒ 400
    let (s2, _) = get_json(&client, &format!("{base}/api/v1/audit/events?source_kind=Nope")).await;
    assert_eq!(s2, 400, "unknown source_kind must 400");

    // limit > 100 ⇒ 400 (no silent clamp)
    let (s3, _) = get_json(&client, &format!("{base}/api/v1/audit/events?limit=101")).await;
    assert_eq!(s3, 400, "limit>100 must 400");

    // cursor for a different chain ⇒ rejected (400)
    let wrong = cursor::encode(
        &AuditCursor { chain_id: THROWAWAY_CHAIN_ID + 1, last_block: 1, last_tx_idx: 0, last_log_idx: 0 },
        SECRET,
    )
    .unwrap();
    let (s4, _) = get_json(&client, &format!("{base}/api/v1/audit/events?cursor={wrong}")).await;
    assert_eq!(s4, 400, "chain-mismatch cursor must be rejected");

    // tampered cursor ⇒ rejected (400)
    let good = cursor::encode(
        &AuditCursor { chain_id: THROWAWAY_CHAIN_ID, last_block: 1, last_tx_idx: 0, last_log_idx: 0 },
        SECRET,
    )
    .unwrap();
    let tampered = format!("{}X", &good[..good.len() - 1]);
    let (s5, _) = get_json(&client, &format!("{base}/api/v1/audit/events?cursor={tampered}")).await;
    assert_eq!(s5, 400, "tampered cursor must be rejected");

    // first page (no cursor) works
    let (s6, body6) = get_json(&client, &format!("{base}/api/v1/audit/events")).await;
    assert_eq!(s6, 200);
    assert_eq!(body6["items"].as_array().unwrap().len(), 1);

    drop_db(&admin_pool, pool, &db).await;
}

#[tokio::test]
async fn event_counts_grouped_and_total_match_seed() {
    let Some(admin) = admin_url() else {
        eprintln!("SKIP: ROME_AUDIT_TEST_PG_ADMIN_URL unset");
        return;
    };
    let (pool, admin_pool, db) = fresh_migrated_db(&admin).await;

    // "Transfer" under two source_kinds — the ambiguity the grouping exists for.
    seed_event(&pool, 1, 0, 0, "Transfer", "ArcToken", "primary").await;
    seed_event(&pool, 1, 0, 1, "Transfer", "ArcToken", "primary").await;
    seed_event(&pool, 2, 0, 0, "Transfer", "UV2", "primary").await;
    seed_event(&pool, 3, 0, 0, "Approval", "ArcToken", "supporting").await;

    let base = serve(state_with(pool.clone())).await;
    let client = reqwest::Client::new();

    let (status, body) = get_json(&client, &format!("{base}/api/v1/audit/event-counts")).await;
    assert_eq!(status, 200);
    assert_eq!(body["total"].as_i64().unwrap(), 4, "total counts all seeded rows");

    let counts = body["counts"].as_array().unwrap();
    let find = |sk: &str, en: &str, pt: &str| -> i64 {
        counts
            .iter()
            .find(|c| c["sourceKind"] == sk && c["eventName"] == en && c["projectionTag"] == pt)
            .map(|c| c["count"].as_i64().unwrap())
            .unwrap_or(0)
    };
    assert_eq!(find("ArcToken", "Transfer", "primary"), 2);
    assert_eq!(find("UV2", "Transfer", "primary"), 1);
    assert_eq!(find("ArcToken", "Approval", "supporting"), 1);
    // "Transfer" is split across source_kinds — never collapsed into one row.
    assert_eq!(counts.iter().filter(|c| c["eventName"] == "Transfer").count(), 2);

    drop_db(&admin_pool, pool, &db).await;
}

// ── absent audit schema: designed disabled state, never a 500 ───────────────

/// Fresh DB WITHOUT the rome-audit migrator — the state every via deployment
/// is in on a chain with no rome-audit stack (the `audit` schema absent).
async fn fresh_unmigrated_db(admin: &str) -> (PgPool, PgPool, String) {
    let admin_pool = PgPool::connect(admin)
        .await
        .expect("connect to admin Postgres — is the test PG running?");
    let db_name = unique_db_name();
    sqlx::query(&format!("CREATE DATABASE {db_name}"))
        .execute(&admin_pool)
        .await
        .expect("CREATE DATABASE");
    let db_url = format!("{}/{db_name}", admin.trim_end_matches("/postgres"));
    let pool = PgPool::connect(&db_url).await.expect("connect to fresh DB");
    (pool, admin_pool, db_name)
}

#[tokio::test]
async fn absent_audit_schema_is_designed_disabled_state_not_500() {
    let Some(admin) = admin_url() else {
        eprintln!("SKIP: ROME_AUDIT_TEST_PG_ADMIN_URL unset");
        return;
    };
    let (pool, admin_pool, db) = fresh_unmigrated_db(&admin).await;
    let base = serve(state_with(pool.clone())).await;
    let client = reqwest::Client::new();

    // counts → 200 with enabled:false and zero counts, not a 42P01-driven 500.
    let (status, body) = get_json(&client, &format!("{base}/api/v1/audit/event-counts")).await;
    assert_eq!(status, 200, "counts must not 500 when audit schema is absent: {body}");
    assert_eq!(body["enabled"], false, "counts must mark the trail disabled: {body}");
    assert_eq!(body["total"].as_i64().unwrap(), 0);
    assert!(body["counts"].as_array().unwrap().is_empty());

    // events → 200 empty page; the filter validators must not 500 either.
    for url in [
        format!("{base}/api/v1/audit/events?limit=25"),
        format!("{base}/api/v1/audit/events?event_name=Transfer"),
    ] {
        let (status, body) = get_json(&client, &url).await;
        assert_eq!(status, 200, "events must not 500 when audit schema is absent: {body}");
        assert!(body["items"].as_array().unwrap().is_empty(), "{body}");
        assert_eq!(body["hasMore"], false);
        assert!(body["nextCursor"].is_null());
    }

    drop_db(&admin_pool, pool, &db).await;
}

#[tokio::test]
async fn migrated_audit_schema_reports_enabled_true() {
    let Some(admin) = admin_url() else {
        eprintln!("SKIP: ROME_AUDIT_TEST_PG_ADMIN_URL unset");
        return;
    };
    let (pool, admin_pool, db) = fresh_migrated_db(&admin).await;
    let base = serve(state_with(pool.clone())).await;
    let client = reqwest::Client::new();

    let (status, body) = get_json(&client, &format!("{base}/api/v1/audit/event-counts")).await;
    assert_eq!(status, 200);
    assert_eq!(body["enabled"], true, "migrated deployment must report enabled:true: {body}");

    drop_db(&admin_pool, pool, &db).await;
}
