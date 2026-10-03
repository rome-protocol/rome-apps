// Integration test: /healthz and /readyz endpoints for rome-via-api.
//
// Mirrors jsonrpsee server pattern from rome-apps/hercules/src/api/admin.rs:89-109.
// Health shape: HTTP 200, body exactly "ok" (plain text).
// SPEC §Operational: `/healthz` = liveness probe, `/readyz` = readiness probe.
//
// Phase 2.7 (Harden): Updated to handle graceful-shutdown tuple return value.
// Added /readyz tests per SPEC §Operational requirement.

use rome_via_api::server::start_health_server;

/// Boot the API server on an ephemeral port; GET /healthz returns 200 "ok".
#[tokio::test]
async fn healthz_returns_200_ok() {
    // Bind to OS-assigned ephemeral port (port 0)
    let (addr, _shutdown) = start_health_server("127.0.0.1:0")
        .await
        .expect("health server should start on ephemeral port");

    let url = format!("http://{}/healthz", addr);
    let resp = reqwest::get(&url)
        .await
        .expect("GET /healthz should succeed");

    assert_eq!(resp.status().as_u16(), 200, "/healthz must return HTTP 200");

    let body = resp.text().await.expect("response body should be readable");
    assert_eq!(body, "ok", "/healthz body must be exactly \"ok\"");
}

/// GET /healthz is idempotent — two consecutive calls both succeed.
#[tokio::test]
async fn healthz_is_idempotent() {
    let (addr, _shutdown) = start_health_server("127.0.0.1:0")
        .await
        .expect("health server should start");

    let url = format!("http://{}/healthz", addr);

    for _ in 0..2 {
        let resp = reqwest::get(&url)
            .await
            .expect("GET /healthz should succeed");
        assert_eq!(resp.status().as_u16(), 200);
    }
}

/// GET /readyz returns 200 "ok" — Phase 1 stub for readiness probe.
/// Phase 2 will replace this with a real DB connectivity check.
#[tokio::test]
async fn readyz_returns_200_ok() {
    let (addr, _shutdown) = start_health_server("127.0.0.1:0")
        .await
        .expect("health server should start on ephemeral port");

    let url = format!("http://{}/readyz", addr);
    let resp = reqwest::get(&url)
        .await
        .expect("GET /readyz should succeed");

    assert_eq!(resp.status().as_u16(), 200, "/readyz must return HTTP 200");

    let body = resp.text().await.expect("response body should be readable");
    assert_eq!(body, "ok", "/readyz body must be exactly \"ok\" in Phase 1 stub");
}

/// Unknown path returns 404 — server does not accept arbitrary routes.
#[tokio::test]
async fn unknown_path_returns_404() {
    let (addr, _shutdown) = start_health_server("127.0.0.1:0")
        .await
        .expect("health server should start on ephemeral port");

    let url = format!("http://{}/unknown", addr);
    let resp = reqwest::get(&url)
        .await
        .expect("request to unknown path should complete");

    assert_eq!(resp.status().as_u16(), 404, "unknown path must return HTTP 404");
}
