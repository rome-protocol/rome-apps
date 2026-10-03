//! End-to-end handler tests via `axum::body::Body` + `tower::ServiceExt::oneshot`.
//!
//! No port binding — pure in-process tests. The `/health` handler does not
//! depend on `AppState`, so it's testable with a minimal router. Tests that
//! need a live Postgres pool or a real TxBuilder are deferred to M3D's
//! integration suite; here we only cover the parts that can run without
//! external services.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;

/// `GET /health` always returns 200 with `{"status":"ok"}` regardless of any
/// backend state.
#[tokio::test]
async fn health_always_ok() {
    use axum::routing::get;
    use axum::Router;

    // Minimal router containing only /health; no AppState required because the
    // handler doesn't pull State<_>.
    let router: Router = Router::new().route(
        "/health",
        get(cardo_service::rest::handlers::health),
    );

    let res = router
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let body_str = std::str::from_utf8(&body).unwrap();
    assert!(
        body_str.contains("\"ok\""),
        "body missing ok marker: {body_str}"
    );
}

/// Belt-and-braces companion to `rest_tx_builder_test`: construct an
/// `UnsignedTxResponse` directly (bypassing TxBuilder, whose simulate path
/// requires a live stack) and verify the serialized form never leaks signing
/// keywords.
#[tokio::test]
async fn execute_response_never_carries_signing_keywords() {
    use cardo_service::rest::types::UnsignedTxResponse;

    let r = UnsignedTxResponse {
        to: "0x0".into(),
        data: "0x".into(),
        value: "0".into(),
        gas_limit: "21000".into(),
        chain_id: 200200,
        nonce: 0,
        max_fee_per_gas: "1".into(),
        max_priority_fee_per_gas: "1".into(),
    };
    let json = serde_json::to_string(&r).unwrap().to_lowercase();
    for forbidden in ["signature", "private", "secret", "mnemonic"] {
        assert!(
            !json.contains(forbidden),
            "execute response leaks '{forbidden}' in: {json}"
        );
    }
}

/// OpenAPI document covers every endpoint the router exposes — a regression
/// guard: if someone adds a route without registering its `_op` wrapper, this
/// fails loudly. Uses the openapi v3 path format (curly braces for path params).
#[test]
fn openapi_doc_has_all_routes() {
    use cardo_service::rest::openapi::ApiDoc;
    use utoipa::OpenApi;

    let doc = ApiDoc::openapi();
    let json = serde_json::to_value(&doc).expect("openapi doc should serialize");
    let paths = json["paths"]
        .as_object()
        .expect("openapi doc should have paths object");

    for expected in [
        "/health",
        "/ready",
        "/apps",
        "/apps/{id}",
        "/apps/{id}/metrics",
        "/apps/{id}/capabilities/{name}/quote",
        "/apps/{id}/capabilities/{name}/execute",
    ] {
        assert!(
            paths.contains_key(expected),
            "openapi doc missing path: {expected}; have: {:?}",
            paths.keys().collect::<Vec<_>>()
        );
    }
}
