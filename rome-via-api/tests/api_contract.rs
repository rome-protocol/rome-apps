/// API contract integration tests — validates OpenAPI document generation.
///
/// Tests that verify the routing structure and response shapes without needing
/// a live DB. DB-dependent integration tests live in the `tests/` repo.
use rome_via_api::api::openapi::ApiDoc;
use utoipa::OpenApi as _;

fn get_openapi_json() -> serde_json::Value {
    let doc = ApiDoc::openapi();
    let json = doc
        .to_pretty_json()
        .expect("OpenAPI document should serialize to JSON");
    serde_json::from_str(&json).expect("OpenAPI JSON should be valid JSON")
}

#[test]
fn openapi_document_is_valid_json() {
    let parsed = get_openapi_json();
    assert!(
        parsed.is_object(),
        "OpenAPI JSON should be an object at root"
    );
}

#[test]
fn openapi_has_required_paths() {
    let parsed = get_openapi_json();
    let paths = parsed["paths"].as_object().expect("paths should be an object");

    for path in [
        "/healthz",
        "/readyz",
        "/api/v1/stats/overview",
        "/api/v1/blocks",
        "/api/v1/blocks/{number}",
        "/api/v1/txs",
        "/api/v1/txs/{hash}",
        // Phase 3
        "/api/v1/tokens",
        "/api/v1/tokens/{address}",
        "/api/v1/tokens/{address}/holders",
        "/api/v1/tokens/{address}/transfers",
        "/api/v1/addresses/{address}",
        "/api/v1/addresses/{address}/txs",
        "/api/v1/search",
    ] {
        assert!(
            paths.contains_key(path),
            "OpenAPI spec is missing path: {path}"
        );
    }
}

#[test]
fn openapi_version_matches_spec() {
    let parsed = get_openapi_json();
    let version = parsed["info"]["version"]
        .as_str()
        .expect("info.version should be a string");
    assert_eq!(version, "3.0.0");
}

#[test]
fn openapi_has_required_schemas() {
    let parsed = get_openapi_json();
    let schemas = parsed["components"]["schemas"]
        .as_object()
        .expect("components.schemas should be an object");

    for schema in [
        "Block", "Tx", "TxStatus", "TxType", "StatsOverview", "ProblemJson",
        // Phase 3
        "TokenSummary", "TokenDetail", "TokenHolder", "TokenTransfer",
        "AddressDetail", "SearchHit", "SearchResults",
    ] {
        assert!(
            schemas.contains_key(schema),
            "OpenAPI spec is missing schema: {schema}"
        );
    }
}

#[test]
fn openapi_block_schema_has_correct_fields() {
    let parsed = get_openapi_json();
    let block = &parsed["components"]["schemas"]["Block"]["properties"];
    assert!(block["number"].is_object(), "Block.number should be documented");
    assert!(block["slot"].is_object(), "Block.slot should be documented");
    assert!(block["txCount"].is_object(), "Block.txCount should be documented (camelCase)");
    assert!(block["gasUsed"].is_object(), "Block.gasUsed should be documented (camelCase)");
    assert!(block["timestamp"].is_object(), "Block.timestamp should be documented");
}

#[test]
fn openapi_tx_schema_has_correct_fields() {
    let parsed = get_openapi_json();
    let tx = &parsed["components"]["schemas"]["Tx"]["properties"];
    assert!(tx["hash"].is_object(), "Tx.hash should be documented");
    assert!(tx["status"].is_object(), "Tx.status should be documented");
    assert!(tx["from"].is_object(), "Tx.from should be documented");
    assert!(tx["value"].is_object(), "Tx.value should be documented");
    assert!(tx["blockNumber"].is_object(), "Tx.blockNumber should be documented (camelCase)");
}

#[test]
fn openapi_stats_overview_has_tps_field() {
    let parsed = get_openapi_json();
    let stats = &parsed["components"]["schemas"]["StatsOverview"]["properties"];
    assert!(
        stats["tps60sEstimate"].is_object() || stats["tps_60s_estimate"].is_object(),
        "StatsOverview should have tps60sEstimate field"
    );
}

/// The Home dashboard + Tokens header consume these chain-wide aggregates.
/// Lock them into the documented OpenAPI contract (camelCase keys).
///
/// `typeCounts` is deliberately absent: the chain-wide Rhea/Remus/Romulus counts
/// cost three whole-table scans of a 13.4M-row table per cache miss (87,627
/// buffers and 665 ms each, measured on hadrian) to produce three integers no
/// surface rendered. The PER-BLOCK counts still exist on `Block`, bounded by
/// `ccc.block_number = eb.params_number`, and BlockList draws its distribution
/// from those. Asserting absence, because re-documenting the field is how the
/// scans come back.
#[test]
fn openapi_stats_overview_has_chainwide_aggregates() {
    let parsed = get_openapi_json();
    let stats = &parsed["components"]["schemas"]["StatsOverview"]["properties"];
    for field in ["txCountTotal", "activeAddresses", "tokenCountTotal"] {
        assert!(
            stats[field].is_object(),
            "StatsOverview should document `{field}`, got: {stats:#}"
        );
    }
    assert!(
        stats["typeCounts"].is_null(),
        "chain-wide typeCounts must not be documented — it cost 3x a 13.4M-row \
         scan per cache miss and nothing rendered it, got: {stats:#}"
    );
}

/// The Tokens + Addresses headers consume two new chain-wide breakdown objects
/// on `/stats/overview`. Lock them into the documented OpenAPI contract
/// (camelCase fields) and verify the nested `TokenKindCounts` /
/// `AddressTypeCounts` schemas resolve (utoipa auto-collects them via the
/// field `$ref`, same as `TypeCounts` — they are not in the explicit
/// `schemas()` list).
#[test]
fn openapi_stats_overview_has_kind_and_address_breakdowns() {
    let parsed = get_openapi_json();
    let stats = &parsed["components"]["schemas"]["StatsOverview"]["properties"];
    for field in ["tokenKindCounts", "addressTypeCounts"] {
        assert!(
            stats[field].is_object(),
            "StatsOverview should document `{field}`, got: {stats:#}"
        );
    }
    let kind_counts = &parsed["components"]["schemas"]["TokenKindCounts"]["properties"];
    for k in ["erc20", "spl", "token2022"] {
        assert!(
            kind_counts[k].is_object(),
            "TokenKindCounts schema should document `{k}`, got: {kind_counts:#}"
        );
    }
    let addr_counts = &parsed["components"]["schemas"]["AddressTypeCounts"]["properties"];
    for k in ["contracts", "eoas", "synthetics"] {
        assert!(
            addr_counts[k].is_object(),
            "AddressTypeCounts schema should document `{k}`, got: {addr_counts:#}"
        );
    }
}

/// The token LIST response now carries `decimals` + `circulatingSupply` so the
/// Supply column renders a decimated amount, not a raw integer.
#[test]
fn openapi_token_summary_has_decimals_and_circulating_supply() {
    let parsed = get_openapi_json();
    let summary = &parsed["components"]["schemas"]["TokenSummary"]["properties"];
    for field in ["decimals", "circulatingSupply"] {
        assert!(
            summary[field].is_object(),
            "TokenSummary should document `{field}`, got: {summary:#}"
        );
    }
}

/// SearchHit.kind must be documented so the UI can rely on it for token
/// kind relabeling (SPL → "Wrapped SPL").
#[test]
fn openapi_search_hit_has_kind_field() {
    let parsed = get_openapi_json();
    let hit = &parsed["components"]["schemas"]["SearchHit"]["properties"];
    assert!(
        hit["kind"].is_object(),
        "SearchHit should document `kind`, got: {hit:#}"
    );
}

/// Verify /healthz handler exists and is a simple async fn.
/// Full HTTP-level testing needs tower::ServiceExt - see test plan below.
///
/// # UI Agent test plan for healthz HTTP layer:
/// ```bash
/// curl http://localhost:8090/healthz   # expect 200 "ok"
/// curl http://localhost:8090/readyz    # expect 200 "ok" or 503 if DB down
/// curl http://localhost:8090/api/v1/openapi.json  # expect JSON
/// curl http://localhost:8090/api/v1/docs           # expect HTML
/// ```
#[test]
fn healthz_handler_fn_exists() {
    // Compile-time proof the function is callable.
    let _: fn() -> _ = rome_via_api::api::health::healthz;
}
