use cardo_service::rest::types::{AppListQuery, QuoteRequest, UnsignedTxResponse};

#[test]
fn app_list_query_defaults() {
    let q: AppListQuery = serde_urlencoded::from_str("").unwrap();
    assert_eq!(q.limit, 50);
    assert_eq!(q.offset, 0);
    assert!(q.tier.is_none());
}

#[test]
fn unsigned_tx_response_serializes_flat() {
    let tx = UnsignedTxResponse {
        to: "0x1234".into(),
        data: "0xcafe".into(),
        value: "0".into(),
        gas_limit: "21000".into(),
        chain_id: 200200,
        nonce: 7,
        max_fee_per_gas: "1000000000".into(),
        max_priority_fee_per_gas: "1".into(),
    };
    let json = serde_json::to_value(&tx).unwrap();
    assert_eq!(json["to"], "0x1234");
    assert_eq!(json["chain_id"], 200200);
    assert_eq!(json["nonce"], 7);
    assert!(
        json.get("signatures").is_none(),
        "must never contain signatures field"
    );
}

#[test]
fn quote_request_accepts_any_inputs() {
    let raw = r#"{"from":"0xabc","inputs":{"vault":"0xdef","amount":"100"}}"#;
    let q: QuoteRequest = serde_json::from_str(raw).unwrap();
    assert_eq!(q.from, "0xabc");
    assert_eq!(q.inputs["vault"], "0xdef");
}
