use cardo_service::manifest::schema::SchemaValidator;

fn validator() -> SchemaValidator {
    let raw = include_str!("fixtures/catalog.schema.json");
    SchemaValidator::from_json_str(raw).expect("schema parses")
}

#[test]
fn accepts_minimal_manifest() {
    let v = validator();
    let body = include_str!("fixtures/manifests/valid-minimal.json");
    let m: serde_json::Value = serde_json::from_str(body).unwrap();
    assert!(v.validate(&m).is_ok());
}

#[test]
fn accepts_full_manifest() {
    let v = validator();
    let body = include_str!("fixtures/manifests/valid-full.json");
    let m: serde_json::Value = serde_json::from_str(body).unwrap();
    assert!(v.validate(&m).is_ok());
}

#[test]
fn rejects_missing_id() {
    let v = validator();
    let body = include_str!("fixtures/manifests/invalid-schema.json");
    let m: serde_json::Value = serde_json::from_str(body).unwrap();
    let err = v.validate(&m).unwrap_err();
    assert!(err.to_string().contains("required") || err.to_string().contains("id"));
}
