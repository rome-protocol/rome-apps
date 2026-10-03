use cardo_service::manifest::signature::{verify_manifest, Verifier};
use std::path::Path;

fn verifier() -> Verifier {
    let keys_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/keys");
    Verifier::from_keys_dir(&keys_dir).expect("keys load")
}

#[test]
fn verifies_signed_minimal() {
    let v = verifier();
    let body = include_str!("fixtures/manifests/signed-minimal.json");
    let m: serde_json::Value = serde_json::from_str(body).unwrap();
    verify_manifest(&v, &m).expect("signed manifest should verify");
}

#[test]
fn rejects_tampered_signature() {
    let v = verifier();
    let body = include_str!("fixtures/manifests/invalid-signature.json");
    let m: serde_json::Value = serde_json::from_str(body).unwrap();
    assert!(verify_manifest(&v, &m).is_err());
}

#[test]
fn canonicalize_sorts_nested_keys_recursively() {
    use cardo_service::manifest::signature::canonicalize;
    let a: serde_json::Value = serde_json::from_str(r#"{"b":{"z":1,"a":2},"a":[3,{"y":4,"x":5}]}"#).unwrap();
    let b: serde_json::Value = serde_json::from_str(r#"{"a":[3,{"x":5,"y":4}],"b":{"a":2,"z":1}}"#).unwrap();
    assert_eq!(canonicalize(&a), canonicalize(&b));

    let c: serde_json::Value = serde_json::from_str(r#"{"b":{"z":1,"a":999},"a":[3,{"y":4,"x":5}]}"#).unwrap();
    assert_ne!(canonicalize(&a), canonicalize(&c));
}
