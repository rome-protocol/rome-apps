use cardo_service::manifest::model::Manifest;

#[test]
fn deserializes_minimal_manifest() {
    let raw = include_str!("fixtures/manifests/valid-minimal.json");
    let m: Manifest = serde_json::from_str(raw).expect("minimal manifest parses");
    assert_eq!(m.id, "test-app");
    assert_eq!(m.name, "Test App");
    assert_eq!(m.tier, "long-tail");
    assert_eq!(m.categories, vec!["yield"]);
    assert_eq!(m.capabilities.len(), 1);
    assert_eq!(m.capabilities[0].name, "ping");
    assert_eq!(m.capabilities[0].kind, "query");
}

#[test]
fn deserializes_full_manifest() {
    let raw = include_str!("fixtures/manifests/valid-full.json");
    let m: Manifest = serde_json::from_str(raw).expect("full manifest parses");
    assert!(m.icon_url.is_some());
    assert!(m.metrics_cache.is_some());
    assert_eq!(m.capabilities.len(), 2);
    assert!(m.capabilities.iter().any(|c| c.kind == "execute"));
    assert!(!m.solana_programs.is_empty());
}

#[test]
fn round_trips_manifest() {
    let raw = include_str!("fixtures/manifests/valid-minimal.json");
    let m: Manifest = serde_json::from_str(raw).unwrap();
    let s = serde_json::to_string(&m).unwrap();
    let m2: Manifest = serde_json::from_str(&s).unwrap();
    assert_eq!(m.id, m2.id);
    assert_eq!(m.capabilities.len(), m2.capabilities.len());
}
