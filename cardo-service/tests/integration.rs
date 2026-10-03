#[tokio::test]
#[ignore] // requires CARDO_POSTGRES_URL; run with `cargo test -- --ignored`
async fn upsert_and_query_roundtrip() {
    let url = std::env::var("CARDO_POSTGRES_URL")
        .expect("set CARDO_POSTGRES_URL to run");
    let store = cardo_service::store::PostgresStore::connect(&url).await.unwrap();
    store.migrate().await.unwrap();

    let raw_str = include_str!("fixtures/manifests/valid-minimal.json");
    let value: serde_json::Value = serde_json::from_str(raw_str).unwrap();
    let manifest: cardo_service::manifest::Manifest = serde_json::from_str(raw_str).unwrap();

    store.upsert_manifest(&manifest, &value).await.unwrap();
    let ids = store.get_manifest_ids().await.unwrap();
    assert!(ids.contains(&"test-app".to_string()));

    // Idempotent upsert
    store.upsert_manifest(&manifest, &value).await.unwrap();
    let ids2 = store.get_manifest_ids().await.unwrap();
    assert_eq!(ids2.iter().filter(|i| *i == "test-app").count(), 1);
}
