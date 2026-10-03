use cardo_service::config::Config;

// These tests manipulate process env. Run serially in one test fn to avoid
// cross-test interference with Jest-style parallelism.

#[test]
fn config_from_env() {
    // All required env set
    std::env::set_var("CARDO_CDN_URL", "https://example.test/manifests");
    std::env::set_var("CARDO_POSTGRES_URL", "postgres://localhost/test");
    std::env::set_var("CARDO_POLL_INTERVAL_SECS", "30");
    let cfg = Config::from_env().expect("should parse");
    assert_eq!(cfg.cdn_url, "https://example.test/manifests");
    assert_eq!(cfg.postgres_url, "postgres://localhost/test");
    assert_eq!(cfg.poll_interval_secs, 30);

    // Default poll interval
    std::env::remove_var("CARDO_POLL_INTERVAL_SECS");
    let cfg2 = Config::from_env().expect("should parse");
    assert_eq!(cfg2.poll_interval_secs, 300);

    // Missing CDN URL
    std::env::remove_var("CARDO_CDN_URL");
    assert!(Config::from_env().is_err());
}
