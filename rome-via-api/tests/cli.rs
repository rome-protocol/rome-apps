// Integration test: CLI argument parsing for rome-via-api.
//
// Mirrors pattern from rome-apps/proxy/src/cli.rs:
//   #[derive(clap::Parser)], `-c/--config`, env fallback `ROME_VIA_API_CONFIG`.
//
// RED phase: fails because `rome_via_api::cli` module does not exist yet.
// Will pass once Phase 2 (GREEN) implements the cli module.

use rome_via_api::cli::Cli;
use clap::Parser;
use serial_test::serial;

/// Parsing `--config` short flag sets `Cli::config` to the given path.
#[test]
fn cli_parses_config_short_flag() {
    let cli = Cli::try_parse_from(["rome-via-api", "-c", "/tmp/via-api.toml"])
        .expect("short -c flag should parse");
    assert_eq!(
        cli.config.as_deref(),
        Some(std::path::Path::new("/tmp/via-api.toml")),
        "config path must match the -c argument"
    );
}

/// Parsing `--config` long flag sets `Cli::config` to the given path.
#[test]
fn cli_parses_config_long_flag() {
    let cli = Cli::try_parse_from(["rome-via-api", "--config", "/tmp/via-api.toml"])
        .expect("long --config flag should parse");
    assert_eq!(
        cli.config.as_deref(),
        Some(std::path::Path::new("/tmp/via-api.toml")),
        "config path must match the --config argument"
    );
}

/// With no CLI flag and no env var, `get_config_path` returns an error.
#[test]
#[serial]
fn cli_config_path_errors_without_flag_or_env() {
    // Ensure env var is unset for this test
    std::env::remove_var("ROME_VIA_API_CONFIG");
    let cli = Cli::try_parse_from(["rome-via-api"])
        .expect("zero-arg parse should succeed");
    assert!(
        cli.get_config_path().is_err(),
        "get_config_path() must fail when neither --config nor ROME_VIA_API_CONFIG is set"
    );
}

/// With no CLI flag but `ROME_VIA_API_CONFIG` set, `get_config_path` returns that path.
#[test]
#[serial]
fn cli_config_path_falls_back_to_env() {
    std::env::set_var("ROME_VIA_API_CONFIG", "/tmp/via-api-env.toml");
    let cli = Cli::try_parse_from(["rome-via-api"])
        .expect("zero-arg parse should succeed");
    let path = cli.get_config_path().expect("env fallback must succeed");
    assert_eq!(path, std::path::PathBuf::from("/tmp/via-api-env.toml"));
    std::env::remove_var("ROME_VIA_API_CONFIG");
}
