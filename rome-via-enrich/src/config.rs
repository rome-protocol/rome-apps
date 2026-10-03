use std::{net::SocketAddr, path::PathBuf, time::Duration};

/// Configuration for rome-via-enrich enrichment workers.
///
/// Loaded from a TOML file pointed to by `ROME_VIA_ENRICH_CONFIG` env var or `-c` CLI flag.
///
/// # Required fields (no defaults)
/// Per-cluster identity values that must come from `rome-protocol/registry`
/// (projected into the deployment template) are required at config-load time —
/// no built-in defaults. A missing or empty value fails the load loudly at
/// service startup, which is preferable to silently running with a localnet
/// vanity ID against a production chain (the previous behavior misclassified
/// every rome-evm tx as cross-chain on every live chain).
///
/// **Terminology note.** Throughout this file and the `cross_chain` worker,
/// "CPI" means a **real** Solana program invocation — i.e., a log line of the
/// form `Program <ID> invoke [<depth>]` produced by `solana_program::invoke`
/// or `invoke_signed`. Precompile read shortcuts on Rome's `CpiProgram`
/// precompile at `0xff..08` (`account_info`, `account_data_at`,
/// `account_u64_at`, `account_lamports`, `pdas_batch_derive`) dispatch as
/// `NonEvmCall::CrossStateEthCall` — they make **no Solana syscall**, emit
/// **no `Program X invoke [N]` line**, and therefore do **not** affect this
/// worker's classification. Only the `Invoke` / `Composed`-dispatching paths
/// (`invoke`, `invoke_signed`, HelperProgram mutation methods, Withdraw)
/// show up as CPIs here (`CrossStateEthCall` is NOT a Solana CPI; see the
/// precompile dispatch in `rome-evm`).
///
/// * `chain_id` — Rome EVM chain id, per `registry/chains/<id>-<slug>/chain.json`
/// * `db_url` — rome_via Postgres connection URL
/// * `rome_evm_program_id` — Solana program id of the rollup's rome-evm program,
///   per `registry/chains/<id>-<slug>/chain.json#romeEvmProgramId`
/// One registry-backed contract label: a per-chain `address → label` mapping,
/// projected into the deployment config from `rome-protocol/registry`. Rendered
/// by the deployment's enrich-config template as an array-of-tables block:
///
/// ```toml
/// [[contract_labels]]
/// address = "0xb342f70d56855f11b0721fcbe2804a200d0f0533"
/// label = "UniswapV2Router02"
/// ```
///
/// Consumed by the `contract_labels` worker as the FIRST label tier — protocol
/// infra that reverts `name()`/`symbol()` (routers, pools, factories, the
/// faucet, Multicall3, all of V3/V4) gets a clean label here without any
/// on-chain read. Plain serde (NOT `deny_unknown_fields`) so adding/removing
/// fields stays backward compatible with deployed configs.
#[derive(serde::Deserialize, Debug, Clone)]
pub struct ContractLabel {
    /// EVM contract address (already lowercased by the registry projector;
    /// the worker lowercases again on read for safety).
    pub address: String,
    /// Human display label, e.g. "UniswapV2Router02".
    pub label: String,
}

/// A registry-projected Solana program label:
/// ```toml
/// [[program_labels]]
/// program_id = "4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg"
/// label = "mangoV4"
/// ```
///
/// Consumed by the `cross_chain` worker to label the depth ≥ 2 CPI target a tx
/// invokes via the `CpiProgram` precompile ("CPI → <label>" on the explorer).
/// The deployment-config projector **excludes** the
/// universal plumbing programs (splToken / splToken2022 / associatedToken /
/// system / memo), so a cached wrapper's inner SPL transfer is absent from the
/// map and skipped — only a genuine DeFi CPI target is labeled. base58 program
/// ids are case-significant and are NOT lowercased (unlike EVM addresses).
#[derive(serde::Deserialize, Debug, Clone)]
pub struct ProgramLabel {
    /// base58 Solana program id (case-significant).
    pub program_id: String,
    /// Human display label — the registry key, verbatim (e.g. "mangoV4").
    pub label: String,
}

#[derive(serde::Deserialize, Debug, Clone)]
pub struct ViaEnrichConfig {
    /// Chain ID being indexed. REQUIRED — sourced from
    /// `registry/chains/<id>-<slug>/chain.json#chainId`.
    pub chain_id: u64,

    /// Connection URL for the rome_via Postgres database (read + write derived tables).
    pub db_url: String,

    /// How often each worker polls for new data (seconds).
    #[serde(default = "default_poll_interval")]
    pub poll_interval_seconds: u64,

    /// Max rows to process per worker poll loop.
    #[serde(default = "default_batch_size")]
    pub batch_size: i64,

    /// Address to bind the health server.
    #[serde(default = "default_health_addr")]
    pub health_addr: SocketAddr,

    /// Optional directory of *.json ABI files to seed method_signatures on startup.
    pub abi_seed_dir: Option<PathBuf>,

    /// Whether to query 4byte.directory for unknown method selectors.
    #[serde(default = "default_fourbyte_enabled")]
    pub fourbyte_enabled: bool,

    /// Proxy JSON-RPC URL for eth_call (name/symbol/decimals, eth_getCode).
    /// Defaults to local dev stack.
    #[serde(default = "default_proxy_url")]
    pub proxy_url: String,

    /// Solana JSON-RPC URL for fetching sol_tx logs (hook execution parsing,
    /// cross-chain classification).
    #[serde(default = "default_solana_rpc_url")]
    pub solana_rpc_url: String,

    /// Meta-Hook Router program ID. Defaults to the localnet sentinel
    /// `MetaHk1111111111111111111111111111111111111` — chains without a
    /// deployed Meta-Hook Router (which is most of them today) match this
    /// sentinel in the deployment config, so the default is correct for the
    /// no-router case. Chains with a real router override per `registry`.
    #[serde(default = "default_meta_hook_program_id")]
    pub meta_hook_program_id: String,

    /// Rome EVM program ID. REQUIRED — sourced from
    /// `registry/chains/<id>-<slug>/chain.json#romeEvmProgramId`. Used by the
    /// `cross_chain` worker to distinguish Rhea (the Solana tx only invokes
    /// rome-evm itself + infra programs; every `Program X invoke [N]` log
    /// line names a program in the infra set) from Romulus (at least one
    /// `Program X invoke [N]` line names a non-infra program — SPL Token,
    /// custom Solana programs, etc.). "Invoke" here = a real Solana CPI
    /// visible in the log stream, NOT a precompile read shortcut on
    /// `CpiProgram` (those dispatch as `CrossStateEthCall` and produce no
    /// invoke line — see the file-level note above). A wrong value
    /// misclassifies every rome-evm tx as cross-chain.
    pub rome_evm_program_id: String,

    /// Additional program IDs to treat as rome infrastructure (alongside the
    /// defaults: ComputeBudget, System, BPF loaders, native loader, Sysvar,
    /// rome-evm, meta-hook router). Any program NOT in the combined set flips
    /// classification to Romulus when it appears in a `Program X invoke [N]`
    /// log line of the Solana tx. Tune per cluster — e.g., add SPL Token here
    /// if rome-evm performs a real CPI into it during internal deposit
    /// bookkeeping (a syscall, not a `CrossStateEthCall` read) and you do not
    /// want that to count as cross-chain.
    #[serde(default)]
    pub extra_infra_programs: Vec<String>,

    /// Registry-backed contract labels: per-chain `address → label` map
    /// projected from `rome-protocol/registry` into the deployment config. The
    /// `contract_labels` worker consults this FIRST — before any on-chain
    /// `name()`/`symbol()` read — so protocol infra that reverts those selectors
    /// (UniswapV2Router02, AavePool, Multicall3, factories, the faucet, all of
    /// V3/V4) still gets a clean label. Empty by default (chains not yet
    /// re-rolled with the tier fall back to the pure on-chain heuristic).
    #[serde(default)]
    pub contract_labels: Vec<ContractLabel>,

    /// Registry-backed Solana program NAMES: `program_id → label` projected from
    /// `rome-protocol/registry`'s `solana/programs/<cluster>.json` (minus the
    /// universal plumbing programs). The `cross_chain` worker uses this only to
    /// NAME a captured depth ≥ 2 CPI target — it does NOT gate capture (an
    /// uncurated target is still indexed; the frontend truncates its id). Empty
    /// by default → captured CPIs show a truncated id until names are projected.
    #[serde(default)]
    pub program_labels: Vec<ProgramLabel>,

    /// Universal Solana programs that are never a meaningful CPI *target* — the
    /// token/system/memo plumbing a DeFi CPI routes through (splToken,
    /// splToken2022, associatedToken, systemProgram, memo). Projected from the
    /// registry. The `cross_chain` worker skips these when picking the depth ≥ 2
    /// CPI target, so a cached wrapper's inner SPL transfer isn't surfaced as a
    /// CPI. Empty default → only DEFAULT_INFRA runtime programs are skipped (SPL
    /// Token would NOT be, so projecting these suppresses per-transfer noise).
    #[serde(default)]
    pub cpi_plumbing_programs: Vec<String>,

    /// How often the throughput_record worker checks for new blocks (seconds).
    /// Defaults to 30.
    pub throughput_record_poll_secs: Option<u64>,

    /// cross_vm_seams worker poll interval (default 10s).
    pub cross_vm_seams_poll_secs: Option<u64>,

    /// Solana cluster name the chain settles on (e.g. "devnet", "testnet"),
    /// surfaced verbatim as the `solChain` field on `cross_chain` worker
    /// Romulus correlation rows. Defaults to "devnet" (preserves current
    /// behavior); set per `registry/chains/<id>-<slug>/chain.json` for chains
    /// on other Solana clusters.
    #[serde(default = "default_solana_cluster")]
    pub solana_cluster: String,

    /// Base URL of this chain's Sourcify instance (e.g.
    /// `https://verify.example.com`), used by the
    /// `verified_labels` worker to check `GET /v2/contract/{chainId}/{address}`
    /// and promote a verified contract's compilation name into
    /// `contract_labels` (provenance `"verified"`). `None` (the default) — no
    /// `verifier_url` key in the deployment TOML — **disables the worker**
    /// entirely: it logs once and returns, matching how other optional
    /// features degrade (e.g. `meta_hook_program_id`'s no-router sentinel).
    /// Zero behavior change for chains not yet re-rolled with this key.
    #[serde(default)]
    pub verifier_url: Option<String>,
}

fn default_poll_interval() -> u64 {
    5
}
fn default_batch_size() -> i64 {
    500
}
fn default_health_addr() -> SocketAddr {
    "0.0.0.0:8092".parse().unwrap()
}
fn default_fourbyte_enabled() -> bool {
    true
}
fn default_proxy_url() -> String {
    "http://localhost:9090".to_string()
}
fn default_solana_rpc_url() -> String {
    "http://solana:8899".to_string()
}
fn default_meta_hook_program_id() -> String {
    "MetaHk1111111111111111111111111111111111111".to_string()
}
fn default_solana_cluster() -> String {
    "devnet".to_string()
}

impl ViaEnrichConfig {
    pub fn throughput_record_poll_interval(&self) -> Duration {
        Duration::from_secs(self.throughput_record_poll_secs.unwrap_or(30))
    }

    pub fn cross_vm_seams_poll_interval(&self) -> Duration {
        Duration::from_secs(self.cross_vm_seams_poll_secs.unwrap_or(10))
    }

    /// Parse a TOML config file from disk.
    pub async fn load(path: &std::path::Path) -> anyhow::Result<Self> {
        let bytes = tokio::fs::read(path)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read config file {:?}: {e}", path))?;
        let text = std::str::from_utf8(&bytes)
            .map_err(|e| anyhow::anyhow!("Config file is not valid UTF-8: {e}"))?;
        let cfg: Self = toml::from_str(text)
            .map_err(|e| anyhow::anyhow!("Failed to parse TOML config: {e}"))?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Validates that the per-cluster required fields have non-empty values.
    ///
    /// Empty strings are forbidden because a templated deployment config
    /// injects values via interpolation; an unset template var renders as
    /// `""`, which would otherwise satisfy serde's "present" check. Fail
    /// loud here instead of letting the cross_chain worker run with an
    /// empty infra-program-id and classify every tx as Romulus.
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.rome_evm_program_id.is_empty() {
            return Err(anyhow::anyhow!(
                "rome_evm_program_id must be set (sourced from \
                 rome-protocol/registry chains/<id>-<slug>/chain.json#romeEvmProgramId); \
                 see rome-via-enrich/src/config.rs"
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Baseline: a TOML with all required fields parses cleanly.
    #[test]
    fn parses_with_all_required_fields() {
        let toml = r#"
            chain_id = 121301
            db_url = "postgres://user:pw@host:5432/db"
            rome_evm_program_id = "romedpkFKEu3JJrYujtNUferyEv47UxvjZe2QcdPwN8"
        "#;
        let cfg: ViaEnrichConfig = toml::from_str(toml).expect("baseline parse");
        cfg.validate().expect("baseline validate");
        assert_eq!(cfg.chain_id, 121301);
        assert_eq!(
            cfg.rome_evm_program_id,
            "romedpkFKEu3JJrYujtNUferyEv47UxvjZe2QcdPwN8"
        );
    }

    /// A deployed TOML that still sets the retired `throughput_record_recompute_secs`
    /// key must keep loading (the struct ignores unknown keys).
    #[test]
    fn retired_throughput_record_recompute_secs_key_still_parses() {
        let toml = r#"
            chain_id = 121301
            db_url = "postgres://user:pw@host:5432/db"
            rome_evm_program_id = "romedpkFKEu3JJrYujtNUferyEv47UxvjZe2QcdPwN8"
            throughput_record_poll_secs = 15
            throughput_record_recompute_secs = 3600
        "#;
        let cfg: ViaEnrichConfig = toml::from_str(toml).expect("retired key must not break parsing");
        cfg.validate().expect("validate");
        assert_eq!(cfg.throughput_record_poll_interval(), Duration::from_secs(15));
    }

    /// Missing `chain_id` must fail to parse — no silent fallback to a stale
    /// devnet default (121220 used to be the implicit fallback).
    #[test]
    fn missing_chain_id_fails_to_parse() {
        let toml = r#"
            db_url = "postgres://user:pw@host:5432/db"
            rome_evm_program_id = "romedpkFKEu3JJrYujtNUferyEv47UxvjZe2QcdPwN8"
        "#;
        let err = toml::from_str::<ViaEnrichConfig>(toml).expect_err("must fail");
        assert!(
            err.to_string().contains("chain_id"),
            "error must mention chain_id, got: {err}"
        );
    }

    /// Missing `rome_evm_program_id` must fail to parse — no silent fallback to
    /// the localnet vanity ID. This is the loud-fail that prevents the
    /// cross_chain worker from misclassifying every rome-evm tx as Romulus on
    /// a production chain whose deployment template forgot the variable.
    #[test]
    fn missing_rome_evm_program_id_fails_to_parse() {
        let toml = r#"
            chain_id = 121301
            db_url = "postgres://user:pw@host:5432/db"
        "#;
        let err = toml::from_str::<ViaEnrichConfig>(toml).expect_err("must fail");
        assert!(
            err.to_string().contains("rome_evm_program_id"),
            "error must mention rome_evm_program_id, got: {err}"
        );
    }

    /// Empty `rome_evm_program_id` must fail at validate() time. Catches the
    /// template-interpolation-of-undefined-var → `""` case (vs the missing-key
    /// case which serde catches).
    #[test]
    fn empty_rome_evm_program_id_fails_validate() {
        let toml = r#"
            chain_id = 121301
            db_url = "postgres://user:pw@host:5432/db"
            rome_evm_program_id = ""
        "#;
        let cfg: ViaEnrichConfig = toml::from_str(toml).expect("parses");
        let err = cfg.validate().expect_err("validate must fail");
        assert!(
            err.to_string().contains("rome_evm_program_id"),
            "error must mention rome_evm_program_id, got: {err}"
        );
    }

    /// Optional fields fall back to safe defaults. `meta_hook_program_id`
    /// defaults to the localnet sentinel that matches the deployment config
    /// for chains without a deployed Meta-Hook Router.
    #[test]
    fn optional_fields_keep_their_defaults() {
        let toml = r#"
            chain_id = 121301
            db_url = "postgres://user:pw@host:5432/db"
            rome_evm_program_id = "romedpkFKEu3JJrYujtNUferyEv47UxvjZe2QcdPwN8"
        "#;
        let cfg: ViaEnrichConfig = toml::from_str(toml).expect("parses");
        assert_eq!(cfg.poll_interval_seconds, 5);
        assert_eq!(cfg.batch_size, 500);
        assert_eq!(
            cfg.meta_hook_program_id,
            "MetaHk1111111111111111111111111111111111111"
        );
        assert!(cfg.fourbyte_enabled);
        assert!(cfg.extra_infra_programs.is_empty());
    }

    /// The registry-backed contract-label tier: an array-of-tables TOML block
    /// (`[[contract_labels]]`) parses into a `Vec<ContractLabel>`. This is the
    /// shape the deployment's enrich-config template renders. Addresses come in
    /// already-lowercased from the projector; we assert that here.
    #[test]
    fn contract_labels_parse_from_array_of_tables() {
        let toml = r#"
            chain_id = 121302
            db_url = "postgres://user:pw@host:5432/db"
            rome_evm_program_id = "RPTWwELXAY4KC9ZPHhaxp7Sq1hHtU3HNEgLbSegCcWf"

            [[contract_labels]]
            address = "0x040f9c2671b2b0c70b32c10727afcdbbf23b92fb"
            label = "MeteoraDAMMv1Factory"
            [[contract_labels]]
            address = "0xb342f70d56855f11b0721fcbe2804a200d0f0533"
            label = "UniswapV2Router02"
        "#;
        let cfg: ViaEnrichConfig = toml::from_str(toml).expect("parses contract_labels");
        assert_eq!(cfg.contract_labels.len(), 2);
        assert_eq!(
            cfg.contract_labels[0].address,
            "0x040f9c2671b2b0c70b32c10727afcdbbf23b92fb"
        );
        assert_eq!(cfg.contract_labels[0].label, "MeteoraDAMMv1Factory");
        assert_eq!(
            cfg.contract_labels[1].address,
            "0xb342f70d56855f11b0721fcbe2804a200d0f0533"
        );
        assert_eq!(cfg.contract_labels[1].label, "UniswapV2Router02");
    }

    /// `contract_labels` is `#[serde(default)]` — a config without the block
    /// parses to an empty vec (backward compatible with chains that haven't
    /// been re-rolled with the label tier).
    #[test]
    fn contract_labels_default_empty() {
        let toml = r#"
            chain_id = 121301
            db_url = "postgres://user:pw@host:5432/db"
            rome_evm_program_id = "romedpkFKEu3JJrYujtNUferyEv47UxvjZe2QcdPwN8"
        "#;
        let cfg: ViaEnrichConfig = toml::from_str(toml).expect("parses");
        assert!(cfg.contract_labels.is_empty());
    }

    /// The registry-backed Solana program-label tier: `[[program_labels]]`
    /// parses into a `Vec<ProgramLabel>` (the cross_chain worker labels CPI
    /// targets from it). base58 program ids are case-significant — preserved
    /// verbatim (NOT lowercased like EVM addresses). Synthetic ids in the test.
    #[test]
    fn program_labels_parse_from_array_of_tables() {
        let toml = r#"
            chain_id = 200010
            db_url = "postgres://user:pw@host:5432/db"
            rome_evm_program_id = "RPTWwELXAY4KC9ZPHhaxp7Sq1hHtU3HNEgLbSegCcWf"

            [[program_labels]]
            program_id = "MangoTestProg11111111111111111111111111111111"
            label = "mangoV4"
            [[program_labels]]
            program_id = "StakePoolTestPrg1111111111111111111111111111"
            label = "stakePool"
        "#;
        let cfg: ViaEnrichConfig = toml::from_str(toml).expect("parses program_labels");
        assert_eq!(cfg.program_labels.len(), 2);
        assert_eq!(
            cfg.program_labels[0].program_id,
            "MangoTestProg11111111111111111111111111111111"
        );
        assert_eq!(cfg.program_labels[0].label, "mangoV4");
        assert_eq!(cfg.program_labels[1].label, "stakePool");
    }

    /// `program_labels` is `#[serde(default)]` — a config without the block
    /// parses to an empty vec (chains not yet re-rolled get no CPI labels).
    #[test]
    fn program_labels_default_empty() {
        let toml = r#"
            chain_id = 200010
            db_url = "postgres://user:pw@host:5432/db"
            rome_evm_program_id = "RPTWwELXAY4KC9ZPHhaxp7Sq1hHtU3HNEgLbSegCcWf"
        "#;
        let cfg: ViaEnrichConfig = toml::from_str(toml).expect("parses");
        assert!(cfg.program_labels.is_empty());
    }

    /// `verifier_url` is `#[serde(default)]` — absent in the TOML parses to
    /// `None`, which is the `verified_labels` worker's disabled signal (zero
    /// behavior change for chains not yet re-rolled with the key).
    #[test]
    fn verifier_url_defaults_none() {
        let toml = r#"
            chain_id = 121301
            db_url = "postgres://user:pw@host:5432/db"
            rome_evm_program_id = "romedpkFKEu3JJrYujtNUferyEv47UxvjZe2QcdPwN8"
        "#;
        let cfg: ViaEnrichConfig = toml::from_str(toml).expect("parses");
        assert_eq!(cfg.verifier_url, None);
    }

    /// A configured `verifier_url` parses through as `Some(..)`.
    #[test]
    fn verifier_url_parses_when_present() {
        let toml = r#"
            chain_id = 200010
            db_url = "postgres://user:pw@host:5432/db"
            rome_evm_program_id = "RPTWwELXAY4KC9ZPHhaxp7Sq1hHtU3HNEgLbSegCcWf"
            verifier_url = "https://verify.example.com"
        "#;
        let cfg: ViaEnrichConfig = toml::from_str(toml).expect("parses");
        assert_eq!(
            cfg.verifier_url,
            Some("https://verify.example.com".to_string())
        );
    }

    /// An extra unknown key in the TOML (e.g. a deployed config that still
    /// carries the now-removed `multicall3_address`) is ignored by serde — the
    /// struct does NOT `deny_unknown_fields`, so dropping the field is a
    /// backward-compatible change for the live config.
    #[test]
    fn unknown_keys_are_ignored() {
        let toml = r#"
            chain_id = 121302
            db_url = "postgres://user:pw@host:5432/db"
            rome_evm_program_id = "RPTWwELXAY4KC9ZPHhaxp7Sq1hHtU3HNEgLbSegCcWf"
            multicall3_address = "0x82cd1dab8aff294264151ee09a4295da0dd60e50"
        "#;
        let cfg: ViaEnrichConfig = toml::from_str(toml).expect("unknown key tolerated");
        assert_eq!(cfg.chain_id, 121302);
    }
}
