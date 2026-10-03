//! Deploy-facing configuration — mirrors `rome-via-sync`'s `SyncConfig`
//! (`rome-via-sync/src/config.rs`) shape exactly: a two-`PgPool` config
//! struct (`source_db_url` = Hercules, `target_db_url` = where the `audit`
//! schema lives), loaded from a TOML file. Secrets are NOT resolved here —
//! the DSNs arrive already-populated via whatever the deployment tooling
//! puts in the config file/env (same as sync/enrich); this crate has no
//! secret-manager code, exactly like its siblings.

use std::net::SocketAddr;

/// Configuration for the rome-audit ingest worker.
///
/// Loaded from a TOML file pointed to by `ROME_AUDIT_CONFIG` env var or `-c` CLI flag.
#[derive(serde::Deserialize, Debug, Clone)]
pub struct AuditConfig {
    /// Chain ID to tag every `audit.chain_event` row with (one Hercules
    /// source DB = one chain; this is the audit-side label, never read
    /// from Hercules itself).
    pub chain_id: u64,

    /// Connection URL for the source Hercules Postgres database (read-only usage).
    pub source_db_url: String,

    /// Connection URL for the target database the `audit` schema lives in
    /// (typically the `rome_via_db` database, alongside VIA's own tables).
    pub target_db_url: String,

    /// How often to poll Hercules for newly-verifiable slots, in milliseconds.
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,

    /// The verified-finality watermark's confirmation lag, in slots (§5.2-i/ii).
    #[serde(default = "default_confirmation_lag")]
    pub confirmation_lag: i64,

    /// Max candidate slots evaluated per tick (bounds one poll's work).
    #[serde(default = "default_max_slots_per_tick")]
    pub max_slots_per_tick: usize,

    /// Address for the HTTP health server.
    #[serde(default = "default_health_addr")]
    pub health_addr: SocketAddr,

    /// P3c resolve→ingest seam config. `None` (the section absent) is
    /// exactly today's P1 behavior: an empty static source map, watermark
    /// still advances, nothing decodes.
    #[serde(default)]
    pub resolve: Option<ResolveSection>,

    /// P5a Tier-3 overlay ingest config. `None` (the section absent) is
    /// exactly today's P1-P4 behavior, byte-identical: the overlay server
    /// never boots, and nothing under `src/overlay/` runs.
    #[serde(default)]
    pub overlay: Option<OverlaySection>,
}

/// The `[overlay]` config table (P5a) — wires the Bloom→audit Tier-3
/// ingest HTTP server into the binary. Presence boots
/// [`crate::overlay::start_overlay_server`] alongside the health server.
///
/// This is the Bloom→audit ingest secret ONLY. The audit→Bloom report-API
/// secret (a different credential, a different direction) is P6 — do not
/// add it here.
#[derive(serde::Deserialize, Clone)]
pub struct OverlaySection {
    /// Address the overlay HTTP server binds to.
    pub listen_addr: String,
    /// Shared HMAC secret Bloom signs `/overlay/*` requests with. Loaded
    /// as plain config/env text like every other DSN in this crate (this
    /// crate has no secret-manager code, see module doc above) — the value
    /// itself is validated here (non-empty, minimum length), never its
    /// provenance.
    pub ingest_secret: String,
}

/// Hand-written so logging the config never prints `ingest_secret`.
impl std::fmt::Debug for OverlaySection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OverlaySection")
            .field("listen_addr", &self.listen_addr)
            .field("ingest_secret", &"<redacted>")
            .finish()
    }
}

impl OverlaySection {
    fn validate(&self) -> anyhow::Result<()> {
        if self.ingest_secret.len() < 32 {
            anyhow::bail!(
                "overlay.ingest_secret must be at least 32 characters (got {}) — a short shared \
                 secret is brute-forceable against HMAC-SHA256",
                self.ingest_secret.len()
            );
        }
        self.listen_addr
            .parse::<std::net::SocketAddr>()
            .map_err(|e| anyhow::anyhow!("overlay.listen_addr {:?} is not a valid address: {e}", self.listen_addr))?;
        Ok(())
    }
}

/// The registry projection an operator hands `rome-audit` (D5: config-
/// supplied pinned commit-SHA + deploy-time projection).
///
/// **Trust boundary, stated plainly (P3c M5).** This crate does not fetch
/// `rome-protocol/registry` itself and cannot verify that `factory`/
/// `storefront`/`morpho`/`listed_assets`/`candidate_pools` here actually
/// match what `commit_sha` points at in the real registry — that
/// correspondence is OPERATOR-ASSERTED at deploy time (whatever tooling
/// projects the registry tree at a pinned SHA into this TOML section is
/// deferred, not built by this crate). What `rome-audit` DOES enforce: the
/// shapes are well-formed (§"conventions" below), and whatever was
/// asserted is recorded VERBATIM into every `capture_manifest`, so a wrong
/// projection is at least reproducible and auditable after the fact, never
/// silently re-derived or re-fetched.
#[derive(serde::Deserialize, Debug, Clone)]
pub struct RegistrySection {
    /// The pinned `rome-protocol/registry` commit this projection was read
    /// at — recorded verbatim into every `capture_manifest.registry_commit_sha`.
    /// Must be a real 40-hex-char SHA, never a branch name or `HEAD` (a
    /// moving ref can't be replayed — H1's determinism requirement). A
    /// 40-hex-char check cannot distinguish a commit SHA from an annotated
    /// TAG object's SHA; that's fine — a tag object is itself immutable
    /// once created, so determinism still holds either way. Rejecting tag
    /// SHAs specifically (vs. commit SHAs) would need a live `git`/registry
    /// lookup this crate doesn't have — deferred, not enforced here.
    pub commit_sha: String,
    /// `0x`-prefixed 20-byte hex address of the registry-resolved
    /// `ArcTokenFactoryV2`.
    pub factory: String,
    /// `0x`-prefixed 20-byte hex address of the registry-resolved
    /// `ArcTokenPurchase` storefront.
    pub storefront: String,
    /// `0x`-prefixed 20-byte hex address of the Morpho Blue singleton on
    /// this chain, if deployed. Absent ⇒ `resolve()` never probes Morpho.
    pub morpho: Option<String>,
    /// Registry-listed Bloom/Arc assets (capture §1.1's authoritativeness
    /// gate's OR-alternative) — `0x`-prefixed 20-byte hex addresses.
    #[serde(default)]
    pub listed_assets: Vec<String>,
    /// Registry-listed pool candidates (capture §2.8 label, never the gate)
    /// — `0x`-prefixed 20-byte hex addresses.
    #[serde(default)]
    pub candidate_pools: Vec<String>,
    /// The Rome protocol multisig address (P4b-ii `code_change`'s
    /// `signer_attribution` — `ROME_MULTISIG`). Absent ⇒ `None` — multisig
    /// attribution is strictly opt-in, never inferred from any other field.
    pub rome_multisig: Option<String>,
    /// `0x`-prefixed 20-byte hex address of the RestrictionsRouter that hosts
    /// the chain's GLOBAL_SANCTIONS module (the OFAC floor). Absent ⇒ None ⇒
    /// exactly today's behavior; the audit only reaches a sanctions module a
    /// seed token's own router routes to. Present ⇒ resolve pins this router
    /// and discovers its GLOBAL_SANCTIONS module via getGlobalModuleAddress,
    /// independent of any token walk. The module is NOT pinned directly — it
    /// is discovered from the router so a module swap is picked up on
    /// re-resolution.
    pub global_sanctions_router: Option<String>,
    /// EVM block number where the pinned `global_sanctions_router` (and its
    /// GLOBAL_SANCTIONS module) started its real on-chain history — the Arc
    /// stack's deploy block. Anchors the pinned floor's backfill from_block so
    /// a clean audit-first chain opens no wasteful [0, watermark] scan and a
    /// retrofit chain opens one bounded gap. Absent ⇒ 0 (genesis) ⇒ byte-
    /// identical to today. Only meaningful with `global_sanctions_router` set.
    pub global_sanctions_router_from_block: Option<i64>,
}

/// The `[resolve]` config table (P3c a.1) — wires `resolve(token)` (P3/P3b)
/// into the ingest worker. Presence turns the worker from
/// `SourceMode::Static` into `SourceMode::Resolved`.
#[derive(serde::Deserialize, Debug, Clone)]
pub struct ResolveSection {
    /// The tokens this worker resolves + captures — `0x`-prefixed 20-byte
    /// hex addresses. Must be non-empty when this section is present AND
    /// [`discovery`](ResolveSection::discovery) is absent (an empty
    /// `[resolve]` with no tokens and no discovery is an authoring mistake,
    /// not a legitimate "resolve nothing" configuration — that's expressed
    /// by omitting the `[resolve]` section entirely). S4: when `discovery`
    /// IS present, `tokens` becomes optional-and-additive — every
    /// factory-admitted, gate-passed discovery candidate unions with
    /// whatever is listed here.
    #[serde(default)]
    pub tokens: Vec<String>,
    /// The keyless, read-only RPC endpoint `EthersResolverRpc` issues
    /// `eth_call`/`eth_getStorageAt`/`eth_getLogs` against.
    pub rpc_url: String,
    /// Periodic re-resolution cadence, in seconds. `0` = boot-only (resolve
    /// once, never again). Default 3600 (hourly).
    #[serde(default = "default_reresolve_interval_secs")]
    pub reresolve_interval_secs: u64,
    pub registry: RegistrySection,
    /// S4 (capture §3b point 5) — on-chain token DISCOVERY mode. Presence
    /// (the table itself, empty or not) enables enumeration of the
    /// registry-pinned factory's admission events (`TokenRegistered` ∪
    /// `TokenCreated`); each candidate still passes the UNCHANGED §1.1 gate
    /// before it enters the resolve set (`resolve::discover_authoritative_tokens`).
    /// Absent ⇒ exactly today's static-list-only behavior — `tokens` stays
    /// required-non-empty and nothing is enumerated.
    #[serde(default)]
    pub discovery: Option<DiscoverySection>,
}

/// The `[resolve.discovery]` table. Empty today — the enabling signal IS
/// the table's presence, not any field inside it (a future discovery-only
/// knob, e.g. a separate re-enumeration cadence, would land here without
/// another shape change).
#[derive(serde::Deserialize, Debug, Clone, Default)]
pub struct DiscoverySection {}

fn default_reresolve_interval_secs() -> u64 {
    3_600
}

/// Explicit byte-by-byte check — no regex crate. Rejects anything that
/// isn't exactly 40 lowercase hex chars: branch names (`main`), `HEAD`,
/// uppercase hex, and a short (39-char) hex string. Does NOT distinguish a
/// commit SHA from a tag OBJECT's own SHA (both are 40-hex and both are
/// immutable once created — see `RegistrySection::commit_sha`'s doc); a
/// lightweight tag ref like `v1.2.3` is rejected only because it isn't
/// itself 40 hex chars, not because this function specifically detects
/// "tag-ness".
fn is_valid_commit_sha(s: &str) -> bool {
    s.len() == 40 && s.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn parse_hex20(field: &str, s: &str) -> anyhow::Result<[u8; 20]> {
    let bytes = hex::decode(s.trim_start_matches("0x"))
        .map_err(|e| anyhow::anyhow!("{field} = {s:?} is not valid hex: {e}"))?;
    bytes
        .try_into()
        .map_err(|v: Vec<u8>| anyhow::anyhow!("{field} = {s:?} must be 20 bytes, got {}", v.len()))
}

impl RegistrySection {
    fn validate(&self) -> anyhow::Result<()> {
        if !is_valid_commit_sha(&self.commit_sha) {
            anyhow::bail!(
                "resolve.registry.commit_sha must be a 40-char lowercase hex SHA (got {:?}) — \
                 never a branch name or `HEAD` (a moving ref breaks H1's determinism \
                 requirement). This check can't distinguish a commit SHA from an annotated \
                 tag's SHA — both are equally immutable, so both pass",
                self.commit_sha
            );
        }
        parse_hex20("resolve.registry.factory", &self.factory)?;
        parse_hex20("resolve.registry.storefront", &self.storefront)?;
        if let Some(m) = &self.morpho {
            parse_hex20("resolve.registry.morpho", m)?;
        }
        for (i, a) in self.listed_assets.iter().enumerate() {
            parse_hex20(&format!("resolve.registry.listed_assets[{i}]"), a)?;
        }
        for (i, a) in self.candidate_pools.iter().enumerate() {
            parse_hex20(&format!("resolve.registry.candidate_pools[{i}]"), a)?;
        }
        if let Some(m) = &self.rome_multisig {
            parse_hex20("resolve.registry.rome_multisig", m)?;
        }
        if let Some(r) = &self.global_sanctions_router {
            parse_hex20("resolve.registry.global_sanctions_router", r)?;
        }
        match (&self.global_sanctions_router, self.global_sanctions_router_from_block) {
            (None, Some(_)) => anyhow::bail!(
                "resolve.registry.global_sanctions_router_from_block is set but \
                 global_sanctions_router is absent — from_block only anchors the pinned floor"
            ),
            (_, Some(b)) if b < 0 => anyhow::bail!(
                "resolve.registry.global_sanctions_router_from_block must be >= 0 (got {b}) — \
                 it is an EVM block number, never the fresh-chain sentinel"
            ),
            _ => {}
        }
        Ok(())
    }
}

impl ResolveSection {
    fn validate(&self) -> anyhow::Result<()> {
        if self.tokens.is_empty() && self.discovery.is_none() {
            anyhow::bail!(
                "resolve.tokens must be non-empty when the [resolve] section is present and \
                 [resolve.discovery] is absent (omit [resolve] entirely to run with no resolution, \
                 P1-style, or add [resolve.discovery] to make tokens optional-and-additive, S4-style)"
            );
        }
        for (i, t) in self.tokens.iter().enumerate() {
            parse_hex20(&format!("resolve.tokens[{i}]"), t)?;
        }
        self.registry.validate()
    }
}

fn default_poll_interval_ms() -> u64 {
    2_000
}

fn default_confirmation_lag() -> i64 {
    32
}

fn default_max_slots_per_tick() -> usize {
    1000
}

fn default_health_addr() -> SocketAddr {
    "0.0.0.0:8093".parse().unwrap()
}

impl AuditConfig {
    /// Parse a TOML config file from disk (same loader shape as `SyncConfig::load`).
    pub async fn load(path: &std::path::Path) -> anyhow::Result<Self> {
        let bytes = tokio::fs::read(path)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read config file {:?}: {e}", path))?;
        let text = std::str::from_utf8(&bytes)
            .map_err(|e| anyhow::anyhow!("Config file is not valid UTF-8: {e}"))?;
        let cfg: Self =
            toml::from_str(text).map_err(|e| anyhow::anyhow!("Failed to parse TOML config: {e}"))?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Fail-fast shape validation (P3c a.1) — a malformed `[resolve]` section
    /// (empty tokens, a non-commit `registry.commit_sha`, an unparseable
    /// address) is refused HERE, at load time, never discovered later as a
    /// runtime resolution failure.
    pub fn validate(&self) -> anyhow::Result<()> {
        if let Some(rs) = &self.resolve {
            rs.validate()?;
        }
        if let Some(ov) = &self.overlay {
            ov.validate()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minimal_config_parses_with_defaults() {
        let cfg: AuditConfig = toml::from_str(
            r#"
            chain_id = 200010
            source_db_url = "postgres://hercules"
            target_db_url = "postgres://rome_via_db"
            "#,
        )
        .unwrap();
        assert_eq!(cfg.chain_id, 200010);
        assert_eq!(cfg.confirmation_lag, 32);
        assert_eq!(cfg.max_slots_per_tick, 1000);
        assert_eq!(cfg.poll_interval_ms, 2_000);
        assert!(cfg.resolve.is_none());
        assert!(
            cfg.overlay.is_none(),
            "overlay absent ⇒ P1-P4 byte-identical behavior, overlay server never boots"
        );
    }

    // ---- P5a: [overlay] section ----

    #[test]
    fn overlay_section_parses_and_validates() {
        let cfg: AuditConfig = toml::from_str(
            r#"
            chain_id = 200010
            source_db_url = "postgres://hercules"
            target_db_url = "postgres://rome_via_db"

            [overlay]
            listen_addr = "0.0.0.0:8094"
            ingest_secret = "01234567890123456789012345678901"
            "#,
        )
        .unwrap();
        cfg.validate().unwrap();
        let ov = cfg.overlay.expect("overlay section must parse");
        assert_eq!(ov.listen_addr, "0.0.0.0:8094");
    }

    #[test]
    fn overlay_short_secret_is_refused_at_load() {
        let cfg: AuditConfig = toml::from_str(
            r#"
            chain_id = 200010
            source_db_url = "postgres://hercules"
            target_db_url = "postgres://rome_via_db"

            [overlay]
            listen_addr = "0.0.0.0:8094"
            ingest_secret = "too-short"
            "#,
        )
        .unwrap();
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("ingest_secret"), "error was: {err}");
    }

    #[test]
    fn overlay_bad_listen_addr_is_refused_at_load() {
        let cfg: AuditConfig = toml::from_str(
            r#"
            chain_id = 200010
            source_db_url = "postgres://hercules"
            target_db_url = "postgres://rome_via_db"

            [overlay]
            listen_addr = "not-an-addr"
            ingest_secret = "01234567890123456789012345678901"
            "#,
        )
        .unwrap();
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("listen_addr"), "error was: {err}");
    }

    // ---- G1: [resolve] section (P3c a.1) ----

    const VALID_SHA: &str = "abcdefabcdefabcdefabcdefabcdefabcdefabcd"; // 40 lowercase hex chars
    const FACTORY: &str = "0xf0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0"; // 20 bytes
    const STOREFRONT: &str = "0xc0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0"; // 20 bytes
    const TOKEN: &str = "0xa0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0"; // 20 bytes

    fn base_toml() -> String {
        format!(
            r#"
            chain_id = 200010
            source_db_url = "postgres://hercules"
            target_db_url = "postgres://rome_via_db"

            [resolve]
            tokens = ["{TOKEN}"]
            rpc_url = "https://rpc.example"

            [resolve.registry]
            commit_sha = "{VALID_SHA}"
            factory = "{FACTORY}"
            storefront = "{STOREFRONT}"
            "#
        )
    }

    #[test]
    fn resolve_section_parses_with_defaults() {
        let cfg: AuditConfig = toml::from_str(&base_toml()).unwrap();
        cfg.validate().unwrap();
        let rs = cfg.resolve.expect("resolve section must parse");
        assert_eq!(rs.tokens, vec![TOKEN.to_string()]);
        assert_eq!(rs.rpc_url, "https://rpc.example");
        assert_eq!(
            rs.reresolve_interval_secs, 3600,
            "default reresolve interval must be 3600s (boot-only is 0, explicit opt-in)"
        );
        assert_eq!(rs.registry.commit_sha, VALID_SHA);
        assert!(rs.registry.morpho.is_none());
        assert!(rs.registry.listed_assets.is_empty());
        assert!(rs.registry.candidate_pools.is_empty());
    }

    #[test]
    fn resolve_section_with_empty_tokens_is_refused() {
        let toml_str = base_toml().replace(&format!(r#"tokens = ["{TOKEN}"]"#), "tokens = []");
        let cfg: AuditConfig = toml::from_str(&toml_str).unwrap();
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("tokens"), "error was: {err}");
    }

    #[test]
    fn registry_commit_sha_must_be_a_40_hex_commit() {
        let refused = ["main", "v1.2.3", "HEAD", &VALID_SHA.to_uppercase(), &VALID_SHA[..39]];
        for bad in refused {
            let toml_str = base_toml().replace(VALID_SHA, bad);
            let cfg: AuditConfig = toml::from_str(&toml_str).unwrap();
            let err = cfg.validate();
            assert!(
                err.is_err(),
                "commit_sha {bad:?} must be refused at load time"
            );
        }

        // The valid case must pass.
        let cfg: AuditConfig = toml::from_str(&base_toml()).unwrap();
        cfg.validate().unwrap();
    }

    #[test]
    fn malformed_registry_address_is_refused_at_load() {
        for field_toml in [
            r#"factory = "not-hex""#,
            r#"factory = "0xdead""#, // too short
        ] {
            let toml_str = base_toml().replace(&format!(r#"factory = "{FACTORY}""#), field_toml);
            let cfg: AuditConfig = toml::from_str(&toml_str).unwrap();
            assert!(
                cfg.validate().is_err(),
                "malformed factory address must be refused: {field_toml}"
            );
        }
    }

    // ---- P4b-ii: rome_multisig ----

    #[test]
    fn rome_multisig_absent_defaults_to_none() {
        let cfg: AuditConfig = toml::from_str(&base_toml()).unwrap();
        cfg.validate().unwrap();
        assert!(cfg.resolve.unwrap().registry.rome_multisig.is_none());
    }

    #[test]
    fn rome_multisig_present_and_valid_parses() {
        const MULTISIG: &str = "0xd0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0";
        let toml_str = format!("{}\nrome_multisig = \"{MULTISIG}\"\n", base_toml());
        let cfg: AuditConfig = toml::from_str(&toml_str).unwrap();
        cfg.validate().unwrap();
        assert_eq!(
            cfg.resolve.unwrap().registry.rome_multisig,
            Some(MULTISIG.to_string())
        );
    }

    #[test]
    fn malformed_rome_multisig_is_refused_at_load() {
        for bad in ["not-hex", "0xdead"] {
            let toml_str = format!("{}\nrome_multisig = \"{bad}\"\n", base_toml());
            let cfg: AuditConfig = toml::from_str(&toml_str).unwrap();
            assert!(
                cfg.validate().is_err(),
                "malformed rome_multisig must be refused: {bad}"
            );
        }
    }

    // ---- Pinned Sanctions-Router coverage-gap closer: global_sanctions_router ----

    #[test]
    fn global_sanctions_router_absent_defaults_to_none() {
        let cfg: AuditConfig = toml::from_str(&base_toml()).unwrap();
        cfg.validate().unwrap();
        assert!(cfg.resolve.unwrap().registry.global_sanctions_router.is_none());
    }

    #[test]
    fn global_sanctions_router_present_and_valid_parses() {
        const ROUTER_ADDR: &str = "0x9090909090909090909090909090909090909090";
        let toml_str = format!("{}\nglobal_sanctions_router = \"{ROUTER_ADDR}\"\n", base_toml());
        let cfg: AuditConfig = toml::from_str(&toml_str).unwrap();
        cfg.validate().unwrap();
        assert_eq!(
            cfg.resolve.unwrap().registry.global_sanctions_router,
            Some(ROUTER_ADDR.to_string())
        );
    }

    #[test]
    fn malformed_global_sanctions_router_is_refused_at_load() {
        for bad in ["not-hex", "0xdead"] {
            let toml_str = format!("{}\nglobal_sanctions_router = \"{bad}\"\n", base_toml());
            let cfg: AuditConfig = toml::from_str(&toml_str).unwrap();
            assert!(
                cfg.validate().is_err(),
                "malformed global_sanctions_router must be refused: {bad}"
            );
        }
    }

    // ---- global_sanctions_router_from_block: operator-configurable deploy-block anchor ----

    #[test]
    fn global_sanctions_router_from_block_absent_defaults_to_none() {
        const ROUTER_ADDR: &str = "0x9090909090909090909090909090909090909090";
        let toml_str = format!("{}\nglobal_sanctions_router = \"{ROUTER_ADDR}\"\n", base_toml());
        let cfg: AuditConfig = toml::from_str(&toml_str).unwrap();
        cfg.validate().unwrap();
        assert_eq!(
            cfg.resolve.unwrap().registry.global_sanctions_router_from_block,
            None,
            "from_block absent => None => resolve_pinned_sanctions_floor falls back to 0 \
             (byte-identical to today)"
        );
    }

    #[test]
    fn global_sanctions_router_from_block_present_and_valid_parses() {
        const ROUTER_ADDR: &str = "0x9090909090909090909090909090909090909090";
        let toml_str = format!(
            "{}\nglobal_sanctions_router = \"{ROUTER_ADDR}\"\nglobal_sanctions_router_from_block = 485480000\n",
            base_toml()
        );
        let cfg: AuditConfig = toml::from_str(&toml_str).unwrap();
        cfg.validate().unwrap();
        assert_eq!(
            cfg.resolve.unwrap().registry.global_sanctions_router_from_block,
            Some(485480000)
        );
    }

    #[test]
    fn zero_global_sanctions_router_from_block_is_accepted() {
        // 0 = genesis is a legitimate anchor (byte-identical to the absent
        // default). Guards against a future tightening of the `< 0` validate
        // rule to `<= 0`, which would wrongly reject an explicit genesis pin.
        const ROUTER_ADDR: &str = "0x9090909090909090909090909090909090909090";
        let toml_str = format!(
            "{}\nglobal_sanctions_router = \"{ROUTER_ADDR}\"\nglobal_sanctions_router_from_block = 0\n",
            base_toml()
        );
        let cfg: AuditConfig = toml::from_str(&toml_str).unwrap();
        cfg.validate().unwrap();
        assert_eq!(
            cfg.resolve.unwrap().registry.global_sanctions_router_from_block,
            Some(0)
        );
    }

    #[test]
    fn global_sanctions_router_from_block_without_router_is_refused_at_load() {
        // from_block set but global_sanctions_router absent — from_block only
        // anchors the pinned floor, which doesn't exist without the router.
        let toml_str = format!("{}\nglobal_sanctions_router_from_block = 1000\n", base_toml());
        let cfg: AuditConfig = toml::from_str(&toml_str).unwrap();
        let err = cfg.validate().unwrap_err();
        assert!(
            err.to_string().contains("global_sanctions_router_from_block"),
            "error was: {err}"
        );
    }

    #[test]
    fn negative_global_sanctions_router_from_block_is_refused_at_load() {
        const ROUTER_ADDR: &str = "0x9090909090909090909090909090909090909090";
        let toml_str = format!(
            "{}\nglobal_sanctions_router = \"{ROUTER_ADDR}\"\nglobal_sanctions_router_from_block = -1\n",
            base_toml()
        );
        let cfg: AuditConfig = toml::from_str(&toml_str).unwrap();
        let err = cfg.validate().unwrap_err();
        assert!(
            err.to_string().contains("global_sanctions_router_from_block"),
            "error was: {err}"
        );
    }

    // ---- S4: [resolve.discovery] — tokens becomes optional-and-additive ----

    #[test]
    fn discovery_absent_is_byte_identical_to_today_tokens_still_required() {
        // Discovery OFF + empty tokens → still REFUSED (unchanged behavior).
        let toml_str = base_toml().replace(&format!(r#"tokens = ["{TOKEN}"]"#), "tokens = []");
        let cfg: AuditConfig = toml::from_str(&toml_str).unwrap();
        assert!(cfg.resolve.as_ref().unwrap().discovery.is_none());
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("tokens"), "error was: {err}");
    }

    #[test]
    fn discovery_present_makes_empty_tokens_valid() {
        let toml_str = base_toml().replace(&format!(r#"tokens = ["{TOKEN}"]"#), "tokens = []")
            + "\n[resolve.discovery]\n";
        let cfg: AuditConfig = toml::from_str(&toml_str).unwrap();
        cfg.validate().unwrap();
        let rs = cfg.resolve.expect("resolve section must parse");
        assert!(rs.tokens.is_empty());
        assert!(rs.discovery.is_some());
    }

    #[test]
    fn discovery_present_is_additive_not_replacing_configured_tokens() {
        // tokens stays non-empty AND discovery is present — both allowed
        // together (S4 point 4: union, never either/or).
        let toml_str = format!("{}\n[resolve.discovery]\n", base_toml());
        let cfg: AuditConfig = toml::from_str(&toml_str).unwrap();
        cfg.validate().unwrap();
        let rs = cfg.resolve.expect("resolve section must parse");
        assert_eq!(rs.tokens, vec![TOKEN.to_string()]);
        assert!(rs.discovery.is_some());
    }
}
