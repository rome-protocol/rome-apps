//! The rome-protocol **registry** seam (capture §1.2 point 4 / IMPL-PLAN §6
//! Q4). `RegistrySource` is the injected interface `resolve()` reads
//! through; two implementations exist: [`ConfigRegistrySource`] (P3c, real —
//! projects the operator-supplied `[resolve.registry]` config table, see
//! `config::RegistrySection`'s doc for the trust boundary this rests on)
//! and the unit-test-only fakes (`FakeRegistry` in `super::fixture` for
//! this crate's own `#[cfg(test)]` unit tests, and a separate one in
//! `tests/common` for the DB-backed integration tests — see that module's
//! doc for why there are two).
//!
//! **D5, closed for P3c, one seam still deferred.** How `rome-audit` itself
//! GETS a `RegistrySource` is now real: `ConfigRegistrySource::from_section`,
//! wired in `main.rs`. What remains deferred is HOW the config it reads got
//! populated — an in-service live fetch against `rome-protocol/registry`
//! (mint a GitHub App token, `raw.githubusercontent.com`, project the tree
//! at the pinned SHA) is real work for a later phase, and would land as
//! just another `RegistrySource` impl, never a change to `resolve()`
//! itself. Until then, the projection is asserted by whatever deploys the
//! config file — this crate can't verify it (see `config::RegistrySection`).

/// What the resolver needs from the registry: the pinned commit (capture
/// §1.2 point 4 — recorded verbatim in `capture_manifest`, never re-resolved
/// after the fact), the authoritativeness-gate listing check (§1.1), and the
/// canonical registry-trusted contracts (factory, storefront) a token's own
/// reads can't supply. Deliberately sync, not async — a real implementation
/// reads a pinned, already-fetched registry snapshot (no network call per
/// lookup), unlike [`super::rpc::ResolverRpc`], which is genuinely
/// per-chain-state and async.
pub trait RegistrySource: Send + Sync {
    /// The pinned commit SHA this registry snapshot was read at — recorded
    /// verbatim into `capture_manifest.registry_commit_sha` (capture §1.2
    /// point 4, H1's determinism requirement: two resolutions against the
    /// same pin must agree even if the live registry has since moved).
    fn commit_sha(&self) -> String;

    /// §1.1's registry-listing half of the authoritativeness gate: is
    /// `token` listed as a Bloom/Arc asset in the registry directly (the
    /// OR-alternative to the on-chain `getTokenImplementation` check).
    fn is_listed_asset(&self, token: [u8; 20]) -> bool;

    /// The registry-resolved `ArcTokenFactoryV2` address — §1.1's
    /// `getTokenImplementation` read target. Registry-authoritative per
    /// capture §1: "the trusted deployed contracts... come from the
    /// registry", never a token-supplied or hardcoded address.
    fn factory(&self) -> [u8; 20];

    /// The registry-resolved canonical `ArcTokenPurchase` storefront
    /// address (capture §2.6 / §1.2 step 2: "storefront: registry").
    fn storefront(&self) -> [u8; 20];

    /// Registry-listed pool candidates for THIS token, if any — a LABEL,
    /// never the capture gate (capture §2.8: "Registry/allowlist are
    /// labels, not the capture gate"). `resolve()` validates every candidate
    /// here the SAME on-chain way as a spine-discovered one
    /// (`token0()`/`token1()` — the `#136` lesson: an on-chain read is
    /// authority, a registry listing never is). Empty by default (most
    /// registries won't list every pool).
    fn candidate_pools(&self) -> Vec<[u8; 20]> {
        Vec::new()
    }

    /// The registry-resolved Morpho Blue singleton address for this chain
    /// (capture §2.9 / §9: "Morpho singleton per chain"). The zero address
    /// means "no Morpho deployment on this chain" — `resolve()` must not
    /// probe it.
    fn morpho(&self) -> [u8; 20] {
        [0u8; 20]
    }

    /// The Rome protocol multisig address (P4b-ii `code_change`'s
    /// `signer_attribution` -- `ROME_MULTISIG`). `None` by default --
    /// multisig attribution is strictly opt-in, never inferred; a
    /// `RegistrySource` that never configures a multisig makes
    /// `ROME_MULTISIG` structurally unreachable for its resolved assets.
    fn multisig(&self) -> Option<[u8; 20]> {
        None
    }

    /// The operator-pinned chain-global RestrictionsRouter that carries the
    /// GLOBAL_SANCTIONS module even though no seed token's own router routes
    /// to it (the Pinned Sanctions-Router coverage-gap closer). `None` by
    /// default — absent config is exactly today's behavior; a
    /// `RegistrySource` that never configures this makes the pinned floor
    /// structurally unreachable.
    fn global_sanctions_router(&self) -> Option<[u8; 20]> {
        None
    }

    /// The EVM block number where the pinned `global_sanctions_router` (and
    /// its GLOBAL_SANCTIONS module) started its real on-chain history — the
    /// deploy-block anchor for the pinned floor's backfill from_block (see
    /// `config::RegistrySection::global_sanctions_router_from_block`'s doc).
    /// `None` by default — absent config falls back to `0` at the call site
    /// (`pass::run_resolve_pass`), byte-identical to before this knob existed.
    fn global_sanctions_router_from_block(&self) -> Option<i64> {
        None
    }
}

/// The real `RegistrySource`, backed by the operator-supplied
/// `[resolve.registry]` config table (D5 — see the module doc and
/// `config::RegistrySection`'s doc for the trust boundary: this crate
/// trusts the config verbatim and cannot verify it against the live
/// registry at `commit_sha`).
#[derive(Debug, Clone)]
pub struct ConfigRegistrySource {
    commit_sha: String,
    factory: [u8; 20],
    storefront: [u8; 20],
    /// Zero address = no Morpho deployment on this chain (matches the trait
    /// default / capture §2.9's "never probe it" contract).
    morpho: [u8; 20],
    listed_assets: std::collections::BTreeSet<[u8; 20]>,
    candidate_pools: Vec<[u8; 20]>,
    /// `None` = no multisig configured (matches the trait default / P4b-ii's
    /// "strictly opt-in" contract).
    multisig: Option<[u8; 20]>,
    /// `None` = no pinned sanctions-router floor configured (matches the
    /// trait default — absent config is exactly today's behavior).
    global_sanctions_router: Option<[u8; 20]>,
    /// `None` = no operator-configured deploy-block anchor (matches the
    /// trait default — the call site falls back to `0`, byte-identical to
    /// today).
    global_sanctions_router_from_block: Option<i64>,
}

fn parse_hex20(field: &str, s: &str) -> anyhow::Result<[u8; 20]> {
    let bytes = hex::decode(s.trim_start_matches("0x"))
        .map_err(|e| anyhow::anyhow!("{field} = {s:?} is not valid hex: {e}"))?;
    bytes
        .try_into()
        .map_err(|v: Vec<u8>| anyhow::anyhow!("{field} = {s:?} must be 20 bytes, got {}", v.len()))
}

impl ConfigRegistrySource {
    /// Projects a `[resolve.registry]` config table into a `RegistrySource`.
    /// Re-validates every address shape (the config-load-time validation in
    /// `config::RegistrySection::validate` is the primary gate, but this
    /// constructor is also reachable directly from tests/other callers, so
    /// it never trusts an already-validated shape blindly).
    pub fn from_section(section: &crate::config::RegistrySection) -> anyhow::Result<Self> {
        let factory = parse_hex20("registry.factory", &section.factory)?;
        let storefront = parse_hex20("registry.storefront", &section.storefront)?;
        let morpho = match &section.morpho {
            Some(m) => parse_hex20("registry.morpho", m)?,
            None => [0u8; 20],
        };
        let listed_assets = section
            .listed_assets
            .iter()
            .enumerate()
            .map(|(i, a)| parse_hex20(&format!("registry.listed_assets[{i}]"), a))
            .collect::<anyhow::Result<_>>()?;
        let candidate_pools = section
            .candidate_pools
            .iter()
            .enumerate()
            .map(|(i, a)| parse_hex20(&format!("registry.candidate_pools[{i}]"), a))
            .collect::<anyhow::Result<_>>()?;
        let multisig = match &section.rome_multisig {
            Some(m) => Some(parse_hex20("registry.rome_multisig", m)?),
            None => None,
        };
        let global_sanctions_router = match &section.global_sanctions_router {
            Some(r) => Some(parse_hex20("registry.global_sanctions_router", r)?),
            None => None,
        };
        // Plain copy, no parse — an i64 block number, unlike the hex-address
        // fields above.
        let global_sanctions_router_from_block = section.global_sanctions_router_from_block;

        Ok(Self {
            commit_sha: section.commit_sha.clone(),
            factory,
            storefront,
            morpho,
            listed_assets,
            candidate_pools,
            multisig,
            global_sanctions_router,
            global_sanctions_router_from_block,
        })
    }
}

impl RegistrySource for ConfigRegistrySource {
    fn commit_sha(&self) -> String {
        self.commit_sha.clone()
    }

    fn is_listed_asset(&self, token: [u8; 20]) -> bool {
        self.listed_assets.contains(&token)
    }

    fn factory(&self) -> [u8; 20] {
        self.factory
    }

    fn storefront(&self) -> [u8; 20] {
        self.storefront
    }

    fn candidate_pools(&self) -> Vec<[u8; 20]> {
        self.candidate_pools.clone()
    }

    fn morpho(&self) -> [u8; 20] {
        self.morpho
    }

    fn multisig(&self) -> Option<[u8; 20]> {
        self.multisig
    }

    fn global_sanctions_router(&self) -> Option<[u8; 20]> {
        self.global_sanctions_router
    }

    fn global_sanctions_router_from_block(&self) -> Option<i64> {
        self.global_sanctions_router_from_block
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RegistrySection;

    fn addr_hex(byte: u8) -> String {
        format!("0x{}", hex::encode([byte; 20]))
    }

    #[test]
    fn config_registry_source_round_trips_every_projected_value() {
        let section = RegistrySection {
            commit_sha: "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef".to_string(),
            factory: addr_hex(0xF0),
            storefront: addr_hex(0xC0),
            morpho: Some(addr_hex(0x90)),
            listed_assets: vec![addr_hex(0xA1), addr_hex(0xA2)],
            candidate_pools: vec![addr_hex(0xB1)],
            rome_multisig: Some(addr_hex(0xD0)),
            global_sanctions_router: Some(addr_hex(0x91)),
            global_sanctions_router_from_block: Some(485480000),
        };

        let source = ConfigRegistrySource::from_section(&section).unwrap();
        assert_eq!(
            source.commit_sha(),
            "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
        );
        assert_eq!(source.factory(), [0xF0u8; 20]);
        assert_eq!(source.storefront(), [0xC0u8; 20]);
        assert_eq!(source.morpho(), [0x90u8; 20]);
        assert!(source.is_listed_asset([0xA1u8; 20]));
        assert!(source.is_listed_asset([0xA2u8; 20]));
        assert!(!source.is_listed_asset([0xA3u8; 20]));
        assert_eq!(source.candidate_pools(), vec![[0xB1u8; 20]]);
        assert_eq!(source.multisig(), Some([0xD0u8; 20]));
        assert_eq!(source.global_sanctions_router(), Some([0x91u8; 20]));
        assert_eq!(source.global_sanctions_router_from_block(), Some(485480000));
    }

    #[test]
    fn config_registry_source_with_no_morpho_projects_the_zero_address() {
        let section = RegistrySection {
            commit_sha: "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef".to_string(),
            factory: addr_hex(0xF0),
            storefront: addr_hex(0xC0),
            morpho: None,
            listed_assets: vec![],
            candidate_pools: vec![],
            rome_multisig: None,
            global_sanctions_router: None,
            global_sanctions_router_from_block: None,
        };

        let source = ConfigRegistrySource::from_section(&section).unwrap();
        assert_eq!(source.morpho(), [0u8; 20]);
        assert_eq!(
            source.multisig(),
            None,
            "no rome_multisig configured => multisig() must be None, never inferred"
        );
    }

    /// P4b-ii: a malformed `rome_multisig` must be refused at construction,
    /// same discipline as every other address field here.
    #[test]
    fn config_registry_source_rejects_malformed_multisig() {
        let section = RegistrySection {
            commit_sha: "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef".to_string(),
            factory: addr_hex(0xF0),
            storefront: addr_hex(0xC0),
            morpho: None,
            listed_assets: vec![],
            candidate_pools: vec![],
            rome_multisig: Some("0xdead".to_string()), // too short
            global_sanctions_router: None,
            global_sanctions_router_from_block: None,
        };
        assert!(ConfigRegistrySource::from_section(&section).is_err());
    }

    // ---- Pinned Sanctions-Router coverage-gap closer: global_sanctions_router ----

    #[test]
    fn config_registry_source_projects_global_sanctions_router() {
        let section = RegistrySection {
            commit_sha: "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef".to_string(),
            factory: addr_hex(0xF0),
            storefront: addr_hex(0xC0),
            morpho: None,
            listed_assets: vec![],
            candidate_pools: vec![],
            rome_multisig: None,
            global_sanctions_router: Some(addr_hex(0x90)),
            global_sanctions_router_from_block: None,
        };
        let source = ConfigRegistrySource::from_section(&section).unwrap();
        assert_eq!(source.global_sanctions_router(), Some([0x90u8; 20]));
    }

    #[test]
    fn config_registry_source_with_no_global_sanctions_router_projects_none() {
        let section = RegistrySection {
            commit_sha: "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef".to_string(),
            factory: addr_hex(0xF0),
            storefront: addr_hex(0xC0),
            morpho: None,
            listed_assets: vec![],
            candidate_pools: vec![],
            rome_multisig: None,
            global_sanctions_router: None,
            global_sanctions_router_from_block: None,
        };
        let source = ConfigRegistrySource::from_section(&section).unwrap();
        assert_eq!(
            source.global_sanctions_router(),
            None,
            "no global_sanctions_router configured => None, never inferred"
        );
    }

    #[test]
    fn config_registry_source_rejects_malformed_global_sanctions_router() {
        let section = RegistrySection {
            commit_sha: "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef".to_string(),
            factory: addr_hex(0xF0),
            storefront: addr_hex(0xC0),
            morpho: None,
            listed_assets: vec![],
            candidate_pools: vec![],
            rome_multisig: None,
            global_sanctions_router: Some("0xdead".to_string()), // too short
            global_sanctions_router_from_block: None,
        };
        assert!(ConfigRegistrySource::from_section(&section).is_err());
    }

    // ---- global_sanctions_router_from_block: plain pass-through, no parse ----

    #[test]
    fn config_registry_source_projects_global_sanctions_router_from_block() {
        let section = RegistrySection {
            commit_sha: "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef".to_string(),
            factory: addr_hex(0xF0),
            storefront: addr_hex(0xC0),
            morpho: None,
            listed_assets: vec![],
            candidate_pools: vec![],
            rome_multisig: None,
            global_sanctions_router: Some(addr_hex(0x90)),
            global_sanctions_router_from_block: Some(485480000),
        };
        let source = ConfigRegistrySource::from_section(&section).unwrap();
        assert_eq!(source.global_sanctions_router_from_block(), Some(485480000));
    }

    #[test]
    fn config_registry_source_with_no_global_sanctions_router_from_block_projects_none() {
        let section = RegistrySection {
            commit_sha: "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef".to_string(),
            factory: addr_hex(0xF0),
            storefront: addr_hex(0xC0),
            morpho: None,
            listed_assets: vec![],
            candidate_pools: vec![],
            rome_multisig: None,
            global_sanctions_router: Some(addr_hex(0x90)),
            global_sanctions_router_from_block: None,
        };
        let source = ConfigRegistrySource::from_section(&section).unwrap();
        assert_eq!(
            source.global_sanctions_router_from_block(),
            None,
            "no from_block configured => None, never inferred"
        );
    }
}
