//! Test doubles for the [`super::rpc::ResolverRpc`] and
//! [`super::registry_source::RegistrySource`] seams. Every resolution test
//! in this crate goes through these fakes — there is no real network/HTTP
//! client anywhere in `rome-audit` (D5, deferred per the task).

use std::collections::BTreeMap;
use std::sync::Mutex;

use super::registry_source::RegistrySource;
use super::rpc::{LogEntry, MarketParams, ResolverRpc, RpcError};

/// A fully in-memory, caller-scripted [`ResolverRpc`]. Every address this
/// fake will ever return is exactly what the test configured — nothing is
/// derived, guessed, or falls back to a literal baked into this file. That
/// property is what [`super::resolver::resolve`]'s no-hardcode canary test
/// checks: every address in a resolved graph must be traceable to a value
/// THIS struct was explicitly given.
#[derive(Debug, Default)]
pub struct FakeRpc {
    token_implementations: BTreeMap<([u8; 20], [u8; 20]), [u8; 20]>, // (factory, token) -> impl
    routers: BTreeMap<[u8; 20], [u8; 20]>,                           // token -> router
    restriction_modules: BTreeMap<([u8; 20], [u8; 32]), [u8; 20]>,   // (token, typeId) -> module
    logs: BTreeMap<([u8; 20], [u8; 32]), Vec<LogEntry>>,             // (address, topic0) -> logs
    /// (router, typeId) -> chain-global module (P3b Axis-2 discovery, §2.4).
    global_modules: BTreeMap<([u8; 20], [u8; 32]), [u8; 20]>,
    /// pair -> (token0, token1) (P3b UV2 spine discovery, §2.8). Absence
    /// means `token0`/`token1` return `Err` — the fake's model of "this
    /// address reverts the call" (not a UV2 pair, or not even a contract).
    pair_tokens: BTreeMap<[u8; 20], ([u8; 20], [u8; 20])>,
    /// marketId -> MarketParams (P3b Morpho discovery, §2.9).
    market_params: BTreeMap<[u8; 32], MarketParams>,
    /// Every address ever handed to this fake via the setters above —
    /// exactly the "known-good" set the no-hardcode canary checks a
    /// resolved graph's addresses against.
    known_addresses: Mutex<std::collections::BTreeSet<[u8; 20]>>,
}

impl FakeRpc {
    pub fn new() -> Self {
        Self::default()
    }

    fn remember(&self, addr: [u8; 20]) {
        self.known_addresses.lock().unwrap().insert(addr);
    }

    pub fn set_token_implementation(
        &mut self,
        factory: [u8; 20],
        token: [u8; 20],
        impl_addr: [u8; 20],
    ) {
        self.remember(factory);
        self.remember(token);
        self.remember(impl_addr);
        self.token_implementations
            .insert((factory, token), impl_addr);
    }

    pub fn set_router(&mut self, token: [u8; 20], router: [u8; 20]) {
        self.remember(token);
        self.remember(router);
        self.routers.insert(token, router);
    }

    pub fn set_restriction_module(&mut self, token: [u8; 20], type_id: [u8; 32], module: [u8; 20]) {
        self.remember(token);
        self.remember(module);
        self.restriction_modules.insert((token, type_id), module);
    }

    pub fn add_log(&mut self, address: [u8; 20], topic0: [u8; 32], log: LogEntry) {
        self.remember(address);
        self.remember(log.address);
        for topic in &log.topics {
            // Topics that happen to encode an address (indexed address
            // args) are right-aligned 32-byte words — record the low 20
            // bytes too, so the canary can trace an address decoded FROM a
            // log back to something this fake actually emitted.
            let mut addr = [0u8; 20];
            addr.copy_from_slice(&topic[12..32]);
            self.remember(addr);
        }
        self.logs.entry((address, topic0)).or_default().push(log);
    }

    /// Every address this fake was ever configured with — the canary's
    /// ground truth.
    pub fn known_addresses(&self) -> std::collections::BTreeSet<[u8; 20]> {
        self.known_addresses.lock().unwrap().clone()
    }

    /// Configures `router.getGlobalModuleAddress(type_id) == module` (P3b
    /// Axis-2 discovery). A `module` of `[0u8;20]` (never configured, or
    /// explicitly set to zero) means "no module registered" — resolve()
    /// must not add a source for it.
    pub fn set_global_module(&mut self, router: [u8; 20], type_id: [u8; 32], module: [u8; 20]) {
        self.remember(router);
        if module != [0u8; 20] {
            self.remember(module);
        }
        self.global_modules.insert((router, type_id), module);
    }

    /// Configures `pair.token0() == token0` and `pair.token1() == token1`
    /// (P3b UV2 spine discovery). A `pair` never given to this method
    /// reverts both calls (models "not a UV2 pair").
    pub fn set_pair(&mut self, pair: [u8; 20], token0: [u8; 20], token1: [u8; 20]) {
        self.remember(pair);
        self.remember(token0);
        self.remember(token1);
        self.pair_tokens.insert(pair, (token0, token1));
    }

    /// Configures `morpho.idToMarketParams(market_id)` (P3b Morpho
    /// discovery, §2.9).
    pub fn set_market(
        &mut self,
        market_id: [u8; 32],
        loan_token: [u8; 20],
        collateral_token: [u8; 20],
    ) {
        self.remember(loan_token);
        self.remember(collateral_token);
        self.market_params.insert(
            market_id,
            MarketParams {
                loan_token,
                collateral_token,
            },
        );
    }
}

#[async_trait::async_trait]
impl ResolverRpc for FakeRpc {
    async fn get_token_implementation(
        &self,
        factory: [u8; 20],
        token: [u8; 20],
    ) -> Result<[u8; 20], RpcError> {
        Ok(self
            .token_implementations
            .get(&(factory, token))
            .copied()
            .unwrap_or([0u8; 20]))
    }

    async fn restrictions_router(&self, token: [u8; 20]) -> Result<[u8; 20], RpcError> {
        self.routers
            .get(&token)
            .copied()
            .ok_or_else(|| RpcError::CallFailed(format!("no router configured for {token:?}")))
    }

    async fn get_restriction_module(
        &self,
        token: [u8; 20],
        type_id: [u8; 32],
    ) -> Result<[u8; 20], RpcError> {
        Ok(self
            .restriction_modules
            .get(&(token, type_id))
            .copied()
            .unwrap_or([0u8; 20]))
    }

    async fn logs_for(
        &self,
        address: [u8; 20],
        topic0: [u8; 32],
    ) -> Result<Vec<LogEntry>, RpcError> {
        Ok(self
            .logs
            .get(&(address, topic0))
            .cloned()
            .unwrap_or_default())
    }

    async fn get_global_module_address(
        &self,
        router: [u8; 20],
        type_id: [u8; 32],
    ) -> Result<[u8; 20], RpcError> {
        Ok(self
            .global_modules
            .get(&(router, type_id))
            .copied()
            .unwrap_or([0u8; 20]))
    }

    async fn token0(&self, pair: [u8; 20]) -> Result<[u8; 20], RpcError> {
        self.pair_tokens
            .get(&pair)
            .map(|(t0, _)| *t0)
            .ok_or_else(|| RpcError::CallFailed(format!("token0() reverted for {pair:?}")))
    }

    async fn token1(&self, pair: [u8; 20]) -> Result<[u8; 20], RpcError> {
        self.pair_tokens
            .get(&pair)
            .map(|(_, t1)| *t1)
            .ok_or_else(|| RpcError::CallFailed(format!("token1() reverted for {pair:?}")))
    }

    async fn id_to_market_params(
        &self,
        _morpho: [u8; 20],
        market_id: [u8; 32],
    ) -> Result<MarketParams, RpcError> {
        self.market_params
            .get(&market_id)
            .copied()
            .ok_or_else(|| RpcError::CallFailed(format!("no market configured for {market_id:?}")))
    }
}

/// A fully in-memory, caller-scripted [`RegistrySource`].
#[derive(Debug, Default)]
pub struct FakeRegistry {
    pub commit_sha: String,
    pub listed_assets: std::collections::BTreeSet<[u8; 20]>,
    pub factory: [u8; 20],
    pub storefront: [u8; 20],
    pub candidate_pools: Vec<[u8; 20]>,
    pub morpho: [u8; 20],
    pub global_sanctions_router: Option<[u8; 20]>,
    pub global_sanctions_router_from_block: Option<i64>,
}

impl FakeRegistry {
    pub fn new(commit_sha: &str, factory: [u8; 20], storefront: [u8; 20]) -> Self {
        Self {
            commit_sha: commit_sha.to_string(),
            listed_assets: Default::default(),
            factory,
            storefront,
            candidate_pools: Vec::new(),
            morpho: [0u8; 20],
            global_sanctions_router: None,
            global_sanctions_router_from_block: None,
        }
    }

    pub fn list_asset(&mut self, token: [u8; 20]) {
        self.listed_assets.insert(token);
    }

    /// Adds a registry-listed pool candidate (P3b §2.8, `#136`) — `resolve()`
    /// must still validate it on-chain, never trust it directly.
    pub fn add_candidate_pool(&mut self, pool: [u8; 20]) {
        self.candidate_pools.push(pool);
    }

    /// Sets the registry-resolved Morpho Blue singleton (P3b §2.9).
    pub fn set_morpho(&mut self, morpho: [u8; 20]) {
        self.morpho = morpho;
    }
}

impl RegistrySource for FakeRegistry {
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

    fn global_sanctions_router(&self) -> Option<[u8; 20]> {
        self.global_sanctions_router
    }

    fn global_sanctions_router_from_block(&self) -> Option<i64> {
        self.global_sanctions_router_from_block
    }
}
