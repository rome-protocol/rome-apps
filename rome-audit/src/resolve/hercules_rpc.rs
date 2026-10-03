//! The LIVE-service [`ResolverRpc`]: log-HISTORY reads
//! (`logs_for`) come from the Hercules source DB; every STATE read
//! (`eth_call`/`eth_getStorageAt`) still goes to live RPC via the wrapped
//! [`EthersResolverRpc`].
//!
//! **Why the split.** The resolver's log walks would otherwise issue one
//! `eth_getLogs` over `[Earliest, Latest]` (`live_rpc::EthersResolverRpc::
//! logs_for`), and the production proxy caps `eth_getLogs` at 12,000 blocks
//! → `-32005 block range too wide`, so the resolver's first pass never
//! completes a real log walk. The proxy's `eth_getLogs` is itself backed by
//! Hercules' `evm_log` table — the same data this crate already holds a
//! read-only `source` pool to — so log history is served straight from the
//! indexed DB (IMPL-PLAN §595 "nothing to re-fetch from RPC"). STATE reads
//! (`getRestrictionModule` / `getTokenImplementation` / `idToMarketParams` /
//! `token0`/`token1` / `getStorageAt`) STAY on live RPC — the M1 / #136
//! live-authority rule: an on-chain read is authority, an indexed copy is
//! not.
//!
//! Only `logs_for` is overridden; every other method DELEGATES verbatim to
//! the inner [`EthersResolverRpc`], so there is exactly one real
//! implementation of each state read.

use ethers::providers::Middleware;
use sqlx::PgPool;

use crate::ingest::hercules_reads::logs_for_address_topic0;

use super::live_rpc::EthersResolverRpc;
use super::rpc::{LogEntry, MarketParams, ResolverRpc, RpcError};

pub struct HerculesResolverRpc<M> {
    source: PgPool,
    inner: EthersResolverRpc<M>,
}

impl<M> HerculesResolverRpc<M> {
    pub fn new(source: PgPool, inner: EthersResolverRpc<M>) -> Self {
        Self { source, inner }
    }
}

#[async_trait::async_trait]
impl<M: Middleware + 'static> ResolverRpc for HerculesResolverRpc<M> {
    async fn get_token_implementation(
        &self,
        factory: [u8; 20],
        token: [u8; 20],
    ) -> Result<[u8; 20], RpcError> {
        self.inner.get_token_implementation(factory, token).await
    }

    async fn restrictions_router(&self, token: [u8; 20]) -> Result<[u8; 20], RpcError> {
        self.inner.restrictions_router(token).await
    }

    async fn get_restriction_module(
        &self,
        token: [u8; 20],
        type_id: [u8; 32],
    ) -> Result<[u8; 20], RpcError> {
        self.inner.get_restriction_module(token, type_id).await
    }

    /// The ONLY overridden method: log history from Hercules, not `eth_getLogs`.
    async fn logs_for(
        &self,
        address: [u8; 20],
        topic0: [u8; 32],
    ) -> Result<Vec<LogEntry>, RpcError> {
        logs_for_address_topic0(&self.source, address, topic0)
            .await
            .map_err(|e| RpcError::CallFailed(e.to_string()))
    }

    async fn get_global_module_address(
        &self,
        router: [u8; 20],
        type_id: [u8; 32],
    ) -> Result<[u8; 20], RpcError> {
        self.inner.get_global_module_address(router, type_id).await
    }

    async fn token0(&self, pair: [u8; 20]) -> Result<[u8; 20], RpcError> {
        self.inner.token0(pair).await
    }

    async fn token1(&self, pair: [u8; 20]) -> Result<[u8; 20], RpcError> {
        self.inner.token1(pair).await
    }

    async fn id_to_market_params(
        &self,
        morpho: [u8; 20],
        market_id: [u8; 32],
    ) -> Result<MarketParams, RpcError> {
        self.inner.id_to_market_params(morpho, market_id).await
    }
}
