//! The resolution RPC seam (capture §1.2 / IMPL-PLAN "M1": "resolution is
//! NOT [no-RPC] — `restrictionsRouter()`, `getRestrictionModule()`,
//! `yieldToken()`, `getTokenImplementation()`, … are `eth_call`s via the
//! registry-resolved RPC"). `ResolverRpc` is that seam: an injected
//! dependency, never a concrete HTTP/Ethers client built into this crate.
//! [`resolve::resolve`](super::resolve) calls only these named methods — it
//! never encodes a raw ABI selector or opens a socket itself, so a fake
//! implementation (used by every test in this module) is a drop-in,
//! zero-network double.
//!
//! **Named capability methods, not a raw `eth_call(address, bytes)`
//! primitive.** A generic byte-blob `eth_call` would push ABI-encoding risk
//! (selector + argument packing) into the resolver, and — worse — several of
//! the getters the capture spec assumes (`token.restrictionsRouter()`,
//! `token.yieldToken()`) do **not exist** on the vendored `ArcToken.sol` I
//! read (`contracts/src/ArcToken.sol`): the contract only
//! exposes `getRestrictionModule(bytes32)` as a public view; `restrictionsRouter`/
//! `yieldToken` are private fields behind a namespaced-storage accessor with
//! no getter. That's a real capture-spec-vs-source gap (flagged in the P3
//! report), not something to paper over by inventing a selector for a
//! function that isn't there. Naming the capability at the trait level
//! (`restrictions_router`, `yield_token`) defers "does a getter exist, and
//! what's its selector" to whoever implements this trait against a real
//! `Provider` — never a Rust-side guess in `resolve()` itself.
//!
//! **`restrictions_router` corrected (P3b, 2026-08-17): storage read, not an
//! `eth_call`.** `ArcToken` has no `restrictionsRouter()` getter at all (not
//! even a private-selector one) — the value is read via `eth_getStorageAt`
//! at the ERC-7201 slot derived in [`super::router_slot`]. The trait method
//! name/shape is unchanged (still a capability-level `Result<[u8;20],
//! RpcError>`); only what backs it in the real implementation changed.
//!
//! D5 (registry-pin real wiring) is the same story one level up: unit/DB
//! tests still use [`FakeRpc`](super::fixture::FakeRpc) exclusively — **but a
//! real implementation now exists** ([`super::live_rpc::EthersResolverRpc`],
//! P3b), backed by a real `ethers::providers::Middleware`, exercised in its
//! own tests via `ethers::providers::Provider::mocked()` (a canned-response
//! transport — no live network call from this crate's test suite).

/// One historical log entry, as returned by [`ResolverRpc::logs_for`] — the
/// resolution-side equivalent of `eth_getLogs` filtered to one
/// `(address, topic0)` pair. Shape mirrors [`crate::types::RawLog`] plus the
/// ordering fields the P0 decoder doesn't need but the fixed-point walk does
/// (capture §1.2: history is `(block_number, tx_index, log_index)`-ordered,
/// never insertion order).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub block_number: i64,
    pub tx_index: i32,
    pub log_index: i32,
    pub address: [u8; 20],
    pub topics: Vec<[u8; 32]>,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RpcError {
    #[error("resolver RPC call failed: {0}")]
    CallFailed(String),
}

/// The subset of Morpho Blue's `MarketParams` (`IMorpho.sol:329`) P3b's
/// discovery needs — `loan_token`/`collateral_token` are exactly what §2.9's
/// "asset as loan OR collateral" filter reads. `oracle`/`irm`/`lltv` aren't
/// carried here: no discovery decision in this crate depends on them, and
/// adding unused fields just to "look complete" is the dead-weight this
/// crate's own module doc warns against (see `resolve/mod.rs`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarketParams {
    pub loan_token: [u8; 20],
    pub collateral_token: [u8; 20],
}

/// The resolution-side RPC seam. Every method here corresponds to one
/// capture §1.2 read. STATE reads (`eth_call`/`eth_getStorageAt`:
/// `get_token_implementation`, `restrictions_router`,
/// `get_restriction_module`, `get_global_module_address`, `token0`/`token1`,
/// `id_to_market_params`) are backed by the registry-resolved live RPC —
/// the M1 / #136 live-authority rule (an on-chain read is authority, an
/// indexed copy is not). LOG-HISTORY reads ([`logs_for`](ResolverRpc::logs_for))
/// are the exception: the live service serves them from the Hercules source
/// DB ([`super::hercules_rpc::HerculesResolverRpc`]), because the production
/// proxy caps `eth_getLogs` at 12,000 blocks (`-32005`) so a `[Earliest,
/// Latest]` walk can never complete — and the proxy's `eth_getLogs` is
/// itself backed by Hercules' `evm_log` table, so it's the same data with no
/// range cap (IMPL-PLAN §595 "nothing to re-fetch from RPC"). The pure-RPC
/// [`super::live_rpc::EthersResolverRpc`] implements every method against a
/// live endpoint (still used as `HerculesResolverRpc`'s delegate for the
/// state reads, and standalone in tests via `Provider::mocked()`).
#[async_trait::async_trait]
pub trait ResolverRpc: Send + Sync {
    /// `ArcTokenFactory(factory).getTokenImplementation(token)` — the §1.1
    /// authoritativeness-gate fallback read. Returns the zero address if
    /// the token isn't registered on this factory (never an error — a
    /// negative answer is a normal, expected outcome the gate interprets).
    async fn get_token_implementation(
        &self,
        factory: [u8; 20],
        token: [u8; 20],
    ) -> Result<[u8; 20], RpcError>;

    /// `token.restrictionsRouter()` — immutable per-token (capture §1.2:
    /// "no setter", one interval `[deploy, ∞)`).
    async fn restrictions_router(&self, token: [u8; 20]) -> Result<[u8; 20], RpcError>;

    /// `token.getRestrictionModule(typeId)` — the CURRENT module for one
    /// type id. Used only as a fixed-point sanity seed; the authoritative
    /// history comes from walking `SpecificRestrictionModuleSet` via
    /// [`logs_for`](Self::logs_for).
    async fn get_restriction_module(
        &self,
        token: [u8; 20],
        type_id: [u8; 32],
    ) -> Result<[u8; 20], RpcError>;

    /// Every historical log at `address` matching `topic0`, in
    /// `(block_number, tx_index, log_index)` order (capture §1.2's
    /// event-driven fixed point is built entirely from calls to this one
    /// method — the module/yield-token/purchase-token/factory/Morpho
    /// history walks all reduce to "fetch this source's logs, decode,
    /// fold").
    async fn logs_for(
        &self,
        address: [u8; 20],
        topic0: [u8; 32],
    ) -> Result<Vec<LogEntry>, RpcError>;

    /// `router.getGlobalModuleAddress(typeId)` — capture §2.4/§1.2's Axis-2
    /// discovery: the chain-global module registered on THIS router for
    /// `typeId` (zero address = none registered). Named at the capability
    /// level, not folded into `logs_for`, because it's an `eth_call` on the
    /// ROUTER, not a log walk.
    async fn get_global_module_address(
        &self,
        router: [u8; 20],
        type_id: [u8; 32],
    ) -> Result<[u8; 20], RpcError>;

    /// `pair.token0()` — capture §2.8's UV2 spine-discovery probe. A `Result`
    /// (not an `Option`) because the real semantics of "not a UV2 pair" is a
    /// REVERT (the address either isn't a contract or doesn't implement the
    /// interface) — callers treat any `Err` here as a negative probe, never
    /// propagate it as a resolution failure.
    async fn token0(&self, pair: [u8; 20]) -> Result<[u8; 20], RpcError>;

    /// `pair.token1()` — see [`Self::token0`].
    async fn token1(&self, pair: [u8; 20]) -> Result<[u8; 20], RpcError>;

    /// `IMorpho(morpho).idToMarketParams(id)` — capture §2.9's authoritative
    /// read. `CreateMarket`'s own log ALSO carries `marketParams`, but per
    /// the `#136` lesson (an on-chain read is authority, a self-declared/
    /// event-carried field never is) this crate re-reads it live rather than
    /// trusting the event's own copy for the in/out-of-scope decision.
    async fn id_to_market_params(
        &self,
        morpho: [u8; 20],
        market_id: [u8; 32],
    ) -> Result<MarketParams, RpcError>;
}
