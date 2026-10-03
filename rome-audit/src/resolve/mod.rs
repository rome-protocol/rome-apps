//! P3 — token-parameterized historical resolution (this task's own
//! numbering; the plan's own numbering calls the same scope §3 P2 — see
//! `tier2/mod.rs`'s identical note on the two label sets). Turns "capture,
//! given a caller-supplied set of addresses/topics" (P1's `IngestConfig`)
//! into "`resolve(token)` derives its OWN source graph" (capture §1.2), plus
//! the two persistence artifacts that graph feeds: [`manifest`]
//! (`capture_manifest`, §1.4 — the frozen, self-recording resolution input)
//! and [`asset_event`] (the C1 many-to-many scope junction).
//!
//! **What's built vs. deferred — read before trusting this module's
//! coverage** (full detail in the P3/P3b reports): resolved — authoritativeness
//! gate (§1.1), the token/router/factory/storefront seed sources, the
//! Axis-1 + yield-blacklist module swap history, the yield-token and
//! purchase-token re-pointing history (P3 core), **plus (P3b, 2026-08-17):
//! the router read is now a real storage read** (`router_slot` +
//! `live_rpc::EthersResolverRpc`, corrected from the P3-era eth_call
//! assumption), GlobalSanctions/Axis-2 module discovery
//! (`router.getGlobalModuleAddress`), UV2 spine-driven pool discovery (§2.8,
//! ITERATED to a fixed point — see `resolver::discover_uv2_pairs`'s doc for
//! why a single pass doesn't suffice here the way it did for P3 core's
//! static walks), and Morpho loan/collateral market discovery (§2.9).
//!
//! **P3c (2026-08-17): the resolve→ingest seam itself** — `pass::run_resolve_pass`
//! wires `resolve(token)`'s output into `run::AuditWorker` (`SourceMode::Resolved`),
//! so the service is actually token-driven end to end, and `ConfigRegistrySource`
//! closes D5's "how does `rome-audit` GET a `RegistrySource`" half (the
//! trust boundary this rests on — the config's registry projection is
//! operator-asserted, not independently verified — is documented on
//! `config::RegistrySection`).
//!
//! Still deferred: the UV2/Morpho full event surface (`Swap`/`Mint`/`Burn`/
//! `Sync`, and Morpho's ~18 non-`CreateMarket` events — only what discovery
//! itself needs is registered in `abi/uv2_pair.rs` / `abi/morpho.rs`);
//! `asset_event`'s scope_filter is documentary only, not enforced as a join
//! predicate (a pre-existing gap shared with yield-token/purchase-token);
//! an IN-SERVICE live fetch against `rome-protocol/registry` (D5's other
//! half — see `registry_source`'s module doc) — the config-supplied path
//! stays the only implementation for now.

pub mod asset_event;
pub mod discovery;
pub mod graph;
pub mod hercules_rpc;
pub mod live_rpc;
pub mod manifest;
pub mod pass;
pub mod registry_source;
pub mod resolver;
pub mod router_slot;
pub mod rpc;
pub mod store;
pub mod walk;

#[cfg(test)]
pub mod fixture;

pub use asset_event::{rebuild_asset_event, AssetManifest};
pub use discovery::{discover_authoritative_tokens, discover_factory_tokens, DiscoveryError};
pub use graph::{ResolvedFrom, ResolvedGraph, ResolvedSource, ScopeFilter, SourceInterval};
pub use hercules_rpc::HerculesResolverRpc;
pub use live_rpc::EthersResolverRpc;
pub use manifest::CaptureManifest;
pub use pass::{merge_ingest_sources, run_resolve_pass, PassError, ResolvedSet};
pub use registry_source::{ConfigRegistrySource, RegistrySource};
pub use resolver::{resolve, resolve_pinned_sanctions_floor, to_ingest_sources, ResolveError};
pub use router_slot::{decode_address_from_storage_word, restrictions_router_slot};
pub use rpc::{LogEntry, MarketParams, ResolverRpc, RpcError};
pub use store::{generated_at_for, insert_backfill_gap, insert_capture_manifest};
// `previously_known_addresses` is `#[deprecated]` (P4a §3, dead since the
// ingest_scope re-key) — re-exported behind `#[allow(deprecated)]` so this
// re-export itself doesn't warn; direct callers still see the deprecation.
#[allow(deprecated)]
pub use store::previously_known_addresses;
