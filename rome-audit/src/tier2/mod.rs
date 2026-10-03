//! Tier-2 derived tables — pure functions of `audit.chain_event` (the audit-trail
//! implementation plan §2 Tier-2 / §13.1 rebuild determinism;
//! this task's own phase numbering calls it "P2", the plan's own numbering
//! calls the same scope §3 P3 — no scope mismatch, just two labels).
//!
//! Every table under this module is **REBUILDABLE**: [`rebuild::rebuild_tier2`]
//! TRUNCATEs it and recomputes it from `audit.chain_event` rows read in the
//! total order P1 established (`block_number, tx_index, log_index`) — never
//! insertion order, never `event_id` order (`event_id` is a DB surrogate,
//! IMPL-PLAN C1). The builders in each submodule are plain, allocation-only
//! functions over `&[ChainEventRow]` — no I/O, no `PgPool` — so the
//! rebuild-determinism property is really "the same input list always
//! produces the same output list", which [`rebuild::rebuild_tier2`] then
//! persists via TRUNCATE + INSERT.
//!
//! **Scope note (P2 honesty, per the task):** `asset_id` resolution
//! (capture §1 `capture_manifest`/`asset_event`) is a later phase — it
//! doesn't exist in this crate yet. So the module a wanted asset's Axis-1
//! module / token address / sanctions module / router resolve to is a
//! **caller-supplied input** ([`fetch::Tier2Config`]), never hardcoded and
//! never looked up from a registry here.

pub mod allowlist;
pub mod code_change;
pub mod denyset;
pub mod exposure;
pub mod fetch;
pub mod gate;
pub mod holder_balance;
pub mod rebuild;
pub mod role_interval;
pub mod router_epoch;
pub mod sale;
pub mod screening;
pub mod yield_blacklist;
pub mod yield_run;

pub use fetch::{AssetSources, ChainEventRow, Tier2Config};
pub use rebuild::rebuild_tier2;
