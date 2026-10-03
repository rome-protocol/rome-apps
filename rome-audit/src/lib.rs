//! rome-audit — the Bloom Continuous Compliance Audit Trail decoder +
//! finalized-ingest pipeline, per the audit-trail implementation plan.
//!
//! - **P0** (§3 "ABI registry, envelope, decoder, phantom guard"): given a
//!   raw EVM log and the caller-resolved contract kind it came from, decode
//!   it into a typed compliance event using an ABI registry that fails
//!   loudly on anything unregistered. No database, no Hercules connection,
//!   no ingest cursor, no finality handling.
//! - **P1** (`ingest` module, §3 P1 / §5.2 / §5.4): reads Hercules' source
//!   DB (read-only, plain queries in [`ingest::hercules_reads`] — the same
//!   two-`PgPool` shape `rome-via-sync`/`rome-via-enrich` use, not a
//!   bespoke access abstraction), applies the audit worker's own
//!   verified-finality watermark, and writes the decoded, append-only
//!   Record into `audit.chain_event`. Tier-2/3/4 (derived tables, reports,
//!   anchoring) are later phases.
//! - **P3** (`resolve` module, this task's own numbering — the plan's own
//!   numbering calls the same scope §3 P2): `resolve(token)` derives that
//!   token's own source graph (capture §1.2's event-driven fixed point)
//!   instead of taking a caller-supplied address/topic set, persists it as
//!   `capture_manifest` (§1.4), and materializes the `asset_event`
//!   many-to-many junction (C1) that scopes shared events to the assets
//!   whose graph they fall in. See `resolve/mod.rs` for exactly what's
//!   resolved vs. deferred.
//!
//! `cli`/`config`/`server`/`run` back the `rome-audit` binary (see
//! `src/main.rs`) and mirror `rome-via-sync`'s crate shape exactly, so a
//! deployment set up like `rome-via-sync`/`rome-via-enrich` is a drop-in
//! fit.

pub mod abi;
pub mod backfill;
pub mod cli;
pub mod config;
pub mod decode;
pub mod ingest;
pub mod overlay;
pub mod registry;
pub mod resolve;
pub mod run;
pub mod server;
pub mod tier2;
pub mod types;

#[cfg(test)]
mod migrations_test;

pub use abi::build_registry;
pub use decode::{decode_log, DecodeError};
pub use registry::{AbiRegistry, ArgSpec, ArgType, EventDescriptor};
pub use types::{ArgValue, DecodedEvent, ProjectionTag, RawLog, SourceKind};
