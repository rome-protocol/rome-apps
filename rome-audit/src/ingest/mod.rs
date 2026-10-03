//! P1 — the finalized-ingest pipeline (IMPL-PLAN §3 P1). Reads Hercules'
//! source DB (read-only, plain queries in [`hercules_reads`] — the SAME
//! two-`PgPool` shape `rome-via-sync`/`rome-via-enrich` already use, not a
//! bespoke access abstraction), applies the audit worker's own
//! verified-finality watermark ([`watermark`]), and writes the decoded,
//! append-only Record into `audit.chain_event` ([`pipeline`]).

pub mod hercules_reads;
pub mod pipeline;
pub mod watermark;

pub use hercules_reads::{
    block_frontier_through_slot, matched_log_slots_in_range, min_produced_block,
    slot_range_for_blocks, MatchedLog, SlotObservation, TxReceiptInfo,
};
pub use pipeline::{
    ensure_watermark_initialized, run_ingest_once, IngestConfig, IngestError, IngestFilter,
    IngestOutcome, SourceSpec,
};
pub use watermark::{SlotDigest, SlotStatusKind, WatermarkTracker, WatermarkVerdict};
