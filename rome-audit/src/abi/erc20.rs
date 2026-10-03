//! `YieldToken` / `PurchaseToken` (P4a) — the chain-wide ERC20s
//! `resolve::resolver` discovers via `ArcToken.YieldTokenUpdated` /
//! `ArcTokenPurchase.PurchaseTokenUpdated` (capture §1.2). Both kinds
//! register **only** `Transfer`, reusing `arc_token::TRANSFER_TOPIC0` —
//! same pattern as `abi/uv2_pair.rs`'s own `Transfer` registration (a
//! DIFFERENT `(source_kind, topic0)` key than `ArcToken`'s, which
//! `AbiRegistry` allows; `event_names()` dedupes by name, so this adds no
//! new fixture-coverage obligation — the name "Transfer" is already
//! covered via `ArcToken`).
//!
//! **THE LANDMINE (read before touching this file again):** the moment
//! either kind gained this registration, `AbiRegistry::has_events_for`
//! flips to `true` for it, so `resolve::pass::run_resolve_pass`'s C1
//! `retain` STOPS dropping it from the live ingest map — without the
//! enforced `ScopeFilter` (`resolve::graph::ScopeFilter::TransferFrom` /
//! `TransferTouches`, wired at BOTH `resolve::asset_event`'s join (Layer 1)
//! AND `ingest::pipeline::IngestFilter` (Layer 2) BEFORE this file ever
//! registered these two events), ingest would ask Hercules for EVERY
//! transfer of a chain-wide token like wUSDC across the WHOLE chain. Both
//! layers were built, tested, and green (`resolve_db.rs`'s
//! `scope_filter_*` tests, `ingest_db.rs`'s `ingest_filter_*` test) BEFORE
//! this registration landed — never re-order that.

use crate::registry::{AbiRegistry, ArgSpec, ArgType, EventDescriptor};
use crate::types::{ProjectionTag, SourceKind};

use super::arc_token::TRANSFER_TOPIC0;

pub(crate) fn register(registry: &mut AbiRegistry) {
    registry.register(
        SourceKind::YieldToken,
        TRANSFER_TOPIC0,
        EventDescriptor {
            event_name: "Transfer",
            projection_tag: ProjectionTag::Primary,
            args: vec![
                ArgSpec::new("from", ArgType::Address, true),
                ArgSpec::new("to", ArgType::Address, true),
                ArgSpec::new("value", ArgType::Uint256, false),
            ],
        },
    );
    registry.register(
        SourceKind::PurchaseToken,
        TRANSFER_TOPIC0,
        EventDescriptor {
            event_name: "Transfer",
            projection_tag: ProjectionTag::Primary,
            args: vec![
                ArgSpec::new("from", ArgType::Address, true),
                ArgSpec::new("to", ArgType::Address, true),
                ArgSpec::new("value", ArgType::Uint256, false),
            ],
        },
    );
}
