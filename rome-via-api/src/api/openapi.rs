/// utoipa OpenAPI document aggregation for rome-via-api.
use utoipa::OpenApi;

use crate::api::models::{
    AddressDetail, AddressTokenHolding, Block, CrossChainCorrelation, HookEntry, SearchHit, SearchResults, StatsOverview,
    GateEvent, TokenDetail, TokenHolder, TokenSummary, TokenTransfer, Tx, TxStatus, TxType,
    CurrentTps, PeakTps, PeakWindowJson, TopBlock, TimeseriesPoint, Histogram, HistogramBucket, Cadence,
    TopWindowRow, TopBlockRow, ThroughputRecord,
};
use crate::error::ProblemJson;

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Rome Via API",
        version = "3.0.0",
        description = "Block explorer REST API for Rome Protocol. Returns confirmed blocks, transactions, tokens, addresses, and search from the rome_via Postgres schema.",
        contact(
            name = "Rome Protocol",
            url = "https://romeprotocol.xyz"
        )
    ),
    paths(
        crate::api::health::healthz,
        crate::api::health::readyz,
        crate::api::stats::overview,
        crate::api::blocks::list_blocks,
        crate::api::blocks::get_block_by_number,
        crate::api::txs::list_txs,
        crate::api::txs::list_cross_vm,
        crate::api::txs::get_tx_by_hash,
        // Phase 3
        crate::api::tokens::list_tokens,
        crate::api::tokens::get_token,
        crate::api::tokens::list_token_holders,
        crate::api::tokens::list_token_transfers,
        crate::api::tokens::list_token_gate_events,
        crate::api::addresses::get_address,
        crate::api::addresses::list_address_txs,
        crate::api::addresses::list_address_tokens,
        crate::api::search::search,
        // Phase 4
        crate::api::cross_chain::list_cross_chain,
        crate::api::hooks::list_hooks,
        // Phase 5 — throughput
        crate::api::throughput::current_tps,
        crate::api::throughput::peak_tps_handler,
        crate::api::throughput::top_blocks,
        crate::api::throughput::timeseries,
        crate::api::throughput::histogram,
        crate::api::throughput::cadence,
        crate::api::throughput::record,
        // Audit — public Tier-1 event browse
        crate::api::audit::list_events,
        crate::api::audit::event_counts,
    ),
    components(
        schemas(
            Block,
            Tx,
            TxStatus,
            TxType,
            StatsOverview,
            ProblemJson,
            // Phase 3
            TokenSummary,
            TokenDetail,
            TokenHolder,
            TokenTransfer,
            GateEvent,
            AddressDetail,
            AddressTokenHolding,
            crate::api::address_kind::AddressKind,
            SearchHit,
            SearchResults,
            // Phase 4
            CrossChainCorrelation,
            HookEntry,
            // Phase 5 — throughput
            CurrentTps,
            PeakTps,
            PeakWindowJson,
            TopBlock,
            TimeseriesPoint,
            Histogram,
            HistogramBucket,
            Cadence,
            TopWindowRow,
            TopBlockRow,
            ThroughputRecord,
            // Audit — public Tier-1 event browse
            crate::api::audit::AuditEvent,
            crate::api::audit::AuditEventCount,
            crate::api::audit::AuditEventCounts,
        )
    ),
    tags(
        (name = "health", description = "Liveness and readiness probes"),
        (name = "stats", description = "Chain-level statistics"),
        (name = "blocks", description = "EVM block queries"),
        (name = "txs", description = "EVM transaction queries"),
        (name = "tokens", description = "Token metadata, holders, transfers"),
        (name = "addresses", description = "Address stats and transaction history"),
        (name = "search", description = "Fuzzy search over blocks, txs, tokens, addresses"),
        (name = "cross-chain", description = "Cross-chain correlation records (Remus/Romulus atomic txs)"),
        (name = "cross-vm", description = "EVM<->Solana seam crossings feed (/cross-vm)"),
        (name = "hooks", description = "Hook registry — token transfer hooks registered on-chain"),
        (name = "throughput", description = "Chain throughput — TPS, peak, timeseries, histogram, cadence"),
        (name = "audit", description = "Public Tier-1 audit event browse over decoded on-chain events (audit.chain_event)"),
    )
)]
pub struct ApiDoc;
