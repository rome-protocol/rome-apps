/// Response DTOs for the rome-via-api REST endpoints.
///
/// All responses are camelCase JSON (via `#[serde(rename_all = "camelCase")]`).
///
/// # BigInt encoding
/// Per SPEC §Conformance: Wei values, large amounts, gas figures are string-encoded.
/// Block numbers (u64 ≤ 2^53) are returned as JSON numbers since they are JS-safe
/// and the UI uses them directly as numbers. This is a documented deviation from strict
/// "all bigints as strings" conformance rule.
///
/// # Timestamps
/// Returned as ISO-8601 UTC strings. The UI computes "ago" client-side via `format.ts`.
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// A single Ethereum block as returned by GET /api/v1/blocks/:number.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Block {
    /// EVM block number (JS-safe u64, returned as JSON number).
    pub number: i64,

    /// Solana slot number containing this block.
    pub slot: i64,

    /// Index of this block within the slot (multi-block slots).
    pub slot_block_idx: i32,

    /// Block hash (0x-prefixed hex).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,

    /// Parent block hash (0x-prefixed hex).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_hash: Option<String>,

    /// Number of transactions in this block.
    pub tx_count: i64,

    /// Total gas used (string-encoded bigint).
    pub gas_used: String,

    /// Sum of per-tx `gas` (gas-limit) for every tx in this block, as a
    /// decimal string. Useful as a denominator alongside `gas_used` to show
    /// "block utilization of requested ceilings" — much more meaningful for
    /// Rome than `gas_used / chain-block-ceiling` (the latter is 48e12 on
    /// Marcus, so the bar is always near-zero). Null when the block has no
    /// indexed txs (e.g. empty block, or txs not yet mirrored).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gas_limit_sum: Option<String>,

    /// Per-block tx-type counts (Rhea / Remus / Romulus), sourced from
    /// `cross_chain_correlations.rome_tx_type` aggregated by `block_number`.
    /// Lets the explorer render a real distribution instead of fabricating
    /// a 70/20/10 split. Counts can lag tx_count slightly if the cross-chain
    /// classifier hasn't caught up to the latest block — in that case
    /// rhea+remus+romulus < tx_count.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub type_counts: Option<TypeCounts>,

    /// Fee recipient (gas-fee receiver) for this block — 0x-prefixed hex.
    /// Sourced from `rome_via.eth_block.gas_recipient`, populated by Hercules
    /// from per-tx `gas_report.gas_recipient` and projected to a flat column
    /// at the block level (one block per recipient run by construction).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gas_recipient: Option<String>,

    /// Block timestamp (ISO-8601 UTC).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
}

/// A single EVM transaction as returned by GET /api/v1/txs/:hash.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Tx {
    /// Transaction hash (0x-prefixed hex).
    pub hash: String,

    /// Transaction status derived from evm_tx_result.status.
    /// - "success": result row present with success status
    /// - "failed": result row present with failure status
    /// - "pending": no result row yet
    pub status: TxStatus,

    /// Transaction type classification (depth-aware — see [`crate::api::models::TxType`]).
    /// Rhea = stayed within rome-evm + infra (a wrapper's inner SPL CPI fires at
    /// depth ≥ 2 under rome-evm and is therefore still Rhea, NOT a native leg);
    /// Romulus = a TOP-LEVEL (depth-1) native Solana instruction composed
    /// alongside the EVM RLP; Remus = cross-rollup (≥ 2 EVM chains, deferred).
    #[serde(rename = "type")]
    pub tx_type: TxType,

    /// Method selector (first 4 bytes of calldata as 0x-prefixed hex).
    /// Method name decoding is Phase 3. Empty string for plain transfers.
    pub method: String,

    /// Sender address (0x-prefixed hex).
    pub from: String,

    /// Recipient address (0x-prefixed hex). Null for contract creation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to: Option<String>,

    /// Value transferred in Wei (string-encoded bigint).
    pub value: String,

    /// Gas consumed by this transaction (string-encoded bigint).
    /// Extracted from the `tx_result.gas_report.gas_value` JSONB.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gas_used: Option<String>,

    /// Effective gas price paid in Wei (string-encoded bigint).
    /// Extracted from the `tx_result.gas_report.gas_price` JSONB.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gas_price: Option<String>,

    /// Per-tx fee recipient (0x-prefixed hex). Same source as the block-level
    /// `gas_recipient` — extracted from `tx_result.gas_report.gas_recipient`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gas_recipient: Option<String>,

    /// Priority portion of the fee in lamports (string-encoded bigint), from the
    /// on-chain `PRIORITY_FEE` marker via `tx_result.gas_report.priority_fee`.
    /// `"0"` for pre-priority txs / when no priority was bid. Pairs with `base`
    /// to show the base-vs-priority split (`gas_used` remains the total).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority_fee: Option<String>,

    /// Base portion of the fee (string-encoded bigint) = `gas_used − priority_fee`.
    /// Derived here from the same gas_report so it stays consistent even on rows
    /// synced before the priority split shipped (no `base` key in their JSONB).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base: Option<String>,

    /// Per-tx gas limit (the ceiling the sender chose when signing) as a
    /// decimal-string i64. Sourced from `evm_tx.gas_limit`. Used together
    /// with `gas_used` to show "X of Y gas (Z%)" on tx-detail.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gas_limit: Option<i64>,

    /// Wire-level transaction type byte: 0=legacy, 1=EIP-2930, 2=EIP-1559,
    /// 0x7E=126=op-stack DepositTransaction. Sourced from `evm_tx.tx_type_byte`.
    /// Lets clients distinguish contract-creation (null `to` + non-empty input)
    /// from a deposit (null `to` carrying tx_type_byte=0x7E in some cases, or
    /// recipient = depositor address in others).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_type_byte: Option<i16>,

    /// Block timestamp of the containing block (ISO-8601 UTC).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,

    /// Block number containing this transaction.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_number: Option<i64>,

    /// Hook executions for this transaction (empty if no hooks fired).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub hook_executions: Vec<TxHookExecution>,

    /// EVM logs emitted during execution. Populated on detail endpoint only.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub logs: Vec<TxLog>,

    /// Count of EVM logs this tx emitted. Populated on list AND detail.
    /// Retained alongside `action_tags` as a raw event count (action tags
    /// dedup multiple Transfer logs into a single `token_transfer` tag, so
    /// `logs_count` and `action_tags.len()` are different units).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logs_count: Option<i32>,

    /// Server-classified action categories for this tx (e.g.
    /// `["cross_chain_call", "swap", "token_transfer"]`). Authoritative —
    /// computed from `to`, `method`, `tx_type_byte`, `cross_chain_type`,
    /// solana legs, and the logs in `tx_result.logs`. The frontend renders
    /// these as chips. See [`crate::classify`] for the taxonomy.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub action_tags: Vec<String>,

    /// Solana-side legs of this tx (empty for Rhea/Remus and pre-classification rows).
    /// Sourced from the same `cross_chain_correlations.solana_legs` JSONB used by
    /// the cross-chain endpoint — inlined here so the tx-detail page does not need
    /// a separate cross-chain lookup.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub solana_legs: Vec<SolanaLeg>,

    /// Transaction origination: "ecdsa" (signature-recovered) or "solana_unsigned"
    /// (Solana-native DoTxUnsigned — `from` is a synthetic address, authorized by `solanaSigner`).
    pub origination: String,

    /// Base58 Solana pubkey that authorized this tx. Present only for solana_unsigned txs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub solana_signer: Option<String>,

    /// Clean protocol/token label for the `to` contract (e.g. "Compound", "Aave V3",
    /// or a token symbol), resolved from `rome_via.contract_labels.display_label`.
    /// Null when the destination has no resolved label (or `to` is a plain EOA / null).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to_label: Option<String>,

    /// Optional secondary detail for the `to` contract label (e.g. "cwUSDC-9"),
    /// from `rome_via.contract_labels.display_label_detail`. Null when absent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to_label_detail: Option<String>,

    /// Decoded ERC-20 token movements ("value moved") for this tx, one per
    /// `Transfer` log, ordered by `log_index`. Sourced from
    /// `rome_via.token_transfers` joined to `rome_via.token_metadata` for
    /// symbol/decimals. Populated on the **detail** endpoint only (like `logs`)
    /// — the list query leaves it empty so the key is omitted there.
    /// Lane-agnostic: keyed by (chain_id, tx_hash), identical for ecdsa and
    /// solana_unsigned origination.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub transfers: Vec<DecodedTransfer>,

    /// Base58 Solana signature of the transaction that SETTLED this EVM tx on
    /// Solana, from `rome_via.evm_tx_sol_tx` (populated for every tx by sync).
    /// This is the "which Solana tx settled this" link — distinct from
    /// `solana_legs` (composed native legs of a Romulus tx) and `solana_signer`
    /// (the authorizing signer of a solana_unsigned tx). Detail-endpoint only;
    /// null when the settlement mapping has not synced yet.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub solana_settlement_sig: Option<String>,

    /// base58 Solana program id this tx invoked via the CpiProgram precompile
    /// (the depth >= 2 inner program), from `cross_chain_correlations.cpi_program`.
    /// Detail-endpoint only; null when the tx isn't a (labeled) CPI.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpi_program: Option<String>,

    /// Registry-curated human label for `cpi_program` (e.g. "mangoV4"), resolved
    /// at index time from the projected program-label map — the Solana analogue
    /// of `to_label`. Null when the program isn't curated.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpi_program_label: Option<String>,

    /// Instruction name of the CPI (the program's Anchor "Instruction: <Name>"
    /// log), e.g. "PlaceOrder". Null when the program emits no such log.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpi_instruction: Option<String>,

    /// Contract address created by this tx, when it is a deployment (`to`
    /// absent). Never stored — derived on read from `from` + `nonce` via the
    /// standard CREATE formula, so it is retroactively correct for every
    /// existing row with no migration or backfill. Null for non-creation txs.
    #[serde(rename = "contractAddress", skip_serializing_if = "Option::is_none")]
    pub contract_address: Option<String>,

    /// Raw calldata length in bytes, from `evm_tx.input_len`. Lets the frontend's
    /// local classifier distinguish contract-creation from a gap row without a
    /// second round-trip. Null when unknown (unindexed / pre-column rows).
    #[serde(rename = "inputLen", skip_serializing_if = "Option::is_none")]
    pub input_len: Option<i32>,

    /// Revert cause for a failed tx (e.g. "Revert(0x…)" or a named error), from
    /// `tx_result.exit_reason.reason` via [`rome_via_classify::revert_reason`]. Detail-endpoint only.
    #[serde(rename = "revertReason", skip_serializing_if = "Option::is_none")]
    pub revert_reason: Option<String>,
}

/// A single decoded ERC-20 token movement on a transaction.
///
/// `amount_raw` is the on-chain integer (base units) as a decimal string.
/// `amount_display` is `amount_raw` shifted by `decimals` (a human-readable
/// decimal string), computed only when `decimals` is known — `None` otherwise
/// so the UI can fall back to the raw amount. Both are strings: token amounts
/// can exceed `u64`/`u128`, so the decimal shift is big-decimal math, never a
/// native-integer divide.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DecodedTransfer {
    /// ERC-20 token contract address (0x-prefixed hex).
    pub token_address: String,
    /// Token symbol from `token_metadata` (e.g. "wUSDC"). Null if unresolved.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
    /// Token decimals from `token_metadata`. Null if the metadata worker has
    /// not filled it yet — in that case `amount_display` is also null.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decimals: Option<i16>,
    /// Sender of the tokens (0x-prefixed hex).
    pub from: String,
    /// Recipient of the tokens (0x-prefixed hex).
    pub to: String,
    /// Raw transferred amount in base units, as a decimal string.
    pub amount_raw: String,
    /// Human-readable amount (`amount_raw` / 10^decimals) as a decimal string.
    /// Null when `decimals` is unknown.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub amount_display: Option<String>,
}

/// A single EVM event log.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TxLog {
    /// Emitting contract address (0x-prefixed hex).
    pub address: String,
    /// Indexed topics (0x-prefixed hex, topic[0] is the event signature).
    pub topics: Vec<String>,
    /// Non-indexed ABI-encoded data (0x-prefixed hex).
    pub data: String,
}

/// A single hook execution result for a transaction.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TxHookExecution {
    /// Hook program address.
    pub hook_address: String,
    /// Hook kind: "evm_kyc" | "native" | "evm_custom" | ...
    pub hook_kind: String,
    /// Execution result: "pass" | "reject" | "error" | "skipped".
    pub result: String,
    /// Human-readable reason (e.g. revert reason for rejected hooks).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Compute units consumed by the hook.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gas_used: Option<i64>,
    /// Resolved hook contract name (from ERC-20-style `name()`), if available.
    /// Empty string means "we queried and got nothing"; null means "not yet
    /// resolved". UIs should fall back to the short address in either case.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hook_name: Option<String>,
}

/// Transaction execution status.
#[derive(Debug, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TxStatus {
    Success,
    Pending,
    Failed,
}

/// Transaction type classification (depth-aware; stamped by the
/// rome-via-enrich `cross_chain` worker from top-level Solana program sets).
/// - **Rhea** — 1 EVM RLP leg → 1 chain (signed `ecdsa` OR `solana_unsigned`;
///   origination is irrelevant). A cached wrapper's inner SPL CPI fires at
///   depth ≥ 2 under rome-evm, so single-chain DeFi stays Rhea.
/// - **Romulus** — an EVM RLP plus a genuinely-composed native Solana leg
///   (a top-level / depth-1 program that is neither infra nor rome-evm).
/// - **Remus** — ≥ 2 EVM RLP legs across ≥ 2 chains. NOT produced by the
///   single-chain enrich pass (sister-rollup legs are not observable here);
///   detecting it needs cross-chain indexing.
#[derive(Debug, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub enum TxType {
    Rhea,
    Remus,
    Romulus,
}

impl TxType {
    /// String form used by the action classifier (`"Rhea" | "Remus" | "Romulus"`).
    pub fn as_str(&self) -> &'static str {
        match self {
            TxType::Rhea => "Rhea",
            TxType::Remus => "Remus",
            TxType::Romulus => "Romulus",
        }
    }
}

/// Per-block count of txs by Rome classification. Embedded in `Block`.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct TypeCounts {
    pub rhea: i64,
    pub remus: i64,
    pub romulus: i64,
}

/// Chain-wide per-kind token counts. Embedded in `StatsOverview` so the Tokens
/// header can render "Wrapped SPL / ERC-20 / Token-2022" real numbers instead of
/// a lone always-0 tile. Counted from `token_metadata.kind` (`COUNT(*) FILTER`)
/// over the whole chain. `kind` is nullable — NULL (unclassified) counts toward
/// none of the three buckets (that's correct: it isn't classified yet).
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct TokenKindCounts {
    /// Plain ERC-20 tokens (`kind = 'ERC-20'`).
    pub erc20: i64,
    /// SPL-backed cached wrappers (`kind = 'SPL'`) — relabeled "Wrapped SPL" in UI.
    pub spl: i64,
    /// Token-2022 wrappers (`kind = 'Token-2022'`).
    pub token2022: i64,
}

/// Chain-wide address-type counts. Embedded in `StatsOverview` so the Addresses
/// header can render "Contracts / EOAs / Synthetics" chain-wide instead of
/// filtering the 50-row page. Classification priority mirrors the per-row UI
/// (synthetic > contract > EOA): `synthetics` = DISTINCT Solana-controlled
/// `from` addresses on `evm_tx`; `contracts` = `address_stats.is_contract`;
/// `eoas` = active addresses − contracts − synthetics, clamped ≥ 0.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct AddressTypeCounts {
    /// Smart-contract addresses (`address_stats.is_contract = true`).
    pub contracts: i64,
    /// Externally-owned accounts (active addresses − contracts − synthetics, ≥ 0).
    pub eoas: i64,
    /// Solana-controlled synthetic addresses (originate `origination <> 'ecdsa'` txs).
    pub synthetics: i64,
}

/// Overview statistics for the explorer home page.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct StatsOverview {
    /// Latest confirmed EVM block number.
    pub latest_block_number: i64,

    /// Latest confirmed Solana slot number.
    pub latest_slot: i64,

    /// Highest slot the SOURCE indexer has written, as last observed by
    /// rome-via-sync. `latest_slot` is how far THIS explorer has got; the gap
    /// between them is the sync lag.
    ///
    /// `None` when the sync has not recorded it yet (fresh DB, or a deploy older
    /// than migration 0017). Consumers must treat absence as "lag unknown" and
    /// stay silent rather than claim to be live — an explorer that presents
    /// itself as caught up while hours behind looks broken, not honest.
    pub source_max_slot: Option<i64>,

    /// Total number of indexed transactions.
    pub total_txs: i64,

    /// Estimated transactions per second over the last 60 seconds.
    /// Returns 0.0 if fewer than 2 blocks in the window.
    pub tps_60s_estimate: f64,

    /// Chain-wide total indexed EVM transactions (`COUNT(*)` over `evm_tx`).
    /// Distinct from `total_txs` (which carries the same value today) — exposed
    /// as an explicit chain-wide aggregate the Home dashboard can trust instead
    /// of deriving from a truncated tx page. Defaults to 0 on an empty chain.
    pub tx_count_total: i64,

    /// Number of distinct addresses with on-chain activity (`COUNT(*)` over
    /// `address_stats`). Powers the "active addresses" tile. 0 on empty chain.
    pub active_addresses: i64,

    /// Chain-wide total of indexed token contracts (`COUNT(*)` over
    /// `token_metadata`). Powers the Tokens-page header count (which previously
    /// counted only the current page). 0 on empty chain.
    pub token_count_total: i64,


    /// Chain-wide per-kind token counts (Wrapped SPL / ERC-20 / Token-2022),
    /// from `token_metadata.kind`. Powers the Tokens-page header breakdown
    /// (previously a lone always-0 "Token-2022" tile). All zero on an empty
    /// chain; NULL (unclassified) kinds count toward none.
    pub token_kind_counts: TokenKindCounts,

    /// Chain-wide address-type counts (Contracts / EOAs / Synthetics). Powers
    /// the Addresses-page header breakdown (previously page-derived over 50
    /// rows). All zero on an empty chain.
    pub address_type_counts: AddressTypeCounts,
}

/// Cursor-paginated response wrapper.
///
/// `next_cursor` is an opaque HMAC-signed token. Pass it as `?cursor=` on the next request.
/// When `null`, there are no more pages.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Page<T: ToSchema + 'static> {
    /// The items on this page.
    pub items: Vec<T>,

    /// Cursor token for the next page (null if this is the last page).
    pub next_cursor: Option<String>,

    /// Total count is not returned (unbounded datasets). Use stats endpoint for totals.
    pub has_more: bool,
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 3: Token / Address / Search DTOs
// ─────────────────────────────────────────────────────────────────────────────

/// Summary of a token for the token list page.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TokenSummary {
    /// Token contract address (0x-prefixed hex).
    pub address: String,
    /// Token symbol (e.g. "USDC").
    pub symbol: Option<String>,
    /// Token name (e.g. "USD Coin").
    pub name: Option<String>,
    /// Token standard ("ERC-20" | "SPL" | "Token-2022"). Nullable: the source
    /// column is NULL for a freshly-discovered, not-yet-classified token (the
    /// "unclassified, needs probing" sentinel — see migration 0213). Modeled as
    /// Option so a NULL doesn't fail decode; omitted from JSON when None (no
    /// fabricated 'ERC-20').
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// Number of current holders (balance > 0).
    pub holders: i64,
    /// Total supply (string-encoded NUMERIC — can exceed JS safe integer).
    pub supply: Option<String>,
    /// Decimal places (`token_metadata.decimals` SMALLINT). Lets the list page
    /// render a decimated amount instead of a raw integer. Null when the
    /// metadata worker has not filled it yet — omitted from JSON when None
    /// (matches `TokenDetail.decimals` typing).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decimals: Option<i16>,
    /// Derived circulating supply for wrapper kinds (`SPL` / `Token-2022`):
    /// `SUM(token_holders.balance) WHERE balance > 0` — the SAME decimal-agnostic
    /// value the detail endpoint returns (raw `supply` is 18-vs-6 misaligned or
    /// literally 0 for cached wrappers). `None` for plain ERC-20s (raw `supply`
    /// is self-consistent) — string-encoded NUMERIC, may exceed JS safe-integer.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub circulating_supply: Option<String>,
    /// True when transfers are currently gated by an on-chain WhitelistRestrictions
    /// module (`transfersAllowed() == false`) — derived by the enrich metadata
    /// worker from chain state, not curated. Null until first probed; omitted from
    /// JSON when None.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gated: Option<bool>,
    /// The wired WhitelistRestrictions module address (0x-hex), when the token has
    /// a per-token allowlist gate. Null for ungated tokens.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restriction_module: Option<String>,
}

/// Full token detail for the token detail page.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TokenDetail {
    /// Token contract address (0x-prefixed hex).
    pub address: String,
    /// Token symbol.
    pub symbol: Option<String>,
    /// Token name.
    pub name: Option<String>,
    /// Token standard ("ERC-20" | "SPL" | "Token-2022"). Nullable — NULL is the
    /// not-yet-classified sentinel (migration 0213). Option so NULL decodes
    /// cleanly; omitted from JSON when None (no fabricated 'ERC-20').
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// Number of current holders.
    pub holders: i64,
    /// Raw on-chain `total_supply()` as a decimal string. For plain ERC-20s
    /// this is self-consistent with balances. For cached SPL_ERC20 / Token-2022
    /// wrappers it is decimal-misaligned (18-dec-scaled against 6-dec balances)
    /// or literally 0 (e.g. wSOL with live holders), so for those kinds the UI
    /// should prefer `circulating_supply` below for display + share math; this
    /// raw value is kept as a secondary / tooltip value.
    pub supply: Option<String>,
    /// Derived circulating supply for wrapper kinds (`SPL` / `Token-2022`):
    /// `SUM(token_holders.balance) WHERE balance > 0`. Decimal-agnostic (same
    /// base-unit scale as the per-holder balances), never divides by a
    /// mismatched / zero on-chain `total_supply`, and is consistent with
    /// holder `share` by construction. `None` for plain ERC-20s (whose raw
    /// `supply` is already self-consistent) — string-encoded NUMERIC, may
    /// exceed JS safe-integer range.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub circulating_supply: Option<String>,
    /// Decimal places.
    pub decimals: Option<i16>,
    /// Last metadata update (ISO-8601 UTC).
    pub updated_at: String,

    // ── Provenance (the trust story) ────────────────────────────────────────
    // The UI derives the trust tier from these + its own config
    // (verifiedTokens registry, canonicalFactories set): NOT derived server-side.
    /// Underlying Solana SPL mint this token wraps (base58 pubkey). Present only
    /// for SPL / Token-2022 wrappers (`mint_id()` returns it); null for plain
    /// ERC-20s, which have no underlying mint.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mint: Option<String>,
    /// The `ERC20SPLFactory` that created this token (0x-prefixed hex), from the
    /// `TokenCreated` event emitter. Null if the token was discovered via a
    /// Transfer log and never matched to a creation event.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub factory: Option<String>,
    /// The account that created this token (0x-prefixed hex), from the
    /// `TokenCreated` event's indexed `creator`. Null if unknown.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub creator: Option<String>,

    // ── Gated-RWA (on-chain-derived) ────────────────────────────────────────
    /// True when transfers are currently gated by an on-chain WhitelistRestrictions
    /// module (`transfersAllowed() == false`) — derived by the enrich metadata
    /// worker from chain state, not curated. Null until first probed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gated: Option<bool>,
    /// The wired WhitelistRestrictions module address (0x-hex), when present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restriction_module: Option<String>,
}

/// A single token holder.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TokenHolder {
    /// Holder address (0x-prefixed hex).
    pub address: String,
    /// Token balance (string-encoded NUMERIC).
    pub balance: String,
    /// Share of total supply (0.0–1.0), if computable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub share: Option<f64>,
}

/// A single ERC-20 Transfer log event.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TokenTransfer {
    /// Transaction hash.
    pub tx_hash: String,
    /// Log index within the transaction.
    pub log_index: i32,
    /// Token contract address.
    pub token_address: String,
    /// Sender address.
    pub from: String,
    /// Recipient address.
    pub to: String,
    /// Amount transferred (string-encoded NUMERIC).
    pub amount: String,
    /// Block number.
    pub block_number: i64,
    /// Block timestamp (ISO-8601 UTC), if available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
}

/// One gate-change event in a gated (Arc-style) token's history.
///
/// Two kinds, distinguished by `eventType`:
/// * `module_wired` — the token attached its TRANSFER_RESTRICTION module
///   (`SpecificRestrictionModuleSet`); `transfersAllowed` is null (no gate state
///   in the event itself).
/// * `transfers_toggled` — the module's gate was opened/closed
///   (`TransfersRestrictionToggled`); `transfersAllowed` carries the new value
///   (false = restricted/gated, true = open).
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GateEvent {
    /// `module_wired` | `transfers_toggled`.
    pub event_type: String,
    /// The WhitelistRestrictions module address (0x).
    pub module_address: String,
    /// The new transfers-allowed value for a toggle (false = gated); null for a
    /// module-wired event.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transfers_allowed: Option<bool>,
    /// Solana slot the event landed in (monotonic ordering key).
    pub slot_number: i64,
    /// Transaction hash the event was emitted in.
    pub tx_hash: String,
    /// Log index within the transaction.
    pub log_index: i32,
    /// Block timestamp (ISO-8601 UTC), if the block is indexed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
}

/// Address detail for the address detail page.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AddressDetail {
    /// The EVM address (0x-prefixed hex).
    pub address: String,
    /// ETH balance in Wei (string-encoded bigint, via eth_getBalance).
    pub balance: String,
    /// Total transaction count (from address_stats).
    pub tx_count: i64,
    /// Whether this address is a smart contract.
    pub is_contract: bool,
    /// Code hash (0x-prefixed hex), if is_contract = true.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code_hash: Option<String>,
    /// First transaction timestamp (ISO-8601 UTC).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_seen: Option<String>,
    /// Most recent transaction timestamp (ISO-8601 UTC).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_seen: Option<String>,
    /// True if this synthetic address is controlled by a Solana account (i.e. it
    /// originates Solana-native txs). Derived directly from `evm_tx` — the
    /// indexer-populated `origination`/`solana_signer` columns ARE the reverse map.
    pub controlled_by_solana: bool,
    /// The controlling Solana pubkey (base58), if controlledBySolana.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub solana_pubkey: Option<String>,

    /// Clean protocol/token label for this address when it is a known contract
    /// (e.g. "Compound", "Aave V3", or a token symbol), resolved from
    /// `rome_via.contract_labels.display_label`. Same source the address tx-list
    /// uses for `Tx::to_label`. Null when the address has no resolved label.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,

    /// Optional secondary detail for the contract label (e.g. "cWETHv3"), from
    /// `rome_via.contract_labels.display_label_detail`. Null when absent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label_detail: Option<String>,

    /// Deployer address (0x) for a directly-deployed contract, from the
    /// creation tx's `from` (contract_creation worker). Null for EOAs,
    /// factory-deployed contracts, and not-yet-backfilled rows.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub creator: Option<String>,

    /// Creation transaction hash (0x) for a directly-deployed contract. Null in
    /// the same cases as `creator`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub creation_tx: Option<String>,

    /// Server-classified address kind (`burn` | `precompile_rome` |
    /// `precompile_eth` | `token` | `contract` | `sol_account` | `eoa`).
    /// `factory` is a client-side refinement of `contract` (deploy-time
    /// config), deliberately not emitted here. See `api::address_kind`.
    pub kind: crate::api::address_kind::AddressKind,
}

/// One token position of an address (GET /addresses/:address/tokens).
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AddressTokenHolding {
    /// Token contract address (0x-prefixed hex, lowercased).
    pub token_address: String,
    /// Token symbol from token_metadata.
    pub symbol: String,
    /// Token name from token_metadata.
    pub name: String,
    /// Token standard ("ERC-20" | "SPL" | "Token-2022"); null while unclassified.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// Token decimals.
    pub decimals: i32,
    /// Current balance in base units (string-encoded bigint).
    pub balance: String,
}

/// A single search result hit.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SearchHit {
    /// Entity type: "tx" | "block" | "address" | "token".
    pub entity_type: String,
    /// Entity identifier (hash / block number / address).
    pub entity_id: String,
    /// Human-readable label shown in the search dropdown.
    pub display_label: String,
    /// UI navigation URL for this entity.
    pub url: String,
    /// Token standard ("ERC-20" | "SPL" | "Token-2022") for `token`-type hits,
    /// resolved from `token_metadata.kind`. Lets the dropdown relabel kind (e.g.
    /// SPL → "Wrapped SPL"). `None` for non-token hits (and omitted from JSON).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
}

/// Search results response.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SearchResults {
    /// Up to 20 top results ordered by (similarity × weight).
    pub results: Vec<SearchHit>,
    /// True if the result set was truncated due to a timeout or partial match.
    pub partial: bool,
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 4: Cross-chain + Hooks DTOs
// ─────────────────────────────────────────────────────────────────────────────

/// An EVM leg of a cross-chain tx.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct EvmLeg {
    pub chain_id: i64,
    pub block_number: Option<i64>,
    pub tx_hash: String,
}

/// A Solana leg of a cross-chain (Romulus) tx.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SolanaLeg {
    pub sol_chain: String,       // "mainnet" | "devnet"
    pub sol_signature: String,   // base58 Solana tx signature
}

/// One Solana "leg" of an EVM transaction. Iterative txs execute across many
/// Solana txs; this carries the intra-block ordinal (`tx_idx`, `instr_idx`) so
/// the full set is presentable in execution order (slot_number, tx_idx,
/// instr_idx) across AND within Solana blocks. Mirrored from hercules's
/// `evm_tx_sol_tx` (rome-sdk #454) by rome-via-sync.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SolTxLeg {
    pub sol_signature: String,
    pub slot_number: i64,
    pub tx_idx: i32,
    pub instr_idx: i32,
}

/// Cross-chain correlation record.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CrossChainCorrelation {
    /// The primary EVM tx hash (on this chain).
    pub rome_tx_hash: String,
    /// Classification: "Rhea" | "Remus" | "Romulus".
    pub rome_tx_type: String,
    /// EVM legs (always includes the tx itself as the first leg).
    pub evm_legs: Vec<EvmLeg>,
    /// Solana legs (empty for Rhea/Remus).
    pub solana_legs: Vec<SolanaLeg>,
    /// Block timestamp (ISO-8601 UTC), if available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
}

/// A hook registration entry from hooks_registry.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct HookEntry {
    /// Token contract address (0x-prefixed hex).
    pub token_address: String,
    /// Hook program address (Solana pubkey or EVM address).
    pub hook_address: String,
    /// Hook kind: "native" | "evm_kyc" | "evm_custom" | ...
    pub hook_kind: String,
    /// When the hook was registered (ISO-8601 UTC).
    pub registered_at: String,
    /// Registration tx hash (0x-prefixed hex), if known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub registered_tx_hash: Option<String>,
    /// Contract name resolved via eth_call `name()`, if available. Empty
    /// string = queried but unnamed; null = not yet resolved.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

/// Tx response wrapped with a `source` field (for cross-chain RPC fallback).
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TxWithSource {
    #[serde(flatten)]
    pub tx: Tx,
    /// "indexed" — served from local DB; "live" — fetched from foreign Proxy.
    pub source: String,
}

/// A Meta-Hook Router invocation (a Solana Token-2022 transfer whose mint
/// has the rome meta-hook router attached as a transfer hook).
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MetaHookInvocation {
    /// Solana transaction signature (base58).
    pub sol_signature: String,
    /// Slot containing the invocation.
    pub slot_number: i64,
    /// Block time (unix epoch seconds), if reported by the validator.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_time: Option<i64>,
    /// Mint address parsed from the router's anchor log (base58).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mint: Option<String>,
    /// Number of hooks the router declared it would execute.
    pub hook_count: i32,
    /// Overall tx outcome: "success" | "reject" | "error" | "unknown".
    pub outcome: String,
    /// Fee-payer pubkey (first account key).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fee_payer: Option<String>,
}

/// Meta-Hook Router invocation detail: the invocation row plus the joined
/// per-hook execution outcomes.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MetaHookInvocationDetail {
    #[serde(flatten)]
    pub invocation: MetaHookInvocation,
    /// Executions observed from the router's per-hook log entries.
    pub hook_executions: Vec<TxHookExecution>,
}

/// Marker re-export so utoipa can generate schema for ProblemJson.
pub use crate::error::ProblemJson as ErrorResponse;

// ── /throughput/* response shapes ────────────────────────────────────────
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CurrentTps {
    pub window_seconds: i64,
    pub total_tps: f64,
    pub application_tps: f64,
    pub total_txs: i64,
    pub oracle_txs: i64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PeakWindowJson {
    pub from_block: i64,
    pub to_block: i64,
    pub blocks: i64,
    pub total_txs: i64,
    pub elapsed_seconds: i64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PeakTps {
    pub tps: f64,
    pub window: Option<PeakWindowJson>,
    pub min_elapsed_seconds: i64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TopBlock {
    pub number: i64,
    pub slot: i64,
    pub tx_count: i64,
    pub gas_used: String,
    pub timestamp: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TimeseriesPoint {
    pub t: i64,
    pub txs: i64,
    pub tps: f64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct HistogramBucket {
    pub label: String,
    pub blocks: i64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Histogram {
    pub buckets: Vec<HistogramBucket>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Cadence {
    pub total_blocks: i64,
    pub filled_blocks: i64,
    pub slots_filled_pct: f64,
    pub slots_per_sec: f64,
    pub avg_slot_ms: f64,
}

/// One entry in the persisted all-time top-10 sustained-TPS windows.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TopWindowRow {
    pub rank: i32,
    pub from_block: i64,
    pub to_block: i64,
    pub from_slot: i64,
    pub to_slot: i64,
    pub elapsed_seconds: i64,
    pub total_txs: i64,
    pub app_txs: i64,
    pub total_tps: f64,
    pub app_tps: f64,
}

/// One entry in the persisted all-time top-10 busiest blocks.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TopBlockRow {
    pub rank: i32,
    pub block_number: i64,
    pub slot_number: i64,
    pub total_txs: i64,
    pub app_txs: i64,
    pub block_timestamp: i64,
}

/// Persisted all-time throughput record: top-10 sustained-TPS windows + top-10 busiest blocks.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ThroughputRecord {
    pub peak_windows: Vec<TopWindowRow>,
    pub busiest_blocks: Vec<TopBlockRow>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_serializes_camel_case() {
        let b = Block {
            number: 100,
            slot: 999,
            slot_block_idx: 0,
            hash: Some("0xabc".to_string()),
            parent_hash: None,
            tx_count: 5,
            gas_used: "21000".to_string(),
            gas_limit_sum: None,
            type_counts: None,
            gas_recipient: None,
            timestamp: Some("2026-04-13T00:00:00Z".to_string()),
        };
        let json = serde_json::to_string(&b).unwrap();
        assert!(json.contains("\"number\":100"), "block number should be JSON number");
        assert!(json.contains("\"slotBlockIdx\":0"), "should use camelCase");
        assert!(json.contains("\"gasUsed\":\"21000\""), "gasUsed should be string");
        assert!(json.contains("\"txCount\":5"), "txCount camelCase");
        assert!(!json.contains("parentHash"), "null parentHash should be omitted");
    }

    #[test]
    fn tx_status_serializes_lowercase() {
        let s = serde_json::to_string(&TxStatus::Success).unwrap();
        assert_eq!(s, "\"success\"");
        let s2 = serde_json::to_string(&TxStatus::Pending).unwrap();
        assert_eq!(s2, "\"pending\"");
        let s3 = serde_json::to_string(&TxStatus::Failed).unwrap();
        assert_eq!(s3, "\"failed\"");
    }

    #[test]
    fn tx_type_serializes() {
        let t = serde_json::to_string(&TxType::Rhea).unwrap();
        assert_eq!(t, "\"Rhea\"");
    }

    #[test]
    fn page_serializes_correctly() {
        let p: Page<Block> = Page {
            items: vec![],
            next_cursor: Some("tok123".to_string()),
            has_more: true,
        };
        let json = serde_json::to_string(&p).unwrap();
        assert!(json.contains("\"nextCursor\":\"tok123\""));
        assert!(json.contains("\"hasMore\":true"));
    }

    #[test]
    fn tx_origination_fields_serialize_camel_case() {
        // Build a minimal Tx with origination="solana_unsigned" and a solanaSigner.
        let tx = Tx {
            hash: "0xabc".to_string(),
            status: TxStatus::Success,
            tx_type: TxType::Rhea,
            method: "0x".to_string(),
            from: "0x1234".to_string(),
            to: None,
            value: "0".to_string(),
            gas_used: None,
            gas_price: None,
            gas_recipient: None,
            priority_fee: None,
            base: None,
            gas_limit: None,
            tx_type_byte: None,
            timestamp: None,
            block_number: None,
            hook_executions: vec![],
            logs: vec![],
            logs_count: None,
            action_tags: vec![],
            solana_legs: vec![],
            origination: "solana_unsigned".to_string(),
            solana_signer: Some("9wJGxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx".to_string()),
            to_label: None,
            to_label_detail: None,
            transfers: vec![],
            solana_settlement_sig: None,
            cpi_program: None,
            cpi_program_label: None,
            cpi_instruction: None,
            contract_address: None,
            input_len: None,
            revert_reason: None,
        };
        let json = serde_json::to_string(&tx).unwrap();
        assert!(
            json.contains("\"origination\":\"solana_unsigned\""),
            "origination field should be present as camelCase key"
        );
        assert!(
            json.contains("\"solanaSigner\":\"9wJG"),
            "solanaSigner should serialize with camelCase key"
        );
    }

    #[test]
    fn tx_ecdsa_omits_solana_signer() {
        let tx = Tx {
            hash: "0xdef".to_string(),
            status: TxStatus::Success,
            tx_type: TxType::Rhea,
            method: "0x".to_string(),
            from: "0x5678".to_string(),
            to: None,
            value: "0".to_string(),
            gas_used: None,
            gas_price: None,
            gas_recipient: None,
            priority_fee: None,
            base: None,
            gas_limit: None,
            tx_type_byte: None,
            timestamp: None,
            block_number: None,
            hook_executions: vec![],
            logs: vec![],
            logs_count: None,
            action_tags: vec![],
            solana_legs: vec![],
            origination: "ecdsa".to_string(),
            solana_signer: None,
            to_label: None,
            to_label_detail: None,
            transfers: vec![],
            solana_settlement_sig: None,
            cpi_program: None,
            cpi_program_label: None,
            cpi_instruction: None,
            contract_address: None,
            input_len: None,
            revert_reason: None,
        };
        let json = serde_json::to_string(&tx).unwrap();
        assert!(json.contains("\"origination\":\"ecdsa\""));
        assert!(!json.contains("solanaSigner"), "solanaSigner should be omitted when None");
    }

    #[test]
    fn tx_contract_label_serializes_camel_case() {
        // A tx whose `to` contract has a resolved protocol label should surface
        // `toLabel` / `toLabelDetail` (camelCase) on the JSON response.
        let tx = Tx {
            hash: "0xfeed".to_string(),
            status: TxStatus::Success,
            tx_type: TxType::Rhea,
            method: "supply(address,uint256)".to_string(),
            from: "0x1111".to_string(),
            to: Some("0x771d2f00".to_string()),
            value: "0".to_string(),
            gas_used: None,
            gas_price: None,
            gas_recipient: None,
            priority_fee: None,
            base: None,
            gas_limit: None,
            tx_type_byte: None,
            timestamp: None,
            block_number: None,
            hook_executions: vec![],
            logs: vec![],
            logs_count: None,
            action_tags: vec![],
            solana_legs: vec![],
            origination: "ecdsa".to_string(),
            solana_signer: None,
            to_label: Some("Compound".to_string()),
            to_label_detail: Some("cwUSDC-9".to_string()),
            transfers: vec![],
            solana_settlement_sig: None,
            cpi_program: None,
            cpi_program_label: None,
            cpi_instruction: None,
            contract_address: None,
            input_len: None,
            revert_reason: None,
        };
        let json = serde_json::to_string(&tx).unwrap();
        assert!(
            json.contains("\"toLabel\":\"Compound\""),
            "toLabel should serialize with camelCase key, got: {json}"
        );
        assert!(
            json.contains("\"toLabelDetail\":\"cwUSDC-9\""),
            "toLabelDetail should serialize with camelCase key, got: {json}"
        );
    }

    #[test]
    fn tx_decoded_transfers_serialize_camel_case() {
        // A tx with a decoded ERC-20 Transfer should surface a `transfers` array,
        // each item camelCase: tokenAddress / amountRaw / amountDisplay.
        let tx = Tx {
            hash: "0xc0de".to_string(),
            status: TxStatus::Success,
            tx_type: TxType::Rhea,
            method: "withdraw(address,uint256)".to_string(),
            from: "0x1111".to_string(),
            to: Some("0x771d2f00".to_string()),
            value: "0".to_string(),
            gas_used: None,
            gas_price: None,
            gas_recipient: None,
            priority_fee: None,
            base: None,
            gas_limit: None,
            tx_type_byte: None,
            timestamp: None,
            block_number: None,
            hook_executions: vec![],
            logs: vec![],
            logs_count: None,
            action_tags: vec![],
            solana_legs: vec![],
            origination: "ecdsa".to_string(),
            solana_signer: None,
            to_label: None,
            to_label_detail: None,
            transfers: vec![DecodedTransfer {
                token_address: "0x9a8b4cb7".to_string(),
                symbol: Some("wUSDC".to_string()),
                decimals: Some(6),
                from: "0x771d2f00".to_string(),
                to: "0x1111".to_string(),
                amount_raw: "1999999".to_string(),
                amount_display: Some("1.999999".to_string()),
            }],
            solana_settlement_sig: None,
            cpi_program: None,
            cpi_program_label: None,
            cpi_instruction: None,
            contract_address: None,
            input_len: None,
            revert_reason: None,
        };
        let json = serde_json::to_string(&tx).unwrap();
        assert!(
            json.contains("\"transfers\":["),
            "transfers array should be present, got: {json}"
        );
        assert!(
            json.contains("\"tokenAddress\":\"0x9a8b4cb7\""),
            "tokenAddress should serialize camelCase, got: {json}"
        );
        assert!(
            json.contains("\"amountRaw\":\"1999999\""),
            "amountRaw should serialize camelCase as a string, got: {json}"
        );
        assert!(
            json.contains("\"amountDisplay\":\"1.999999\""),
            "amountDisplay should serialize camelCase, got: {json}"
        );
        assert!(
            json.contains("\"symbol\":\"wUSDC\""),
            "symbol should be present, got: {json}"
        );
        assert!(
            json.contains("\"decimals\":6"),
            "decimals should be a JSON number, got: {json}"
        );
    }

    #[test]
    fn tx_empty_transfers_omits_key() {
        // No decoded transfers → the `transfers` key is omitted entirely.
        let tx = Tx {
            hash: "0xfade".to_string(),
            status: TxStatus::Success,
            tx_type: TxType::Rhea,
            method: "0x".to_string(),
            from: "0x2222".to_string(),
            to: None,
            value: "0".to_string(),
            gas_used: None,
            gas_price: None,
            gas_recipient: None,
            priority_fee: None,
            base: None,
            gas_limit: None,
            tx_type_byte: None,
            timestamp: None,
            block_number: None,
            hook_executions: vec![],
            logs: vec![],
            logs_count: None,
            action_tags: vec![],
            solana_legs: vec![],
            origination: "ecdsa".to_string(),
            solana_signer: None,
            to_label: None,
            to_label_detail: None,
            transfers: vec![],
            solana_settlement_sig: None,
            cpi_program: None,
            cpi_program_label: None,
            cpi_instruction: None,
            contract_address: None,
            input_len: None,
            revert_reason: None,
        };
        let json = serde_json::to_string(&tx).unwrap();
        assert!(
            !json.contains("transfers"),
            "transfers should be omitted when empty, got: {json}"
        );
    }

    #[test]
    fn decoded_transfer_unknown_decimals_omits_amount_display() {
        // When decimals is unknown, amountDisplay is None and must be omitted,
        // but amountRaw (the raw integer string) is always present.
        let t = DecodedTransfer {
            token_address: "0xabc".to_string(),
            symbol: None,
            decimals: None,
            from: "0x1".to_string(),
            to: "0x2".to_string(),
            amount_raw: "12345".to_string(),
            amount_display: None,
        };
        let json = serde_json::to_string(&t).unwrap();
        assert!(
            json.contains("\"amountRaw\":\"12345\""),
            "amountRaw should always be present, got: {json}"
        );
        assert!(
            !json.contains("amountDisplay"),
            "amountDisplay should be omitted when None, got: {json}"
        );
        assert!(
            !json.contains("symbol"),
            "symbol should be omitted when None, got: {json}"
        );
        assert!(
            !json.contains("decimals"),
            "decimals should be omitted when None, got: {json}"
        );
    }

    #[test]
    fn tx_without_contract_label_omits_keys() {
        // A tx whose `to` contract has no label row must omit both keys entirely.
        let tx = Tx {
            hash: "0xbeef".to_string(),
            status: TxStatus::Success,
            tx_type: TxType::Rhea,
            method: "0x".to_string(),
            from: "0x2222".to_string(),
            to: Some("0xdeadbeef".to_string()),
            value: "0".to_string(),
            gas_used: None,
            gas_price: None,
            gas_recipient: None,
            priority_fee: None,
            base: None,
            gas_limit: None,
            tx_type_byte: None,
            timestamp: None,
            block_number: None,
            hook_executions: vec![],
            logs: vec![],
            logs_count: None,
            action_tags: vec![],
            solana_legs: vec![],
            origination: "ecdsa".to_string(),
            solana_signer: None,
            to_label: None,
            to_label_detail: None,
            transfers: vec![],
            solana_settlement_sig: None,
            cpi_program: None,
            cpi_program_label: None,
            cpi_instruction: None,
            contract_address: None,
            input_len: None,
            revert_reason: None,
        };
        let json = serde_json::to_string(&tx).unwrap();
        assert!(
            !json.contains("toLabel"),
            "toLabel should be omitted when None, got: {json}"
        );
        assert!(
            !json.contains("toLabelDetail"),
            "toLabelDetail should be omitted when None, got: {json}"
        );
    }

    #[test]
    fn token_detail_provenance_serializes_camel_case() {
        // A fully-resolved wrapper carries its underlying Solana mint (base58),
        // the factory that created it, and the creator — all camelCase keys.
        let td = TokenDetail {
            address: "0x9a8b4cb7326033d72ca393c6b4c0d7fb904fa900".to_string(),
            symbol: Some("wUSDC".to_string()),
            name: Some("Wrapped USDC".to_string()),
            kind: Some("SPL".to_string()),
            holders: 3,
            supply: Some("1000000".to_string()),
            circulating_supply: None,
            decimals: Some(6),
            updated_at: "2026-06-04T00:00:00+00:00".to_string(),
            mint: Some("4zMMC9srt5Ri5X14GAgXhaHii3GnPAEERYPJgZJDncDU".to_string()),
            factory: Some("0x86149124d74ebb3aa41a19641b700e88202b6285".to_string()),
            creator: Some("0x1f4946be340f06c46a50e65084790968abcc48f6".to_string()),
            gated: None,
            restriction_module: None,
        };
        let json = serde_json::to_string(&td).unwrap();
        assert!(
            json.contains("\"mint\":\"4zMMC9srt5Ri5X14GAgXhaHii3GnPAEERYPJgZJDncDU\""),
            "mint (base58) should serialize as camelCase key, got: {json}"
        );
        assert!(
            json.contains("\"factory\":\"0x86149124d74ebb3aa41a19641b700e88202b6285\""),
            "factory should serialize as camelCase key, got: {json}"
        );
        assert!(
            json.contains("\"creator\":\"0x1f4946be340f06c46a50e65084790968abcc48f6\""),
            "creator should serialize as camelCase key, got: {json}"
        );
    }

    #[test]
    fn token_detail_provenance_omits_when_none() {
        // A plain ERC-20 (no underlying mint, never seen via TokenCreated) carries
        // none of the provenance fields — all three keys are omitted entirely.
        let td = TokenDetail {
            address: "0xdeadbeef".to_string(),
            symbol: Some("LP".to_string()),
            name: Some("Uniswap V2".to_string()),
            kind: Some("ERC-20".to_string()),
            holders: 0,
            supply: None,
            circulating_supply: None,
            decimals: Some(18),
            updated_at: "2026-06-04T00:00:00+00:00".to_string(),
            mint: None,
            factory: None,
            creator: None,
            gated: None,
            restriction_module: None,
        };
        let json = serde_json::to_string(&td).unwrap();
        assert!(!json.contains("mint"), "mint should be omitted when None, got: {json}");
        assert!(!json.contains("factory"), "factory should be omitted when None, got: {json}");
        assert!(!json.contains("creator"), "creator should be omitted when None, got: {json}");
    }

    // ── circulating supply for wrapper kinds ────────────────────────────────
    // A cached SPL wrapper's raw on-chain total_supply is decimal-misaligned or
    // 0, so the detail endpoint derives `circulating_supply` = SUM(holder
    // balances) for SPL / Token-2022 kinds. Present (camelCase) for wrappers;
    // omitted entirely for plain ERC-20s (whose raw supply is self-consistent).
    #[test]
    fn token_detail_circulating_supply_some_emits_camel_case() {
        let td = TokenDetail {
            address: "0x9a8b4cb7326033d72ca393c6b4c0d7fb904fa900".to_string(),
            symbol: Some("wSOL".to_string()),
            name: Some("Wrapped SOL".to_string()),
            kind: Some("SPL".to_string()),
            holders: 5,
            // Raw on-chain supply is 0 (the bug) — kept as the secondary value.
            supply: Some("0".to_string()),
            // Derived from SUM(holder balances) — the real circulating amount.
            circulating_supply: Some("123456789".to_string()),
            decimals: Some(9),
            updated_at: "2026-06-04T00:00:00+00:00".to_string(),
            mint: Some("So11111111111111111111111111111111111111112".to_string()),
            factory: None,
            creator: None,
            gated: None,
            restriction_module: None,
        };
        let json = serde_json::to_string(&td).unwrap();
        assert!(
            json.contains("\"circulatingSupply\":\"123456789\""),
            "circulating_supply=Some should serialize as camelCase key, got: {json}"
        );
        // Raw supply is still present as the secondary value.
        assert!(
            json.contains("\"supply\":\"0\""),
            "raw supply should still serialize, got: {json}"
        );
    }

    #[test]
    fn token_detail_circulating_supply_none_omits() {
        // Plain ERC-20: raw `supply` is self-consistent, no derived circulating
        // supply — the key is omitted entirely.
        let td = TokenDetail {
            address: "0xdeadbeef".to_string(),
            symbol: Some("LP".to_string()),
            name: Some("Uniswap V2".to_string()),
            kind: Some("ERC-20".to_string()),
            holders: 2,
            supply: Some("1000000000000000000".to_string()),
            circulating_supply: None,
            decimals: Some(18),
            updated_at: "2026-06-04T00:00:00+00:00".to_string(),
            mint: None,
            factory: None,
            creator: None,
            gated: None,
            restriction_module: None,
        };
        let json = serde_json::to_string(&td).unwrap();
        assert!(
            !json.contains("circulatingSupply"),
            "circulating_supply=None should be omitted entirely, got: {json}"
        );
    }

    #[test]
    fn problem_json_serializes_type_field() {
        let p = ErrorResponse {
            problem_type: "https://example.com/errors/not-found".to_string(),
            title: "Not Found".to_string(),
            status: 404,
            detail: Some("block 999 not found".to_string()),
            instance: None,
        };
        let json = serde_json::to_string(&p).unwrap();
        assert!(json.contains("\"type\":\"https://example.com/errors/not-found\""));
        assert!(!json.contains("\"instance\""), "null instance should be omitted");
    }

    // ── C1: AddressDetail contract label ────────────────────────────────────
    // The address-detail endpoint resolves the `to` contract's clean label
    // from `rome_via.contract_labels` (same source the address tx-list uses for
    // `toLabel`). A labelled contract (e.g. the Compound Comet) must surface
    // `label` / `labelDetail` as camelCase keys; an unlabelled address omits
    // both entirely. Mirrors the `Tx::to_label` serde contract.
    #[test]
    fn address_detail_label_serializes_camel_case() {
        let d = AddressDetail {
            address: "0x771d2f0e9c2a1d05c5e9c2a1d05c5e9c2a1d05c5".to_string(),
            balance: "0".to_string(),
            tx_count: 42,
            is_contract: true,
            code_hash: None,
            first_seen: None,
            last_seen: None,
            controlled_by_solana: false,
            solana_pubkey: None,
            label: Some("Compound".to_string()),
            label_detail: Some("cWETHv3".to_string()),
            creator: None,
            creation_tx: None,
            kind: crate::api::address_kind::AddressKind::Contract,
        };
        let json = serde_json::to_string(&d).unwrap();
        assert!(
            json.contains("\"label\":\"Compound\""),
            "label should serialize with camelCase key, got: {json}"
        );
        assert!(
            json.contains("\"labelDetail\":\"cWETHv3\""),
            "labelDetail should serialize with camelCase key, got: {json}"
        );
        assert!(
            json.contains("\"kind\":\"contract\""),
            "kind should serialize as its snake_case variant string, got: {json}"
        );
    }

    #[test]
    fn address_detail_without_label_omits_keys() {
        // A plain EOA (no contract_labels row) must omit both keys entirely.
        let d = AddressDetail {
            address: "0xdeadbeef00000000000000000000000000000000".to_string(),
            balance: "0".to_string(),
            tx_count: 1,
            is_contract: false,
            code_hash: None,
            first_seen: None,
            last_seen: None,
            controlled_by_solana: false,
            solana_pubkey: None,
            label: None,
            label_detail: None,
            creator: None,
            creation_tx: None,
            kind: crate::api::address_kind::AddressKind::Eoa,
        };
        let json = serde_json::to_string(&d).unwrap();
        assert!(
            !json.contains("\"label\""),
            "label should be omitted when None, got: {json}"
        );
        assert!(
            !json.contains("labelDetail"),
            "labelDetail should be omitted when None, got: {json}"
        );
    }

    // ── C3: token `kind` is nullable ────────────────────────────────────────
    // The `kind` column is nullable (migration 0213 dropped NOT NULL — NULL is
    // the "unclassified, needs probing" sentinel). The DTO must model it as
    // Option so a freshly-discovered token with kind=NULL doesn't fail decode
    // (latent 500 on the whole /tokens response). Some emits the value; None is
    // omitted (no fabricated 'ERC-20' — that's the audit's explicit forbid).
    #[test]
    fn token_summary_kind_some_emits_none_omits() {
        let classified = TokenSummary {
            address: "0x9a8b4cb7326033d72ca393c6b4c0d7fb904fa900".to_string(),
            symbol: Some("wUSDC".to_string()),
            name: Some("Wrapped USDC".to_string()),
            kind: Some("SPL".to_string()),
            holders: 3,
            supply: Some("1000000".to_string()),
            decimals: Some(6),
            circulating_supply: Some("1000000".to_string()),
            gated: None,
            restriction_module: None,
        };
        let json = serde_json::to_string(&classified).unwrap();
        assert!(
            json.contains("\"kind\":\"SPL\""),
            "kind=Some should serialize the value, got: {json}"
        );

        let unclassified = TokenSummary {
            address: "0xabc0000000000000000000000000000000000000".to_string(),
            symbol: None,
            name: None,
            kind: None,
            holders: 0,
            supply: None,
            decimals: None,
            circulating_supply: None,
            gated: None,
            restriction_module: None,
        };
        let json = serde_json::to_string(&unclassified).unwrap();
        assert!(
            !json.contains("kind"),
            "kind=None should be omitted entirely (no fabricated ERC-20), got: {json}"
        );
    }

    #[test]
    fn token_detail_kind_some_emits_none_omits() {
        let unclassified = TokenDetail {
            address: "0xabc0000000000000000000000000000000000000".to_string(),
            symbol: None,
            name: None,
            kind: None,
            holders: 0,
            supply: None,
            circulating_supply: None,
            decimals: None,
            updated_at: "2026-06-04T00:00:00+00:00".to_string(),
            mint: None,
            factory: None,
            creator: None,
            gated: None,
            restriction_module: None,
        };
        let json = serde_json::to_string(&unclassified).unwrap();
        assert!(
            !json.contains("kind"),
            "kind=None should be omitted entirely, got: {json}"
        );
    }

    // ── C5: settling Solana signature on tx detail ──────────────────────────
    // Every tx maps to its settling Solana signature via
    // `rome_via.evm_tx_sol_tx`; the detail endpoint surfaces it as
    // `solanaSettlementSig` (camelCase). Present when known; omitted when the
    // mapping row is absent (pending sync). Mirrors the `solana_signer` serde
    // contract.
    #[test]
    fn tx_solana_settlement_sig_serializes_camel_case() {
        let tx = Tx {
            hash: "0xabc".to_string(),
            status: TxStatus::Success,
            tx_type: TxType::Rhea,
            method: "0x".to_string(),
            from: "0x1234".to_string(),
            to: None,
            value: "0".to_string(),
            gas_used: None,
            gas_price: None,
            gas_recipient: None,
            priority_fee: None,
            base: None,
            gas_limit: None,
            tx_type_byte: None,
            timestamp: None,
            block_number: None,
            hook_executions: vec![],
            logs: vec![],
            logs_count: None,
            action_tags: vec![],
            solana_legs: vec![],
            origination: "ecdsa".to_string(),
            solana_signer: None,
            to_label: None,
            to_label_detail: None,
            transfers: vec![],
            solana_settlement_sig: Some(
                "5j7s8K9wELXAY4KC9ZPHhaxp7Sq1hHtU3HNEgLbSegCcWf3xZ".to_string(),
            ),
            cpi_program: None,
            cpi_program_label: None,
            cpi_instruction: None,
            contract_address: None,
            input_len: None,
            revert_reason: None,
        };
        let json = serde_json::to_string(&tx).unwrap();
        assert!(
            json.contains("\"solanaSettlementSig\":\"5j7s8K9wELXAY4KC9ZPHhaxp7Sq1hHtU3HNEgLbSegCcWf3xZ\""),
            "solanaSettlementSig should serialize with camelCase key, got: {json}"
        );
    }

    #[test]
    fn tx_without_solana_settlement_sig_omits_key() {
        let tx = Tx {
            hash: "0xdef".to_string(),
            status: TxStatus::Success,
            tx_type: TxType::Rhea,
            method: "0x".to_string(),
            from: "0x5678".to_string(),
            to: None,
            value: "0".to_string(),
            gas_used: None,
            gas_price: None,
            gas_recipient: None,
            priority_fee: None,
            base: None,
            gas_limit: None,
            tx_type_byte: None,
            timestamp: None,
            block_number: None,
            hook_executions: vec![],
            logs: vec![],
            logs_count: None,
            action_tags: vec![],
            solana_legs: vec![],
            origination: "ecdsa".to_string(),
            solana_signer: None,
            to_label: None,
            to_label_detail: None,
            transfers: vec![],
            solana_settlement_sig: None,
            cpi_program: None,
            cpi_program_label: None,
            cpi_instruction: None,
            contract_address: None,
            input_len: None,
            revert_reason: None,
        };
        let json = serde_json::to_string(&tx).unwrap();
        assert!(
            !json.contains("solanaSettlementSig"),
            "solanaSettlementSig should be omitted when None, got: {json}"
        );
    }

    // ── /stats/overview chain-wide aggregates (Fix C7) ───────────────────────
    // The Home dashboard + Tokens header consume these chain-wide totals.
    // Lock the JSON contract: camelCase keys + nested typeCounts object.
    /// The chain-wide Rhea/Remus/Romulus counts cost three whole-table scans of
    /// cross_chain_correlations — 87,627 buffers and 665 ms EACH, measured on
    /// hadrian at 13.4M rows — to produce three integers that nothing renders.
    /// The per-BLOCK counts (bounded by `ccc.block_number = eb.params_number`) are
    /// a different thing and stay; BlockList draws its distribution from those.
    ///
    /// Asserting absence, because re-adding the field is how the scans come back.
    #[test]
    fn stats_overview_does_not_carry_chain_wide_type_counts() {
        let s = StatsOverview {
            latest_block_number: 1,
            latest_slot: 2,
            source_max_slot: Some(3),
            total_txs: 4,
            tps_60s_estimate: 0.0,
            tx_count_total: 4,
            active_addresses: 0,
            token_count_total: 0,
            token_kind_counts: TokenKindCounts::default(),
            address_type_counts: AddressTypeCounts::default(),
        };
        let v = serde_json::to_value(&s).expect("serializes");
        let obj = v.as_object().expect("object");
        assert!(
            !obj.contains_key("typeCounts"),
            "chain-wide typeCounts must not be served: it costs 3x a 13.4M-row scan \
             per cache miss and no surface renders it. Per-block counts live on Block."
        );
    }

    /// `sourceMaxSlot` is what lights up the explorer's catching-up banner, and the
    /// deployed client keys on that exact camelCase name (rome-via
    /// `SyncBanner.tsx` / `api.ts`). serde's rename_all does the conversion, so a
    /// field rename here would not fail any Rust test — it would just make the
    /// banner permanently silent. Pin the wire name.
    #[test]
    fn stats_overview_exposes_source_max_slot_as_camel_case() {
        let s = StatsOverview {
            latest_block_number: 10,
            latest_slot: 999,
            source_max_slot: Some(1_500),
            total_txs: 0,
            tps_60s_estimate: 0.0,
            tx_count_total: 0,
            active_addresses: 0,
            token_count_total: 0,
            token_kind_counts: TokenKindCounts::default(),
            address_type_counts: AddressTypeCounts::default(),
        };
        let v = serde_json::to_value(&s).expect("serializes");
        assert_eq!(v["sourceMaxSlot"], 1_500);
        assert!(v.get("source_max_slot").is_none(), "must be camelCase on the wire");

        // Absent must serialize as null, NOT as 0 and NOT as latest_slot — either
        // would assert a lag the sync never reported.
        let unknown = StatsOverview { source_max_slot: None, ..s };
        let v2 = serde_json::to_value(&unknown).expect("serializes");
        assert!(v2["sourceMaxSlot"].is_null(), "unknown lag must be null: {v2}");
    }

    #[test]
    fn stats_overview_chainwide_aggregates_serialize_camel_case() {
        let s = StatsOverview {
            latest_block_number: 100,
            latest_slot: 999,
            source_max_slot: None,
            total_txs: 4242,
            tps_60s_estimate: 1.5,
            tx_count_total: 4242,
            active_addresses: 37,
            token_count_total: 12,
            token_kind_counts: TokenKindCounts::default(),
            address_type_counts: AddressTypeCounts::default(),
        };
        let json = serde_json::to_string(&s).unwrap();
        // new top-level chain-wide aggregate keys, all camelCase numbers
        assert!(json.contains("\"txCountTotal\":4242"), "txCountTotal camelCase number, got: {json}");
        assert!(json.contains("\"activeAddresses\":37"), "activeAddresses camelCase number, got: {json}");
        assert!(json.contains("\"tokenCountTotal\":12"), "tokenCountTotal camelCase number, got: {json}");
        // pre-existing fields preserved
        assert!(json.contains("\"latestBlockNumber\":100"), "latestBlockNumber preserved");
        assert!(json.contains("\"totalTxs\":4242"), "totalTxs preserved");
        // The chain-wide typeCounts object is deliberately GONE — it cost three
        // whole-table scans of a 13.4M-row table per cache miss and nothing rendered
        // it. Per-block counts live on Block and are bounded by block_number.
        assert!(
            !json.contains("typeCounts"),
            "chain-wide typeCounts must stay removed, got: {json}"
        );
    }

    #[test]
    fn stats_overview_empty_chain_serializes_zeros_without_type_counts() {
        let s = StatsOverview {
            latest_block_number: 0,
            latest_slot: 0,
            source_max_slot: None,
            total_txs: 0,
            tps_60s_estimate: 0.0,
            tx_count_total: 0,
            active_addresses: 0,
            token_count_total: 0,
            token_kind_counts: TokenKindCounts::default(),
            address_type_counts: AddressTypeCounts::default(),
        };
        let json = serde_json::to_string(&s).unwrap();
        assert!(
            !json.contains("typeCounts"),
            "chain-wide typeCounts must stay removed on an empty chain too, got: {json}"
        );
        assert!(json.contains("\"txCountTotal\":0"), "txCountTotal 0 on empty chain");
    }

    // ── SearchHit.kind (Fix 2 — token kind relabeling) ───────────────────────
    #[test]
    fn search_hit_kind_serializes_when_present() {
        let hit = SearchHit {
            entity_type: "token".to_string(),
            entity_id: "0xabc".to_string(),
            display_label: "WUSDC Wrapped USDC 0xabc".to_string(),
            url: "/token/0xabc".to_string(),
            kind: Some("SPL".to_string()),
        };
        let json = serde_json::to_string(&hit).unwrap();
        assert!(json.contains("\"kind\":\"SPL\""), "kind should serialize, got: {json}");
        assert!(json.contains("\"entityType\":\"token\""), "entityType camelCase preserved");
    }

    #[test]
    fn search_hit_kind_omitted_when_none() {
        let hit = SearchHit {
            entity_type: "address".to_string(),
            entity_id: "0xdef".to_string(),
            display_label: "0xdef".to_string(),
            url: "/address/0xdef".to_string(),
            kind: None,
        };
        let json = serde_json::to_string(&hit).unwrap();
        assert!(!json.contains("kind"), "None kind should be omitted, got: {json}");
    }

    // ── Fix 1: token kind breakdown on /stats/overview ───────────────────────
    // The Tokens header consumes a chain-wide per-kind breakdown so it can show
    // "Wrapped SPL / ERC-20 / Token-2022" instead of a lone always-0 tile. Lock
    // the exact JSON shape: nested `tokenKindCounts` object with `erc20` / `spl`
    // / `token2022` keys (all camelCase JSON numbers).
    #[test]
    fn token_kind_counts_serializes_camel_case() {
        let c = TokenKindCounts {
            erc20: 7,
            spl: 4,
            token2022: 2,
        };
        let json = serde_json::to_string(&c).unwrap();
        assert_eq!(
            json, "{\"erc20\":7,\"spl\":4,\"token2022\":2}",
            "TokenKindCounts must serialize exactly erc20/spl/token2022, got: {json}"
        );
    }

    // ── Fix 2: address type breakdown on /stats/overview ─────────────────────
    // The Addresses header consumes a chain-wide breakdown so it can show
    // "Contracts / EOAs / Synthetics" chain-wide. Lock the exact JSON shape:
    // nested `addressTypeCounts` object with `contracts` / `eoas` / `synthetics`.
    #[test]
    fn address_type_counts_serializes_camel_case() {
        let c = AddressTypeCounts {
            contracts: 3,
            eoas: 30,
            synthetics: 4,
        };
        let json = serde_json::to_string(&c).unwrap();
        assert_eq!(
            json, "{\"contracts\":3,\"eoas\":30,\"synthetics\":4}",
            "AddressTypeCounts must serialize exactly contracts/eoas/synthetics, got: {json}"
        );
    }

    // The two new breakdown objects must appear (nested, camelCase keys) on the
    // full StatsOverview response alongside the existing aggregates.
    #[test]
    fn stats_overview_breakdowns_serialize_camel_case() {
        let s = StatsOverview {
            latest_block_number: 100,
            latest_slot: 999,
            source_max_slot: None,
            total_txs: 4242,
            tps_60s_estimate: 1.5,
            tx_count_total: 4242,
            active_addresses: 37,
            token_count_total: 13,
            token_kind_counts: TokenKindCounts {
                erc20: 7,
                spl: 4,
                token2022: 2,
            },
            address_type_counts: AddressTypeCounts {
                contracts: 3,
                eoas: 30,
                synthetics: 4,
            },
        };
        let json = serde_json::to_string(&s).unwrap();
        assert!(
            json.contains("\"tokenKindCounts\":{\"erc20\":7,\"spl\":4,\"token2022\":2}"),
            "tokenKindCounts nested object, got: {json}"
        );
        assert!(
            json.contains("\"addressTypeCounts\":{\"contracts\":3,\"eoas\":30,\"synthetics\":4}"),
            "addressTypeCounts nested object, got: {json}"
        );
    }

    #[test]
    fn stats_overview_breakdowns_default_zero_on_empty_chain() {
        let s = StatsOverview {
            latest_block_number: 0,
            latest_slot: 0,
            source_max_slot: None,
            total_txs: 0,
            tps_60s_estimate: 0.0,
            tx_count_total: 0,
            active_addresses: 0,
            token_count_total: 0,
            token_kind_counts: TokenKindCounts::default(),
            address_type_counts: AddressTypeCounts::default(),
        };
        let json = serde_json::to_string(&s).unwrap();
        assert!(
            json.contains("\"tokenKindCounts\":{\"erc20\":0,\"spl\":0,\"token2022\":0}"),
            "empty-chain tokenKindCounts all-zero, got: {json}"
        );
        assert!(
            json.contains("\"addressTypeCounts\":{\"contracts\":0,\"eoas\":0,\"synthetics\":0}"),
            "empty-chain addressTypeCounts all-zero, got: {json}"
        );
    }

    // ── Fix 3: token list decimals + circulatingSupply ───────────────────────
    // The token LIST previously returned only raw `supply`, so the UI rendered a
    // raw undecimated integer. Add `decimals` + `circulatingSupply` (the same
    // derived value the detail endpoint computes for wrappers). Some emits;
    // circulatingSupply=None is omitted (decimals follows TokenDetail typing).
    #[test]
    fn token_summary_decimals_and_circulating_supply_emit() {
        let t = TokenSummary {
            address: "0x9a8b4cb7326033d72ca393c6b4c0d7fb904fa900".to_string(),
            symbol: Some("wUSDC".to_string()),
            name: Some("Wrapped USDC".to_string()),
            kind: Some("SPL".to_string()),
            holders: 3,
            supply: Some("0".to_string()),
            decimals: Some(6),
            circulating_supply: Some("162397368".to_string()),
            gated: None,
            restriction_module: None,
        };
        let json = serde_json::to_string(&t).unwrap();
        assert!(
            json.contains("\"decimals\":6"),
            "decimals should serialize as a JSON number, got: {json}"
        );
        assert!(
            json.contains("\"circulatingSupply\":\"162397368\""),
            "circulatingSupply should serialize as a camelCase string, got: {json}"
        );
    }

    #[test]
    fn token_summary_none_circulating_supply_omitted() {
        // Plain ERC-20: no derived circulating supply → the key is omitted; a
        // None decimals is omitted too (skip_serializing_if).
        let t = TokenSummary {
            address: "0xdeadbeef".to_string(),
            symbol: Some("LP".to_string()),
            name: Some("Uniswap V2".to_string()),
            kind: Some("ERC-20".to_string()),
            holders: 2,
            supply: Some("1000000000000000000".to_string()),
            decimals: None,
            circulating_supply: None,
            gated: None,
            restriction_module: None,
        };
        let json = serde_json::to_string(&t).unwrap();
        assert!(
            !json.contains("circulatingSupply"),
            "circulatingSupply=None should be omitted, got: {json}"
        );
        assert!(
            !json.contains("decimals"),
            "decimals=None should be omitted, got: {json}"
        );
    }
}
