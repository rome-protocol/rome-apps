//! Action-tag classifier — the *authoritative* implementation of the Rome Via
//! transaction-action taxonomy.
//!
//! The block explorer renders two axes per tx:
//!
//! 1. Network type — Rhea / Remus / Romulus (already on `Tx.tx_type`).
//! 2. **Action tags** — what the tx *did* (categorical). This module emits
//!    those tags as a `Vec<String>`, serialized into the API response as
//!    `actionTags`.
//!
//! The frontend (`rome-via/src/lib/tx-activity.ts`) carries an identical
//! classifier that runs in mock mode and as a fallback when the backend
//! response lacks `actionTags`. Both must produce the same tag list for the
//! same inputs — see [`docs/CLASSIFIER.md`] for the contract.
//!
//! Tags are deduplicated (first occurrence wins, primary-first order) so a
//! swap that emits multiple Transfer logs yields `["swap", "token_transfer"]`,
//! not three Transfer tags.
//!
//! Phase 1 limitation: HelperProgram (0xFF…09) and Withdraw (0x42…16)
//! sub-methods are recognized by address only; the resolved sub-method name
//! ships in `Tx.method` and is shown by the frontend's chip `detail` field.

use serde_json::Value;

/// Wire-level tx type byte for op-stack-style deposit transactions.
const ROME_DEPOSIT_TYPE_BYTE: i16 = 0x7e;

// ── Rome precompile addresses ────────────────────────────────────────────────
const CPI_PRECOMPILE: &str = "0xff00000000000000000000000000000000000008";
const SYSTEM_PRECOMPILE: &str = "0xff00000000000000000000000000000000000007";
const HELPER_PRECOMPILE: &str = "0xff00000000000000000000000000000000000009";
const WITHDRAW_PRECOMPILE: &str = "0x4200000000000000000000000000000000000016";

// ── Canonical Ethereum event topics (lowercase) ─────────────────────────────
const TOPIC_TRANSFER: &str =
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const TOPIC_APPROVAL: &str =
    "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925";
const TOPIC_SWAP_V2: &str =
    "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822";
const TOPIC_SWAP_V3: &str =
    "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67";
const TOPIC_TRANSFER_SINGLE: &str =
    "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62";
const TOPIC_TRANSFER_BATCH: &str =
    "0x4a39dc06d4c0dbc64b50af37ef0f9d05e9bbb56eb8b8c037d8c5e88c5f2e7f6f";
const TOPIC_PAIR_MINT: &str =
    "0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f";
const TOPIC_PAIR_BURN: &str =
    "0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496";
const ADDR_ZERO_TOPIC: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000000";
// Only referenced by tests now — the classifier no longer treats the zero
// address as "absent" (a call to 0x0 is a normal burn target, not a creation).
#[cfg(test)]
const ZERO_ADDR: &str = "0x0000000000000000000000000000000000000000";

/// Inputs to the classifier — assembled from a `Tx` row plus the raw
/// `tx_result.logs` JSONB array (so we don't have to fully parse logs into
/// `Vec<TxLog>` when we only need topic[0] from each).
#[derive(Debug)]
pub struct ClassifyInput<'a> {
    pub tx_type: &'a str,           // "Rhea" | "Remus" | "Romulus"
    pub method: &'a str,            // resolved signature OR raw "0x…" selector OR "0x"
    pub to: Option<&'a str>,        // lowercased on entry
    pub value_wei: &'a str,         // decimal string ("0" when zero)
    pub tx_type_byte: Option<i16>,
    pub solana_leg_count: usize,
    /// `tx_result.logs` JSONB array — may be `None` if tx_result is null.
    pub logs: Option<&'a Value>,
    /// Byte length of the tx `input` (init code for a creation, calldata otherwise),
    /// from `evm_tx.input_len`. `None` when decode failed / unindexed — creation
    /// detection must treat unknown as "not proven" (never tag on absence).
    pub input_len: Option<i32>,
}

#[derive(Debug, Default)]
struct Builder {
    tags: Vec<String>,
    seen: std::collections::HashSet<String>,
}

impl Builder {
    fn push(&mut self, tag: &str) {
        if self.seen.insert(tag.to_string()) {
            self.tags.push(tag.to_string());
        }
    }
}

fn is_zero_value(wei: &str) -> bool {
    wei.is_empty() || wei == "0" || wei.chars().all(|c| c == '0')
}

/// Contract creation needs POSITIVE evidence, not just an absent `to`.
///
/// The RLP decoder deliberately NULLs `method` for creations (init code has
/// no 4-byte selector), so `method` can't distinguish a real deploy from a
/// NULL-`to` row with empty data. The only positive signal that survives is
/// `input_len` — a real creation always carries non-empty init code; `None`
/// (decode failed / unindexed) or `0` (a historical indexing gap left
/// Solana-originated DoTxUnsigned rows in this shape) must NOT tag a
/// creation. `to == 0x0` is a normal call/burn target, not "absent" —
/// Ethereum creations always have `to` fully absent, never the zero address.
fn is_contract_creation(to: Option<&str>, input_len: Option<i32>) -> bool {
    let to_absent = matches!(to, None | Some(""));
    to_absent && input_len.is_some_and(|n| n > 0)
}

fn is_empty_method(method: &str) -> bool {
    matches!(method, "0x" | "" | "(method)")
}

fn topic_is_zero(t: &str) -> bool {
    t.eq_ignore_ascii_case(ADDR_ZERO_TOPIC)
}

/// Iterate `tx_result.logs` JSONB array (if present) and call `f` on each
/// `(topics: &Vec<Value>, log: &Value)`.
fn for_each_log<F: FnMut(&Vec<Value>)>(logs: Option<&Value>, mut f: F) {
    if let Some(arr) = logs.and_then(|v| v.as_array()) {
        for log in arr {
            if let Some(topics) = log.get("topics").and_then(|t| t.as_array()) {
                f(topics);
            }
        }
    }
}

fn has_topic(logs: Option<&Value>, topic0: &str) -> bool {
    let mut found = false;
    for_each_log(logs, |topics| {
        if found {
            return;
        }
        if let Some(t0) = topics.first().and_then(|v| v.as_str()) {
            if t0.eq_ignore_ascii_case(topic0) {
                found = true;
            }
        }
    });
    found
}

/// Classify a tx into one or more action tags. Returns them in priority
/// order (most informative first).
pub fn classify(input: &ClassifyInput<'_>) -> Vec<String> {
    let mut b = Builder::default();

    let value_wei = input.value_wei;
    let has_native_value = !is_zero_value(value_wei);
    let method = input.method;
    let empty_method = is_empty_method(method);
    let to_lower = input.to.unwrap_or("").to_ascii_lowercase();
    let legs = input.solana_leg_count;
    let logs = input.logs;

    // ── Rome-specific (primary axis) ────────────────────────────────────────
    if is_contract_creation(input.to, input.input_len) {
        b.push("contract_creation");
    } else if input.tx_type_byte == Some(ROME_DEPOSIT_TYPE_BYTE) && empty_method {
        b.push("rome_deposit");
    } else if input.tx_type == "Romulus" && empty_method && legs >= 2 && !has_native_value {
        b.push("solana_only");
    } else if to_lower == CPI_PRECOMPILE {
        b.push("solana_cpi");
    } else if to_lower == SYSTEM_PRECOMPILE {
        b.push("system_call");
    } else if to_lower == HELPER_PRECOMPILE {
        // The gas-wrapper unwrap leg leaves no logs and no value — the
        // resolved sub-method is the only signal. Headline it `unwrap`.
        if method == "deposit_from_ata(uint256)" {
            b.push("unwrap");
        }
        b.push("helper_call");
    } else if to_lower == WITHDRAW_PRECOMPILE {
        // Wrap legs of the gas wrapper (to PDA-owned ATA / to PDA).
        if method == "withdraw_to_ata(uint256)" || method == "withdraw_to_pda(uint256)" {
            b.push("wrap");
        }
        b.push("withdraw_precompile");
    } else if input.tx_type == "Romulus" && !empty_method && legs >= 2 {
        b.push("cross_chain_call");
    }

    // ── Protocol actions (method-name axis) ─────────────────────────────────
    // Name well-known protocol calls by their (decoded) method so the explorer
    // reads "Oracle refresh / Supply / Borrow / Bridge-out / Faucet" instead of
    // a generic "Call". Chain-agnostic: matches the resolved method signature
    // (supplied by the method_decoder enrich worker), NOT per-chain addresses.
    // Runs after the Rome-specific axis so a precompile/creation still wins.
    // Mirror any change in rome-via/src/lib/tx-activity.ts.
    let mname = method.split('(').next().unwrap_or("");
    match mname {
        // `refresh` = legacy per-asset keeper; `refreshAll` = PriceBook keeper (one tx per
        // tick, all assets). Both are oracle infrastructure — see is_oracle_method / ORACLE_SELECTORS.
        "refresh" | "refreshAll" => b.push("oracle_refresh"),
        "supply" | "supplyTo" | "supplyFrom" => b.push("lend_supply"),
        // `withdraw` is overloaded — WETH unwrap is `withdraw(uint256)` (tagged
        // `unwrap` below); only the *To/*From variants are unambiguously lending.
        "withdrawTo" | "withdrawFrom" => b.push("lend_withdraw"),
        "borrow" => b.push("lend_borrow"),
        "repay" | "repayWithATokens" => b.push("lend_repay"),
        "liquidationCall" | "absorb" | "buyCollateral" => b.push("liquidate"),
        "bridgeOutToSolana" => b.push("bridge_out"),
        "claimTokens" | "drip" | "faucet" => b.push("faucet_claim"),
        "aggregate" | "aggregate3" | "aggregate3Value" | "tryAggregate"
        | "tryBlockAndAggregate" => b.push("multicall"),
        "activate" => b.push("account_activate"),
        "collect" => b.push("collect_fees"),
        "increaseLiquidity" => b.push("add_liquidity"),
        "decreaseLiquidity" => b.push("remove_liquidity"),
        _ => {}
    }
    // Lending `withdraw` carries an asset address (Comet: 2 args; Aave: 3) —
    // matched by full signature so WETH `withdraw(uint256)` stays `unwrap`.
    if method == "withdraw(address,uint256)" || method == "withdraw(address,uint256,address)" {
        b.push("lend_withdraw");
    }

    // ── Generic EVM (log-derived) ───────────────────────────────────────────
    if has_topic(logs, TOPIC_SWAP_V2) || has_topic(logs, TOPIC_SWAP_V3) {
        b.push("swap");
    }
    if has_topic(logs, TOPIC_PAIR_MINT) {
        b.push("add_liquidity");
    }
    if has_topic(logs, TOPIC_PAIR_BURN) {
        b.push("remove_liquidity");
    }

    // WETH wrap / unwrap by method.
    if method == "deposit()" && has_native_value {
        b.push("wrap");
    } else if method == "withdraw(uint256)" {
        b.push("unwrap");
    }

    // ERC-20 / ERC-721 Transfer logs — categorize by from/to zero topic and
    // by topic-count (3 = ERC-20, 4 = ERC-721 with indexed tokenId).
    for_each_log(logs, |topics| {
        let Some(t0) = topics.first().and_then(|v| v.as_str()) else { return };
        if !t0.eq_ignore_ascii_case(TOPIC_TRANSFER) {
            return;
        }
        let erc721 = topics.len() == 4;
        let from_topic = topics.get(1).and_then(|v| v.as_str()).unwrap_or("");
        let to_topic = topics.get(2).and_then(|v| v.as_str()).unwrap_or("");
        if topic_is_zero(from_topic) {
            b.push(if erc721 { "nft_mint" } else { "token_mint" });
        } else if topic_is_zero(to_topic) {
            b.push(if erc721 { "nft_burn" } else { "token_burn" });
        } else {
            b.push(if erc721 { "nft_transfer" } else { "token_transfer" });
        }
    });

    // ERC-1155.
    if has_topic(logs, TOPIC_TRANSFER_SINGLE) || has_topic(logs, TOPIC_TRANSFER_BATCH) {
        b.push("nft_transfer");
    }

    // Approval.
    if has_topic(logs, TOPIC_APPROVAL)
        || method == "approve(address,uint256)"
        || method == "setApprovalForAll(address,bool)"
    {
        b.push("token_approval");
    }

    // ── Fallback ────────────────────────────────────────────────────────────
    if b.tags.is_empty() {
        if has_native_value && empty_method {
            b.push("coin_transfer");
        } else if !empty_method {
            b.push("contract_call");
        } else {
            b.push("empty_call");
        }
    }

    b.tags
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn topic_for(addr: &str) -> String {
        // Pad address (40 hex chars) to 64 hex chars by prepending 24 zeros.
        let hex = addr.strip_prefix("0x").unwrap_or(addr).to_lowercase();
        format!("0x{}{}", "0".repeat(64 - hex.len()), hex)
    }

    fn input<'a>(
        tx_type: &'a str,
        method: &'a str,
        to: Option<&'a str>,
        value_wei: &'a str,
        tx_type_byte: Option<i16>,
        legs: usize,
        logs: Option<&'a Value>,
    ) -> ClassifyInput<'a> {
        // Default helper: input_len unknown (None). Creation-detection tests use
        // `input_il` to pin the init-code byte length explicitly.
        input_il(tx_type, method, to, value_wei, tx_type_byte, legs, logs, None)
    }

    #[allow(clippy::too_many_arguments)]
    fn input_il<'a>(
        tx_type: &'a str,
        method: &'a str,
        to: Option<&'a str>,
        value_wei: &'a str,
        tx_type_byte: Option<i16>,
        legs: usize,
        logs: Option<&'a Value>,
        input_len: Option<i32>,
    ) -> ClassifyInput<'a> {
        ClassifyInput {
            tx_type,
            method,
            to,
            value_wei,
            tx_type_byte,
            solana_leg_count: legs,
            logs,
            input_len,
        }
    }

    #[test]
    fn rome_precompile_addresses() {
        for (addr, expected) in [
            (CPI_PRECOMPILE, "solana_cpi"),
            (SYSTEM_PRECOMPILE, "system_call"),
            (HELPER_PRECOMPILE, "helper_call"),
            (WITHDRAW_PRECOMPILE, "withdraw_precompile"),
        ] {
            let tags = classify(&input("Rhea", "0x", Some(addr), "0", None, 0, None));
            assert_eq!(tags[0], expected);
        }
    }

    #[test]
    fn rome_deposit_classified_by_tx_type_byte() {
        let tags = classify(&input(
            "Rhea",
            "0x",
            Some("0xabcd000000000000000000000000000000000000"),
            "1000",
            Some(0x7e),
            0,
            None,
        ));
        assert_eq!(tags, vec!["rome_deposit"]);
    }

    #[test]
    fn solana_only_romulus() {
        let tags = classify(&input(
            "Romulus",
            "0x",
            Some("0xabcd000000000000000000000000000000000000"),
            "0",
            None,
            45,
            None,
        ));
        assert_eq!(tags, vec!["solana_only"]);
    }

    #[test]
    fn cross_chain_call_romulus() {
        let tags = classify(&input(
            "Romulus",
            "swapExactTokensForTokens(uint256,uint256,address[],address,uint256)",
            Some("0xabcd000000000000000000000000000000000000"),
            "0",
            None,
            2,
            None,
        ));
        assert_eq!(tags[0], "cross_chain_call");
    }

    #[test]
    fn token_transfer_log() {
        let logs = json!([
            {
                "address": "0xabcd000000000000000000000000000000000000",
                "topics": [TOPIC_TRANSFER, topic_for("0xaa11"), topic_for("0xbb22")],
                "data": "0x"
            }
        ]);
        let tags = classify(&input(
            "Rhea",
            "0xa9059cbb",
            Some("0xabcd000000000000000000000000000000000000"),
            "0",
            None,
            0,
            Some(&logs),
        ));
        assert!(tags.contains(&"token_transfer".to_string()));
    }

    #[test]
    fn token_mint_log_from_zero() {
        let logs = json!([
            {
                "address": "0xabcd000000000000000000000000000000000000",
                "topics": [TOPIC_TRANSFER, ADDR_ZERO_TOPIC, topic_for("0xbb22")],
                "data": "0x"
            }
        ]);
        let tags = classify(&input("Rhea", "0x", Some("0xabcd000000000000000000000000000000000000"), "0", None, 0, Some(&logs)));
        assert!(tags.contains(&"token_mint".to_string()));
    }

    #[test]
    fn token_burn_log_to_zero() {
        let logs = json!([
            {
                "address": "0xabcd000000000000000000000000000000000000",
                "topics": [TOPIC_TRANSFER, topic_for("0xaa11"), ADDR_ZERO_TOPIC],
                "data": "0x"
            }
        ]);
        let tags = classify(&input("Rhea", "0x", Some("0xabcd000000000000000000000000000000000000"), "0", None, 0, Some(&logs)));
        assert!(tags.contains(&"token_burn".to_string()));
    }

    #[test]
    fn erc721_transfer_has_4_topics() {
        let logs = json!([
            {
                "address": "0xabcd000000000000000000000000000000000000",
                "topics": [TOPIC_TRANSFER, topic_for("0xaa11"), topic_for("0xbb22"), topic_for("0x0001")],
                "data": "0x"
            }
        ]);
        let tags = classify(&input("Rhea", "0x", Some("0xabcd000000000000000000000000000000000000"), "0", None, 0, Some(&logs)));
        assert!(tags.contains(&"nft_transfer".to_string()));
        assert!(!tags.contains(&"token_transfer".to_string()));
    }

    #[test]
    fn swap_log_takes_priority() {
        let logs = json!([
            {"address": "0xpool", "topics": [TOPIC_SWAP_V2, topic_for("0xaa"), topic_for("0xbb")], "data": "0x"},
            {"address": "0xtok",  "topics": [TOPIC_TRANSFER, topic_for("0xaa"), topic_for("0xbb")], "data": "0x"}
        ]);
        let tags = classify(&input(
            "Rhea",
            "swapExactTokensForTokens(uint256,uint256,address[],address,uint256)",
            Some("0xabcd000000000000000000000000000000000000"),
            "0",
            None,
            0,
            Some(&logs),
        ));
        // Swap appears before token_transfer in the output (in priority order).
        let swap_idx = tags.iter().position(|t| t == "swap").unwrap();
        let xfer_idx = tags.iter().position(|t| t == "token_transfer").unwrap();
        assert!(swap_idx < xfer_idx);
    }

    #[test]
    fn approval_from_log_or_method() {
        // From method.
        let tags = classify(&input("Rhea", "approve(address,uint256)", Some("0xabcd000000000000000000000000000000000000"), "0", None, 0, None));
        assert!(tags.contains(&"token_approval".to_string()));
        // From log.
        let logs = json!([{"address": "0xabcd", "topics": [TOPIC_APPROVAL, topic_for("0xaa"), topic_for("0xbb")], "data": "0x"}]);
        let tags = classify(&input("Rhea", "0x", Some("0xabcd000000000000000000000000000000000000"), "0", None, 0, Some(&logs)));
        assert!(tags.contains(&"token_approval".to_string()));
    }

    #[test]
    fn fallback_coin_transfer_contract_call_empty() {
        assert_eq!(classify(&input("Rhea", "0x", Some("0xabcd000000000000000000000000000000000000"), "1000", None, 0, None)), vec!["coin_transfer"]);
        assert_eq!(classify(&input("Rhea", "mint(address)", Some("0xabcd000000000000000000000000000000000000"), "0", None, 0, None)), vec!["contract_call"]);
        assert_eq!(classify(&input("Rhea", "0x", Some("0xabcd000000000000000000000000000000000000"), "0", None, 0, None)), vec!["empty_call"]);
    }

    #[test]
    fn deploy_with_empty_method_but_init_code_is_creation() {
        // THE reported symptom (live rubicon 0x982d…055f): the RLP decoder
        // deliberately NULLs method_id for creations, so `method` arrives as "0x".
        // The only positive creation signal that survives is input_len > 0 (init
        // code). `to` absent + input_len > 0 MUST tag contract_creation — the
        // method-non-emptiness heuristic mis-tagged every ECDSA deploy empty_call.
        let tags = classify(&input_il("Rhea", "0x", None, "0", None, 0, None, Some(2000)));
        assert_eq!(tags[0], "contract_creation", "deploy w/ init code, got {tags:?}");
    }

    #[test]
    fn null_to_zero_input_len_is_not_creation() {
        // Historical indexing gap (#433): Solana-originated (DoTxUnsigned) rows
        // left with NULL `to` and EMPTY calldata (input_len 0). A real creation
        // always carries init code, so input_len 0 must NOT tag contract_creation.
        let tags = classify(&input_il("Rhea", "0x", None, "0", None, 0, None, Some(0)));
        assert!(
            !tags.contains(&"contract_creation".to_string()),
            "to=NULL with input_len 0 must not tag contract_creation, got {tags:?}"
        );
        assert_eq!(tags, vec!["empty_call"]);
    }

    #[test]
    fn null_to_unknown_input_len_is_not_creation() {
        // Decode failed / unindexed → input_len None. Cannot prove a creation;
        // must NOT tag one (never tag on absence of evidence).
        let tags = classify(&input_il("Rhea", "0x", None, "0", None, 0, None, None));
        assert!(
            !tags.contains(&"contract_creation".to_string()),
            "to=NULL with unknown input_len must not tag contract_creation, got {tags:?}"
        );
    }

    #[test]
    fn call_to_zero_address_is_not_creation() {
        // A tx TO the zero address (with calldata) is a real call/burn — Ethereum
        // creations have `to` ABSENT, never `to == 0x000…0`. Treating the zero
        // address as "absent" mis-tagged burns as creations (classify.rs bug).
        let tags = classify(&input_il(
            "Rhea",
            "burn(uint256)",
            Some(ZERO_ADDR),
            "0",
            None,
            0,
            None,
            Some(36),
        ));
        assert!(
            !tags.contains(&"contract_creation".to_string()),
            "call to 0x0 with calldata must not tag contract_creation, got {tags:?}"
        );
    }

    #[test]
    fn no_duplicate_tags() {
        // Two Transfer logs (same kind) → one token_transfer tag.
        let logs = json!([
            {"address": "0xa", "topics": [TOPIC_TRANSFER, topic_for("0xaa"), topic_for("0xbb")], "data": "0x"},
            {"address": "0xb", "topics": [TOPIC_TRANSFER, topic_for("0xcc"), topic_for("0xdd")], "data": "0x"}
        ]);
        let tags = classify(&input("Rhea", "0x", Some("0xabcd000000000000000000000000000000000000"), "0", None, 0, Some(&logs)));
        assert_eq!(tags.iter().filter(|t| *t == "token_transfer").count(), 1);
    }

    #[test]
    fn raw_selector_falls_back_to_contract_call() {
        let tags = classify(&input("Rhea", "0xb94f3733", Some("0xabcd000000000000000000000000000000000000"), "0", None, 0, None));
        assert_eq!(tags, vec!["contract_call"]);
    }

    #[test]
    fn protocol_actions_by_method_name() {
        let to = Some("0xabcd000000000000000000000000000000000000");
        for (method, expected) in [
            ("refresh()", "oracle_refresh"),
            ("refreshAll(bytes32[])", "oracle_refresh"),
            ("supply(address,uint256)", "lend_supply"),
            ("supply(address,uint256,address,uint16)", "lend_supply"),
            ("withdraw(address,uint256)", "lend_withdraw"),
            ("withdraw(address,uint256,address)", "lend_withdraw"),
            ("withdrawTo(address,address,uint256)", "lend_withdraw"),
            ("borrow(address,uint256,uint256,uint16,address)", "lend_borrow"),
            ("repay(address,uint256,uint256,address)", "lend_repay"),
            ("liquidationCall(address,address,address,uint256,bool)", "liquidate"),
            ("absorb(address,address[])", "liquidate"),
            ("bridgeOutToSolana(bytes32,uint256,address)", "bridge_out"),
            ("claimTokens()", "faucet_claim"),
            ("aggregate3((address,bool,bytes)[])", "multicall"),
            ("activate(address)", "account_activate"),
            ("collect((uint256,address,uint128,uint128))", "collect_fees"),
        ] {
            let tags = classify(&input("Rhea", method, to, "0", None, 0, None));
            assert!(
                tags.contains(&expected.to_string()),
                "method {method} should tag {expected}, got {tags:?}"
            );
        }
    }

    #[test]
    fn weth_withdraw_is_unwrap_not_lend() {
        // WETH `withdraw(uint256)` must stay `unwrap`, never `lend_withdraw`.
        let tags = classify(&input("Rhea", "withdraw(uint256)", Some("0xabcd000000000000000000000000000000000000"), "0", None, 0, None));
        assert!(tags.contains(&"unwrap".to_string()));
        assert!(!tags.contains(&"lend_withdraw".to_string()));
    }

    #[test]
    fn gas_wrapper_unwrap_leg_tagged_unwrap_first() {
        // Direct HelperProgram.deposit_from_ata(uint256) call — the gas-wrapper
        // unwrap leg. Must headline `unwrap`, with `helper_call` as detail.
        let tags = classify(&input(
            "Rhea",
            "deposit_from_ata(uint256)",
            Some(HELPER_PRECOMPILE),
            "0",
            None,
            0,
            None,
        ));
        assert_eq!(tags[0], "unwrap", "got {tags:?}");
        assert!(tags.contains(&"helper_call".to_string()));
    }

    #[test]
    fn gas_wrapper_wrap_legs_tagged_wrap_first() {
        // Direct Withdraw.withdraw_to_ata / withdraw_to_pda calls — the wrap
        // legs. Must headline `wrap`, with `withdraw_precompile` as detail.
        for method in ["withdraw_to_ata(uint256)", "withdraw_to_pda(uint256)"] {
            let tags = classify(&input(
                "Rhea",
                method,
                Some(WITHDRAW_PRECOMPILE),
                "0",
                None,
                0,
                None,
            ));
            assert_eq!(tags[0], "wrap", "method {method} got {tags:?}");
            assert!(tags.contains(&"withdraw_precompile".to_string()));
        }
    }

    #[test]
    fn helper_precompile_other_methods_stay_bare_helper_call() {
        // Non-wrapper helper methods must NOT pick up wrap/unwrap tags.
        for method in ["create_ata(address)", "swap_gas_to_lamports(uint64)", "0x"] {
            let tags = classify(&input(
                "Rhea",
                method,
                Some(HELPER_PRECOMPILE),
                "0",
                None,
                0,
                None,
            ));
            assert_eq!(tags[0], "helper_call", "method {method} got {tags:?}");
            assert!(!tags.contains(&"unwrap".to_string()));
            assert!(!tags.contains(&"wrap".to_string()));
        }
    }

    #[test]
    fn oracle_refresh_tagged_for_filter() {
        // The default-hide filter keys off this tag + the 0xf8ac93e8 selector.
        let tags = classify(&input("Rhea", "refresh()", Some("0xc63af5d67d2a655a087bf635f3980dce041963de"), "0", None, 0, None));
        assert_eq!(tags, vec!["oracle_refresh"]);
    }
}
