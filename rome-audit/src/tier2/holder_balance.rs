//! `holder_balance` (P4b-ii) — an asset token's per-holder running balance,
//! folded from its own `Transfer(from, to, value)` stream in
//! `(block_number, tx_index, log_index)` order, ONE row per `(address,
//! block)` = the END-OF-BLOCK value (same-block deltas for the same
//! address collapse into a single row — never one row per event).
//!
//! **`address(0)` is never a holder** — mint (`from=0x0`) never subtracts
//! from it, burn (`to=0x0`) never adds to it; it is simply excluded from
//! the touched-address set for the block, by construction (not a
//! special-cased skip on an otherwise-recorded balance).
//!
//! **Negative balances are STORED, never clamped** — the rome-via-enrich
//! `holders` worker's #506 lesson (`GREATEST(0, …)` silently hides an
//! order-dependent underflow, which then "self-heals" the moment a batch
//! reorders and becomes unauditable). `integrity_alarm` is a PER-ROW,
//! freshly-recomputed fact — "is THIS row's ending balance negative" —
//! never a sticky flag: a later block's row that recovers to `>= 0` reads
//! `integrity_alarm = false`, even though an earlier row for the same
//! address was flagged `true`.
//!
//! **Malformed `value`** (fails [`super::fetch::arg_uint256`]) skips the
//! WHOLE event — never invents a delta, never partially applies one side
//! of a transfer.

use std::collections::{BTreeMap, BTreeSet};

use bigdecimal::BigDecimal;

use super::fetch::{arg_address, arg_uint256, ChainEventRow, ZERO_ADDRESS};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HolderBalanceRow {
    pub address: [u8; 20],
    pub block_number: i64,
    pub balance: BigDecimal,
    pub integrity_alarm: bool,
}

/// `events` MUST already be the asset token's OWN `Transfer` rows, in
/// `(block_number, tx_index, log_index)` order (reuse the same
/// `transfer_events` already fetched for `exposure`/`screening` — no
/// separate fetch needed).
pub fn build_holder_balances(events: &[ChainEventRow]) -> Vec<HolderBalanceRow> {
    let mut running: BTreeMap<[u8; 20], BigDecimal> = BTreeMap::new();
    let mut result: Vec<HolderBalanceRow> = Vec::new();
    let mut current_block: Option<i64> = None;
    let mut touched: BTreeSet<[u8; 20]> = BTreeSet::new();

    for ev in events {
        if ev.event_name != "Transfer" {
            continue;
        }
        let (Some(from), Some(to), Some(value)) = (
            arg_address(&ev.args, "from"),
            arg_address(&ev.args, "to"),
            arg_uint256(&ev.args, "value"),
        ) else {
            continue; // malformed — skip the WHOLE event, never a partial apply
        };

        if current_block != Some(ev.block_number) {
            if let Some(block) = current_block {
                flush_block(block, &touched, &running, &mut result);
            }
            touched.clear();
            current_block = Some(ev.block_number);
        }

        if from != ZERO_ADDRESS {
            let bal = running.entry(from).or_insert_with(|| BigDecimal::from(0));
            *bal -= &value;
            touched.insert(from);
        }
        if to != ZERO_ADDRESS {
            let bal = running.entry(to).or_insert_with(|| BigDecimal::from(0));
            *bal += &value;
            touched.insert(to);
        }
    }
    if let Some(block) = current_block {
        flush_block(block, &touched, &running, &mut result);
    }

    result.sort_by_key(|r| (r.address, r.block_number));
    result
}

/// Emits one row per address touched in `block`, reading each one's CURRENT
/// running balance and recomputing `integrity_alarm` fresh (never a sticky
/// flag carried from an earlier block).
fn flush_block(
    block: i64,
    touched: &BTreeSet<[u8; 20]>,
    running: &BTreeMap<[u8; 20], BigDecimal>,
    result: &mut Vec<HolderBalanceRow>,
) {
    for addr in touched {
        let balance = running
            .get(addr)
            .cloned()
            .unwrap_or_else(|| BigDecimal::from(0));
        let integrity_alarm = balance < BigDecimal::from(0);
        result.push(HolderBalanceRow {
            address: *addr,
            block_number: block,
            balance,
            integrity_alarm,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn bd(s: &str) -> BigDecimal {
        BigDecimal::from_str(s).unwrap()
    }
    fn addr_hex(a: [u8; 20]) -> String {
        format!("0x{}", hex::encode(a))
    }

    fn transfer_ev(
        event_id: i64,
        block: i64,
        tx_index: i32,
        log_index: i32,
        from: [u8; 20],
        to: [u8; 20],
        value: &str,
    ) -> ChainEventRow {
        ChainEventRow {
            event_id,
            block_number: block,
            tx_index,
            log_index,
            event_name: "Transfer".to_string(),
            args: serde_json::json!({ "from": addr_hex(from), "to": addr_hex(to), "value": value }),
            tx_signer: None,
        }
    }

    const HOLDER_A: [u8; 20] = [0xA1; 20];
    const HOLDER_B: [u8; 20] = [0xB1; 20];

    // ---- 31. one_row_per_address_block_end_of_block_value ----
    #[test]
    fn one_row_per_address_block_end_of_block_value() {
        let events = vec![
            transfer_ev(1, 100, 0, 0, [0u8; 20], HOLDER_A, "10"), // mint 10
            transfer_ev(2, 100, 0, 1, [0u8; 20], HOLDER_A, "5"),  // mint 5 more, SAME block
        ];
        let rows = build_holder_balances(&events);
        assert_eq!(
            rows,
            vec![HolderBalanceRow {
                address: HOLDER_A,
                block_number: 100,
                balance: bd("15"),
                integrity_alarm: false,
            }],
            "two same-block deltas for the same address must collapse into ONE end-of-block row"
        );
    }

    // ---- 32. zero_address_never_a_holder ----
    #[test]
    fn zero_address_never_a_holder() {
        let events = vec![
            transfer_ev(1, 100, 0, 0, [0u8; 20], HOLDER_A, "10"), // mint
            transfer_ev(2, 200, 0, 0, HOLDER_A, [0u8; 20], "10"), // burn
        ];
        let rows = build_holder_balances(&events);
        assert!(
            rows.iter().all(|r| r.address != [0u8; 20]),
            "the zero address must never appear as a holder row, mint or burn"
        );
        assert_eq!(rows.len(), 2, "HOLDER_A gets a row at block 100 (mint) and block 200 (burn)");
        assert_eq!(rows[1].balance, bd("0"));
    }

    // ---- 33. negative_balance_stored_with_alarm_never_clamped ----
    #[test]
    fn negative_balance_stored_with_alarm_never_clamped() {
        // HOLDER_A sends 10 with NO prior credit at all — an impossible
        // real-world transfer, but this builder never validates that; it
        // just folds whatever the log stream says.
        let events = vec![transfer_ev(1, 100, 0, 0, HOLDER_A, HOLDER_B, "10")];
        let rows = build_holder_balances(&events);
        let a_row = rows.iter().find(|r| r.address == HOLDER_A).unwrap();
        assert_eq!(a_row.balance, bd("-10"), "the negative balance must be STORED verbatim, never clamped to 0");
        assert!(a_row.integrity_alarm, "a negative balance must set integrity_alarm");
    }

    // ---- 34. balance_carries_forward_across_blocks ----
    #[test]
    fn balance_carries_forward_across_blocks() {
        let events = vec![
            transfer_ev(1, 100, 0, 0, [0u8; 20], HOLDER_A, "10"),
            // block 150 touches HOLDER_B only — HOLDER_A gets NO row here.
            transfer_ev(2, 150, 0, 0, [0u8; 20], HOLDER_B, "1"),
            // block 200: HOLDER_A touched again — its row must reflect the
            // CUMULATIVE balance since block 100, not just this block's delta.
            transfer_ev(3, 200, 0, 0, [0u8; 20], HOLDER_A, "5"),
        ];
        let rows = build_holder_balances(&events);
        let a_rows: Vec<_> = rows.iter().filter(|r| r.address == HOLDER_A).collect();
        assert_eq!(a_rows.len(), 2, "HOLDER_A has no row at block 150 — it wasn't touched there");
        assert_eq!(a_rows[0].block_number, 100);
        assert_eq!(a_rows[0].balance, bd("10"));
        assert_eq!(a_rows[1].block_number, 200);
        assert_eq!(a_rows[1].balance, bd("15"), "must carry the running balance forward across the untouched block");
    }

    #[test]
    fn recovered_positive_balance_clears_the_alarm_on_a_later_row() {
        let events = vec![
            transfer_ev(1, 100, 0, 0, HOLDER_A, HOLDER_B, "10"), // -10, alarm
            transfer_ev(2, 200, 0, 0, [0u8; 20], HOLDER_A, "20"), // +20 -> +10, no alarm
        ];
        let rows = build_holder_balances(&events);
        let a_rows: Vec<_> = rows.iter().filter(|r| r.address == HOLDER_A).collect();
        assert!(a_rows[0].integrity_alarm);
        assert!(!a_rows[1].integrity_alarm, "a later row that recovers to >= 0 must read alarm = false");
    }

    #[test]
    fn malformed_value_skips_the_whole_event() {
        let mut ev = transfer_ev(1, 100, 0, 0, [0u8; 20], HOLDER_A, "not-a-number");
        ev.args["value"] = serde_json::json!("not-a-number");
        assert_eq!(build_holder_balances(&[ev]), vec![]);
    }
}
