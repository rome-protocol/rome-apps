//! `yield_blacklist_interval` (P4b-i) — ← `YieldBlacklistUpdated(account,
//! isBlacklisted)`, from every module in an asset's
//! `AssetSources::yield_blacklist_modules` (capture §2.3: an asset can swap
//! its yield-blacklist module over time, so more than one module's history
//! can legitimately apply to the same asset). Same open/close-per-address
//! shape as [`super::allowlist`]: `isBlacklisted=true` opens an interval,
//! `false` closes it, idempotent against redundant re-adds, a stray remove
//! is a no-op, `address(0)` defensively skipped (never a real blacklist
//! target on-chain).
//!
//! Callers merge every configured module's fetched events by position
//! ([`super::fetch::merge_events_by_position`]) before calling this builder
//! — the builder itself is agnostic to which module a given event came from
//! (the table has no `module_address` column; only the interval matters).

use std::collections::BTreeMap;

use super::fetch::{arg_address, arg_bool, ChainEventRow, ZERO_ADDRESS};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct YieldBlacklistIntervalRow {
    pub address: [u8; 20],
    pub from_block: i64,
    pub to_block: Option<i64>,
    pub opened_by_event: i64,
    pub closed_by_event: Option<i64>,
}

/// `events` MUST already be `YieldBlacklistUpdated` rows — merged across
/// every configured `yield_blacklist_modules` for this asset, in
/// `(block_number, tx_index, log_index)` order.
pub fn build_yield_blacklist_intervals(events: &[ChainEventRow]) -> Vec<YieldBlacklistIntervalRow> {
    // address -> (from_block, opened_by_event) for the currently-open interval.
    let mut open: BTreeMap<[u8; 20], (i64, i64)> = BTreeMap::new();
    let mut closed = Vec::new();

    for ev in events {
        if ev.event_name != "YieldBlacklistUpdated" {
            continue;
        }
        let Some(account) = arg_address(&ev.args, "account") else {
            continue;
        };
        let Some(is_blacklisted) = arg_bool(&ev.args, "isBlacklisted") else {
            continue;
        };
        if account == ZERO_ADDRESS {
            continue; // never a real blacklist target; defensive skip
        }

        if is_blacklisted {
            // Idempotent against a redundant add while already open.
            open.entry(account)
                .or_insert((ev.block_number, ev.event_id));
        } else if let Some((from_block, opened_by_event)) = open.remove(&account) {
            closed.push(YieldBlacklistIntervalRow {
                address: account,
                from_block,
                to_block: Some(ev.block_number),
                opened_by_event,
                closed_by_event: Some(ev.event_id),
            });
        }
        // A remove with nothing open (stray/duplicate remove) is a no-op.
    }

    let mut result = closed;
    for (address, (from_block, opened_by_event)) in open {
        result.push(YieldBlacklistIntervalRow {
            address,
            from_block,
            to_block: None,
            opened_by_event,
            closed_by_event: None,
        });
    }
    result.sort_by_key(|r| (r.address, r.from_block));
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(event_id: i64, block: i64, args: serde_json::Value) -> ChainEventRow {
        ChainEventRow {
            event_id,
            block_number: block,
            tx_index: 0,
            log_index: 0,
            event_name: "YieldBlacklistUpdated".to_string(),
            args,
            tx_signer: None,
        }
    }

    fn addr(b: u8) -> [u8; 20] {
        [b; 20]
    }
    fn addr_hex(b: u8) -> String {
        format!("0x{}", hex::encode(addr(b)))
    }

    #[test]
    fn yield_blacklist_add_then_remove_closes_one_interval() {
        let events = vec![
            ev(
                1,
                100,
                serde_json::json!({"account": addr_hex(0xAA), "isBlacklisted": true}),
            ),
            ev(
                2,
                200,
                serde_json::json!({"account": addr_hex(0xAA), "isBlacklisted": false}),
            ),
        ];
        let rows = build_yield_blacklist_intervals(&events);
        assert_eq!(
            rows,
            vec![YieldBlacklistIntervalRow {
                address: addr(0xAA),
                from_block: 100,
                to_block: Some(200),
                opened_by_event: 1,
                closed_by_event: Some(2),
            }]
        );
    }

    #[test]
    fn yield_blacklist_add_with_no_remove_stays_open() {
        let events = vec![ev(
            1,
            100,
            serde_json::json!({"account": addr_hex(0xAA), "isBlacklisted": true}),
        )];
        let rows = build_yield_blacklist_intervals(&events);
        assert_eq!(
            rows,
            vec![YieldBlacklistIntervalRow {
                address: addr(0xAA),
                from_block: 100,
                to_block: None,
                opened_by_event: 1,
                closed_by_event: None,
            }]
        );
    }

    #[test]
    fn yield_blacklist_zero_address_never_an_interval() {
        let events = vec![ev(
            1,
            100,
            serde_json::json!({"account": addr_hex(0), "isBlacklisted": true}),
        )];
        assert_eq!(build_yield_blacklist_intervals(&events), vec![]);
    }

    #[test]
    fn yield_blacklist_idempotent_re_add_keeps_original_open() {
        let events = vec![
            ev(
                1,
                100,
                serde_json::json!({"account": addr_hex(0xAA), "isBlacklisted": true}),
            ),
            ev(
                2,
                150,
                serde_json::json!({"account": addr_hex(0xAA), "isBlacklisted": true}),
            ),
        ];
        let rows = build_yield_blacklist_intervals(&events);
        assert_eq!(
            rows,
            vec![YieldBlacklistIntervalRow {
                address: addr(0xAA),
                from_block: 100,
                to_block: None,
                opened_by_event: 1,
                closed_by_event: None,
            }],
            "a redundant re-add must not re-open (and must not lose) the original from_block"
        );
    }

    #[test]
    fn yield_blacklist_stray_remove_is_a_no_op() {
        let events = vec![ev(
            1,
            100,
            serde_json::json!({"account": addr_hex(0xAA), "isBlacklisted": false}),
        )];
        assert_eq!(build_yield_blacklist_intervals(&events), vec![]);
    }
}
