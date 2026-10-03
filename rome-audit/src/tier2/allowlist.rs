//! `allowlist_interval` — ← `WhitelistStatusChanged(account, isWhitelisted)`
//! only (IMPL-PLAN §2: the canonical signal; `Added`/`RemovedFromWhitelist`
//! are redundant and never separately counted — this builder doesn't even
//! look at them). An add opens an interval per address; a remove closes it.
//! `address(0)` never appears on-chain (gated mint/burn revert, spec §4.1)
//! — defended here too (skipped, never inserted) so a malformed upstream
//! decode can't smuggle a phantom interval in.

use std::collections::BTreeMap;

use super::fetch::{arg_address, arg_bool, ChainEventRow, ZERO_ADDRESS};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllowlistIntervalRow {
    pub address: [u8; 20],
    pub from_block: i64,
    pub to_block: Option<i64>,
    pub opened_by_event: i64,
    pub closed_by_event: Option<i64>,
}

/// `events` MUST already be `WhitelistStatusChanged` rows from one Axis-1
/// module, in `(block_number, tx_index, log_index)` order (`fetch::fetch_events`
/// guarantees this) — other event names are ignored defensively, not relied
/// on as a filter.
pub fn build_allowlist_intervals(events: &[ChainEventRow]) -> Vec<AllowlistIntervalRow> {
    // address -> (from_block, opened_by_event) for the currently-open interval.
    let mut open: BTreeMap<[u8; 20], (i64, i64)> = BTreeMap::new();
    let mut closed = Vec::new();

    for ev in events {
        if ev.event_name != "WhitelistStatusChanged" {
            continue;
        }
        let Some(account) = arg_address(&ev.args, "account") else {
            continue;
        };
        let Some(is_whitelisted) = arg_bool(&ev.args, "isWhitelisted") else {
            continue;
        };
        if account == ZERO_ADDRESS {
            continue; // never real on-chain; defensive skip
        }

        if is_whitelisted {
            // Idempotent against a redundant add while already open: `entry`
            // only inserts when absent, so a repeat add never re-opens (and
            // never loses) the existing interval's `from_block`.
            open.entry(account)
                .or_insert((ev.block_number, ev.event_id));
        } else if let Some((from_block, opened_by_event)) = open.remove(&account) {
            closed.push(AllowlistIntervalRow {
                address: account,
                from_block,
                to_block: Some(ev.block_number),
                opened_by_event,
                closed_by_event: Some(ev.event_id),
            });
        }
        // A remove with nothing open (stray/duplicate remove) is a no-op —
        // never a negative-duration or phantom interval.
    }

    let mut result = closed;
    for (address, (from_block, opened_by_event)) in open {
        result.push(AllowlistIntervalRow {
            address,
            from_block,
            to_block: None,
            opened_by_event,
            closed_by_event: None,
        });
    }
    // Deterministic output order independent of the closed/open split above
    // (which depends only on chain_event content, but sorting makes
    // rebuild-identity trivially visible in a test assertion too).
    result.sort_by_key(|r| (r.address, r.from_block));
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(event_id: i64, block: i64, name: &str, args: serde_json::Value) -> ChainEventRow {
        ChainEventRow {
            event_id,
            block_number: block,
            tx_index: 0,
            log_index: 0,
            event_name: name.to_string(),
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
    fn add_then_remove_closes_one_interval() {
        let events = vec![
            ev(
                1,
                100,
                "WhitelistStatusChanged",
                serde_json::json!({"account": addr_hex(0xAA), "isWhitelisted": true}),
            ),
            ev(
                2,
                200,
                "WhitelistStatusChanged",
                serde_json::json!({"account": addr_hex(0xAA), "isWhitelisted": false}),
            ),
        ];
        let rows = build_allowlist_intervals(&events);
        assert_eq!(
            rows,
            vec![AllowlistIntervalRow {
                address: addr(0xAA),
                from_block: 100,
                to_block: Some(200),
                opened_by_event: 1,
                closed_by_event: Some(2),
            }]
        );
    }

    #[test]
    fn add_with_no_remove_stays_open() {
        let events = vec![ev(
            1,
            100,
            "WhitelistStatusChanged",
            serde_json::json!({"account": addr_hex(0xAA), "isWhitelisted": true}),
        )];
        let rows = build_allowlist_intervals(&events);
        assert_eq!(
            rows,
            vec![AllowlistIntervalRow {
                address: addr(0xAA),
                from_block: 100,
                to_block: None,
                opened_by_event: 1,
                closed_by_event: None,
            }]
        );
    }

    #[test]
    fn zero_address_never_produces_an_interval() {
        let events = vec![ev(
            1,
            100,
            "WhitelistStatusChanged",
            serde_json::json!({"account": addr_hex(0), "isWhitelisted": true}),
        )];
        assert_eq!(build_allowlist_intervals(&events), vec![]);
    }
}
