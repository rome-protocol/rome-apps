//! `sanction_denyset_interval` — ← `Sanctioned(account)` / `Unsanctioned(account)`,
//! keyed by MODULE (chain-global — spec §4.2: "router-scoped, not per-token
//! and not per-asset"). Same open/close-per-address shape as
//! [`super::allowlist`], but two distinct event names instead of one bool arg.

use std::collections::BTreeMap;

use super::fetch::{arg_address, ChainEventRow};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DenysetIntervalRow {
    pub address: [u8; 20],
    pub from_block: i64,
    pub to_block: Option<i64>,
    pub opened_by_event: i64,
    pub closed_by_event: Option<i64>,
}

/// `events` MUST already be `Sanctioned`/`Unsanctioned` rows from one
/// `GlobalSanctions` module, in `(block_number, tx_index, log_index)` order.
pub fn build_denyset_intervals(events: &[ChainEventRow]) -> Vec<DenysetIntervalRow> {
    let mut open: BTreeMap<[u8; 20], (i64, i64)> = BTreeMap::new();
    let mut closed = Vec::new();

    for ev in events {
        match ev.event_name.as_str() {
            "Sanctioned" => {
                let Some(account) = arg_address(&ev.args, "account") else {
                    continue;
                };
                open.entry(account)
                    .or_insert((ev.block_number, ev.event_id));
            }
            "Unsanctioned" => {
                let Some(account) = arg_address(&ev.args, "account") else {
                    continue;
                };
                if let Some((from_block, opened_by_event)) = open.remove(&account) {
                    closed.push(DenysetIntervalRow {
                        address: account,
                        from_block,
                        to_block: Some(ev.block_number),
                        opened_by_event,
                        closed_by_event: Some(ev.event_id),
                    });
                }
            }
            _ => {}
        }
    }

    let mut result = closed;
    for (address, (from_block, opened_by_event)) in open {
        result.push(DenysetIntervalRow {
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

    fn ev(event_id: i64, block: i64, name: &str, account: [u8; 20]) -> ChainEventRow {
        ChainEventRow {
            event_id,
            block_number: block,
            tx_index: 0,
            log_index: 0,
            event_name: name.to_string(),
            args: serde_json::json!({ "account": format!("0x{}", hex::encode(account)) }),
            tx_signer: None,
        }
    }

    #[test]
    fn sanction_then_unsanction_closes_one_interval() {
        let events = vec![
            ev(1, 100, "Sanctioned", [0xAA; 20]),
            ev(2, 200, "Unsanctioned", [0xAA; 20]),
        ];
        let rows = build_denyset_intervals(&events);
        assert_eq!(
            rows,
            vec![DenysetIntervalRow {
                address: [0xAA; 20],
                from_block: 100,
                to_block: Some(200),
                opened_by_event: 1,
                closed_by_event: Some(2),
            }]
        );
    }

    #[test]
    fn sanction_with_no_unsanction_stays_open() {
        let events = vec![ev(1, 100, "Sanctioned", [0xAA; 20])];
        let rows = build_denyset_intervals(&events);
        assert_eq!(
            rows,
            vec![DenysetIntervalRow {
                address: [0xAA; 20],
                from_block: 100,
                to_block: None,
                opened_by_event: 1,
                closed_by_event: None,
            }]
        );
    }
}
