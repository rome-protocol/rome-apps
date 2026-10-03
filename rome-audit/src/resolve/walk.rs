//! The fixed-point interval-builder (capture §1.2): every "walk this
//! source's set-address history" step — module swaps, yield-token
//! re-pointing, purchase-token re-pointing — reduces to the same shape:
//! a chronological sequence of "this address was set to X here" events,
//! folded into block intervals where each event OPENS a new interval and
//! CLOSES the immediately preceding one at the same block. The final event's
//! interval stays open (`to_block: None` — still in scope as of resolution
//! time).

/// One "address was set" event, in the caller's own decode order (this
/// function sorts, so callers don't need to pre-sort).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SetEvent {
    pub block_number: i64,
    pub tx_index: i32,
    pub log_index: i32,
    pub address: [u8; 20],
}

/// Folds a set-event history into `(address, from_block, to_block)` triples.
/// Sorts by `(block_number, tx_index, log_index)` first (capture §1.2's
/// total order) — callers may hand this events in RPC-response order, which
/// is not guaranteed to be chronological.
///
/// Two consecutive events that happen to set the SAME address are NOT
/// coalesced (unlike Tier-2's `gate_interval`) — a re-affirming set is a
/// real, distinct on-chain event and its own interval boundary; capture
/// spec's superset-capture principle (§1.3) argues against silently
/// collapsing anything at capture time.
pub fn fold_intervals(mut events: Vec<SetEvent>) -> Vec<([u8; 20], i64, Option<i64>)> {
    events.sort_by_key(|e| (e.block_number, e.tx_index, e.log_index));
    let mut out = Vec::with_capacity(events.len());
    for (i, ev) in events.iter().enumerate() {
        let to_block = events.get(i + 1).map(|next| next.block_number);
        out.push((ev.address, ev.block_number, to_block));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(b: u8) -> [u8; 20] {
        [b; 20]
    }

    #[test]
    fn single_event_stays_open() {
        let events = vec![SetEvent {
            block_number: 100,
            tx_index: 0,
            log_index: 0,
            address: addr(1),
        }];
        let intervals = fold_intervals(events);
        assert_eq!(intervals, vec![(addr(1), 100, None)]);
    }

    #[test]
    fn two_events_close_the_first_open_the_second() {
        let events = vec![
            SetEvent {
                block_number: 100,
                tx_index: 0,
                log_index: 0,
                address: addr(1),
            },
            SetEvent {
                block_number: 200,
                tx_index: 0,
                log_index: 0,
                address: addr(2),
            },
        ];
        let intervals = fold_intervals(events);
        assert_eq!(
            intervals,
            vec![(addr(1), 100, Some(200)), (addr(2), 200, None)]
        );
    }

    #[test]
    fn out_of_order_input_is_sorted_first() {
        let events = vec![
            SetEvent {
                block_number: 200,
                tx_index: 0,
                log_index: 0,
                address: addr(2),
            },
            SetEvent {
                block_number: 100,
                tx_index: 0,
                log_index: 0,
                address: addr(1),
            },
        ];
        let intervals = fold_intervals(events);
        assert_eq!(
            intervals,
            vec![(addr(1), 100, Some(200)), (addr(2), 200, None)]
        );
    }
}
