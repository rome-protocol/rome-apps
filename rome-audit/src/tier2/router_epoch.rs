//! `router_sanctions_epoch` — ← `ModuleTypeRegistered` (opens an epoch) /
//! `ModuleTypeRemoved` + `GlobalImplementationUpdated` (close it), filtered
//! to `typeId == GLOBAL_SANCTIONS_TYPE` (spec §4.2 — these three events fire
//! for every registered module type on a `RestrictionsRouter`, not just
//! sanctions). Router-scoped, chain-global — NOT per-asset.

use crate::abi::router::GLOBAL_SANCTIONS_TYPE;

use super::fetch::{arg_address, arg_bool, arg_bytes32, ChainEventRow, ZERO_ADDRESS};
use super::gate::Position;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouterEpochRow {
    pub module_address: [u8; 20],
    pub from_block: i64,
    pub to_block: Option<i64>,
    /// Full position of the opening event — P4b-i addition, mirroring
    /// [`super::gate::GateIntervalRow`]'s M2 boundary convention. **DB shape
    /// unchanged** (`audit.router_sanctions_epoch` stays block-granular,
    /// migration 0003) — these live on the in-memory row only, for
    /// [`Self::contains_position`], which [`super::screening`]'s
    /// `screening_gap` builder needs: block-only scoping would treat a
    /// same-block, pre-registration transfer as already inside the epoch
    /// (a false gap).
    pub open_tx_index: i32,
    pub open_log_index: i32,
    /// Full position of the closing event, `None` iff the epoch is still open.
    pub close_tx_index: Option<i32>,
    pub close_log_index: Option<i32>,
    pub opened_by_event: i64,
    pub closed_by_event: Option<i64>,
}

impl RouterEpochRow {
    pub fn open_position(&self) -> Position {
        (self.from_block, self.open_tx_index, self.open_log_index)
    }

    pub fn close_position(&self) -> Option<Position> {
        match (self.to_block, self.close_tx_index, self.close_log_index) {
            (Some(b), Some(t), Some(l)) => Some((b, t, l)),
            _ => None,
        }
    }

    /// True iff `pos` sits inside this epoch's half-open `[open_position,
    /// close_position)` — the same M2 convention as
    /// [`super::gate::GateIntervalRow::contains_position`].
    pub fn contains_position(&self, pos: Position) -> bool {
        pos >= self.open_position() && self.close_position().is_none_or(|cp| pos < cp)
    }
}

/// `events` MUST already be `ModuleTypeRegistered`/`ModuleTypeRemoved`/
/// `GlobalImplementationUpdated` rows from one router, in `(block_number,
/// tx_index, log_index)` order. Non-`GLOBAL_SANCTIONS_TYPE` registrations
/// for other module types on the same router are ignored.
pub fn build_router_epochs(events: &[ChainEventRow]) -> Vec<RouterEpochRow> {
    let mut result = Vec::new();
    // (module_address, from_block, open_tx_index, open_log_index,
    // opened_by_event) of the currently-open epoch.
    let mut current: Option<([u8; 20], i64, i32, i32, i64)> = None;

    for ev in events {
        match ev.event_name.as_str() {
            "ModuleTypeRegistered" => {
                let Some(type_id) = arg_bytes32(&ev.args, "typeId") else {
                    continue;
                };
                if type_id != GLOBAL_SANCTIONS_TYPE {
                    continue;
                }
                let Some(is_global) = arg_bool(&ev.args, "isGlobal") else {
                    continue;
                };
                if !is_global {
                    continue;
                }
                let Some(module) = arg_address(&ev.args, "globalImplementation") else {
                    continue;
                };
                // A registration landing on top of an already-open epoch
                // (re-registering the type) closes the previous one first —
                // defensive; not the expected path (ModuleTypeRegistered
                // normally fires once per type on a router).
                if let Some((prev_module, from_block, open_tx_index, open_log_index, opened_by_event)) =
                    current.take()
                {
                    result.push(RouterEpochRow {
                        module_address: prev_module,
                        from_block,
                        to_block: Some(ev.block_number),
                        open_tx_index,
                        open_log_index,
                        close_tx_index: Some(ev.tx_index),
                        close_log_index: Some(ev.log_index),
                        opened_by_event,
                        closed_by_event: Some(ev.event_id),
                    });
                }
                if module != ZERO_ADDRESS {
                    current = Some((module, ev.block_number, ev.tx_index, ev.log_index, ev.event_id));
                }
            }
            "ModuleTypeRemoved" => {
                let Some(type_id) = arg_bytes32(&ev.args, "typeId") else {
                    continue;
                };
                if type_id != GLOBAL_SANCTIONS_TYPE {
                    continue;
                }
                if let Some((module, from_block, open_tx_index, open_log_index, opened_by_event)) =
                    current.take()
                {
                    result.push(RouterEpochRow {
                        module_address: module,
                        from_block,
                        to_block: Some(ev.block_number),
                        open_tx_index,
                        open_log_index,
                        close_tx_index: Some(ev.tx_index),
                        close_log_index: Some(ev.log_index),
                        opened_by_event,
                        closed_by_event: Some(ev.event_id),
                    });
                }
            }
            "GlobalImplementationUpdated" => {
                let Some(type_id) = arg_bytes32(&ev.args, "typeId") else {
                    continue;
                };
                if type_id != GLOBAL_SANCTIONS_TYPE {
                    continue;
                }
                if let Some((module, from_block, open_tx_index, open_log_index, opened_by_event)) =
                    current.take()
                {
                    result.push(RouterEpochRow {
                        module_address: module,
                        from_block,
                        to_block: Some(ev.block_number),
                        open_tx_index,
                        open_log_index,
                        close_tx_index: Some(ev.tx_index),
                        close_log_index: Some(ev.log_index),
                        opened_by_event,
                        closed_by_event: Some(ev.event_id),
                    });
                }
                // A non-zero new implementation reopens a fresh epoch
                // immediately (the router stays sanctions-enforced, just
                // against a different module implementation); a zero
                // implementation leaves the epoch closed (un-set).
                if let Some(new_module) = arg_address(&ev.args, "newGlobalImplementation") {
                    if new_module != ZERO_ADDRESS {
                        current = Some((new_module, ev.block_number, ev.tx_index, ev.log_index, ev.event_id));
                    }
                }
            }
            _ => {}
        }
    }

    if let Some((module, from_block, open_tx_index, open_log_index, opened_by_event)) = current {
        result.push(RouterEpochRow {
            module_address: module,
            from_block,
            to_block: None,
            open_tx_index,
            open_log_index,
            close_tx_index: None,
            close_log_index: None,
            opened_by_event,
            closed_by_event: None,
        });
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr_hex(b: u8) -> String {
        format!("0x{}", hex::encode([b; 20]))
    }
    fn type_id_hex() -> String {
        format!("0x{}", hex::encode(GLOBAL_SANCTIONS_TYPE))
    }
    fn other_type_id_hex() -> String {
        format!("0x{}", hex::encode([0x99u8; 32]))
    }

    fn registered_ev(event_id: i64, block: i64, type_id: String, module: u8) -> ChainEventRow {
        ChainEventRow {
            event_id,
            block_number: block,
            tx_index: 0,
            log_index: 0,
            event_name: "ModuleTypeRegistered".to_string(),
            args: serde_json::json!({
                "typeId": type_id, "isGlobal": true, "globalImplementation": addr_hex(module)
            }),
            tx_signer: None,
        }
    }
    fn removed_ev(event_id: i64, block: i64, type_id: String) -> ChainEventRow {
        ChainEventRow {
            event_id,
            block_number: block,
            tx_index: 0,
            log_index: 0,
            event_name: "ModuleTypeRemoved".to_string(),
            args: serde_json::json!({ "typeId": type_id }),
            tx_signer: None,
        }
    }

    #[test]
    fn registered_with_no_removal_stays_open() {
        let events = vec![registered_ev(1, 100, type_id_hex(), 0x11)];
        let rows = build_router_epochs(&events);
        assert_eq!(
            rows,
            vec![RouterEpochRow {
                module_address: [0x11; 20],
                from_block: 100,
                to_block: None,
                open_tx_index: 0,
                open_log_index: 0,
                close_tx_index: None,
                close_log_index: None,
                opened_by_event: 1,
                closed_by_event: None,
            }]
        );
    }

    #[test]
    fn registered_then_removed_closes_epoch() {
        let events = vec![
            registered_ev(1, 100, type_id_hex(), 0x11),
            removed_ev(2, 200, type_id_hex()),
        ];
        let rows = build_router_epochs(&events);
        assert_eq!(
            rows,
            vec![RouterEpochRow {
                module_address: [0x11; 20],
                from_block: 100,
                to_block: Some(200),
                open_tx_index: 0,
                open_log_index: 0,
                close_tx_index: Some(0),
                close_log_index: Some(0),
                opened_by_event: 1,
                closed_by_event: Some(2),
            }]
        );
    }

    #[test]
    fn contains_position_is_half_open_on_the_full_tuple_not_just_block() {
        // M2 mutation target: a block-granular check would treat a position
        // in the SAME block as the open event, but BEFORE it, as inside.
        let epoch = RouterEpochRow {
            module_address: [0x11; 20],
            from_block: 100,
            to_block: None,
            open_tx_index: 1,
            open_log_index: 0,
            close_tx_index: None,
            close_log_index: None,
            opened_by_event: 1,
            closed_by_event: None,
        };
        assert!(!epoch.contains_position((100, 0, 5)), "before the open position, same block — outside");
        assert!(epoch.contains_position((100, 1, 0)), "at the open position — inside");
        assert!(epoch.contains_position((100, 3, 9)), "after the open position — inside");
    }

    #[test]
    fn other_module_types_on_the_same_router_are_ignored() {
        let events = vec![
            registered_ev(1, 100, other_type_id_hex(), 0x22),
            registered_ev(2, 150, type_id_hex(), 0x11),
        ];
        let rows = build_router_epochs(&events);
        assert_eq!(
            rows,
            vec![RouterEpochRow {
                module_address: [0x11; 20],
                from_block: 150,
                to_block: None,
                open_tx_index: 0,
                open_log_index: 0,
                close_tx_index: None,
                close_log_index: None,
                opened_by_event: 2,
                closed_by_event: None,
            }]
        );
    }
}
