//! `role_interval` (P4b-ii) — ← `RoleGranted(role, account, sender)` /
//! `RoleRevoked(role, account, sender)`, SOURCE-scoped: an open/close
//! interval per `(role, account)` on ONE source contract, same open/close
//! shape as [`super::allowlist`]/[`super::yield_blacklist`] but keyed on a
//! 2-tuple instead of a bare address (`sender` — who granted/revoked it —
//! is not part of the key; only `(role, account)` identifies an interval).
//!
//! **Built ONCE per DISTINCT source address** (module doc on
//! [`super::rebuild::rebuild_tier2`]): the table's PK is
//! `(chain_id, source_contract, role, account, from_block)`, and a source
//! contract shared by two assets (a common storefront, factory, or router)
//! must contribute exactly ONE row set, never one per asset that happens to
//! reference it — the PK itself is the tripwire for a caller that gets this
//! wrong (a double-build would violate the PK on the second insert).
//! `rebuild_tier2` computes the union of every configured asset's
//! role-bearing sources into a `BTreeSet<[u8; 20]>` before calling
//! [`build_role_intervals`], so this module itself is agnostic to how many
//! assets share the source it's given.
//!
//! [`super::code_change::signer_attribution`] is this table's consumer:
//! "does `signer` hold role `X` on source `S` at block `B`" is answered by
//! looking up `S` in the `role_intervals_by_source` map `rebuild_tier2`
//! keeps in memory (never persisted separately — the DB table IS that map,
//! this is just its build-time in-memory form).

use std::collections::BTreeMap;

use super::fetch::{arg_address, arg_bytes32, ChainEventRow};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleIntervalRow {
    pub role: [u8; 32],
    pub account: [u8; 20],
    pub from_block: i64,
    pub to_block: Option<i64>,
    pub opened_by_event: i64,
    pub closed_by_event: Option<i64>,
}

/// `events` MUST already be `RoleGranted`/`RoleRevoked` rows from ONE
/// source contract, in `(block_number, tx_index, log_index)` order. Keyed
/// on `(role, account)` — a stray revoke (nothing open) is a no-op; a
/// redundant grant while already open is idempotent (keeps the original
/// `from_block`, same discipline as `allowlist`/`yield_blacklist`).
pub fn build_role_intervals(events: &[ChainEventRow]) -> Vec<RoleIntervalRow> {
    // (role, account) -> (from_block, opened_by_event) for the
    // currently-open interval.
    let mut open: BTreeMap<([u8; 32], [u8; 20]), (i64, i64)> = BTreeMap::new();
    let mut closed = Vec::new();

    for ev in events {
        let Some(role) = arg_bytes32(&ev.args, "role") else {
            continue;
        };
        let Some(account) = arg_address(&ev.args, "account") else {
            continue;
        };
        let key = (role, account);

        match ev.event_name.as_str() {
            "RoleGranted" => {
                open.entry(key).or_insert((ev.block_number, ev.event_id));
            }
            "RoleRevoked" => {
                if let Some((from_block, opened_by_event)) = open.remove(&key) {
                    closed.push(RoleIntervalRow {
                        role,
                        account,
                        from_block,
                        to_block: Some(ev.block_number),
                        opened_by_event,
                        closed_by_event: Some(ev.event_id),
                    });
                }
                // A revoke with nothing open (stray/duplicate revoke) is a
                // no-op — never a negative-duration or phantom interval.
            }
            _ => {}
        }
    }

    let mut result = closed;
    for ((role, account), (from_block, opened_by_event)) in open {
        result.push(RoleIntervalRow {
            role,
            account,
            from_block,
            to_block: None,
            opened_by_event,
            closed_by_event: None,
        });
    }
    result.sort_by_key(|r| (r.role, r.account, r.from_block));
    result
}

/// `signer` holds `role` on this source at `block`, INCLUSIVE both ends
/// (`from_block <= block <= to_block`), block-granular (P4b-ii's
/// `signer_attribution` — deliberately simpler than `gate_interval`'s
/// tuple-granular convention: a role-holding fact only needs to answer
/// "at this block", never "before/after this exact log within the block").
pub fn role_open_at(interval: &RoleIntervalRow, block: i64) -> bool {
    interval.from_block <= block && interval.to_block.is_none_or(|tb| block <= tb)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(event_id: i64, block: i64, name: &str, role: u8, account: u8) -> ChainEventRow {
        ChainEventRow {
            event_id,
            block_number: block,
            tx_index: 0,
            log_index: 0,
            event_name: name.to_string(),
            args: serde_json::json!({
                "role": format!("0x{}", hex::encode([role; 32])),
                "account": format!("0x{}", hex::encode([account; 20])),
                "sender": format!("0x{}", hex::encode([0x99u8; 20])),
            }),
            tx_signer: None,
        }
    }

    // 1. grant_then_revoke_closes_one_interval
    #[test]
    fn grant_then_revoke_closes_one_interval() {
        let events = vec![
            ev(1, 100, "RoleGranted", 0x01, 0xAA),
            ev(2, 200, "RoleRevoked", 0x01, 0xAA),
        ];
        let rows = build_role_intervals(&events);
        assert_eq!(
            rows,
            vec![RoleIntervalRow {
                role: [0x01; 32],
                account: [0xAA; 20],
                from_block: 100,
                to_block: Some(200),
                opened_by_event: 1,
                closed_by_event: Some(2),
            }]
        );
    }

    // 2. initialize_five_grants_open_five_intervals (5 roles one tx, key on
    // (role, account) — proves the key is the 2-tuple, not a bare address).
    #[test]
    fn initialize_five_grants_open_five_intervals() {
        let events: Vec<ChainEventRow> = (1..=5u8)
            .map(|role| ev(role as i64, 100, "RoleGranted", role, 0xAA))
            .collect();
        let rows = build_role_intervals(&events);
        assert_eq!(rows.len(), 5, "five distinct roles on the same account must open five intervals");
        for (i, role) in (1..=5u8).enumerate() {
            assert_eq!(rows[i].role, [role; 32]);
            assert_eq!(rows[i].account, [0xAA; 20]);
            assert_eq!(rows[i].to_block, None);
        }
    }

    // 3. grant_revoke_regrant_opens_second_interval
    #[test]
    fn grant_revoke_regrant_opens_second_interval() {
        let events = vec![
            ev(1, 100, "RoleGranted", 0x01, 0xAA),
            ev(2, 200, "RoleRevoked", 0x01, 0xAA),
            ev(3, 300, "RoleGranted", 0x01, 0xAA),
        ];
        let rows = build_role_intervals(&events);
        assert_eq!(
            rows,
            vec![
                RoleIntervalRow {
                    role: [0x01; 32],
                    account: [0xAA; 20],
                    from_block: 100,
                    to_block: Some(200),
                    opened_by_event: 1,
                    closed_by_event: Some(2),
                },
                RoleIntervalRow {
                    role: [0x01; 32],
                    account: [0xAA; 20],
                    from_block: 300,
                    to_block: None,
                    opened_by_event: 3,
                    closed_by_event: None,
                },
            ]
        );
    }

    // 4. redundant_grant_keeps_original_from_block
    #[test]
    fn redundant_grant_keeps_original_from_block() {
        let events = vec![
            ev(1, 100, "RoleGranted", 0x01, 0xAA),
            ev(2, 150, "RoleGranted", 0x01, 0xAA),
        ];
        let rows = build_role_intervals(&events);
        assert_eq!(
            rows,
            vec![RoleIntervalRow {
                role: [0x01; 32],
                account: [0xAA; 20],
                from_block: 100,
                to_block: None,
                opened_by_event: 1,
                closed_by_event: None,
            }],
            "a redundant re-grant must not re-open (and must not lose) the original from_block"
        );
    }

    // 5. stray_revoke_without_grant_is_noop
    #[test]
    fn stray_revoke_without_grant_is_noop() {
        let events = vec![ev(1, 100, "RoleRevoked", 0x01, 0xAA)];
        assert_eq!(build_role_intervals(&events), vec![]);
    }

    // 6. distinct_roles_do_not_interact
    #[test]
    fn distinct_roles_do_not_interact() {
        let events = vec![
            ev(1, 100, "RoleGranted", 0x01, 0xAA),
            ev(2, 200, "RoleRevoked", 0x02, 0xAA), // different role, same account — no-op
        ];
        let rows = build_role_intervals(&events);
        assert_eq!(
            rows,
            vec![RoleIntervalRow {
                role: [0x01; 32],
                account: [0xAA; 20],
                from_block: 100,
                to_block: None,
                opened_by_event: 1,
                closed_by_event: None,
            }],
            "role 0x01's grant must stay open — role 0x02's revoke targets a different key entirely"
        );
    }

    #[test]
    fn role_open_at_is_inclusive_both_ends() {
        let interval = RoleIntervalRow {
            role: [0x01; 32],
            account: [0xAA; 20],
            from_block: 100,
            to_block: Some(200),
            opened_by_event: 1,
            closed_by_event: Some(2),
        };
        assert!(role_open_at(&interval, 100));
        assert!(role_open_at(&interval, 200));
        assert!(role_open_at(&interval, 150));
        assert!(!role_open_at(&interval, 99));
        assert!(!role_open_at(&interval, 201));
    }
}
