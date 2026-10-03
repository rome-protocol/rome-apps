//! `code_change` (P4b-ii) — governance/attribution correlations over the
//! per-asset code/module surface: which implementation a token/storefront
//! upgraded to, which module a restriction-type slot points at, which
//! typeId a router's global-module registry carries, and WHO signed the
//! transaction that changed it (`signer_attribution`).
//!
//! **`before` is NEVER read from event args** — it is always the previous
//! row's `after` within the SAME partition `(asset_id, kind, subject)`,
//! ordered `(block_number, tx_index, log_index)`. The first row in a
//! partition has `before = NULL`. This is deliberate: an event like
//! `Upgraded(implementation)` only carries the NEW value; the "from" value
//! is a derived fact of this table's own history, never re-derived from
//! chain state.
//!
//! **FACTORY-MEDIATED UPGRADE = TWO rows, no dedupe (documented decision).**
//! `ArcTokenFactory.upgradeToken` calls `UUPSUpgradeable(token).upgradeToAndCall`,
//! which itself emits the token's OWN `Upgraded(newImplementation)` (log N),
//! THEN the factory emits its own `TokenUpgraded(token, newImplementation)`
//! (log N+1) — both in the SAME tx. Both merge into the SAME
//! `(asset_id, UPGRADE, token_address)` partition ordered by log position,
//! so an upgrade from X to Y done THIS way produces two consecutive rows:
//! `(before=X, after=Y)` then `(before=Y, after=Y)` — a self-loop, not a
//! duplicate. This is intentional: the two events are genuinely two
//! DIFFERENT contracts' logs, and this table's job is to record every
//! upgrade-shaped fact that fired, not to collapse same-tx co-emissions.
//!
//! **ROUTER_GLOBAL / STOREFRONT_UPGRADE are FAN-OUT kinds** — one shared
//! router/storefront's event stream produces the SAME `(before, after,
//! event_id)` chain for every asset that references it, but a DIFFERENT
//! `signer_attribution` per asset (attribution is asset-scoped: the same
//! transaction signer might hold a role on asset A's own token but not on
//! asset B's, even though both share the router that changed).
//!
//! **Known gap, deferred on purpose (MED-1, P4b-ii review):** the
//! router's and factory's OWN `Upgraded` events (their own UUPS
//! implementation swap — `abi::router`/`abi::factory` register it, so
//! Tier-1 captures it) are NOT correlated into any `code_change` row here.
//! A router/factory upgrade is a governance change that can affect every
//! asset wired to it, exactly the same fan-out shape `ROUTER_GLOBAL` /
//! `STOREFRONT_UPGRADE` already have — but adding a `ROUTER_UPGRADE`/
//! `FACTORY_UPGRADE` kind needs its own CHECK-constraint migration and a
//! spec decision on shape, both out of this phase's 5-kind scope. Not an
//! oversight — a scoped-out decision, recorded here so it doesn't read as
//! a silent capture-vs-correlate gap.

use std::collections::HashMap;

use super::fetch::{arg_address, arg_bytes32, ChainEventRow};
use super::role_interval::{role_open_at, RoleIntervalRow};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodeChangeKind {
    Upgrade,
    ModuleSet,
    ModuleLinked,
    RouterGlobal,
    StorefrontUpgrade,
}

impl CodeChangeKind {
    pub fn as_db_str(&self) -> &'static str {
        match self {
            CodeChangeKind::Upgrade => "UPGRADE",
            CodeChangeKind::ModuleSet => "MODULE_SET",
            CodeChangeKind::ModuleLinked => "MODULE_LINKED",
            CodeChangeKind::RouterGlobal => "ROUTER_GLOBAL",
            CodeChangeKind::StorefrontUpgrade => "STOREFRONT_UPGRADE",
        }
    }
}

/// `USER` is in the DB `CHECK` constraint's vocabulary but is NEVER emitted
/// by any builder in this crate (P5 — Solana-lane synthetic-sender
/// attribution is out of scope here). Declared honestly rather than
/// omitted: a future P5 builder can emit it without a migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignerAttribution {
    IssuerKey,
    RomeMultisig,
    User,
    Unknown,
}

impl SignerAttribution {
    pub fn as_db_str(&self) -> &'static str {
        match self {
            SignerAttribution::IssuerKey => "ISSUER_KEY",
            SignerAttribution::RomeMultisig => "ROME_MULTISIG",
            SignerAttribution::User => "USER",
            SignerAttribution::Unknown => "UNKNOWN",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodeChangeRow {
    pub kind: CodeChangeKind,
    /// Variable width: a 20-byte address (`UPGRADE`/`STOREFRONT_UPGRADE`'s
    /// token/storefront subject) or a 32-byte typeId
    /// (`MODULE_SET`/`MODULE_LINKED`/`ROUTER_GLOBAL`'s subject).
    pub subject: Vec<u8>,
    pub block_number: i64,
    pub tx_index: i32,
    pub log_index: i32,
    pub before: Option<[u8; 20]>,
    pub after: Option<[u8; 20]>,
    pub signer_attribution: SignerAttribution,
    pub event_id: i64,
}

/// A code-change fact's position + before/after chain, BEFORE
/// signer-attribution — the intermediate shape [`chain_raw`] produces and
/// every `build_*_changes` fn in this module returns. Kept separate from
/// [`CodeChangeRow`] so the chain (position-only, no config dependency) and
/// the attribution (config-dependent: role sources + multisig) can each be
/// unit-tested independently — exactly the ordered test list's split
/// between tests 8-15 (chain) and 16-22 (`signer_attribution`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainedChange {
    pub kind: CodeChangeKind,
    pub subject: Vec<u8>,
    pub block_number: i64,
    pub tx_index: i32,
    pub log_index: i32,
    pub before: Option<[u8; 20]>,
    pub after: Option<[u8; 20]>,
    pub event_id: i64,
    pub tx_signer: Option<[u8; 20]>,
}

impl ChainedChange {
    /// Attributes this change's signer — see [`signer_attribution`] for the
    /// precedence rules — and produces the DB-ready [`CodeChangeRow`].
    pub fn attribute(
        &self,
        role_sources: &[[u8; 20]],
        role_intervals: &HashMap<[u8; 20], Vec<RoleIntervalRow>>,
        multisig: Option<[u8; 20]>,
    ) -> CodeChangeRow {
        CodeChangeRow {
            kind: self.kind,
            subject: self.subject.clone(),
            block_number: self.block_number,
            tx_index: self.tx_index,
            log_index: self.log_index,
            before: self.before,
            after: self.after,
            event_id: self.event_id,
            signer_attribution: signer_attribution(
                self.tx_signer,
                self.block_number,
                role_sources,
                role_intervals,
                multisig,
            ),
        }
    }
}

/// One extracted (not-yet-chained) fact: `subject` + the new `after` value
/// (`None` only for a ROUTER_GLOBAL `ModuleTypeRemoved`) + the event it
/// came from.
type RawChange<'a> = (Vec<u8>, Option<[u8; 20]>, &'a ChainEventRow);

/// Sorts `raw` by `(subject, block_number, tx_index, log_index)` and
/// computes the `before`-chain per subject: row N's `before` = row N-1's
/// `after` within the SAME subject; the first row of a new subject gets
/// `before = None`. Final output is re-sorted to natural total order
/// `(block_number, tx_index, log_index, subject)` — the chain values
/// themselves don't depend on this final order, only on the per-subject
/// walk above.
fn chain_raw(kind: CodeChangeKind, mut raw: Vec<RawChange>) -> Vec<ChainedChange> {
    raw.sort_by(|a, b| {
        (&a.0, a.2.block_number, a.2.tx_index, a.2.log_index).cmp(&(
            &b.0,
            b.2.block_number,
            b.2.tx_index,
            b.2.log_index,
        ))
    });

    let mut result = Vec::new();
    let mut prev: Option<(Vec<u8>, Option<[u8; 20]>)> = None;
    for (subject, after, ev) in raw {
        let before = match &prev {
            Some((s, a)) if *s == subject => *a,
            _ => None,
        };
        result.push(ChainedChange {
            kind,
            subject: subject.clone(),
            block_number: ev.block_number,
            tx_index: ev.tx_index,
            log_index: ev.log_index,
            before,
            after,
            event_id: ev.event_id,
            tx_signer: ev.tx_signer,
        });
        prev = Some((subject, after));
    }

    result.sort_by(|a, b| {
        (a.block_number, a.tx_index, a.log_index, &a.subject).cmp(&(
            b.block_number,
            b.tx_index,
            b.log_index,
            &b.subject,
        ))
    });
    result
}

/// UPGRADE ← the token's OWN `Upgraded(implementation)` (source = the asset
/// token itself, `token_events`) merged with the factory's
/// `TokenUpgraded(token, newImplementation)` FILTERED to
/// `args.token == token_address` (`factory_events` — unfiltered input, the
/// filter is this fn's job, test 9's scope defect guard). Subject is fixed
/// at `token_address` for both sources.
pub fn build_upgrade_changes(
    token_events: &[ChainEventRow],
    factory_events: &[ChainEventRow],
    token_address: [u8; 20],
) -> Vec<ChainedChange> {
    let mut raw: Vec<RawChange> = Vec::new();
    for ev in token_events {
        if ev.event_name != "Upgraded" {
            continue;
        }
        let Some(after) = arg_address(&ev.args, "implementation") else {
            continue;
        };
        raw.push((token_address.to_vec(), Some(after), ev));
    }
    for ev in factory_events {
        if ev.event_name != "TokenUpgraded" {
            continue;
        }
        let Some(token) = arg_address(&ev.args, "token") else {
            continue;
        };
        if token != token_address {
            continue; // scoped to THIS asset's token — test 9
        }
        let Some(after) = arg_address(&ev.args, "newImplementation") else {
            continue;
        };
        raw.push((token_address.to_vec(), Some(after), ev));
    }
    chain_raw(CodeChangeKind::Upgrade, raw)
}

/// MODULE_SET ← the token's own
/// `SpecificRestrictionModuleSet(typeId, moduleAddress)`; subject = typeId
/// (partitions the chain per restriction type — test 11).
pub fn build_module_set_changes(token_events: &[ChainEventRow]) -> Vec<ChainedChange> {
    let mut raw: Vec<RawChange> = Vec::new();
    for ev in token_events {
        if ev.event_name != "SpecificRestrictionModuleSet" {
            continue;
        }
        let Some(type_id) = arg_bytes32(&ev.args, "typeId") else {
            continue;
        };
        let Some(after) = arg_address(&ev.args, "moduleAddress") else {
            continue;
        };
        raw.push((type_id.to_vec(), Some(after), ev));
    }
    chain_raw(CodeChangeKind::ModuleSet, raw)
}

/// MODULE_LINKED ← the factory's
/// `ModuleLinked(tokenAddress, moduleAddress, moduleType)`, FILTERED to
/// `args.tokenAddress == token_address` — NOTE the arg name is
/// `tokenAddress`, not `token` (test 10's exact-key trap: a wrong guessed
/// key means `arg_address` returns `None` and the row is silently, safely
/// dropped, never misattributed to the wrong field). Subject = moduleType.
pub fn build_module_linked_changes(
    factory_events: &[ChainEventRow],
    token_address: [u8; 20],
) -> Vec<ChainedChange> {
    let mut raw: Vec<RawChange> = Vec::new();
    for ev in factory_events {
        if ev.event_name != "ModuleLinked" {
            continue;
        }
        let Some(token) = arg_address(&ev.args, "tokenAddress") else {
            continue;
        };
        if token != token_address {
            continue;
        }
        let Some(module_type) = arg_bytes32(&ev.args, "moduleType") else {
            continue;
        };
        let Some(after) = arg_address(&ev.args, "moduleAddress") else {
            continue;
        };
        raw.push((module_type.to_vec(), Some(after), ev));
    }
    chain_raw(CodeChangeKind::ModuleLinked, raw)
}

/// ROUTER_GLOBAL ← ONE router's `ModuleTypeRegistered` /
/// `GlobalImplementationUpdated` / `ModuleTypeRemoved` stream — built ONCE
/// per router (no per-asset filtering; every typeId the router has ever
/// registered contributes, NOT filtered to `GLOBAL_SANCTIONS_TYPE` — this
/// is a broader table than `router_sanctions_epoch`). Callers fan the
/// SAME returned `Vec` out to every asset whose `router_address` equals
/// this router (test 12/14 pattern — `rebuild_tier2` re-attributes per
/// asset via [`ChainedChange::attribute`], never re-chains). `isGlobal =
/// false` forces `globalImplementation = address(0)` on-chain (verified
/// against `RestrictionsRouter.sol`) — stored verbatim as `Some([0;20])`,
/// distinct from `ModuleTypeRemoved`'s `after = None`.
pub fn build_router_global_changes(router_events: &[ChainEventRow]) -> Vec<ChainedChange> {
    let mut raw: Vec<RawChange> = Vec::new();
    for ev in router_events {
        match ev.event_name.as_str() {
            "ModuleTypeRegistered" => {
                let Some(type_id) = arg_bytes32(&ev.args, "typeId") else {
                    continue;
                };
                let Some(after) = arg_address(&ev.args, "globalImplementation") else {
                    continue;
                };
                raw.push((type_id.to_vec(), Some(after), ev));
            }
            "GlobalImplementationUpdated" => {
                let Some(type_id) = arg_bytes32(&ev.args, "typeId") else {
                    continue;
                };
                let Some(after) = arg_address(&ev.args, "newGlobalImplementation") else {
                    continue;
                };
                raw.push((type_id.to_vec(), Some(after), ev));
            }
            "ModuleTypeRemoved" => {
                let Some(type_id) = arg_bytes32(&ev.args, "typeId") else {
                    continue;
                };
                raw.push((type_id.to_vec(), None, ev));
            }
            _ => {}
        }
    }
    chain_raw(CodeChangeKind::RouterGlobal, raw)
}

/// STOREFRONT_UPGRADE ← ONE storefront's own `Upgraded(implementation)` —
/// built ONCE per storefront, subject fixed at the storefront's own
/// address; callers fan the result out to every asset sharing it (test 14).
pub fn build_storefront_upgrade_changes(
    storefront_events: &[ChainEventRow],
    storefront: [u8; 20],
) -> Vec<ChainedChange> {
    let mut raw: Vec<RawChange> = Vec::new();
    for ev in storefront_events {
        if ev.event_name != "Upgraded" {
            continue;
        }
        let Some(after) = arg_address(&ev.args, "implementation") else {
            continue;
        };
        raw.push((storefront.to_vec(), Some(after), ev));
    }
    chain_raw(CodeChangeKind::StorefrontUpgrade, raw)
}

/// Attributes a state-changing tx's signer to one of four buckets, in this
/// PRECEDENCE order (tests 16-21):
/// 1. No `tx_signer` at all ⇒ `UNKNOWN` (checked first — everything below
///    needs a signer to compare against).
/// 2. `multisig` is configured AND `signer == multisig` ⇒ `ROME_MULTISIG`
///    (checked BEFORE any role lookup — test 16: a multisig signer that
///    also happens to hold a role must still read `ROME_MULTISIG`, never
///    `ISSUER_KEY`).
/// 3. `multisig` is `None` ⇒ `ROME_MULTISIG` is structurally unreachable
///    (test 19 — even a signer of the literal zero address never matches
///    an absent multisig).
/// 4. `signer` holds ANY role, open at `block`, on ANY of `role_sources`
///    ⇒ `ISSUER_KEY`.
/// 5. Otherwise ⇒ `UNKNOWN`.
pub fn signer_attribution(
    tx_signer: Option<[u8; 20]>,
    block: i64,
    role_sources: &[[u8; 20]],
    role_intervals: &HashMap<[u8; 20], Vec<RoleIntervalRow>>,
    multisig: Option<[u8; 20]>,
) -> SignerAttribution {
    let Some(signer) = tx_signer else {
        return SignerAttribution::Unknown;
    };

    if let Some(m) = multisig {
        if signer == m {
            return SignerAttribution::RomeMultisig;
        }
    }

    for source in role_sources {
        let Some(intervals) = role_intervals.get(source) else {
            continue;
        };
        if intervals
            .iter()
            .any(|iv| iv.account == signer && role_open_at(iv, block))
        {
            return SignerAttribution::IssuerKey;
        }
    }

    SignerAttribution::Unknown
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(event_id: i64, block: i64, tx_index: i32, log_index: i32, name: &str, args: serde_json::Value) -> ChainEventRow {
        ChainEventRow {
            event_id,
            block_number: block,
            tx_index,
            log_index,
            event_name: name.to_string(),
            args,
            tx_signer: None,
        }
    }

    fn addr_hex(a: [u8; 20]) -> String {
        format!("0x{}", hex::encode(a))
    }
    fn type_id_hex(a: [u8; 32]) -> String {
        format!("0x{}", hex::encode(a))
    }

    const TOKEN: [u8; 20] = [0x22; 20];

    // ---- 8. upgrade_before_is_previous_rows_after ----
    #[test]
    fn upgrade_before_is_previous_rows_after() {
        let token_events = vec![
            ev(1, 100, 0, 0, "Upgraded", serde_json::json!({ "implementation": addr_hex([0x01; 20]) })),
            ev(2, 200, 0, 0, "Upgraded", serde_json::json!({ "implementation": addr_hex([0x02; 20]) })),
        ];
        let rows = build_upgrade_changes(&token_events, &[], TOKEN);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].before, None);
        assert_eq!(rows[0].after, Some([0x01; 20]));
        assert_eq!(rows[1].before, Some([0x01; 20]), "row N's before must equal row N-1's after");
        assert_eq!(rows[1].after, Some([0x02; 20]));
    }

    // ---- 9. factory_token_upgraded_scoped_to_this_token ----
    #[test]
    fn factory_token_upgraded_scoped_to_this_token() {
        let other_token = [0x33; 20];
        let factory_events = vec![
            ev(1, 100, 0, 0, "TokenUpgraded", serde_json::json!({
                "token": addr_hex(TOKEN), "newImplementation": addr_hex([0x01; 20])
            })),
            ev(2, 150, 0, 0, "TokenUpgraded", serde_json::json!({
                "token": addr_hex(other_token), "newImplementation": addr_hex([0x99; 20])
            })),
        ];
        let rows = build_upgrade_changes(&[], &factory_events, TOKEN);
        assert_eq!(rows.len(), 1, "the OTHER token's TokenUpgraded must never leak into this asset's chain");
        assert_eq!(rows[0].after, Some([0x01; 20]));
        assert_eq!(rows[0].event_id, 1);
    }

    // ---- 10. module_linked_filters_on_tokenAddress_arg ----
    #[test]
    fn module_linked_filters_on_token_address_arg() {
        // Correct arg name -> row present.
        let good = vec![ev(1, 100, 0, 0, "ModuleLinked", serde_json::json!({
            "tokenAddress": addr_hex(TOKEN), "moduleAddress": addr_hex([0x01; 20]), "moduleType": type_id_hex([0xAA; 32])
        }))];
        assert_eq!(build_module_linked_changes(&good, TOKEN).len(), 1);

        // WRONG arg key `token` (not `tokenAddress`) -> arg_address returns
        // None -> row missing, never misattributed.
        let bad = vec![ev(2, 100, 0, 0, "ModuleLinked", serde_json::json!({
            "token": addr_hex(TOKEN), "moduleAddress": addr_hex([0x01; 20]), "moduleType": type_id_hex([0xAA; 32])
        }))];
        assert_eq!(
            build_module_linked_changes(&bad, TOKEN),
            vec![],
            "a ModuleLinked log keyed on the wrong arg name must produce NO row, never a guess"
        );
    }

    // ---- 11. module_set_chains_partition_per_type_id ----
    #[test]
    fn module_set_chains_partition_per_type_id() {
        let type_a = [0xA1; 32];
        let type_b = [0xB1; 32];
        let events = vec![
            ev(1, 100, 0, 0, "SpecificRestrictionModuleSet", serde_json::json!({
                "typeId": type_id_hex(type_a), "moduleAddress": addr_hex([0x01; 20])
            })),
            ev(2, 150, 0, 0, "SpecificRestrictionModuleSet", serde_json::json!({
                "typeId": type_id_hex(type_b), "moduleAddress": addr_hex([0x02; 20])
            })),
            ev(3, 200, 0, 0, "SpecificRestrictionModuleSet", serde_json::json!({
                "typeId": type_id_hex(type_a), "moduleAddress": addr_hex([0x03; 20])
            })),
        ];
        let rows = build_module_set_changes(&events);
        assert_eq!(rows.len(), 3);
        let a_rows: Vec<_> = rows.iter().filter(|r| r.subject == type_a.to_vec()).collect();
        let b_rows: Vec<_> = rows.iter().filter(|r| r.subject == type_b.to_vec()).collect();
        assert_eq!(a_rows.len(), 2);
        assert_eq!(a_rows[0].before, None);
        assert_eq!(a_rows[0].after, Some([0x01; 20]));
        assert_eq!(a_rows[1].before, Some([0x01; 20]), "type_a's second row must chain off type_a's own first row");
        assert_eq!(a_rows[1].after, Some([0x03; 20]));
        assert_eq!(b_rows.len(), 1);
        assert_eq!(b_rows[0].before, None, "type_b must NEVER inherit type_a's chain");
    }

    // ---- 12. router_global_fans_out_to_every_asset_on_that_router ----
    #[test]
    fn router_global_fans_out_to_every_asset_on_that_router() {
        let router_events = vec![ev(1, 100, 0, 0, "ModuleTypeRegistered", serde_json::json!({
            "typeId": type_id_hex([0xC1; 32]), "isGlobal": true, "globalImplementation": addr_hex([0x01; 20])
        }))];
        let chain = build_router_global_changes(&router_events);
        assert_eq!(chain.len(), 1);
        // The SAME chain fans out — no re-chaining, just re-attribution per
        // asset (proven here by confirming the chain is asset-agnostic: it
        // carries no asset_id field at all, so a caller trivially reuses it
        // for both "ASSET_A" and "ASSET_B").
        let role_intervals = HashMap::new();
        let row_a = chain[0].attribute(&[], &role_intervals, None);
        let row_b = chain[0].attribute(&[], &role_intervals, None);
        assert_eq!(row_a.after, Some([0x01; 20]));
        assert_eq!(row_a, row_b, "identical chain + identical (empty) role context must attribute identically");
    }

    // ---- 13. router_global_chain_registered_updated_removed ----
    #[test]
    fn router_global_chain_registered_updated_removed() {
        let type_a = [0xC1; 32];
        let type_b = [0xC2; 32];
        let router_events = vec![
            ev(1, 100, 0, 0, "ModuleTypeRegistered", serde_json::json!({
                "typeId": type_id_hex(type_a), "isGlobal": true, "globalImplementation": addr_hex([0x01; 20])
            })),
            ev(2, 150, 0, 0, "ModuleTypeRegistered", serde_json::json!({
                "typeId": type_id_hex(type_b), "isGlobal": true, "globalImplementation": addr_hex([0x11; 20])
            })),
            ev(3, 200, 0, 0, "GlobalImplementationUpdated", serde_json::json!({
                "typeId": type_id_hex(type_a), "newGlobalImplementation": addr_hex([0x02; 20])
            })),
            ev(4, 300, 0, 0, "ModuleTypeRemoved", serde_json::json!({ "typeId": type_id_hex(type_a) })),
        ];
        let rows = build_router_global_changes(&router_events);
        let a_rows: Vec<_> = rows.iter().filter(|r| r.subject == type_a.to_vec()).collect();
        let b_rows: Vec<_> = rows.iter().filter(|r| r.subject == type_b.to_vec()).collect();

        assert_eq!(a_rows.len(), 3);
        assert_eq!((a_rows[0].before, a_rows[0].after), (None, Some([0x01; 20])));
        assert_eq!((a_rows[1].before, a_rows[1].after), (Some([0x01; 20]), Some([0x02; 20])));
        assert_eq!((a_rows[2].before, a_rows[2].after), (Some([0x02; 20]), None), "Removed -> after = NULL");

        assert_eq!(b_rows.len(), 1, "type_b's own registration must be untouched by type_a's removal");
        assert_eq!(b_rows[0].after, Some([0x11; 20]));
    }

    // ---- 14. storefront_upgrade_fans_out_to_assets_sharing_it ----
    #[test]
    fn storefront_upgrade_fans_out_to_assets_sharing_it() {
        let storefront = [0x55; 20];
        let events = vec![ev(1, 100, 0, 0, "Upgraded", serde_json::json!({ "implementation": addr_hex([0x01; 20]) }))];
        let chain = build_storefront_upgrade_changes(&events, storefront);
        assert_eq!(chain.len(), 1);
        assert_eq!(chain[0].subject, storefront.to_vec());
        assert_eq!(chain[0].after, Some([0x01; 20]));
        // Same fan-out contract as router_global — asset-agnostic chain,
        // re-attributed per asset by the caller.
    }

    // ---- 15. factory_mediated_upgrade_yields_two_rows_consistent_chain ----
    #[test]
    fn factory_mediated_upgrade_yields_two_rows_consistent_chain() {
        let token_events = vec![ev(1, 100, 0, 0, "Upgraded", serde_json::json!({ "implementation": addr_hex([0x02; 20]) }))]; // log0
        let factory_events = vec![ev(2, 100, 0, 1, "TokenUpgraded", serde_json::json!({
            "token": addr_hex(TOKEN), "newImplementation": addr_hex([0x02; 20])
        }))]; // log1, same tx
        let rows = build_upgrade_changes(&token_events, &factory_events, TOKEN);
        assert_eq!(rows.len(), 2, "no dedupe — two distinct logs, two rows");
        assert_eq!((rows[0].before, rows[0].after), (None, Some([0x02; 20])));
        assert_eq!((rows[1].before, rows[1].after), (Some([0x02; 20]), Some([0x02; 20])), "self-loop by position, not a duplicate");
    }

    // ---- signer_attribution: 16-22 ----

    fn role_iv(account: [u8; 20], from: i64, to: Option<i64>) -> RoleIntervalRow {
        RoleIntervalRow {
            role: [0x01; 32],
            account,
            from_block: from,
            to_block: to,
            opened_by_event: 900,
            closed_by_event: to.map(|_| 901),
        }
    }

    const SOURCE: [u8; 20] = [0x11; 20];
    const SIGNER: [u8; 20] = [0xAA; 20];
    const MULTISIG: [u8; 20] = [0xD0; 20];

    #[test]
    fn multisig_signer_is_rome_multisig_even_with_roles() {
        let mut role_intervals = HashMap::new();
        role_intervals.insert(SOURCE, vec![role_iv(MULTISIG, 0, None)]); // the multisig ALSO holds a role
        let attr = signer_attribution(Some(MULTISIG), 100, &[SOURCE], &role_intervals, Some(MULTISIG));
        assert_eq!(attr, SignerAttribution::RomeMultisig, "checked BEFORE any role lookup");
    }

    #[test]
    fn role_on_any_asset_source_is_issuer_key() {
        let mut role_intervals = HashMap::new();
        role_intervals.insert(SOURCE, vec![role_iv(SIGNER, 0, None)]);
        let attr = signer_attribution(Some(SIGNER), 100, &[SOURCE], &role_intervals, Some(MULTISIG));
        assert_eq!(attr, SignerAttribution::IssuerKey);
    }

    #[test]
    fn no_role_no_multisig_is_unknown() {
        let role_intervals = HashMap::new();
        let attr = signer_attribution(Some(SIGNER), 100, &[SOURCE], &role_intervals, Some(MULTISIG));
        assert_eq!(attr, SignerAttribution::Unknown);
    }

    #[test]
    fn multisig_none_never_rome_multisig() {
        let role_intervals = HashMap::new();
        // signer is the literal zero address, multisig is None — even a
        // trivially-matching-looking signer must never read ROME_MULTISIG.
        let attr = signer_attribution(Some([0u8; 20]), 100, &[SOURCE], &role_intervals, None);
        assert_eq!(attr, SignerAttribution::Unknown);
    }

    #[test]
    fn missing_tx_signer_is_unknown() {
        let mut role_intervals = HashMap::new();
        role_intervals.insert(SOURCE, vec![role_iv(SIGNER, 0, None)]);
        let attr = signer_attribution(None, 100, &[SOURCE], &role_intervals, Some(MULTISIG));
        assert_eq!(attr, SignerAttribution::Unknown);
    }

    #[test]
    fn role_open_at_block_is_inclusive_both_ends() {
        let mut role_intervals = HashMap::new();
        role_intervals.insert(SOURCE, vec![role_iv(SIGNER, 100, Some(200))]);
        let at = |b: i64| signer_attribution(Some(SIGNER), b, &[SOURCE], &role_intervals, None);
        assert_eq!(at(100), SignerAttribution::IssuerKey, "@100 (from_block) — inclusive");
        assert_eq!(at(200), SignerAttribution::IssuerKey, "@200 (to_block) — inclusive");
        assert_eq!(at(201), SignerAttribution::Unknown, "@201 — past the closed interval");
        assert_eq!(at(99), SignerAttribution::Unknown, "@99 — before the interval opened");
    }

    #[test]
    fn user_variant_exists_but_is_never_emitted() {
        // P5 stub — the CHECK constraint's vocabulary includes USER, but no
        // builder in this crate ever constructs it (grep-confirmed: `User`
        // appears only in this enum definition and this test).
        assert_eq!(SignerAttribution::User.as_db_str(), "USER");
    }
}
