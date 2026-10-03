//! `sale` (P4b-ii) — pairs a storefront's `PurchaseMade(buyer, tokenContract,
//! amount, pricePaid)` with its two settlement legs: the token leg (asset
//! `Transfer(from=storefront, to=buyer)`) and the payment leg (a purchase
//! token's `Transfer(from=buyer, to=storefront)`). Either leg missing
//! ⇒ `NULL` for that FK — the `NULL` itself IS the "leg missing" flag, no
//! redundant boolean column.
//!
//! **ONE walk per storefront, across ALL configured assets sharing it**
//! (the HIGH-1 pattern from [`super::screening`], applied here for the same
//! structural reason): the PAYMENT leg's candidate pool lives on a SHARED
//! purchase-token contract — an asset's own purchase tokens can be the very
//! same ERC-20 another asset sold through the same storefront uses. Two
//! INDEPENDENT per-asset walks would each draw from an UNSHARED copy of
//! that pool and could each "nearest-preceding-unmatch" the SAME payment
//! leg — a double-attribution bug. The fix: one combined walk, where the
//! payment-leg stack is genuinely shared (never asset-keyed) and popping it
//! for one asset's purchase removes it for every other asset's purchase in
//! the same tx. The TOKEN leg has no such risk (each asset's own token
//! contract is a disjoint event stream by construction) but is walked
//! asset-keyed in the SAME pass for symmetry and a single code path.
//!
//! **Pairing is purely positional** — nearest preceding unmatched candidate
//! of each leg kind, resetting at every tx boundary — never by argument
//! tuple (buyer/amount). Consistent with this crate's `screening` doctrine:
//! "Do NOT match on value."

use std::collections::HashMap;

use bigdecimal::BigDecimal;

use super::fetch::{arg_address, arg_uint256, ChainEventRow};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SaleRow {
    pub asset_id: String,
    pub block_number: i64,
    pub tx_index: i32,
    pub purchase_log_index: i32,
    pub buyer: [u8; 20],
    pub amount: BigDecimal,
    pub price_paid: BigDecimal,
    pub purchase_event: i64,
    pub token_transfer_event: Option<i64>,
    pub payment_transfer_event: Option<i64>,
}

enum SaleItem<'a> {
    Purchase {
        asset_id: &'a str,
        event: &'a ChainEventRow,
    },
    TokenLeg {
        asset_id: &'a str,
        event: &'a ChainEventRow,
    },
    PaymentLeg {
        event: &'a ChainEventRow,
    },
}

impl SaleItem<'_> {
    fn position(&self) -> (i64, i32, i32) {
        let ev = match self {
            SaleItem::Purchase { event, .. } => event,
            SaleItem::TokenLeg { event, .. } => event,
            SaleItem::PaymentLeg { event } => event,
        };
        (ev.block_number, ev.tx_index, ev.log_index)
    }
}

/// `purchases` = ONE storefront's full `PurchaseMade` stream, UNTAGGED —
/// this fn matches each event's `tokenContract` against `assets` to find
/// its owning `asset_id`; a `PurchaseMade` for a token that isn't in
/// `assets` at all is dropped (test 26 — the scope defect this whole
/// pattern exists to prevent: never guessed, never misattributed).
/// `assets` = `(asset_id, token_address)` for every configured asset
/// sharing this storefront.
/// `token_legs` = `(asset_id, event)` — each asset's OWN token `Transfer`
/// stream, UNFILTERED (filtered to `from == storefront` inside).
/// `payment_legs` = the UNION, across every asset in `assets`, of
/// `Transfer` events on ANY of that asset's (historical) purchase tokens,
/// UNFILTERED (filtered to `to == storefront` inside) — CHAIN-GLOBAL, no
/// asset tag: a payment leg's candidacy doesn't depend on which asset
/// eventually claims it.
pub fn build_sales(
    purchases: &[ChainEventRow],
    assets: &[(String, [u8; 20])],
    token_legs: &[(String, ChainEventRow)],
    payment_legs: &[ChainEventRow],
    storefront: [u8; 20],
) -> Vec<SaleRow> {
    // tokenContract -> asset_id, for the PurchaseMade match.
    let by_token: HashMap<[u8; 20], &str> = assets
        .iter()
        .map(|(id, token)| (*token, id.as_str()))
        .collect();

    let mut items: Vec<SaleItem> = Vec::new();
    for ev in purchases {
        if ev.event_name != "PurchaseMade" {
            continue;
        }
        let Some(token_contract) = arg_address(&ev.args, "tokenContract") else {
            continue;
        };
        let Some(asset_id) = by_token.get(&token_contract) else {
            continue; // unconfigured token — never guessed, never attributed (test 26)
        };
        items.push(SaleItem::Purchase {
            asset_id,
            event: ev,
        });
    }
    for (asset_id, ev) in token_legs {
        if ev.event_name != "Transfer" {
            continue;
        }
        let Some(from) = arg_address(&ev.args, "from") else {
            continue;
        };
        if from != storefront {
            continue;
        }
        items.push(SaleItem::TokenLeg {
            asset_id,
            event: ev,
        });
    }
    for ev in payment_legs {
        if ev.event_name != "Transfer" {
            continue;
        }
        let Some(to) = arg_address(&ev.args, "to") else {
            continue;
        };
        if to != storefront {
            continue;
        }
        items.push(SaleItem::PaymentLeg { event: ev });
    }
    items.sort_by_key(|it| it.position());

    let mut rows = Vec::new();
    let mut token_stacks: HashMap<&str, Vec<&ChainEventRow>> = HashMap::new();
    let mut payment_stack: Vec<&ChainEventRow> = Vec::new();
    let mut current_tx: Option<(i64, i32)> = None;

    for item in &items {
        let (block, tx_index, _) = item.position();
        let tx_key = (block, tx_index);
        if current_tx != Some(tx_key) {
            token_stacks.clear();
            payment_stack.clear();
            current_tx = Some(tx_key);
        }

        match item {
            SaleItem::TokenLeg { asset_id, event } => {
                token_stacks.entry(asset_id).or_default().push(event);
            }
            SaleItem::PaymentLeg { event } => {
                payment_stack.push(event);
            }
            SaleItem::Purchase { asset_id, event } => {
                let (Some(buyer), Some(amount), Some(price_paid)) = (
                    arg_address(&event.args, "buyer"),
                    arg_uint256(&event.args, "amount"),
                    arg_uint256(&event.args, "pricePaid"),
                ) else {
                    continue;
                };
                let token_transfer_event = token_stacks
                    .get_mut(*asset_id)
                    .and_then(|s| s.pop())
                    .map(|e| e.event_id);
                let payment_transfer_event = payment_stack.pop().map(|e| e.event_id);
                rows.push(SaleRow {
                    asset_id: asset_id.to_string(),
                    block_number: event.block_number,
                    tx_index: event.tx_index,
                    purchase_log_index: event.log_index,
                    buyer,
                    amount,
                    price_paid,
                    purchase_event: event.event_id,
                    token_transfer_event,
                    payment_transfer_event,
                });
            }
        }
    }

    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    const STOREFRONT: [u8; 20] = [0x55; 20];
    const TOKEN_A: [u8; 20] = [0xA0; 20];
    const TOKEN_B: [u8; 20] = [0xB0; 20];
    const BUYER: [u8; 20] = [0x01; 20];

    fn addr_hex(a: [u8; 20]) -> String {
        format!("0x{}", hex::encode(a))
    }
    fn bd(s: &str) -> BigDecimal {
        BigDecimal::from_str(s).unwrap()
    }

    #[allow(clippy::too_many_arguments)]
    fn purchase_ev(
        event_id: i64,
        block: i64,
        tx_index: i32,
        log_index: i32,
        token: [u8; 20],
        buyer: [u8; 20],
        amount: &str,
        price: &str,
    ) -> ChainEventRow {
        ChainEventRow {
            event_id,
            block_number: block,
            tx_index,
            log_index,
            event_name: "PurchaseMade".to_string(),
            args: serde_json::json!({
                "buyer": addr_hex(buyer), "tokenContract": addr_hex(token),
                "amount": amount, "pricePaid": price,
            }),
            tx_signer: None,
        }
    }

    fn transfer_ev(
        event_id: i64,
        block: i64,
        tx_index: i32,
        log_index: i32,
        from: [u8; 20],
        to: [u8; 20],
    ) -> ChainEventRow {
        ChainEventRow {
            event_id,
            block_number: block,
            tx_index,
            log_index,
            event_name: "Transfer".to_string(),
            args: serde_json::json!({ "from": addr_hex(from), "to": addr_hex(to), "value": "1" }),
            tx_signer: None,
        }
    }

    fn assets_a() -> Vec<(String, [u8; 20])> {
        vec![("A".to_string(), TOKEN_A)]
    }

    // ---- 23. purchase_pairs_nearest_preceding_unmatched_legs ----
    #[test]
    fn purchase_pairs_nearest_preceding_unmatched_legs() {
        let purchases = vec![purchase_ev(1, 100, 0, 2, TOKEN_A, BUYER, "10", "5")];
        let token_legs = vec![("A".to_string(), transfer_ev(2, 100, 0, 1, STOREFRONT, BUYER))];
        let payment_legs = vec![transfer_ev(3, 100, 0, 0, BUYER, STOREFRONT)];
        let rows = build_sales(&purchases, &assets_a(), &token_legs, &payment_legs, STOREFRONT);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].amount, bd("10"));
        assert_eq!(rows[0].price_paid, bd("5"));
        assert_eq!(rows[0].token_transfer_event, Some(2));
        assert_eq!(rows[0].payment_transfer_event, Some(3));
    }

    // ---- 24. second_pm_cannot_reuse_consumed_legs ----
    #[test]
    fn second_pm_cannot_reuse_consumed_legs() {
        let purchases = vec![
            purchase_ev(1, 100, 0, 1, TOKEN_A, BUYER, "10", "5"),
            purchase_ev(2, 100, 0, 2, TOKEN_A, BUYER, "10", "5"),
        ];
        let token_legs = vec![("A".to_string(), transfer_ev(3, 100, 0, 0, STOREFRONT, BUYER))]; // only ONE token leg
        // LOW-1 (P4b-ii review): a single payment leg too — this test's
        // name claims BOTH leg kinds can't be reused; before this it only
        // guarded the token leg (payment_legs was empty, so the payment side
        // was vacuously "unmatched" for both PMs regardless of consumption).
        let payment_legs = vec![transfer_ev(4, 100, 0, 0, BUYER, STOREFRONT)]; // only ONE payment leg
        let rows = build_sales(&purchases, &assets_a(), &token_legs, &payment_legs, STOREFRONT);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].token_transfer_event, Some(3), "the first PM consumes the only token leg");
        assert_eq!(rows[0].payment_transfer_event, Some(4), "the first PM consumes the only payment leg");
        assert_eq!(rows[1].token_transfer_event, None, "the second PM must NOT reuse the consumed token leg");
        assert_eq!(rows[1].payment_transfer_event, None, "the second PM must NOT reuse the consumed payment leg");
    }

    // ---- 25. two_full_purchases_in_one_tx_pair_in_order ----
    #[test]
    fn two_full_purchases_in_one_tx_pair_in_order() {
        let purchases = vec![
            purchase_ev(1, 100, 0, 1, TOKEN_A, BUYER, "10", "5"),
            purchase_ev(2, 100, 0, 4, TOKEN_A, BUYER, "20", "9"),
        ];
        let token_legs = vec![
            ("A".to_string(), transfer_ev(3, 100, 0, 0, STOREFRONT, BUYER)), // TL@0
            ("A".to_string(), transfer_ev(4, 100, 0, 3, STOREFRONT, BUYER)), // TL@3
        ];
        let payment_legs = vec![
            transfer_ev(5, 100, 0, 2, BUYER, STOREFRONT), // PL@2 (between the two PMs)
        ];
        let rows = build_sales(&purchases, &assets_a(), &token_legs, &payment_legs, STOREFRONT);
        assert_eq!(rows.len(), 2);
        // PM@1 (amount 10) takes nearest preceding: TL@0, no payment leg preceding it yet.
        assert_eq!(rows[0].token_transfer_event, Some(3));
        assert_eq!(rows[0].payment_transfer_event, None);
        // PM@4 (amount 20) takes nearest preceding unmatched: TL@3, PL@2.
        assert_eq!(rows[1].token_transfer_event, Some(4));
        assert_eq!(rows[1].payment_transfer_event, Some(5));
    }

    // ---- 26. purchase_for_other_token_is_ignored (THE scope defect) ----
    #[test]
    fn purchase_for_other_token_is_ignored() {
        let purchases = vec![purchase_ev(1, 100, 0, 0, TOKEN_B, BUYER, "10", "5")]; // TOKEN_B not in `assets_a()`
        let rows = build_sales(&purchases, &assets_a(), &[], &[], STOREFRONT);
        assert_eq!(
            rows,
            vec![],
            "a PurchaseMade for an UNCONFIGURED token must produce zero rows — never guessed"
        );
    }

    // ---- 27. leg_direction_filter ----
    #[test]
    fn leg_direction_filter() {
        let purchases = vec![purchase_ev(1, 100, 0, 2, TOKEN_A, BUYER, "10", "5")];
        // Wrong-direction Transfers: token leg TO the storefront (not FROM),
        // payment leg FROM the storefront (not TO) — neither is a valid
        // candidate and must not be picked up.
        let token_legs = vec![("A".to_string(), transfer_ev(2, 100, 0, 1, BUYER, STOREFRONT))];
        let payment_legs = vec![transfer_ev(3, 100, 0, 0, STOREFRONT, BUYER)];
        let rows = build_sales(&purchases, &assets_a(), &token_legs, &payment_legs, STOREFRONT);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].token_transfer_event, None, "wrong-direction Transfer is not a token-leg candidate");
        assert_eq!(rows[0].payment_transfer_event, None, "wrong-direction Transfer is not a payment-leg candidate");
    }

    // ---- 28. payment_leg_found_on_historical_purchase_token ----
    #[test]
    fn payment_leg_found_on_historical_purchase_token() {
        let old_purchase_token_transfer = transfer_ev(2, 100, 0, 0, BUYER, STOREFRONT);
        let purchases = vec![purchase_ev(1, 100, 0, 1, TOKEN_A, BUYER, "10", "5")];
        // `payment_legs` here represents the union across the asset's FULL
        // purchase_tokens HISTORY (an old, no-longer-current token) — the
        // caller's job, this fn just consumes whatever it's handed.
        let payment_legs = vec![old_purchase_token_transfer];
        let rows = build_sales(&purchases, &assets_a(), &[], &payment_legs, STOREFRONT);
        assert_eq!(rows[0].payment_transfer_event, Some(2), "a historical purchase token's Transfer must still pair");
    }
}
