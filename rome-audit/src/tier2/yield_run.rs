//! `yield_run` + `yield_credit` (P4b-i) — capture §4 rule 1 ("Yield credits
//! — RUN-based, not same-tx"): `distributeYieldWithLimit` emits
//! `YieldDistributed(amount, token)` **only on the completing window**
//! (Rome's practical `maxHolders=1` ceiling makes every real distribution a
//! multi-tx walk); `amount` is the run's declared INPUT total, not
//! Σ(credits) — restricted holders' shares stay withheld in the contract.
//! So a **run** = every yield-token `Transfer(from=ArcToken)` credit since
//! the previous completion, **closed by** the next `YieldDistributed`.
//!
//! **Grouping walks the FULL total order, never tx-scoped** (unlike
//! [`super::screening`]'s per-tx stack): a run legitimately spans many
//! transactions, and — for the single-shot case — same-tx credits with a
//! LOWER `log_index` than the closing `YieldDistributed` land in the SAME
//! run purely because the merged stream is walked in position order; no
//! special-casing needed for "single-shot" vs "multi-tx".
//!
//! **`run_seq` is a DENSE 0..N index in total order, NOT an inserted-row
//! surrogate** (deviates from the IMPL-PLAN's literal DDL, which sketched an
//! identity column) — assigned by enumerating the closed-run sequence
//! produced by walking the naturally-ordered merged stream, so it is
//! reproducible byte-for-byte across two independently-seeded databases
//! regardless of `chain_event` insertion order (C1: never key/order on a
//! surrogate).
//!
//! **Per-asset scope filtering is load-bearing, not a fetch-time
//! optimization:** the yield-token `Transfer` stream is fetched by
//! `source_contract = yield_token`, which — when two assets share the same
//! yield token — contains BOTH assets' credits. This builder re-filters to
//! `args.from == arc_token_address` (this asset's OWN ArcToken) before
//! accumulating; skipping that filter would leak asset B's credits into
//! asset A's runs. Symmetrically, `YieldDistributed` is fetched from the
//! asset's own ArcToken but re-filtered to `args.token == yield_token`
//! (an ArcToken can point at more than one yield token over its history).

use bigdecimal::BigDecimal;

use super::fetch::{arg_address, arg_uint256, ChainEventRow};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct YieldRunRow {
    pub yield_token: [u8; 20],
    pub run_seq: i64,
    pub total_amount: Option<BigDecimal>,
    pub credited_total: BigDecimal,
    pub withheld_remainder: Option<BigDecimal>,
    pub over_credit: bool,
    pub closed_by_event: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct YieldCreditRow {
    pub yield_token: [u8; 20],
    pub run_seq: i64,
    pub block_number: i64,
    pub tx_index: i32,
    pub log_index: i32,
    pub holder: [u8; 20],
    pub share: BigDecimal,
    pub credit_event: i64,
}

enum Kind {
    Credit,
    Close,
}

/// `transfer_events` = the yield token's OWN `Transfer` rows (unfiltered by
/// `from` — this fn does the filtering); `distributed_events` = this
/// asset's ArcToken's `YieldDistributed` rows (unfiltered by `token` — same
/// deal). Returns `(runs, credits)`, both already carrying the dense
/// `run_seq`.
pub fn build_yield_runs(
    transfer_events: &[ChainEventRow],
    distributed_events: &[ChainEventRow],
    yield_token: [u8; 20],
    arc_token_address: [u8; 20],
) -> (Vec<YieldRunRow>, Vec<YieldCreditRow>) {
    let mut items: Vec<(i64, i32, i32, Kind, &ChainEventRow)> = Vec::new();

    for ev in transfer_events {
        if ev.event_name != "Transfer" {
            continue;
        }
        let Some(from) = arg_address(&ev.args, "from") else {
            continue;
        };
        if from != arc_token_address {
            continue; // LOAD-BEARING: excludes another asset's credits on a shared yield token
        }
        items.push((ev.block_number, ev.tx_index, ev.log_index, Kind::Credit, ev));
    }
    for ev in distributed_events {
        if ev.event_name != "YieldDistributed" {
            continue;
        }
        let Some(token) = arg_address(&ev.args, "token") else {
            continue;
        };
        if token != yield_token {
            continue; // this ArcToken pointed at a DIFFERENT yield token for this event
        }
        items.push((ev.block_number, ev.tx_index, ev.log_index, Kind::Close, ev));
    }
    items.sort_by_key(|(b, t, l, _, _)| (*b, *t, *l));

    // (total_amount, credited_total, withheld_remainder, over_credit,
    // closed_by_event, credit events) per finished/trailing run, in order.
    #[allow(clippy::type_complexity)]
    let mut runs_data: Vec<(
        Option<BigDecimal>,
        BigDecimal,
        Option<BigDecimal>,
        bool,
        Option<i64>,
        Vec<&ChainEventRow>,
    )> = Vec::new();
    let mut current: Vec<&ChainEventRow> = Vec::new();

    for (_, _, _, kind, ev) in &items {
        match kind {
            Kind::Credit => current.push(ev),
            Kind::Close => {
                // L1 (P4b-i review): a malformed `amount` still
                // CLOSES the run — it must NEVER skip-and-leave-open, or
                // the run boundary is swallowed and the NEXT close would
                // silently absorb this run's credits too. Unknown total
                // (never invented) ⇒ `total_amount`/`withheld_remainder`
                // both `None`, `over_credit` stays `false` (nothing to
                // compare against) — but the close still happens.
                let total_amount = arg_uint256(&ev.args, "amount");
                let credited_total = sum_shares(&current);
                let (remainder, over_credit) = match &total_amount {
                    Some(total) => {
                        let r = total - &credited_total;
                        let over = r < BigDecimal::from(0);
                        (Some(r), over)
                    }
                    None => (None, false),
                };
                runs_data.push((
                    total_amount,
                    credited_total,
                    remainder,
                    over_credit,
                    Some(ev.event_id),
                    std::mem::take(&mut current),
                ));
            }
        }
    }
    if !current.is_empty() {
        let credited_total = sum_shares(&current);
        runs_data.push((None, credited_total, None, false, None, current));
    }

    let mut runs = Vec::new();
    let mut credits = Vec::new();
    for (run_seq, (total_amount, credited_total, withheld_remainder, over_credit, closed_by_event, credit_events)) in
        runs_data.into_iter().enumerate()
    {
        let run_seq = run_seq as i64;
        for ev in &credit_events {
            let (Some(holder), Some(share)) =
                (arg_address(&ev.args, "to"), arg_uint256(&ev.args, "value"))
            else {
                continue;
            };
            credits.push(YieldCreditRow {
                yield_token,
                run_seq,
                block_number: ev.block_number,
                tx_index: ev.tx_index,
                log_index: ev.log_index,
                holder,
                share,
                credit_event: ev.event_id,
            });
        }
        runs.push(YieldRunRow {
            yield_token,
            run_seq,
            total_amount,
            credited_total,
            withheld_remainder,
            over_credit,
            closed_by_event,
        });
    }
    (runs, credits)
}

fn sum_shares(events: &[&ChainEventRow]) -> BigDecimal {
    events
        .iter()
        .filter_map(|ev| arg_uint256(&ev.args, "value"))
        .fold(BigDecimal::from(0), |acc, v| acc + v)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    const YIELD_TOKEN: [u8; 20] = [0x77; 20];
    const ARC_TOKEN_A: [u8; 20] = [0xAA; 20];
    const ARC_TOKEN_B: [u8; 20] = [0xBB; 20];

    fn addr_hex(a: [u8; 20]) -> String {
        format!("0x{}", hex::encode(a))
    }

    fn credit_ev(event_id: i64, block: i64, tx_index: i32, log_index: i32, from: [u8; 20], to: [u8; 20], value: &str) -> ChainEventRow {
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

    fn close_ev(event_id: i64, block: i64, tx_index: i32, log_index: i32, token: [u8; 20], amount: &str) -> ChainEventRow {
        ChainEventRow {
            event_id,
            block_number: block,
            tx_index,
            log_index,
            event_name: "YieldDistributed".to_string(),
            args: serde_json::json!({ "amount": amount, "token": addr_hex(token) }),
            tx_signer: None,
        }
    }

    fn bd(s: &str) -> BigDecimal {
        BigDecimal::from_str(s).unwrap()
    }

    #[test]
    fn multi_tx_yield_walk_groups_one_run_with_withheld_remainder() {
        let transfers = vec![
            credit_ev(1, 100, 0, 0, ARC_TOKEN_A, [0x01; 20], "10"),
            credit_ev(2, 101, 0, 0, ARC_TOKEN_A, [0x02; 20], "10"),
            credit_ev(3, 102, 0, 0, ARC_TOKEN_A, [0x03; 20], "10"),
        ];
        let distributed = vec![close_ev(4, 103, 0, 0, YIELD_TOKEN, "40")];
        let (runs, credits) = build_yield_runs(&transfers, &distributed, YIELD_TOKEN, ARC_TOKEN_A);
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].run_seq, 0);
        assert_eq!(runs[0].total_amount, Some(bd("40")));
        assert_eq!(runs[0].credited_total, bd("30"));
        assert_eq!(runs[0].withheld_remainder, Some(bd("10")));
        assert!(!runs[0].over_credit);
        assert_eq!(runs[0].closed_by_event, Some(4));
        assert_eq!(credits.len(), 3);
        assert!(credits.iter().all(|c| c.run_seq == 0));
    }

    #[test]
    fn single_shot_same_tx_credits_belong_to_closing_run() {
        // Same tx: credit at log_index=0, close at log_index=1.
        let transfers = vec![credit_ev(1, 100, 0, 0, ARC_TOKEN_A, [0x01; 20], "5")];
        let distributed = vec![close_ev(2, 100, 0, 1, YIELD_TOKEN, "5")];
        let (runs, credits) = build_yield_runs(&transfers, &distributed, YIELD_TOKEN, ARC_TOKEN_A);
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].credited_total, bd("5"));
        assert_eq!(runs[0].withheld_remainder, Some(bd("0")));
        assert_eq!(credits.len(), 1);
        assert_eq!(credits[0].run_seq, 0);
    }

    #[test]
    fn yield_distributed_zero_closes_empty_run() {
        let distributed = vec![close_ev(1, 100, 0, 0, YIELD_TOKEN, "0")];
        let (runs, credits) = build_yield_runs(&[], &distributed, YIELD_TOKEN, ARC_TOKEN_A);
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].total_amount, Some(bd("0")));
        assert_eq!(runs[0].credited_total, bd("0"));
        assert_eq!(runs[0].withheld_remainder, Some(bd("0")));
        assert!(!runs[0].over_credit);
        assert_eq!(credits, vec![]);
    }

    /// L1 (P4b-i review): a malformed `YieldDistributed.amount` must
    /// still CLOSE the run it belongs to (as `total_amount = NULL`) — the
    /// WRONG behavior is to skip-and-leave-open, which swallows the run
    /// boundary and lets the NEXT close absorb both runs' credits into one.
    #[test]
    fn malformed_amount_still_closes_the_run_never_merges_two_runs() {
        let transfers = vec![
            credit_ev(1, 100, 0, 0, ARC_TOKEN_A, [0x01; 20], "10"),
            credit_ev(2, 102, 0, 0, ARC_TOKEN_A, [0x03; 20], "20"),
        ];
        let distributed = vec![
            close_ev(3, 101, 0, 0, YIELD_TOKEN, "not-a-number"), // malformed — closes run 0 anyway
            close_ev(4, 103, 0, 0, YIELD_TOKEN, "20"), // closes run 1
        ];
        let (runs, credits) = build_yield_runs(&transfers, &distributed, YIELD_TOKEN, ARC_TOKEN_A);

        assert_eq!(runs.len(), 2, "the malformed close must still end run 0 — TWO runs, not one");
        assert_eq!(runs[0].total_amount, None, "malformed amount ⇒ unknown total, never invented");
        assert_eq!(runs[0].credited_total, bd("10"));
        assert_eq!(runs[0].withheld_remainder, None);
        assert!(!runs[0].over_credit);
        assert_eq!(runs[0].closed_by_event, Some(3), "the boundary is preserved — run 0 IS closed");

        assert_eq!(runs[1].total_amount, Some(bd("20")));
        assert_eq!(runs[1].credited_total, bd("20"), "run 1's credit must NOT include run 0's 10");
        assert_eq!(runs[1].closed_by_event, Some(4));

        assert_eq!(credits.len(), 2);
        assert_eq!(credits[0].run_seq, 0);
        assert_eq!(credits[1].run_seq, 1);
    }

    #[test]
    fn over_credit_flags_alarm_never_clamps() {
        let transfers = vec![
            credit_ev(1, 100, 0, 0, ARC_TOKEN_A, [0x01; 20], "30"),
            credit_ev(2, 101, 0, 0, ARC_TOKEN_A, [0x02; 20], "30"),
        ];
        let distributed = vec![close_ev(3, 102, 0, 0, YIELD_TOKEN, "40")];
        let (runs, _credits) = build_yield_runs(&transfers, &distributed, YIELD_TOKEN, ARC_TOKEN_A);
        assert_eq!(runs.len(), 1);
        assert_eq!(
            runs[0].withheld_remainder,
            Some(bd("-20")),
            "60 credited against 40 declared — remainder must be the NEGATIVE -20, never clamped to 0"
        );
        assert!(runs[0].over_credit);
    }

    #[test]
    fn two_assets_sharing_a_yield_token_do_not_cross() {
        // The SAME yield_token contract carries Transfers from BOTH
        // ArcToken A and ArcToken B — asset A's run must count only A's.
        let transfers = vec![
            credit_ev(1, 100, 0, 0, ARC_TOKEN_A, [0x01; 20], "10"),
            credit_ev(2, 101, 0, 0, ARC_TOKEN_B, [0x02; 20], "999"),
            credit_ev(3, 102, 0, 0, ARC_TOKEN_A, [0x03; 20], "10"),
        ];
        let distributed = vec![close_ev(4, 103, 0, 0, YIELD_TOKEN, "20")];
        let (runs, credits) = build_yield_runs(&transfers, &distributed, YIELD_TOKEN, ARC_TOKEN_A);
        assert_eq!(runs.len(), 1);
        assert_eq!(
            runs[0].credited_total,
            bd("20"),
            "asset B's 999 credit must never enter asset A's run"
        );
        assert_eq!(credits.len(), 2);
        assert!(credits.iter().all(|c| c.credit_event != 2));
    }

    #[test]
    fn trailing_credits_form_an_open_run() {
        let transfers = vec![credit_ev(1, 100, 0, 0, ARC_TOKEN_A, [0x01; 20], "7")];
        let (runs, credits) = build_yield_runs(&transfers, &[], YIELD_TOKEN, ARC_TOKEN_A);
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].total_amount, None);
        assert_eq!(runs[0].withheld_remainder, None);
        assert_eq!(runs[0].credited_total, bd("7"));
        assert!(!runs[0].over_credit);
        assert_eq!(runs[0].closed_by_event, None);
        assert_eq!(credits.len(), 1);
    }

    #[test]
    fn run_seq_is_dense_in_total_order_regardless_of_input_order() {
        let transfers_natural = vec![
            credit_ev(1, 100, 0, 0, ARC_TOKEN_A, [0x01; 20], "1"),
            credit_ev(2, 102, 0, 0, ARC_TOKEN_A, [0x02; 20], "1"),
        ];
        let distributed_natural = vec![
            close_ev(3, 101, 0, 0, YIELD_TOKEN, "1"),
            close_ev(4, 103, 0, 0, YIELD_TOKEN, "1"),
        ];
        let (runs_a, _) = build_yield_runs(&transfers_natural, &distributed_natural, YIELD_TOKEN, ARC_TOKEN_A);

        // Same logical events, reversed input-list order — the builder must
        // sort by natural position before walking, so the result is identical.
        let transfers_reversed = vec![
            credit_ev(2, 102, 0, 0, ARC_TOKEN_A, [0x02; 20], "1"),
            credit_ev(1, 100, 0, 0, ARC_TOKEN_A, [0x01; 20], "1"),
        ];
        let distributed_reversed = vec![
            close_ev(4, 103, 0, 0, YIELD_TOKEN, "1"),
            close_ev(3, 101, 0, 0, YIELD_TOKEN, "1"),
        ];
        let (runs_b, _) = build_yield_runs(&transfers_reversed, &distributed_reversed, YIELD_TOKEN, ARC_TOKEN_A);

        assert_eq!(runs_a, runs_b);
        assert_eq!(runs_a.iter().map(|r| r.run_seq).collect::<Vec<_>>(), vec![0, 1]);
        assert_eq!(runs_a[0].closed_by_event, Some(3));
        assert_eq!(runs_a[1].closed_by_event, Some(4));
    }

    #[test]
    fn uint256_round_trips_through_numeric_78_0() {
        let max_u256 = "115792089237316195423570985008687907853269984665640564039457584007913129639935";
        let transfers = vec![credit_ev(1, 100, 0, 0, ARC_TOKEN_A, [0x01; 20], max_u256)];
        let (_runs, credits) = build_yield_runs(&transfers, &[], YIELD_TOKEN, ARC_TOKEN_A);
        assert_eq!(credits[0].share, bd(max_u256));
    }
}
