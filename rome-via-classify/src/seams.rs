//! Cross-VM seam classification — which EVM<->Solana boundaries a transaction crosses.
//!
//! This is the Rust port of the frontend classifier that has until now been the only
//! implementation (rome-via `src/lib/tx-activity.ts::crossVmSeams`). Living only in the
//! browser meant the Cross-chain screen had to fetch a recency window of transactions
//! and filter it client-side, so under load the window contained nothing but ordinary
//! EVM traffic and the screen rendered empty. Owning the rule here lets the seam be
//! persisted and filtered in SQL.
//!
//! The three seams are independent — a transaction can sit on more than one. An empty
//! result means a regular EVM transaction, not a cross-chain one.

/// CpiProgram precompile. Calling it directly is an EVM->Solana crossing even when the
/// action-tag classifier did not label the call.
pub const CPI_PRECOMPILE: &str = "0xff00000000000000000000000000000000000008";
/// OP-stack deposit type byte; marks inbound bridge deposits on rows that predate tags.
pub const DEPOSIT_TX_TYPE_BYTE: i16 = 0x7e;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Seam {
    /// An EVM tx reaching into Solana (CPI / Romulus / the CpiProgram precompile).
    EvmToSol,
    /// A Solana L1 user authoring an EVM tx (DoTxUnsigned / ed25519 / synthetic sender).
    SolToEvm,
    /// CCTP / Wormhole / Withdraw-precompile movements between chains.
    Bridge,
}

impl Seam {
    pub fn as_str(&self) -> &'static str {
        match self {
            Seam::EvmToSol => "evm_to_sol",
            Seam::SolToEvm => "sol_to_evm",
            Seam::Bridge => "bridge",
        }
    }
}

/// Everything the seam rules read. Every field is either a real column in `rome_via`
/// or the output of [`crate::classify`], so an enrich worker can compute this offline.
#[derive(Debug, Default)]
pub struct SeamInput<'a> {
    /// Output of `classify()` for this tx.
    pub action_tags: &'a [String],
    /// `evm_tx.to_addr`, lowercased by the caller.
    pub to: Option<&'a str>,
    /// `cross_chain_correlations.rome_tx_type`, COALESCEd to "Rhea".
    pub tx_type: &'a str,
    /// `evm_tx.origination` ("ecdsa" | "solana_unsigned" | "solana_ed25519").
    pub origination: &'a str,
    /// Whether the sender is a Solana-controlled synthetic address.
    pub controlled_by_solana: bool,
    /// `evm_tx.tx_type_byte`.
    pub tx_type_byte: Option<i16>,
    /// Whether `cross_chain_correlations` captured a depth-2 CPI target for this tx —
    /// a real EVM->Solana program invocation (rome-dex, mango, a bridge program, ...).
    /// The depth-aware classifier stopped labelling these `Romulus` (correctly — the
    /// CpiProgram-precompile invoke is not a top-level native leg), so without this
    /// signal they fall out of the crossing feed even though they reached into Solana.
    pub has_cpi_target: bool,
}

fn has(tags: &[String], t: &str) -> bool {
    tags.iter().any(|x| x == t)
}

/// Classify a transaction onto zero or more EVM<->Solana seams.
///
/// Mirrors `tx-activity.ts::crossVmSeams` branch for branch; the parity tests below are
/// the contract. Note `bridge_in` is checked even though the backend never emits it —
/// dropping the check here would silently diverge from the frontend rule.
pub fn seams(input: &SeamInput<'_>) -> Vec<Seam> {
    let tags = input.action_tags;
    let mut out = Vec::new();

    if has(tags, "solana_cpi")
        || has(tags, "cross_chain_call")
        || input.to.map(|t| t.eq_ignore_ascii_case(CPI_PRECOMPILE)).unwrap_or(false)
        || input.tx_type == "Romulus"
        // A captured depth-2 CPI target (cross_chain_correlations.cpi_program) is a real
        // EVM->Solana program invocation — the tx reached into Solana. After the
        // depth-aware fix these are `Rhea` (the precompile invoke is not a top-level
        // native leg), so this is the only signal that keeps them in the crossing feed.
        || input.has_cpi_target
    {
        out.push(Seam::EvmToSol);
    }

    if input.origination == "solana_unsigned"
        || input.origination == "solana_ed25519"
        || input.controlled_by_solana
    {
        out.push(Seam::SolToEvm);
    }

    if has(tags, "bridge_out")
        || has(tags, "bridge_in")
        || has(tags, "withdraw_precompile")
        // Inbound deposits are tagged rome_deposit (the backend never emits bridge_in);
        // the 0x7E type byte covers tag-less rows from older indexing.
        || has(tags, "rome_deposit")
        || input.tx_type_byte == Some(DEPOSIT_TX_TYPE_BYTE)
    {
        out.push(Seam::Bridge);
    }

    out
}

/// True when the tx sits on at least one seam (the `isCrossVm` predicate).
pub fn is_cross_vm(input: &SeamInput<'_>) -> bool {
    !seams(input).is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tags(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }
    fn base<'a>(t: &'a [String]) -> SeamInput<'a> {
        SeamInput { action_tags: t, tx_type: "Rhea", origination: "ecdsa", ..Default::default() }
    }

    // ── evm_to_sol: four independent triggers ────────────────────────────────
    #[test]
    fn evm_to_sol_from_each_trigger() {
        for t in ["solana_cpi", "cross_chain_call"] {
            let tt = tags(&[t]);
            assert_eq!(seams(&base(&tt)), vec![Seam::EvmToSol], "tag {t}");
        }
        let none = tags(&[]);
        let mut by_to = base(&none);
        by_to.to = Some(CPI_PRECOMPILE);
        assert_eq!(seams(&by_to), vec![Seam::EvmToSol], "to == CpiProgram precompile");
        let mut by_type = base(&none);
        by_type.tx_type = "Romulus";
        assert_eq!(seams(&by_type), vec![Seam::EvmToSol], "rome_tx_type Romulus");
    }

    /// A tx that CPI'd a Solana program at depth-2 (its `cpi_program` was captured by
    /// the cross_chain worker) reached into Solana and IS an evm_to_sol crossing — even
    /// though it is `Rhea` (not Romulus), its `to` is a router (not the precompile), and
    /// it carries no `solana_cpi` action tag. This is the fix for the ~33k rome-dex /
    /// mango / bridge CPI crossings that fell out of the feed when the depth-aware
    /// classifier stopped calling them Romulus and nothing added them back.
    #[test]
    fn evm_to_sol_from_cpi_target() {
        let none = tags(&[]);
        let mut i = base(&none); // Rhea, ecdsa, no tags, no precompile `to`
        i.has_cpi_target = true;
        assert_eq!(
            seams(&i),
            vec![Seam::EvmToSol],
            "a captured depth-2 CPI target is an EVM->Solana crossing"
        );
    }

    #[test]
    fn cpi_precompile_match_is_case_insensitive() {
        let none = tags(&[]);
        let mut i = base(&none);
        let upper = CPI_PRECOMPILE.to_uppercase();
        i.to = Some(&upper);
        assert_eq!(seams(&i), vec![Seam::EvmToSol]);
    }

    // ── sol_to_evm ───────────────────────────────────────────────────────────
    #[test]
    fn sol_to_evm_from_origination_or_synthetic_sender() {
        let none = tags(&[]);
        for o in ["solana_unsigned", "solana_ed25519"] {
            let mut i = base(&none);
            i.origination = o;
            assert_eq!(seams(&i), vec![Seam::SolToEvm], "origination {o}");
        }
        let mut i = base(&none);
        i.controlled_by_solana = true;
        assert_eq!(seams(&i), vec![Seam::SolToEvm]);
    }

    // ── bridge: every trigger, including the ones the backend never emits ────
    #[test]
    fn bridge_from_each_tag_and_deposit_type_byte() {
        for t in ["bridge_out", "bridge_in", "withdraw_precompile", "rome_deposit"] {
            let tt = tags(&[t]);
            assert_eq!(seams(&base(&tt)), vec![Seam::Bridge], "tag {t}");
        }
        let none = tags(&[]);
        let mut i = base(&none);
        i.tx_type_byte = Some(DEPOSIT_TX_TYPE_BYTE);
        assert_eq!(seams(&i), vec![Seam::Bridge], "0x7E deposit type byte");
    }

    // ── the negative case that the whole feature turns on ────────────────────
    #[test]
    fn ordinary_evm_tx_sits_on_no_seam() {
        // A plain Rhea transfer — the shape that floods the feed under load and made
        // the client-side-filtered Cross-chain screen render empty.
        let tt = tags(&["coin_transfer"]);
        let i = SeamInput {
            action_tags: &tt,
            to: Some("0x1111111111111111111111111111111111111111"),
            tx_type: "Rhea",
            origination: "ecdsa",
            controlled_by_solana: false,
            tx_type_byte: Some(2),
            has_cpi_target: false,
        };
        assert!(seams(&i).is_empty());
        assert!(!is_cross_vm(&i));
    }

    #[test]
    fn a_tx_can_sit_on_several_seams_at_once() {
        let tt = tags(&["solana_cpi", "bridge_out"]);
        let mut i = base(&tt);
        i.origination = "solana_unsigned";
        assert_eq!(seams(&i), vec![Seam::EvmToSol, Seam::SolToEvm, Seam::Bridge]);
    }

    #[test]
    fn seam_labels_match_the_persisted_and_wire_format() {
        assert_eq!(Seam::EvmToSol.as_str(), "evm_to_sol");
        assert_eq!(Seam::SolToEvm.as_str(), "sol_to_evm");
        assert_eq!(Seam::Bridge.as_str(), "bridge");
    }
}
