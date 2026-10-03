/// Cross-chain classification worker.
///
/// Classifies each EVM tx as Rhea | Remus | Romulus and writes to
/// `cross_chain_correlations`. Runs on an infinite poll loop advancing
/// the `enrich_cursors` row for worker='cross_chain'.
///
/// # Terminology
/// "CPI" / "invoke" here = a **real** Solana program invocation, i.e., a log
/// line of the form `Program <ID> invoke [<depth>]` produced by the Solana
/// runtime when `solana_program::invoke` / `invoke_signed` fires. Precompile
/// read shortcuts on Rome's `CpiProgram` precompile at `0xff..08`
/// (`account_info`, `account_data_at`, `account_u64_at`, `account_lamports`,
/// `pdas_batch_derive`) dispatch as `NonEvmCall::CrossStateEthCall` — they
/// make no Solana syscall and produce no `Program X invoke [N]` line, so they
/// have **no effect on classification**. Only `Invoke` / `Composed` paths
/// (`invoke`, `invoke_signed`, HelperProgram mutation methods, Withdraw)
/// influence the verdict here (`CrossStateEthCall` is NOT a Solana CPI; see the
/// precompile dispatch in `rome-evm` for the full table).
///
/// # Classification (depth-aware — Rhea / Remus / Romulus)
/// For every `sol_signature` linked to an EVM tx we fetch the Solana tx and
/// inspect **only the top-level (depth-1) programs** — the programs invoked
/// directly by the transaction's own instructions, parsed from the
/// `Program <ID> invoke [<N>]` log lines where `N` is the CPI depth. Inner CPIs
/// (depth ≥ 2) are deliberately ignored.
///
/// - **Rhea** — the default: 1 EVM RLP → 1 chain. Three routes in: (a) the
///   unsigned/synthetic lane (`solana_unsigned`) — a regular EVM tx, never
///   Romulus; (b) a proxy-relayed tx — its top-level set is just the rome-evm
///   program (the EVM leg) plus infra (`DEFAULT_INFRA` ∪ per-cluster rome-evm +
///   meta-hook IDs ∪ `extra_infra_programs`); (c) a signed SDK tx that composed
///   nothing beyond the execution prelude. **A cached wrapper's inner SPL
///   `transfer` CPI fires at depth ≥ 2 under rome-evm, so it does NOT flip the
///   tx to Romulus** — the key correctness property. (Before the depth-aware
///   fix this worker inspected *any* CPI depth and mislabeled single-chain DeFi
///   — Comet supply/withdraw, Uniswap swaps — as Romulus.)
/// - **Romulus** — a *signed* EVM RLP (`origination != solana_unsigned`)
///   self-submitted via the SDK, plus ≥ 1 additional *native* Solana
///   instruction: a top-level (depth-1) program that is neither infra nor the
///   rome-evm program — a genuinely-composed native leg the submitter added
///   beyond what the RLP needs to execute. (The ATA-create prelude is infra and
///   does not count.)
/// - **Remus** — ≥ 2 EVM RLP legs across ≥ 2 chains. **NOT produced here:** the
///   sister-chain legs live in other rollups' DBs and cannot be observed in
///   this single-chain enrich pass, so Remus stays structurally unproduced.
///   That is correct and expected (the UI hides the Remus tile). Detecting
///   Remus requires cross-chain / sister-rollup indexing — a separate concern.
///
/// # Auditability
/// When a tx is classified Romulus we log the exact set of top-level non-infra,
/// non-rome-evm programs that drove the decision (`flipping_programs`). Tune the
/// `extra_infra_programs` config key to suppress false positives specific to
/// your cluster.
use serde_json::json;
use sqlx::PgPool;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tracing::{debug, info, warn};

const COMPUTE_BUDGET: &str = "ComputeBudget111111111111111111111111111111";
const SYSTEM_PROGRAM: &str = "11111111111111111111111111111111";
/// Associated Token Account program. A top-level `create` / `createIdempotent`
/// is the SDK/proxy **execution prelude** — it ensures a token account exists so
/// the RLP's SPL operation can land — NOT a composed native leg. The ATA program
/// only creates/manages token accounts (it never moves value), so it is
/// unambiguously prelude and belongs in the infra set. (SPL Token / Token-2022
/// are deliberately NOT infra: a top-level token *transfer* the submitter
/// composed is a plausible Romulus signal and must stay a flip.)
const ATA_PROGRAM: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";

/// Programs that are always rome infrastructure regardless of cluster. These
/// never represent user-intent cross-chain work:
/// * ComputeBudget, System — Solana runtime fixtures every tx sets up
/// * ATA — the token-account-setup prelude that accompanies SPL execution
/// * BPF / Native loaders — only involved in program deployment
/// * Sysvar — read-only runtime accounts (rarely shown as an invocation, but
///   including for safety).
const DEFAULT_INFRA: &[&str] = &[
    COMPUTE_BUDGET,
    SYSTEM_PROGRAM,
    ATA_PROGRAM,
    "BPFLoader1111111111111111111111111111111111",
    "BPFLoader2111111111111111111111111111111111",
    "BPFLoaderUpgradeab1e11111111111111111111111",
    "NativeLoader1111111111111111111111111111111",
    "Sysvar1111111111111111111111111111111111111",
];

/// Whether a correlation row carries anything a reader cannot already derive.
///
/// `Rhea` is the `COALESCE` default, an empty `solana_legs` is the default, and
/// no CPI target means there is nothing to label — such a row restates only
/// defaults plus data already held in `evm_tx` / `eth_block`. Both consumers
/// (`rome-via-api`'s `TX_SELECT` and the `cross_vm_seams` worker) read the table
/// through `COALESCE(ccc.rome_tx_type, 'Rhea')` and
/// `COALESCE(jsonb_array_length(ccc.solana_legs), 0)`, so for them an absent row
/// and a default row are the same value.
fn is_informative(rome_tx_type: &str, solana_legs_empty: bool, has_cpi_target: bool) -> bool {
    rome_tx_type != "Rhea" || !solana_legs_empty || has_cpi_target
}

pub async fn run(
    pool: PgPool,
    chain_id: i64,
    solana_rpc_url: String,
    solana_cluster: String,
    rome_evm_program_id: String,
    meta_hook_program_id: String,
    extra_infra_programs: Vec<String>,
    program_labels: Vec<(String, String)>,
    cpi_plumbing_programs: Vec<String>,
    poll_interval: Duration,
    batch_size: i64,
) -> anyhow::Result<()> {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;

    let mut infra: HashSet<String> = DEFAULT_INFRA.iter().map(|s| s.to_string()).collect();
    // Keep a copy of the rome-evm program id: it is folded into `infra` (so a
    // top-level rome-evm instruction never counts as a native leg) AND passed
    // explicitly to `classify` so the rome-evm leg is excluded even if infra
    // tuning ever drops it. The rome-evm program at top level is the EVM leg,
    // not a composed native leg.
    let rome_evm_program_id = rome_evm_program_id;
    infra.insert(rome_evm_program_id.clone());
    infra.insert(meta_hook_program_id);
    infra.extend(extra_infra_programs);
    info!(
        worker = "cross_chain",
        infra_count = infra.len(),
        "infra program set built"
    );

    // Registry-projected CPI-target NAME map (program_id → label), built once.
    // NAME-ONLY: it supplies the human label for a captured target; it does NOT
    // gate capture — an uncurated target is still indexed (with a None label;
    // the frontend truncates the id). See `extract_cpi_target`.
    let program_labels: HashMap<String, String> = program_labels.into_iter().collect();
    // CPI-target plumbing skip-set: the universal token/system/compute/loader
    // programs that are never a meaningful CPI target. `infra` already carries
    // compute/system/ATA/loaders/rome-evm; `cpi_plumbing_programs` (registry-
    // projected: splToken/splToken2022/associatedToken/system/memo) adds the
    // token programs deliberately kept OUT of `infra` (a top-level SPL transfer
    // is a Romulus signal, but at depth ≥ 2 it is plumbing).
    let cpi_plumbing: HashSet<String> =
        infra.iter().cloned().chain(cpi_plumbing_programs).collect();
    info!(
        worker = "cross_chain",
        program_label_count = program_labels.len(),
        cpi_plumbing_count = cpi_plumbing.len(),
        "CPI program-label name map + plumbing skip-set built"
    );

    // F6/E-N7: bounded cursor hold-back across polls when an RPC read or DB
    // write misses.
    let mut hold = super::rpc_verdict::HoldState::clear();
    const MAX_HOLD: u32 = 30;

    loop {
        let cursor: i64 = sqlx::query_scalar(
            "SELECT COALESCE(last_processed, 0)
             FROM rome_via.enrich_cursors
             WHERE chain_id = $1 AND worker = 'cross_chain'",
        )
        .bind(chain_id)
        .fetch_optional(&pool)
        .await?
        .unwrap_or(0);

        // origination drives the Rhea/Romulus gate (see `classify`): the
        // unsigned/synthetic lane is never Romulus. LEFT JOIN evm_tx +
        // COALESCE('ecdsa') mirrors the API's derivation (txs.rs) — a missing
        // evm_tx row defaults to 'ecdsa' (Romulus-capable), the conservative
        // non-masking default.
        let rows: Vec<(String, i64, Option<i64>, Option<f64>, String)> = sqlx::query_as(
            r#"
            SELECT ebt.tx_hash, ebt.slot_number, eb.params_number, eb.params_block_timestamp::FLOAT8,
                   COALESCE(et.origination, 'ecdsa') AS origination
            FROM rome_via.eth_block_txs ebt
            JOIN rome_via.eth_block eb
                ON eb.slot_number = ebt.slot_number
                AND eb.slot_block_idx = ebt.slot_block_idx
                AND eb.chain_id = ebt.chain_id
            LEFT JOIN rome_via.evm_tx et
                ON et.tx_hash = ebt.tx_hash
                AND et.chain_id = ebt.chain_id
            WHERE ebt.chain_id = $1 AND ebt.slot_number > $2
            ORDER BY ebt.slot_number ASC, ebt.tx_idx ASC
            LIMIT $3
            "#,
        )
        .bind(chain_id)
        .bind(cursor)
        .bind(batch_size)
        .fetch_all(&pool)
        .await
        .unwrap_or_default();

        if rows.is_empty() {
            tokio::time::sleep(poll_interval).await;
            continue;
        }

        // E-C1: defer the (possibly LIMIT-cut) top slot so a full batch never
        // skips a boundary slot's tail. Fetch query unchanged; only what we
        // process + persist changes.
        let slots: Vec<i64> = rows.iter().map(|r| r.1).collect();
        let plan = super::batch_cursor::plan_batch(&slots, batch_size as usize);
        if plan.giant_slot {
            warn!(worker = "cross_chain", slot = slots[0],
                  "single slot fills an entire batch; tail beyond batch_size cannot be deferred (unexpected on one-block-per-slot chains)");
        }

        let mut earliest_miss: Option<(i64, super::rpc_verdict::MissKind)> = None;

        for (tx_hash, slot_number, block_number, ts_epoch, origination) in &rows[..plan.process_count] {
            let sol_sigs: Vec<(String,)> = match sqlx::query_as(
                "SELECT sol_signature FROM rome_via.evm_tx_sol_tx
                 WHERE chain_id = $1 AND evm_tx_hash = $2",
            )
            .bind(chain_id)
            .bind(tx_hash)
            .fetch_all(&pool)
            .await
            {
                Ok(v) => v,
                Err(e) => {
                    warn!(%tx_hash, error = %e, "evm_tx_sol_tx lookup failed — deferring tx");
                    if earliest_miss.is_none() {
                        earliest_miss = Some((*slot_number, super::rpc_verdict::db_miss_kind(&e)));
                    }
                    continue;
                }
            };
            // F6/SEV-1: sol_sigs legitimately empty means the settlement leg
            // hasn't been mirrored into evm_tx_sol_tx yet (sync lag), NOT "no
            // Solana legs exist" — grounding confirmed 0 permanent zero-leg
            // txs. Defer as transient rather than classifying (and persisting)
            // from data known to be incomplete.
            if sol_sigs.is_empty() {
                if earliest_miss.is_none() {
                    earliest_miss = Some((*slot_number, super::rpc_verdict::MissKind::Transient));
                }
                continue;
            }

            // Union the TOP-LEVEL (depth-1) program sets across every sol_sig.
            // Multi-sig txs (DoTxIterative / Holder / Batch) span several
            // sol_signatures; a native leg composed in any of them counts.
            // Inner CPIs (depth ≥ 2 — e.g. a cached wrapper's SPL transfer
            // under rome-evm) are excluded by `fetch_tx_programs`.
            let mut top_level: HashSet<String> = HashSet::new();
            let mut cpi_target: Option<CpiTarget> = None;
            let mut row_missed = false;
            let mut row_transient = false;
            for (sig,) in &sol_sigs {
                match fetch_tx_programs(&http, &solana_rpc_url, &program_labels, &cpi_plumbing, sig).await {
                    Ok(Some((progs, target))) => {
                        top_level.extend(progs);
                        // First CPI target across the tx's sol_sigs wins (a
                        // multi-sig iterative tx CPIs from one of its legs).
                        if cpi_target.is_none() {
                            cpi_target = target;
                        }
                    }
                    // E-N7/F4: Ok(None) = pruned / null / meta-absent / truncated =
                    // a TERMINAL miss (won't resolve) — defer, no verdict written.
                    Ok(None) => {
                        debug!(sig, "sol tx not retrievable/truncated — deferring tx");
                        row_missed = true;
                    }
                    // E-N7/F2: transport Err = TRANSIENT miss (recovers) — hold the
                    // row indefinitely; a recovered leg may reveal a native instr.
                    Err(e) => {
                        warn!(sig, error = %e, "RPC getTransaction failed — deferring tx");
                        row_missed = true;
                        row_transient = true;
                    }
                }
            }
            if row_missed {
                // rows are slot-ascending, so the first miss is the earliest. A row
                // with ANY transient component is held indefinitely; a purely
                // terminal row is given up on after MAX_HOLD.
                if earliest_miss.is_none() {
                    let kind = if row_transient {
                        super::rpc_verdict::MissKind::Transient
                    } else {
                        super::rpc_verdict::MissKind::Terminal
                    };
                    earliest_miss = Some((*slot_number, kind));
                }
                continue;
            }

            let (rome_tx_type, flipping) =
                classify(&top_level, &infra, &rome_evm_program_id, origination);
            if rome_tx_type == "Romulus" {
                info!(
                    tx_hash,
                    flipping_programs = ?flipping,
                    "classified as Romulus (top-level native instruction)"
                );
            }

            // F2 — real leg counts.
            //
            // `solana_legs` = the composed *native* Solana legs. By the model
            // contract these are empty for Rhea/Remus and present only for
            // Romulus (a Solana tx carrying a top-level native instruction next
            // to the EVM leg). We emit the carrying sol_signatures only when a
            // native leg was actually observed; a Rhea wrapper's settlement
            // sigs are NOT composed native legs and are left out.
            let solana_legs: Vec<serde_json::Value> = if rome_tx_type == "Romulus" {
                sol_sigs
                    .iter()
                    .map(|(sig,)| json!({ "solChain": solana_cluster, "solSignature": sig }))
                    .collect()
            } else {
                Vec::new()
            };
            let solana_legs_is_empty = solana_legs.is_empty();
            // `evm_legs` = the EVM DoTx leg we observe for this tx on THIS
            // chain. We deliberately do NOT fabricate cross-chain Remus legs:
            // sister-rollup legs live in other rollups' DBs and are not visible
            // in this single-chain pass. One honest leg is the right answer.
            let evm_legs = json!([{
                "chainId": chain_id,
                "blockNumber": block_number,
                "txHash": tx_hash,
            }]);
            let ts: Option<chrono::DateTime<chrono::Utc>> =
                ts_epoch.and_then(|e| chrono::DateTime::from_timestamp(e as i64, 0));
            let block_number_val = block_number.unwrap_or(0);

            // Only persist a row that says something. An ordinary EVM tx yields
            // `rome_tx_type = 'Rhea'` (the COALESCE default), empty `solana_legs`,
            // and no CPI target — a row whose every field restates either the
            // default or data already in `evm_tx` / `eth_block`. Both consumers
            // read it as `COALESCE(ccc.rome_tx_type, 'Rhea')` +
            // `COALESCE(jsonb_array_length(ccc.solana_legs), 0)`, so an absent row
            // and a default row are already indistinguishable to them.
            //
            // Forward-only: existing rows are left alone. The informative case is
            // still UPSERTed, and the uninformative case runs as a bare UPDATE so
            // a reclassify pass can still downgrade a stale label — an UPDATE that
            // matches nothing is an index probe, not a write.
            let informative =
                is_informative(rome_tx_type, solana_legs_is_empty, cpi_target.is_some());

            let result = if informative {
                sqlx::query(
                    r#"
                INSERT INTO rome_via.cross_chain_correlations
                    (chain_id, tx_hash, rome_tx_type, evm_legs, solana_legs, block_number, timestamp,
                     cpi_program, cpi_program_label, cpi_instruction)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (chain_id, tx_hash)
                DO UPDATE SET
                    rome_tx_type      = EXCLUDED.rome_tx_type,
                    evm_legs          = EXCLUDED.evm_legs,
                    solana_legs       = EXCLUDED.solana_legs,
                    block_number      = EXCLUDED.block_number,
                    timestamp         = EXCLUDED.timestamp,
                    cpi_program       = EXCLUDED.cpi_program,
                    cpi_program_label = EXCLUDED.cpi_program_label,
                    cpi_instruction   = EXCLUDED.cpi_instruction
                "#,
                )
            } else {
                sqlx::query(
                    r#"
                UPDATE rome_via.cross_chain_correlations SET
                    rome_tx_type      = $3,
                    evm_legs          = $4,
                    solana_legs       = $5,
                    block_number      = $6,
                    timestamp         = $7,
                    cpi_program       = $8,
                    cpi_program_label = $9,
                    cpi_instruction   = $10
                WHERE chain_id = $1 AND tx_hash = $2
                "#,
                )
            }
            .bind(chain_id)
            .bind(tx_hash)
            .bind(rome_tx_type)
            .bind(&evm_legs)
            .bind(serde_json::Value::Array(solana_legs))
            .bind(block_number_val)
            .bind(ts)
            .bind(cpi_target.as_ref().map(|t| t.program.clone()))
            .bind(cpi_target.as_ref().and_then(|t| t.label.clone()))
            .bind(cpi_target.as_ref().and_then(|t| t.instruction.clone()))
            .execute(&pool)
            .await;

            match result {
                Ok(_) => debug!(tx_hash, rome_tx_type, "classified tx"),
                Err(e) => {
                    warn!(tx_hash, error = %e, "cross_chain write failed");
                    if earliest_miss.is_none() {
                        earliest_miss = Some((*slot_number, super::rpc_verdict::db_miss_kind(&e)));
                    }
                }
            }
        }

        // F6/E-N7: don't commit past an RPC miss OR a swallowed DB write. Hold
        // the cursor just below the earliest miss so that slot re-processes
        // next poll. A TERMINAL miss (pruned tx / constraint violation) is
        // given up on after MAX_HOLD polls — advance past ONLY that stuck slot
        // so one dead row can't wedge the worker; a TRANSIENT miss (transport
        // Err / non-constraint DB error) is held indefinitely (a total outage
        // stalls, visible as a stale cursor, rather than force-bleeding lost
        // rows).
        let outcome =
            super::rpc_verdict::valve_persist_cursor(plan.new_cursor, earliest_miss, hold, MAX_HOLD);
        hold = outcome.hold;
        if outcome.gave_up {
            warn!(worker = "cross_chain",
                "terminal miss unresolved for MAX_HOLD polls; advancing past stuck slot [F6]");
        }
        let persist_cursor = outcome.persist;

        sqlx::query(
            r#"
            INSERT INTO rome_via.enrich_cursors (chain_id, worker, last_processed, last_processed_at)
            VALUES ($1, 'cross_chain', $2, NOW())
            ON CONFLICT (chain_id, worker)
            DO UPDATE SET last_processed = EXCLUDED.last_processed,
                          last_processed_at = EXCLUDED.last_processed_at
            "#,
        )
        .bind(chain_id)
        .bind(persist_cursor)
        .execute(&pool)
        .await?;

        tracing::debug!(
            worker = "cross_chain",
            processed = rows.len(),
            new_cursor = persist_cursor,
            "batch done"
        );

        // E-N7: pace retries in real time on any miss (loop otherwise only sleeps
        // when the batch is empty; without this a fast-failing RPC hot-spins and
        // burns MAX_HOLD in seconds). [E-N7 F2]
        if earliest_miss.is_some() {
            tokio::time::sleep(poll_interval).await;
        }
    }
}

/// Parse a single Solana log line of the form `Program <ID> invoke [<N>]`,
/// returning `(program_id, depth)`. The Solana runtime prints this line every
/// time `solana_program::invoke` / `invoke_signed` fires; `N` is the CPI
/// invocation depth (1 = a top-level instruction in the tx, ≥ 2 = an inner
/// CPI under a parent program). Returns `None` for any other log line
/// (`Program <ID> success`, `Program log: …`, `Program data: …`, etc.).
fn parse_invoke_line(line: &str) -> Option<(String, u32)> {
    let rest = line.strip_prefix("Program ")?;
    let idx = rest.find(" invoke [")?;
    let program = rest[..idx].to_string();
    let after = &rest[idx + " invoke [".len()..];
    let close = after.find(']')?;
    let depth: u32 = after[..close].parse().ok()?;
    Some((program, depth))
}

/// From a Solana tx's log lines, return the set of **top-level (depth-1)**
/// program IDs — the programs invoked directly by the transaction's own
/// instructions, NOT inner CPIs.
///
/// This is the classification-relevant distinction: a cached-wrapper
/// `.transfer` CPIs to SPL Token / ATA / System *inside* EVM execution, i.e.
/// at depth ≥ 2 under the rome-evm program. Those inner programs appear in the
/// log stream but are NOT top-level, so they must not flip a single-chain DeFi
/// tx to Romulus. Only a genuinely-composed *native* instruction shows up at
/// depth 1 next to the rome-evm leg.
fn top_level_programs(log_lines: &[&str]) -> HashSet<String> {
    let mut top = HashSet::new();
    for line in log_lines {
        if let Some((program, depth)) = parse_invoke_line(line) {
            if depth == 1 {
                top.insert(program);
            }
        }
    }
    top
}

/// A CPI target captured from a settling Solana tx's logs: the depth ≥ 2 program
/// a Rome tx invoked via the `CpiProgram` precompile (`0xff..08`), plus its
/// human label (registry-curated, when available) and instruction name.
#[derive(Debug, Clone, PartialEq)]
struct CpiTarget {
    /// base58 program id of the depth ≥ 2 inner program (always set).
    program: String,
    /// registry-curated label (e.g. "mangoV4") when the program is curated;
    /// `None` for an uncurated target — the frontend then renders a truncated
    /// id. The registry only NAMES the target; it does not gate capture.
    label: Option<String>,
    /// the program's Anchor `Program log: Instruction: <Name>` line, when emitted.
    instruction: Option<String>,
}

/// Extract the CPI target from a Solana tx's ordered log lines: the FIRST
/// depth ≥ 2 program that is NOT universal plumbing. `plumbing` is the runtime
/// skip-set (token / system / ATA / memo / compute / loaders — sourced from the
/// registry, not hardcoded). That program is the CPI target **regardless of
/// whether it is named**: an uncurated target is still captured with
/// `label: None` and the frontend renders a truncated id, so a real CPI is
/// always indexed straight from the tx. `program_labels` only supplies the human
/// name. A cached wrapper's inner SPL transfer (depth-2 = SPL Token) is plumbing
/// → skipped, so there is no per-token-transfer noise.
///
/// The instruction name (if any) is the program's own Anchor `Program log:
/// Instruction: <Name>` line, emitted before the program makes any deeper CPI.
/// We stop scanning at the next `invoke [`/`success` line so a child program's
/// instruction is never mis-attributed (a non-Anchor target yields `None`).
///
/// Returns `None` when no non-plumbing depth ≥ 2 program is present.
fn extract_cpi_target(
    log_lines: &[&str],
    program_labels: &HashMap<String, String>,
    plumbing: &HashSet<String>,
) -> Option<CpiTarget> {
    for (i, line) in log_lines.iter().enumerate() {
        let Some((program, depth)) = parse_invoke_line(line) else {
            continue;
        };
        if depth < 2 || plumbing.contains(&program) {
            continue;
        }
        // First real (non-plumbing) depth ≥ 2 program = the CPI target. Capture
        // it whether or not it is named — the registry name is optional.
        let mut instruction = None;
        for next in &log_lines[i + 1..] {
            if next.contains(" invoke [") || next.ends_with(" success") {
                break;
            }
            if let Some(name) = next.strip_prefix("Program log: Instruction: ") {
                instruction = Some(name.to_string());
                break;
            }
        }
        return Some(CpiTarget {
            program: program.clone(),
            label: program_labels.get(&program).cloned(),
            instruction,
        });
    }
    None
}

/// Fetch a Solana tx and return the set of **top-level (depth-1)** program IDs
/// invoked by the tx's own instructions. Inner CPIs (depth ≥ 2 — e.g. a
/// cached wrapper's SPL `transfer` under rome-evm) are intentionally excluded;
/// see `top_level_programs`. Read-shortcut selectors that dispatch as
/// `CrossStateEthCall` produce no invoke line at all and are therefore never
/// returned; see the module-level terminology note.
async fn fetch_tx_programs(
    http: &reqwest::Client,
    rpc_url: &str,
    program_labels: &HashMap<String, String>,
    plumbing: &HashSet<String>,
    sig: &str,
) -> anyhow::Result<Option<(HashSet<String>, Option<CpiTarget>)>> {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [sig, {
            "commitment": "confirmed",
            "maxSupportedTransactionVersion": 1,
        }],
    });
    let resp: serde_json::Value = http
        .post(rpc_url)
        .json(&body)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let Some(result) = resp.get("result") else {
        return Ok(None);
    };
    if result.is_null() {
        return Ok(None);
    }
    let empty = Vec::new();
    let logs = result
        .get("meta")
        .and_then(|m| m.get("logMessages"))
        .and_then(|l| l.as_array())
        .unwrap_or(&empty);
    let lines: Vec<&str> = logs.iter().filter_map(|e| e.as_str()).collect();
    // E-N7/F4: no log lines means `meta` was absent (meta:null) — we cannot
    // classify. Treat as not-reliably-retrievable (Ok(None)) so the caller
    // defers, matching batch_trace's fetch and the E-N1 sentinel gate.
    if lines.is_empty() {
        return Ok(None);
    }
    // E-N7: truncated logs are incomplete — a depth-1 native program could sit
    // beyond the cut. Treat truncation as "not reliably retrievable" (Ok(None))
    // so the caller defers the tx rather than misclassifying from partial logs.
    if lines.iter().any(|l| l.contains("Log truncated")) {
        return Ok(None);
    }
    Ok(Some((
        top_level_programs(&lines),
        extract_cpi_target(&lines, program_labels, plumbing),
    )))
}

/// Classify a tx from its set of **top-level (depth-1)** invoked programs.
///
/// Rule (operator's canonical taxonomy — submission-path driven):
/// * **Romulus** IFF `origination != "solana_unsigned"` AND ≥ 1 top-level
///   program is neither infra nor the rome-evm program — a genuinely-composed
///   *native* Solana leg the submitter added beyond what the RLP needs to
///   execute (the signed-RLP, SDK-self-submitted path).
/// * otherwise → **Rhea** (the default — 1 EVM RLP → 1 chain). Routes in:
///   the unsigned/synthetic lane (gated to Rhea up front); proxy-relayed (no
///   instruction beyond the execution prelude → no flip, by construction); and
///   a cached wrapper's inner SPL CPI at depth ≥ 2, absent from `top_level`
///   (the depth-aware fix for the prior any-depth bug).
///
/// **Remus is NOT produced here.** Remus = ≥ 2 EVM RLP legs across ≥ 2 chains;
/// the sister-chain legs live in *other* rollups' DBs and cannot be observed in
/// this single-chain enrich pass, so Remus stays structurally unproduced. That
/// is correct and expected (the UI hides the Remus tile). Detecting Remus
/// requires cross-chain / sister-rollup indexing — a separate concern.
///
/// `origination` IS an input (operator's rule): the unsigned/synthetic lane
/// (`solana_unsigned`) is a regular EVM tx, gated to Rhea up front — never
/// Romulus. The token-account-setup prelude (ATA `createIdempotent`) lives in
/// `infra` (see `ATA_PROGRAM`), so it is not mistaken for a composed native leg.
///
/// Returns the classification plus the sorted list of non-infra, non-rome-evm
/// top-level programs that drove a Romulus verdict (empty for Rhea). The list
/// lets operators audit false-positive Romulus calls and tune
/// `extra_infra_programs`.
pub fn classify(
    top_level: &HashSet<String>,
    infra: &HashSet<String>,
    rome_evm_program_id: &str,
    origination: &str,
) -> (&'static str, Vec<String>) {
    // Origination gate (operator's taxonomy): the unsigned/synthetic lane
    // (`solana_unsigned` — DoTxUnsigned, `from` is a synthetic address) is a
    // *regular* single-chain EVM tx and is NEVER Romulus, regardless of any
    // top-level program. Romulus is exclusively the signed-RLP, SDK-self-
    // submitted path (`ecdsa` today; the signed `solana_ed25519` lane later —
    // both stay Romulus-capable). Proxy-relayed txs also can't be Romulus, but
    // that holds *by construction*: the proxy composes nothing beyond the
    // execution prelude, so they never carry a flip.
    if origination == "solana_unsigned" {
        return ("Rhea", Vec::new());
    }
    let mut flipping: Vec<String> = top_level
        .iter()
        .filter(|p| !infra.contains(*p) && p.as_str() != rome_evm_program_id)
        .cloned()
        .collect();
    if flipping.is_empty() {
        ("Rhea", Vec::new())
    } else {
        flipping.sort();
        ("Romulus", flipping)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Test fixtures ────────────────────────────────────────────────────────
    // Stand-in canonical program ids (real-looking base58, not load-bearing).
    const ROME_EVM: &str = "RPTWwELXAY4KC9ZPHhaxp7Sq1hHtU3HNEgLbSegCcWf";
    const META_HOOK: &str = "MetaHk1111111111111111111111111111111111111";
    const SPL_TOKEN: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
    // ATA_PROGRAM is now a module-level const (folded into DEFAULT_INFRA) and is
    // in scope here via `use super::*`.

    /// Infra set as `run` builds it: DEFAULT_INFRA ∪ {rome-evm, meta-hook}.
    fn infra_set() -> HashSet<String> {
        let mut s: HashSet<String> = DEFAULT_INFRA.iter().map(|x| x.to_string()).collect();
        s.insert(ROME_EVM.to_string());
        s.insert(META_HOOK.to_string());
        s
    }

    fn set(items: &[&str]) -> HashSet<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    // ── classify(): depth-aware top-level rule ───────────────────────────────

    /// A plain single-chain EVM tx: top-level is just rome-evm + ComputeBudget
    /// (both infra) → Rhea. Origination is irrelevant.
    #[test]
    fn classify_rhea_single_evm_leg() {
        let top_level = set(&[ROME_EVM, COMPUTE_BUDGET]);
        let (kind, flipping) = classify(&top_level, &infra_set(), ROME_EVM, "ecdsa");
        assert_eq!(kind, "Rhea");
        assert!(flipping.is_empty());
    }

    /// THE BUG / REGRESSION LOCK: a cached-wrapper `.transfer` CPIs to SPL
    /// Token + ATA + System *inside* EVM execution (depth ≥ 2 under rome-evm).
    /// Those inner programs must NOT appear in the TOP-LEVEL set, so the tx is
    /// Rhea — NOT Romulus. We model the input the way the depth-aware
    /// `fetch_tx_programs` produces it: only the depth-1 program(s) are present.
    #[test]
    fn classify_rhea_wrapper_inner_spl_is_not_romulus() {
        // top-level = {rome-evm} only. SPL/ATA/System were invoked at depth ≥ 2
        // and are therefore absent from this set by construction.
        let top_level = set(&[ROME_EVM, COMPUTE_BUDGET]);
        let (kind, flipping) = classify(&top_level, &infra_set(), ROME_EVM, "ecdsa");
        assert_eq!(
            kind, "Rhea",
            "wrapper inner-CPI to SPL Token must stay Rhea — inner programs are not top-level"
        );
        assert!(flipping.is_empty());
    }

    /// A genuinely-composed native leg: SPL Token invoked at TOP LEVEL (depth 1)
    /// alongside the rome-evm leg → Romulus.
    #[test]
    fn classify_romulus_composed_native_toplevel() {
        let top_level = set(&[ROME_EVM, COMPUTE_BUDGET, SPL_TOKEN]);
        let (kind, flipping) = classify(&top_level, &infra_set(), ROME_EVM, "ecdsa");
        assert_eq!(kind, "Romulus");
        assert_eq!(flipping, vec![SPL_TOKEN.to_string()]);
    }

    /// REGRESSION LOCK (ATA prelude): a top-level ATA `createIdempotent` is the
    /// SDK/proxy execution *prelude* — it ensures the `transfer_spl` destination
    /// ATA exists so the RLP can execute — NOT a composed native leg. It must
    /// stay Rhea. Before this fix the ATA program was absent from DEFAULT_INFRA,
    /// so a top-level ATA-create flipped single-chain SPL flows to Romulus (the
    /// lone false-positive on Hadrian). The ATA program only creates/manages
    /// token accounts (never moves value), so it is unambiguously prelude.
    #[test]
    fn classify_ata_prelude_is_rhea() {
        let top_level = set(&[ROME_EVM, COMPUTE_BUDGET, ATA_PROGRAM]);
        let (kind, flipping) = classify(&top_level, &infra_set(), ROME_EVM, "ecdsa");
        assert_eq!(
            kind, "Rhea",
            "top-level ATA createIdempotent is execution prelude, not a composed native leg"
        );
        assert!(flipping.is_empty());
    }

    /// ORIGINATION GATE — the unsigned/synthetic lane (`solana_unsigned`,
    /// DoTxUnsigned) is a *regular* EVM tx and is NEVER Romulus, even with a
    /// non-infra program at the top level. Operator's rule: Romulus is
    /// exclusively the signed-RLP, SDK-self-submitted path. The same top-level
    /// set on the ecdsa lane IS Romulus — proving the gate, not the program set,
    /// is what suppresses it.
    #[test]
    fn classify_solana_unsigned_is_never_romulus() {
        let top_level = set(&[ROME_EVM, COMPUTE_BUDGET, SPL_TOKEN]);
        let (kind, flipping) = classify(&top_level, &infra_set(), ROME_EVM, "solana_unsigned");
        assert_eq!(
            kind, "Rhea",
            "solana_unsigned (synthetic lane) is a regular EVM tx — never Romulus"
        );
        assert!(flipping.is_empty());
        let (kind_ecdsa, _) = classify(&top_level, &infra_set(), ROME_EVM, "ecdsa");
        assert_eq!(kind_ecdsa, "Romulus", "same set on the ecdsa lane → Romulus");
    }

    /// The signed `solana_ed25519` lane (future) stays Romulus-capable — the
    /// gate excludes only `solana_unsigned`, not every Solana-origin tx.
    #[test]
    fn classify_solana_ed25519_stays_romulus_capable() {
        let top_level = set(&[ROME_EVM, COMPUTE_BUDGET, SPL_TOKEN]);
        let (kind, _) = classify(&top_level, &infra_set(), ROME_EVM, "solana_ed25519");
        assert_eq!(kind, "Romulus", "signed ed25519 lane + composed leg → Romulus");
    }

    /// rome-evm at top level is excluded even if it is NOT in the infra set —
    /// the explicit `rome_evm_program_id` arg guards against that.
    #[test]
    fn classify_rome_evm_excluded_even_if_not_in_infra() {
        let infra: HashSet<String> = DEFAULT_INFRA.iter().map(|x| x.to_string()).collect();
        let top_level = set(&[ROME_EVM, COMPUTE_BUDGET]);
        let (kind, flipping) = classify(&top_level, &infra, ROME_EVM, "ecdsa");
        assert_eq!(kind, "Rhea");
        assert!(flipping.is_empty());
    }

    #[test]
    fn classify_rhea_for_empty_program_set() {
        let top_level: HashSet<String> = HashSet::new();
        let (kind, flipping) = classify(&top_level, &infra_set(), ROME_EVM, "ecdsa");
        assert_eq!(kind, "Rhea");
        assert!(flipping.is_empty());
    }

    #[test]
    fn classify_romulus_reports_sorted_flipping_programs() {
        let top_level = set(&[
            "Zprogram1111111111111111111111111111111111",
            "Aprogram1111111111111111111111111111111111",
            ROME_EVM,
        ]);
        let (kind, flipping) = classify(&top_level, &infra_set(), ROME_EVM, "ecdsa");
        assert_eq!(kind, "Romulus");
        assert_eq!(flipping[0], "Aprogram1111111111111111111111111111111111");
        assert_eq!(flipping[1], "Zprogram1111111111111111111111111111111111");
    }

    // ── parse_invoke_line(): depth parsing ───────────────────────────────────

    #[test]
    fn parse_invoke_line_extracts_program_and_depth() {
        assert_eq!(
            parse_invoke_line(&format!("Program {ROME_EVM} invoke [1]")),
            Some((ROME_EVM.to_string(), 1))
        );
        assert_eq!(
            parse_invoke_line(&format!("Program {SPL_TOKEN} invoke [2]")),
            Some((SPL_TOKEN.to_string(), 2))
        );
    }

    #[test]
    fn parse_invoke_line_ignores_non_invoke_lines() {
        assert_eq!(parse_invoke_line("Program log: hello"), None);
        assert_eq!(
            parse_invoke_line(&format!("Program {ROME_EVM} success")),
            None
        );
        assert_eq!(parse_invoke_line("Program data: AbCd"), None);
    }

    // ── top_level_programs(): depth-1 bucketing ──────────────────────────────

    /// The wrapper transfer log shape: rome-evm at depth 1, SPL Token + ATA at
    /// depth 2 (inner CPIs). Only rome-evm (depth 1) is returned.
    #[test]
    fn top_level_programs_keeps_only_depth_one() {
        let logs = vec![
            format!("Program {COMPUTE_BUDGET} invoke [1]"),
            format!("Program {COMPUTE_BUDGET} success"),
            format!("Program {ROME_EVM} invoke [1]"),
            format!("Program {SPL_TOKEN} invoke [2]"),
            "Program log: Instruction: Transfer".to_string(),
            format!("Program {SPL_TOKEN} success"),
            format!("Program {ATA_PROGRAM} invoke [2]"),
            format!("Program {ATA_PROGRAM} success"),
            format!("Program {ROME_EVM} success"),
        ];
        let lines: Vec<&str> = logs.iter().map(|s| s.as_str()).collect();
        let top = top_level_programs(&lines);
        assert!(top.contains(ROME_EVM), "rome-evm is depth 1");
        assert!(top.contains(COMPUTE_BUDGET), "ComputeBudget is depth 1");
        assert!(
            !top.contains(SPL_TOKEN),
            "SPL Token is depth 2 (inner CPI under rome-evm) — must be excluded"
        );
        assert!(
            !top.contains(ATA_PROGRAM),
            "ATA program is depth 2 — must be excluded"
        );
        assert_eq!(top.len(), 2);
    }

    /// A composed native leg: SPL Token invoked at depth 1 directly → kept.
    #[test]
    fn top_level_programs_keeps_composed_native_depth_one() {
        let logs = vec![
            format!("Program {ROME_EVM} invoke [1]"),
            format!("Program {ROME_EVM} success"),
            format!("Program {SPL_TOKEN} invoke [1]"),
            format!("Program {SPL_TOKEN} success"),
        ];
        let lines: Vec<&str> = logs.iter().map(|s| s.as_str()).collect();
        let top = top_level_programs(&lines);
        assert!(top.contains(ROME_EVM));
        assert!(top.contains(SPL_TOKEN), "SPL Token at depth 1 is a composed native leg");
        assert_eq!(top.len(), 2);
    }

    /// End-to-end on the structural facts: wrapper-transfer log → top-level set
    /// → classify → Rhea. The inner SPL CPI never flips the verdict.
    #[test]
    fn wrapper_transfer_logs_classify_as_rhea() {
        let logs = vec![
            format!("Program {COMPUTE_BUDGET} invoke [1]"),
            format!("Program {COMPUTE_BUDGET} success"),
            format!("Program {ROME_EVM} invoke [1]"),
            format!("Program {SPL_TOKEN} invoke [2]"),
            format!("Program {SPL_TOKEN} success"),
            format!("Program {ROME_EVM} success"),
        ];
        let lines: Vec<&str> = logs.iter().map(|s| s.as_str()).collect();
        let top = top_level_programs(&lines);
        let (kind, flipping) = classify(&top, &infra_set(), ROME_EVM, "ecdsa");
        assert_eq!(kind, "Rhea");
        assert!(flipping.is_empty());
    }

    // ── extract_cpi_target(): always index the depth≥2 CPI target from the tx ──
    // The CPI target is the FIRST depth≥2 program that is NOT universal plumbing
    // (SPL Token / Token-2022 / ATA / System / Memo / compute / loaders — the
    // runtime skip-set, sourced from the registry, NOT the name map). The
    // registry name map only supplies the human LABEL: an uncurated target is
    // STILL captured (label None → the frontend truncates the id). A cached
    // wrapper's inner SPL transfer (depth-2 = SPL Token) is plumbing → skipped,
    // so there's no per-token-transfer noise. Synthetic ids; none are real.
    const MANGO: &str = "MangoTestProg11111111111111111111111111111111";
    const UNCURATED: &str = "UncuratedDexTestPrg11111111111111111111111111";

    fn labels(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }
    /// The universal-plumbing skip-set the worker builds at runtime — the
    /// token/system/compute programs that are never a meaningful CPI target.
    fn plumbing() -> HashSet<String> {
        [SPL_TOKEN, COMPUTE_BUDGET, SYSTEM_PROGRAM, ATA_PROGRAM]
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    /// THE FALLBACK (regression lock): an UNCURATED, non-plumbing depth-2 program
    /// is STILL captured — the registry only NAMES it, it does not gate capture.
    /// label None → the frontend renders a truncated id ("CPI → Eo7W…UaB · Swap").
    #[test]
    fn cpi_target_captures_uncurated_target_with_no_label() {
        let logs = vec![
            format!("Program {ROME_EVM} invoke [1]"),
            format!("Program {UNCURATED} invoke [2]"),
            "Program log: Instruction: Swap".to_string(),
            format!("Program {SPL_TOKEN} invoke [3]"),
            format!("Program {SPL_TOKEN} success"),
            format!("Program {UNCURATED} success"),
            format!("Program {ROME_EVM} success"),
        ];
        let lines: Vec<&str> = logs.iter().map(|s| s.as_str()).collect();
        // EMPTY name map — nothing curated. Must STILL capture the target.
        let t = extract_cpi_target(&lines, &HashMap::new(), &plumbing())
            .expect("uncurated target must STILL be captured — registry only names it");
        assert_eq!(t.program, UNCURATED);
        assert_eq!(t.label, None, "no registry name → None; the frontend truncates the id");
        assert_eq!(t.instruction.as_deref(), Some("Swap"));
    }

    #[test]
    fn cpi_target_uses_registry_label_when_curated() {
        let logs = vec![
            format!("Program {COMPUTE_BUDGET} invoke [1]"),
            format!("Program {ROME_EVM} invoke [1]"),
            format!("Program {MANGO} invoke [2]"),
            "Program log: Instruction: PlaceOrder".to_string(),
            format!("Program {SPL_TOKEN} invoke [3]"),
            format!("Program {SPL_TOKEN} success"),
            format!("Program {MANGO} success"),
            format!("Program {ROME_EVM} success"),
        ];
        let lines: Vec<&str> = logs.iter().map(|s| s.as_str()).collect();
        let t = extract_cpi_target(&lines, &labels(&[(MANGO, "mangoV4")]), &plumbing())
            .expect("mango target");
        assert_eq!(t.program, MANGO);
        assert_eq!(t.label.as_deref(), Some("mangoV4"));
        assert_eq!(t.instruction.as_deref(), Some("PlaceOrder"));
    }

    #[test]
    fn cpi_target_skips_plumbing_then_picks_real_target() {
        // A token approve (SPL Token = plumbing) at depth-2 BEFORE the real
        // target is skipped; the real (even uncurated) program wins.
        let logs = vec![
            format!("Program {ROME_EVM} invoke [1]"),
            format!("Program {SPL_TOKEN} invoke [2]"),
            "Program log: Instruction: Approve".to_string(),
            format!("Program {SPL_TOKEN} success"),
            format!("Program {UNCURATED} invoke [2]"),
            "Program log: Instruction: Swap".to_string(),
            format!("Program {UNCURATED} success"),
            format!("Program {ROME_EVM} success"),
        ];
        let lines: Vec<&str> = logs.iter().map(|s| s.as_str()).collect();
        let t = extract_cpi_target(&lines, &HashMap::new(), &plumbing()).expect("real target");
        assert_eq!(t.program, UNCURATED);
        assert_eq!(t.instruction.as_deref(), Some("Swap"));
    }

    #[test]
    fn cpi_target_none_for_plumbing_only_transfer() {
        // Pure cached-wrapper SPL transfer: SPL Token at depth-2 = plumbing → no row.
        let logs = vec![
            format!("Program {ROME_EVM} invoke [1]"),
            format!("Program {SPL_TOKEN} invoke [2]"),
            "Program log: Instruction: Transfer".to_string(),
            format!("Program {SPL_TOKEN} success"),
            format!("Program {ROME_EVM} success"),
        ];
        let lines: Vec<&str> = logs.iter().map(|s| s.as_str()).collect();
        assert!(extract_cpi_target(&lines, &HashMap::new(), &plumbing()).is_none());
    }

    #[test]
    fn cpi_target_instruction_none_when_no_anchor_log() {
        // Non-Anchor target (no "Instruction:" line) → captured, instruction None.
        // Must NOT borrow a child program's instruction.
        let logs = vec![
            format!("Program {ROME_EVM} invoke [1]"),
            format!("Program {UNCURATED} invoke [2]"),
            format!("Program {SPL_TOKEN} invoke [3]"),
            "Program log: Instruction: Transfer".to_string(),
            format!("Program {SPL_TOKEN} success"),
            format!("Program {UNCURATED} success"),
            format!("Program {ROME_EVM} success"),
        ];
        let lines: Vec<&str> = logs.iter().map(|s| s.as_str()).collect();
        let t = extract_cpi_target(&lines, &HashMap::new(), &plumbing()).expect("target");
        assert_eq!(t.program, UNCURATED);
        assert_eq!(t.instruction, None, "must not borrow the inner SPL Transfer log");
    }

    #[test]
    fn cpi_target_none_when_depth1_only() {
        let logs = vec![
            format!("Program {ROME_EVM} invoke [1]"),
            format!("Program {ROME_EVM} success"),
        ];
        let lines: Vec<&str> = logs.iter().map(|s| s.as_str()).collect();
        assert!(
            extract_cpi_target(&lines, &HashMap::new(), &plumbing()).is_none(),
            "no depth≥2 → none"
        );
    }

    #[test]
    fn ordinary_evm_tx_is_not_worth_a_row() {
        // The 99% case: default type, no legs, no CPI target. Every field would
        // restate a default, and both readers COALESCE an absent row to exactly
        // these values.
        assert!(!is_informative("Rhea", true, false));
    }

    #[test]
    fn anything_beyond_the_defaults_earns_a_row() {
        assert!(is_informative("Romulus", true, false), "non-default type");
        assert!(is_informative("Rhea", false, false), "carries solana legs");
        assert!(is_informative("Rhea", true, true), "carries a CPI target");
        assert!(
            is_informative("Romulus", false, true),
            "all three signals at once"
        );
    }
}
