//! One-shot maintenance operations for rome-via-enrich.
//!
//! Reached only via `rome-via-enrich maintenance <op>` ([`crate::cli::Command::Maintenance`]);
//! `main` branches into these functions and returns immediately after — they
//! never run as part of the daemon (no migrations, no health server, no
//! supervised workers).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use tokio::sync::{Mutex, Semaphore};
use tracing::{debug, info, warn};

use crate::workers::holders::{self, DecodedTransfer};

/// Outcome of reconciling one decoded transfer against `token_transfers`.
enum Outcome {
    /// A stored row exists with a different `amount` — the u128-truncation-era
    /// corruption this op exists to heal.
    Healed,
    /// A stored row exists and already matches — nothing to do.
    Unchanged,
    /// No stored row exists for (chain_id, tx_hash, log_index). `block_unresolved`
    /// is only ever `true` when `apply` was set (dry-run never performs the block
    /// lookup — see [`reconcile_transfer`]), and means the insert was skipped
    /// rather than fabricated.
    Missing { block_unresolved: bool },
}

/// Re-extract every ERC-20 Transfer from `rome_via.evm_tx_result.tx_result`
/// (via the already-fixed [`holders::extract_transfers`]) and reconcile each
/// one against `rome_via.token_transfers`:
///
/// - a stored row whose `amount` disagrees gets healed (the old code truncated
///   to the low 128 bits of the uint256 `data` word — see `holders::hex_to_numeric`);
/// - a transfer with no stored row at all gets inserted (rows swallowed by the
///   historical `receipt_params`-vs-`tx_result` bug).
///
/// Dry-run unless `apply` is set — either way every row is scanned and every
/// counter reflects what *would* change; `apply` is only what gates the writes.
///
/// # Why bounded to `slot_number <= holders cursor`
///
/// The `holders` worker gates each transfer's `token_holders` balance delta on
/// its `token_transfers` INSERT being a *fresh* insert (`rows_affected() == 1`);
/// a repeat insert of the same `(chain_id, tx_hash, log_index)` is an
/// `ON CONFLICT DO NOTHING` no-op, which the worker reads as "already applied,
/// skip the delta" (see `holders::transfer_was_newly_inserted`). If this op
/// pre-inserted a row for a slot the worker has not reached yet, the worker's
/// own later INSERT for that row would become that no-op — so its balance
/// delta would be skipped **forever**, not merely deferred. A slot at-or-below
/// the worker's persisted cursor is, by construction, one the worker has
/// already fully processed (deltas already applied there), so healing/inserting
/// at-or-below the cursor cannot collide with work the worker still owns.
/// Bounding to the cursor — rather than "all slots" — is therefore a
/// correctness requirement, not just a safety margin.
///
/// This function never touches `rome_via.token_holders`; balances are a
/// separate, later op.
pub async fn reextract_transfers(pool: &PgPool, chain_id: i64, apply: bool) -> anyhow::Result<()> {
    const CHUNK: usize = 2000;
    const LOG_EVERY: u64 = 50_000;

    let cursor: i64 = sqlx::query_scalar(
        "SELECT COALESCE(last_processed, 0)
         FROM rome_via.enrich_cursors
         WHERE chain_id = $1 AND worker = 'holders'",
    )
    .bind(chain_id)
    .fetch_optional(pool)
    .await?
    .unwrap_or(0);

    if cursor == 0 {
        warn!(
            chain_id,
            "reextract_transfers: holders cursor is 0 (unset, or genuinely at genesis) — \
             there is nothing safe to bound the scan to; refusing to run unbounded. \
             Re-run once the holders worker has made progress on this chain."
        );
        return Ok(());
    }

    let mode = if apply { "APPLY" } else { "DRY-RUN" };
    info!(chain_id, cursor, mode, "reextract_transfers: starting");

    let mut rows_scanned: u64 = 0;
    let mut transfers_seen: u64 = 0;
    let mut amount_healed: u64 = 0;
    let mut inserted_missing: u64 = 0;
    let mut unchanged: u64 = 0;
    let mut block_unresolved: u64 = 0;

    // Keyset pagination cursor — advanced to the last row of each page.
    let mut last_slot: i64 = 0;
    let mut last_hash: String = String::new();

    loop {
        let rows: Vec<(String, i64, serde_json::Value)> = sqlx::query_as(
            "SELECT tx_hash, slot_number, tx_result
             FROM rome_via.evm_tx_result
             WHERE chain_id = $1 AND tx_result IS NOT NULL AND slot_number <= $2
               AND (slot_number, tx_hash) > ($3, $4)
             ORDER BY slot_number ASC, tx_hash ASC
             LIMIT 2000",
        )
        .bind(chain_id)
        .bind(cursor)
        .bind(last_slot)
        .bind(&last_hash)
        .fetch_all(pool)
        .await?;

        let page_len = rows.len();
        if page_len == 0 {
            break;
        }

        for (tx_hash, slot_number, tx_result) in &rows {
            rows_scanned += 1;

            for t in holders::extract_transfers(tx_result) {
                transfers_seen += 1;
                match reconcile_transfer(pool, chain_id, tx_hash, &t, apply).await? {
                    Outcome::Healed => amount_healed += 1,
                    Outcome::Unchanged => unchanged += 1,
                    Outcome::Missing { block_unresolved: true } => {
                        inserted_missing += 1;
                        block_unresolved += 1;
                    }
                    Outcome::Missing { block_unresolved: false } => {
                        inserted_missing += 1;
                    }
                }
            }

            last_slot = *slot_number;
            last_hash = tx_hash.clone();

            if rows_scanned.is_multiple_of(LOG_EVERY) {
                info!(
                    rows_scanned,
                    transfers_seen,
                    amount_healed,
                    inserted_missing,
                    unchanged,
                    block_unresolved,
                    "reextract_transfers: progress"
                );
            }
        }

        if page_len < CHUNK {
            break;
        }
    }

    info!(
        mode,
        cursor,
        rows_scanned,
        transfers_seen,
        amount_healed,
        inserted_missing,
        unchanged,
        block_unresolved,
        "reextract_transfers: done"
    );

    Ok(())
}

/// Reconcile one decoded transfer against its `token_transfers` row, applying
/// the fix only when `apply` is set (dry-run still reports what would happen).
///
/// The block-number/timestamp lookup for a missing row is only ever performed
/// when `apply` is set — a dry run counts missing rows without paying for the
/// extra join, so `block_unresolved` is always `0` on a dry run; it becomes
/// meaningful once `--apply` runs for real.
async fn reconcile_transfer(
    pool: &PgPool,
    chain_id: i64,
    tx_hash: &str,
    t: &DecodedTransfer,
    apply: bool,
) -> anyhow::Result<Outcome> {
    let existing: Option<String> = sqlx::query_scalar(
        "SELECT amount::text FROM rome_via.token_transfers
         WHERE chain_id = $1 AND tx_hash = $2 AND log_index = $3",
    )
    .bind(chain_id)
    .bind(tx_hash)
    .bind(t.log_index as i32)
    .fetch_optional(pool)
    .await?;

    match existing {
        Some(a) if a != t.amount => {
            if apply {
                sqlx::query(
                    "UPDATE rome_via.token_transfers SET amount = $1::NUMERIC
                     WHERE chain_id = $2 AND tx_hash = $3 AND log_index = $4",
                )
                .bind(&t.amount)
                .bind(chain_id)
                .bind(tx_hash)
                .bind(t.log_index as i32)
                .execute(pool)
                .await?;
            }
            Ok(Outcome::Healed)
        }
        Some(_) => Ok(Outcome::Unchanged),
        None => {
            if !apply {
                return Ok(Outcome::Missing { block_unresolved: false });
            }

            // Resolve block_number + block timestamp the SAME way holders::run
            // does (identical query + `holders::epoch_to_dt` decode), so a
            // healed/inserted row is indistinguishable from one the live worker
            // wrote. A block not (yet) indexed is a genuine "can't resolve",
            // not a transient race worth retrying here — this op runs once,
            // offline; skip rather than fabricate `block_number = 0`.
            let block_lookup: Option<(i64, Option<f64>)> = sqlx::query_as(
                "SELECT COALESCE(b.params_number, 0), b.params_block_timestamp::FLOAT8
                 FROM rome_via.eth_block_txs t
                 JOIN rome_via.eth_block b
                     ON b.chain_id = t.chain_id
                     AND b.slot_number = t.slot_number
                     AND b.slot_block_idx = t.slot_block_idx
                 WHERE t.tx_hash = $1 AND t.chain_id = $2
                 LIMIT 1",
            )
            .bind(tx_hash)
            .bind(chain_id)
            .fetch_optional(pool)
            .await?;

            let Some((block_number, epoch)) = block_lookup else {
                return Ok(Outcome::Missing { block_unresolved: true });
            };
            let block_ts = holders::epoch_to_dt(epoch);

            sqlx::query(
                "INSERT INTO rome_via.token_transfers
                     (chain_id, tx_hash, log_index, token_address, from_addr, to_addr,
                      amount, block_number, timestamp)
                 VALUES ($1, $2, $3, $4, $5, $6, $7::NUMERIC, $8, $9)
                 ON CONFLICT (chain_id, tx_hash, log_index) DO NOTHING",
            )
            .bind(chain_id)
            .bind(tx_hash)
            .bind(t.log_index as i32)
            .bind(&t.token_address)
            .bind(&t.from_addr)
            .bind(&t.to_addr)
            .bind(&t.amount)
            .bind(block_number)
            .bind(block_ts)
            .execute(pool)
            .await?;

            Ok(Outcome::Missing { block_unresolved: false })
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────
// F3 balance backfill — heal token_holders.balance to on-chain truth
// ─────────────────────────────────────────────────────────────────────────

/// ERC-20 `balanceOf(address)` selector: `keccak256("balanceOf(address)")[0..4]`.
const BALANCE_OF_SELECTOR: &str = "70a08231";

/// The EVM zero address — excluded from candidate pairs (mint/burn legs are
/// not real holders) and never queried on-chain, matching the `holders`
/// worker's own zero-address skip in `apply_balance_delta`'s caller.
const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

/// Build the `eth_call` calldata for ERC-20 `balanceOf(holder)`: the 4-byte
/// selector followed by the holder address left-padded to a 32-byte word.
/// Accepts the holder with or without a `0x` prefix; case-insensitive.
///
/// Pure — no I/O — unit-tested below.
fn balance_of_calldata(holder: &str) -> String {
    let hex = holder.trim_start_matches("0x").to_lowercase();
    format!("0x{BALANCE_OF_SELECTOR}{hex:0>64}")
}

/// Decode an `eth_call` hex result for `balanceOf` into a decimal string.
/// An empty payload (bare `"0x"`, which some proxies return for a
/// zero-value / reverted read) decodes to `"0"`; anything else is parsed as
/// a full-width uint256 — matching `holders::hex_to_numeric`'s "parse the
/// whole word, never truncate" rule, since a holder's balance is exactly the
/// kind of value that rule exists to protect.
///
/// Pure — no I/O — unit-tested below.
fn decode_balance(hex_result: &str) -> anyhow::Result<String> {
    let hex = hex_result.trim_start_matches("0x");
    if hex.is_empty() {
        return Ok("0".to_string());
    }
    let n = ethers::types::U256::from_str_radix(hex, 16)
        .map_err(|e| anyhow::anyhow!("failed to parse balanceOf result {hex_result:?}: {e}"))?;
    Ok(n.to_string())
}

/// What a backfill pair needs, given the stored `token_holders.balance` (if
/// any) and the on-chain truth. Shared by both dry-run (classification only)
/// and apply (drives the write) so the two modes can never disagree about
/// what a given `(stored, onchain)` pair means.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BackfillAction {
    /// Stored value disagrees with on-chain (or no row exists at all while
    /// on-chain is non-zero) — write the on-chain value.
    Heal,
    /// Stored value already equals on-chain — nothing to do.
    AlreadyOk,
    /// On-chain balance is exactly zero — delete the stored row (mirrors the
    /// live `holders` worker's delete-at-0 behavior in `apply_balance_delta`)
    /// rather than storing a `"0"` row.
    ZeroDelete,
}

/// Pure decision function — no I/O — unit-tested below.
fn backfill_decision(stored: Option<&str>, onchain: &str) -> BackfillAction {
    if onchain == "0" {
        return BackfillAction::ZeroDelete;
    }
    match stored {
        Some(s) if s == onchain => BackfillAction::AlreadyOk,
        _ => BackfillAction::Heal,
    }
}

/// Aggregate counters for one `backfill_balances` run. `healed` / `zeroed` /
/// `skipped` are apply-only; `drift` / `zero_balances` / `missing_rows` are
/// dry-run-only; `already_ok` and `errors` are meaningful in both modes.
#[derive(Debug, Default)]
struct Counters {
    healed: u64,
    already_ok: u64,
    zeroed: u64,
    skipped: u64,
    drift: u64,
    zero_balances: u64,
    missing_rows: u64,
    errors: u64,
}

/// Terminal result of running the apply-mode algorithm on one pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ApplyOutcome {
    Healed,
    AlreadyOk,
    Zeroed,
    Skipped,
}

/// Retry budget for the apply-mode per-pair algorithm (see `apply_pair`).
const MAX_RETRIES: u32 = 6;
/// How long to wait for rome_via + the holders worker to catch up past a
/// head before the ONE-TIME batch wait gives up (see `backfill_balances`). Must
/// exceed the holders worker's steady-state lag (~100s+ on Hadrian), so it is
/// generous — it is paid once per run, not per pair.
const SYNC_TIMEOUT: Duration = Duration::from_secs(300);
/// Poll interval while waiting for sync (see `wait_for_holders_sync`).
const SYNC_POLL: Duration = Duration::from_secs(1);
/// Log a progress line every this many completed pairs.
const LOG_EVERY: u64 = 500;

/// Heal `rome_via.token_holders.balance` rows corrupted by the old
/// `GREATEST(0, …)` clamp (see `holders::apply_balance_delta`'s doc comment)
/// by setting each holder's balance to the on-chain `balanceOf` truth.
/// On-chain is the ONLY valid ground truth here: the transfer log is
/// incomplete for wrapper tokens whose credits arrive Solana-side (they never
/// emit an ERC-20 Transfer log rome-via-enrich can see), so replaying
/// `token_transfers` — the way [`reextract_transfers`] heals `amount` — can't
/// heal `balance`.
///
/// # Why a naive read-then-write is unsafe
///
/// The proxy's `eth_call` **ignores the block-tag parameter and always reads
/// the LIVE head**, while rome-via-sync/enrich lag that head by seconds. So a
/// plain "read `balanceOf`, write it" races the live `holders` worker: a
/// transfer can land on-chain and get indexed+applied by `holders` in the
/// gap between this op's read and its write, and the write would silently
/// clobber that worker-applied delta with a now-stale on-chain snapshot.
///
/// The fix is optimistic concurrency, retried up to [`MAX_RETRIES`] times per
/// pair:
///
/// 1. Snapshot the live head (`hb` = `eth_blockNumber`) and the on-chain
///    balance (`B` = `balanceOf`) together.
/// 2. Wait ([`wait_for_holders_sync`]) until rome_via has synced past `hb`
///    AND the `holders` worker's cursor has applied every delta at-or-below
///    `hb` — i.e. until `token_transfers` is guaranteed complete for every
///    block `<= hb`.
/// 3. Inside a row-locking transaction, re-check whether any transfer
///    touching this holder landed **after** `hb`. If one did, `B` was
///    already stale the instant it was read (that transfer will still apply
///    its own delta against whatever we write) — roll back and retry with a
///    fresh `hb`/`B`. If none did, no transfer the worker hasn't already
///    accounted for can affect this balance, so `B` is safe to write.
///
/// A sync wait that never catches up within [`SYNC_TIMEOUT`] is treated as a
/// standalone bail-out (not one of the [`MAX_RETRIES`] attempts) — the
/// worker being genuinely behind isn't something retrying the same wait
/// fixes; the pair is skipped and reported so the operator can re-run later.
///
/// # Dry-run vs apply
///
/// Dry-run (default, no `apply`) only performs the read + compare — no
/// waiting, no locking, no writes — and tallies drift / zero-balance /
/// missing-row counts so the scope of the corruption can be measured before
/// committing to `--apply`.
///
/// # Candidate pairs
///
/// Every `(token, holder)` pair that ever appears as a transfer counterparty
/// (excluding [`ZERO_ADDRESS`]) unioned with every pair that currently has a
/// `token_holders` row — so both "a row exists but is wrong" and "a row is
/// missing entirely" are covered. Optionally scoped to one `token`.
/// Processed with bounded concurrency (a `tokio::sync::Semaphore` sized to
/// `concurrency`); progress is logged every [`LOG_EVERY`] completed pairs.
pub async fn backfill_balances(
    pool: &PgPool,
    chain_id: i64,
    proxy_url: &str,
    apply: bool,
    token: Option<String>,
    concurrency: usize,
) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;

    let token_filter = token.map(|t| t.to_lowercase());
    let mode = if apply { "APPLY" } else { "DRY-RUN" };

    let pairs = candidate_pairs(pool, chain_id, token_filter.as_deref()).await?;
    let total_pairs = pairs.len() as u64;
    info!(
        chain_id,
        mode,
        total_pairs,
        concurrency,
        token = ?token_filter,
        "backfill_balances: starting"
    );

    // Apply mode: do the "holders worker caught up to head" wait ONCE for the
    // whole run — NOT per pair. The worker's steady-state lag (~100s+ on
    // Hadrian) makes a per-pair wait both impossibly slow and prone to timing
    // out on every pair. Snapshot the head `hb`, wait until the holders cursor
    // has applied every delta <= hb, then reconcile each pair against this fixed
    // `hb` (see `apply_pair`).
    let batch_hb: Option<i64> = if apply {
        let hb = eth_block_number(&client, proxy_url).await?;
        info!(chain_id, hb, "backfill_balances: waiting once for holders worker to reach head before apply");
        if !wait_for_holders_sync(pool, chain_id, hb).await? {
            anyhow::bail!(
                "holders worker did not reach head block {hb} within timeout; \
                 aborting apply — re-run when the worker is current"
            );
        }
        info!(hb, "backfill_balances: holders caught up to head; beginning apply");
        Some(hb)
    } else {
        None
    };

    let semaphore = Arc::new(Semaphore::new(concurrency.max(1)));
    let counters = Arc::new(Mutex::new(Counters::default()));
    let processed = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::with_capacity(pairs.len());
    for (token_address, holder) in pairs {
        // Acquired here (in the driving loop, not inside the spawned task) so
        // the loop itself blocks once `concurrency` pairs are in flight,
        // rather than spawning every pair up front and letting them queue —
        // bounds actual concurrent work, not just concurrent bookkeeping.
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore is never closed");
        let pool = pool.clone();
        let client = client.clone();
        let proxy_url = proxy_url.to_string();
        let counters = counters.clone();
        let processed = processed.clone();

        handles.push(tokio::spawn(async move {
            let _permit = permit;

            if apply {
                let outcome = apply_pair(
                    &pool,
                    &client,
                    &proxy_url,
                    chain_id,
                    &token_address,
                    &holder,
                    batch_hb.expect("apply mode always sets batch_hb"),
                )
                .await;
                let mut c = counters.lock().await;
                match outcome {
                    ApplyOutcome::Healed => c.healed += 1,
                    ApplyOutcome::AlreadyOk => c.already_ok += 1,
                    ApplyOutcome::Zeroed => c.zeroed += 1,
                    ApplyOutcome::Skipped => c.skipped += 1,
                }
            } else {
                match dry_run_pair(&pool, &client, &proxy_url, chain_id, &token_address, &holder)
                    .await
                {
                    Ok((stored, onchain)) => {
                        let mut c = counters.lock().await;
                        match backfill_decision(stored.as_deref(), &onchain) {
                            BackfillAction::ZeroDelete => c.zero_balances += 1,
                            BackfillAction::AlreadyOk => c.already_ok += 1,
                            BackfillAction::Heal if stored.is_some() => c.drift += 1,
                            BackfillAction::Heal => c.missing_rows += 1,
                        }
                    }
                    Err(e) => {
                        warn!(%token_address, %holder, error = %e, "backfill_balances: dry-run read failed");
                        counters.lock().await.errors += 1;
                    }
                }
            }

            let n = processed.fetch_add(1, Ordering::Relaxed) + 1;
            if n.is_multiple_of(LOG_EVERY) {
                info!(processed = n, total_pairs, "backfill_balances: progress");
            }
        }));
    }

    for h in handles {
        if let Err(e) = h.await {
            warn!(error = %e, "backfill_balances: pair task panicked");
        }
    }

    let c = counters.lock().await;
    info!(
        mode,
        total_pairs,
        healed = c.healed,
        already_ok = c.already_ok,
        zeroed = c.zeroed,
        skipped = c.skipped,
        drift = c.drift,
        zero_balances = c.zero_balances,
        missing_rows = c.missing_rows,
        errors = c.errors,
        "backfill_balances: done"
    );

    Ok(())
}

/// Distinct `(token_address, holder)` pairs to check — see the "Candidate
/// pairs" section of [`backfill_balances`]'s doc comment.
async fn candidate_pairs(
    pool: &PgPool,
    chain_id: i64,
    token: Option<&str>,
) -> anyhow::Result<Vec<(String, String)>> {
    let rows: Vec<(String, String)> = match token {
        Some(t) => {
            sqlx::query_as(
                "SELECT DISTINCT token_address, holder FROM (
                    SELECT token_address, from_addr AS holder FROM rome_via.token_transfers
                      WHERE chain_id = $1 AND from_addr <> $2 AND token_address = $3
                    UNION
                    SELECT token_address, to_addr FROM rome_via.token_transfers
                      WHERE chain_id = $1 AND to_addr <> $2 AND token_address = $3
                    UNION
                    SELECT token_address, holder_address FROM rome_via.token_holders
                      WHERE chain_id = $1 AND token_address = $3
                 ) x",
            )
            .bind(chain_id)
            .bind(ZERO_ADDRESS)
            .bind(t)
            .fetch_all(pool)
            .await?
        }
        None => {
            sqlx::query_as(
                "SELECT DISTINCT token_address, holder FROM (
                    SELECT token_address, from_addr AS holder FROM rome_via.token_transfers
                      WHERE chain_id = $1 AND from_addr <> $2
                    UNION
                    SELECT token_address, to_addr FROM rome_via.token_transfers
                      WHERE chain_id = $1 AND to_addr <> $2
                    UNION
                    SELECT token_address, holder_address FROM rome_via.token_holders
                      WHERE chain_id = $1
                 ) x",
            )
            .bind(chain_id)
            .bind(ZERO_ADDRESS)
            .fetch_all(pool)
            .await?
        }
    };
    Ok(rows)
}

/// Fetch the proxy's current head block number via `eth_blockNumber`. This is
/// the LIVE head — the same head `eth_call` below reads regardless of block
/// tag (see [`backfill_balances`]'s doc) — so `hb` is the snapshot height the
/// balance read and the later invalidate-check are both anchored to.
async fn eth_block_number(client: &reqwest::Client, proxy_url: &str) -> anyhow::Result<i64> {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_blockNumber",
        "params": []
    });
    let resp: serde_json::Value = client
        .post(proxy_url)
        .json(&body)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let hex = resp
        .get("result")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("eth_blockNumber: missing result in {resp}"))?;
    i64::from_str_radix(hex.trim_start_matches("0x"), 16)
        .map_err(|e| anyhow::anyhow!("eth_blockNumber: bad hex {hex:?}: {e}"))
}

/// Read `balanceOf(holder)` on `token` via the proxy's `eth_call` — always
/// the live head regardless of the `"latest"` tag (the proxy quirk this
/// whole algorithm exists to work around; see [`backfill_balances`]'s doc).
async fn eth_call_balance_of(
    client: &reqwest::Client,
    proxy_url: &str,
    token: &str,
    holder: &str,
) -> anyhow::Result<String> {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_call",
        "params": [
            { "to": token, "data": balance_of_calldata(holder) },
            "latest"
        ]
    });
    let resp: serde_json::Value = client
        .post(proxy_url)
        .json(&body)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let hex = resp
        .get("result")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("eth_call balanceOf({token}): missing result in {resp}"))?;
    decode_balance(hex)
}

/// Dry-run: fetch on-chain truth for one pair and the stored balance, with NO
/// waiting, locking, or writing — see [`backfill_balances`]'s doc for why
/// apply mode needs those and a point-in-time read doesn't.
async fn dry_run_pair(
    pool: &PgPool,
    client: &reqwest::Client,
    proxy_url: &str,
    chain_id: i64,
    token_address: &str,
    holder: &str,
) -> anyhow::Result<(Option<String>, String)> {
    let _hb = eth_block_number(client, proxy_url).await?;
    let onchain = eth_call_balance_of(client, proxy_url, token_address, holder).await?;
    let stored: Option<String> = sqlx::query_scalar(
        "SELECT balance::text FROM rome_via.token_holders
         WHERE chain_id = $1 AND token_address = $2 AND holder_address = $3",
    )
    .bind(chain_id)
    .bind(token_address)
    .bind(holder)
    .fetch_optional(pool)
    .await?;
    Ok((stored, onchain))
}

/// Poll until rome_via has synced past `hb` AND the `holders` worker has
/// applied every delta at-or-below that height — see step 2 of
/// [`backfill_balances`]'s doc. Polls every [`SYNC_POLL`]; gives up
/// (`Ok(false)`) after [`SYNC_TIMEOUT`].
async fn wait_for_holders_sync(pool: &PgPool, chain_id: i64, hb: i64) -> anyhow::Result<bool> {
    let deadline = tokio::time::Instant::now() + SYNC_TIMEOUT;
    loop {
        let synced_past_hb: bool = sqlx::query_scalar(
            "SELECT COALESCE(MAX(params_number), -1) >= $2
             FROM rome_via.eth_block WHERE chain_id = $1",
        )
        .bind(chain_id)
        .bind(hb)
        .fetch_one(pool)
        .await?;

        if synced_past_hb {
            let hb_slot: Option<i64> = sqlx::query_scalar(
                "SELECT slot_number FROM rome_via.eth_block
                 WHERE chain_id = $1 AND params_number = $2 LIMIT 1",
            )
            .bind(chain_id)
            .bind(hb)
            .fetch_optional(pool)
            .await?;

            if let Some(hb_slot) = hb_slot {
                let holders_cursor: i64 = sqlx::query_scalar(
                    "SELECT COALESCE(last_processed, 0) FROM rome_via.enrich_cursors
                     WHERE chain_id = $1 AND worker = 'holders'",
                )
                .bind(chain_id)
                .fetch_optional(pool)
                .await?
                .unwrap_or(0);

                if holders_cursor >= hb_slot {
                    return Ok(true);
                }
            }
        }

        if tokio::time::Instant::now() >= deadline {
            return Ok(false);
        }
        tokio::time::sleep(SYNC_POLL).await;
    }
}

/// Steps 3's transaction body: lock the row, re-check for a post-`hb`
/// transfer that would invalidate this `(hb, onchain)` snapshot, and either
/// commit a heal/zero/no-op or roll back for the caller to retry.
/// `Ok(None)` = invalidated, rolled back, caller should retry with a fresh
/// snapshot. `Ok(Some(_))` = committed; this is the terminal outcome.
async fn apply_pair_txn(
    pool: &PgPool,
    chain_id: i64,
    token_address: &str,
    holder: &str,
    hb: i64,
    onchain: &str,
) -> anyhow::Result<Option<ApplyOutcome>> {
    let mut txn = pool.begin().await?;

    let stored: Option<String> = sqlx::query_scalar(
        "SELECT balance::text FROM rome_via.token_holders
         WHERE chain_id = $1 AND token_address = $2 AND holder_address = $3
         FOR UPDATE",
    )
    .bind(chain_id)
    .bind(token_address)
    .bind(holder)
    .fetch_optional(&mut *txn)
    .await?;

    let invalidated: bool = sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1 FROM rome_via.token_transfers
             WHERE chain_id = $1 AND token_address = $2
               AND (from_addr = $3 OR to_addr = $3)
               AND block_number > $4
         )",
    )
    .bind(chain_id)
    .bind(token_address)
    .bind(holder)
    .bind(hb)
    .fetch_one(&mut *txn)
    .await?;

    if invalidated {
        txn.rollback().await?;
        return Ok(None);
    }

    let outcome = match backfill_decision(stored.as_deref(), onchain) {
        BackfillAction::ZeroDelete => {
            sqlx::query(
                "DELETE FROM rome_via.token_holders
                 WHERE chain_id = $1 AND token_address = $2 AND holder_address = $3",
            )
            .bind(chain_id)
            .bind(token_address)
            .bind(holder)
            .execute(&mut *txn)
            .await?;
            ApplyOutcome::Zeroed
        }
        BackfillAction::AlreadyOk => ApplyOutcome::AlreadyOk,
        BackfillAction::Heal => {
            sqlx::query(
                "INSERT INTO rome_via.token_holders
                     (chain_id, token_address, holder_address, balance, updated_at)
                 VALUES ($1, $2, $3, $4::NUMERIC, NOW())
                 ON CONFLICT (chain_id, token_address, holder_address) DO UPDATE
                     SET balance = EXCLUDED.balance, updated_at = NOW()",
            )
            .bind(chain_id)
            .bind(token_address)
            .bind(holder)
            .bind(onchain)
            .execute(&mut *txn)
            .await?;
            ApplyOutcome::Healed
        }
    };

    txn.commit().await?;
    Ok(Some(outcome))
}

/// Apply mode for one `(token, holder)` pair — the full optimistic-
/// concurrency algorithm from [`backfill_balances`]'s doc, up to
/// [`MAX_RETRIES`] attempts. Each attempt re-reads `hb`/`B` fresh, so a retry
/// after an invalidated attempt races the CURRENT chain state, not a stale
/// snapshot. A sync-wait that never catches up is NOT one of the retried
/// attempts — it bails out immediately (see [`wait_for_holders_sync`]).
/// Apply mode for one `(token, holder)` pair against a BATCH-level head `hb`.
///
/// The "worker caught up to `hb`" wait is done ONCE per run by the caller (see
/// [`backfill_balances`]) — NOT here — because the holders worker's steady-state
/// lag (measured ~100s+ on Hadrian) makes a per-pair wait both impossibly slow
/// and prone to timing out on every pair. With the batch wait already
/// satisfied (`holders cursor >= slot(hb)`, so every delta <= hb is applied):
///
/// - read `balanceOf` (B, at live head >= hb);
/// - in a row-locking txn, check for any transfer touching this holder with
///   `block_number > hb`. NONE → the pair is unchanged since `hb`, so B (still
///   its live balance) is safe to write. SOME → the pair is being actively
///   traded past `hb`; skip it — the live A″ holders worker maintains those
///   correctly going forward, and writing a live-head B for an actively-moving
///   pair could double/under-count against the worker's own deltas.
///
/// Retries only cover transient RPC/DB errors ([`MAX_RETRIES`]); an
/// invalidation is a terminal Skip, not a retry (re-reading wouldn't un-trade
/// the pair). Residual (documented, accepted on a testnet explorer): a transfer
/// that landed in `(hb, live-head]` but is not yet synced into `token_transfers`
/// is invisible to the invalidate-check, so a pair traded in the last ~sync-lag
/// seconds could be mis-set; re-running the backfill later converges it.
async fn apply_pair(
    pool: &PgPool,
    client: &reqwest::Client,
    proxy_url: &str,
    chain_id: i64,
    token_address: &str,
    holder: &str,
    hb: i64,
) -> ApplyOutcome {
    for attempt in 1..=MAX_RETRIES {
        let onchain = match eth_call_balance_of(client, proxy_url, token_address, holder).await {
            Ok(b) => b,
            Err(e) => {
                warn!(%token_address, %holder, attempt, error = %e, "backfill_balances: balanceOf failed, retrying");
                continue;
            }
        };

        match apply_pair_txn(pool, chain_id, token_address, holder, hb, &onchain).await {
            Ok(Some(outcome)) => return outcome,
            Ok(None) => {
                debug!(%token_address, %holder, hb, "backfill_balances: pair traded after batch head — skipping (A″ maintains it forward)");
                return ApplyOutcome::Skipped;
            }
            Err(e) => {
                warn!(%token_address, %holder, attempt, error = %e, "backfill_balances: txn failed, retrying");
                continue;
            }
        }
    }

    warn!(%token_address, %holder, "backfill_balances: exhausted retries — skipping pair");
    ApplyOutcome::Skipped
}

/// Opt-in backfill of `cross_vm_seams.is_oracle` for oracle-keeper crossings on THIS
/// chain — the on-demand replacement for the retired auto-migration 0235.
///
/// The forward path (`is_oracle_method` at write + the recheck sweep) already flags
/// keeper crossings on EVERY chain going forward, so a fresh chain never needs this.
/// A chain that ran pre-recognition code, though, accumulates unflagged keeper history
/// outside the trailing recheck window; run this ONCE there (e.g. hadrian) to flag that
/// backlog. Other chains — including mainnet — stay forward-only by simply not running
/// it, which is why this is a maintenance op and not a migration. Idempotent (only flips
/// currently-unflagged rows) and keyed on the shared `ORACLE_SELECTORS`, so it covers
/// every keeper selector (legacy `refresh()` + PriceBook `refreshAll()`). Dry-run unless `apply`.
pub async fn backfill_oracle_flag(pool: &PgPool, chain_id: i64, apply: bool) -> anyhow::Result<()> {
    let pending: i64 = sqlx::query_scalar(oracle_flag_count_query())
        .bind(chain_id)
        .bind(rome_via_classify::ORACLE_SELECTORS)
        .fetch_one(pool)
        .await?;
    if !apply {
        info!(
            chain_id, pending,
            "backfill_oracle_flag: DRY-RUN — keeper crossings that would be flagged is_oracle (0 = nothing to do; forward-only chains stay here)"
        );
        return Ok(());
    }
    let res = sqlx::query(oracle_flag_update_query())
        .bind(chain_id)
        .bind(rome_via_classify::ORACLE_SELECTORS)
        .execute(pool)
        .await?;
    info!(chain_id, pending, flagged = res.rows_affected(), "backfill_oracle_flag: APPLY done");
    Ok(())
}

/// Count keeper crossings on this chain that are NOT yet flagged `is_oracle`.
fn oracle_flag_count_query() -> &'static str {
    r#"
    SELECT count(*)
    FROM rome_via.cross_vm_seams cvs
    JOIN rome_via.evm_tx et ON et.chain_id = cvs.chain_id AND et.tx_hash = cvs.tx_hash
    WHERE cvs.chain_id = $1 AND et.method_id = ANY($2) AND NOT cvs.is_oracle
    "#
}

/// Flag every not-yet-flagged keeper crossing on this chain. Idempotent.
fn oracle_flag_update_query() -> &'static str {
    r#"
    UPDATE rome_via.cross_vm_seams cvs
    SET is_oracle = true
    FROM rome_via.evm_tx et
    WHERE cvs.chain_id = $1 AND et.chain_id = cvs.chain_id AND et.tx_hash = cvs.tx_hash
      AND et.method_id = ANY($2) AND NOT cvs.is_oracle
    "#
}

// ─────────────────────────────────────────────────────────────
// Unit tests — pure helpers only (see module docs above for the I/O side,
// validated on Hadrian rather than unit-tested).
// ─────────────────────────────────────────────────────────────
#[cfg(test)]
mod backfill_balances_tests {
    use super::*;

    // ── backfill_oracle_flag queries ──

    #[test]
    fn oracle_flag_queries_target_unflagged_keeper_crossings() {
        let c = oracle_flag_count_query();
        let u = oracle_flag_update_query();
        // Both scope to this chain and the shared ORACLE_SELECTORS set ($2), and only
        // touch currently-unflagged rows so the op is idempotent + covers every keeper selector.
        assert!(c.contains("rome_via.cross_vm_seams") && c.contains("cvs.chain_id = $1"));
        assert!(c.contains("method_id = ANY($2)") && c.contains("NOT cvs.is_oracle"));
        assert!(u.contains("UPDATE rome_via.cross_vm_seams") && u.contains("SET is_oracle = true"));
        assert!(u.contains("method_id = ANY($2)"), "uses the ORACLE_SELECTORS set, not a single selector");
        assert!(u.contains("NOT cvs.is_oracle"), "idempotent — only flips currently-unflagged keeper rows");
    }

    // ── balance_of_calldata ──

    #[test]
    fn balance_of_calldata_known_holder() {
        let calldata = balance_of_calldata("0xAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAa");
        assert_eq!(
            calldata,
            "0x70a08231000000000000000000000000aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        // "0x" + 4-byte selector (8 hex) + 32-byte arg (64 hex) = 0x + 72 hex chars.
        assert_eq!(calldata.len(), 2 + 72);
    }

    #[test]
    fn balance_of_calldata_without_0x_prefix_handled() {
        let calldata = balance_of_calldata("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        assert_eq!(
            calldata,
            "0x70a08231000000000000000000000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        );
    }

    // ── decode_balance ──

    #[test]
    fn decode_balance_known_value() {
        // 1 USDC (6 decimals) = 1_000_000 = 0x0f4240.
        let hex = "0x00000000000000000000000000000000000000000000000000000000000f4240";
        assert_eq!(decode_balance(hex).unwrap(), "1000000");
    }

    #[test]
    fn decode_balance_max_uint256() {
        let hex = format!("0x{}", "f".repeat(64));
        assert_eq!(
            decode_balance(&hex).unwrap(),
            "115792089237316195423570985008687907853269984665640564039457584007913129639935"
        );
    }

    #[test]
    fn decode_balance_bare_0x_is_zero() {
        assert_eq!(decode_balance("0x").unwrap(), "0");
    }

    // ── backfill_decision ──

    #[test]
    fn backfill_decision_onchain_zero_deletes_even_with_stored_value() {
        assert_eq!(
            backfill_decision(Some("5"), "0"),
            BackfillAction::ZeroDelete
        );
    }

    #[test]
    fn backfill_decision_matching_stored_is_already_ok() {
        assert_eq!(
            backfill_decision(Some("100"), "100"),
            BackfillAction::AlreadyOk
        );
    }

    #[test]
    fn backfill_decision_mismatched_stored_heals() {
        assert_eq!(backfill_decision(Some("50"), "100"), BackfillAction::Heal);
    }

    #[test]
    fn backfill_decision_missing_row_nonzero_onchain_heals() {
        assert_eq!(backfill_decision(None, "100"), BackfillAction::Heal);
    }
}
