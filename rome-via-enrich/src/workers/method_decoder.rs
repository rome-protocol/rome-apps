/// Method decoder worker.
///
/// On startup: seeds `method_signatures` with hardcoded Rome-specific selectors.
/// Runtime: polls `evm_tx` for rows with an unrecognised `method_id`, then queries
/// 4byte.directory at ≤2 req/s. Rate-limited and non-blocking — decode failures are
/// logged and skipped, never stall the worker. When a selector has multiple
/// signatures registered in 4byte (common: legit + later collision-bait spam),
/// the lowest-id (oldest) entry wins — see `fetch_fourbyte` for rationale.
use sqlx::PgPool;
use std::time::Duration;
use tracing::{debug, info, warn};

#[path = "method_seed_generated.rs"]
mod method_seed_generated;
use method_seed_generated::GENERATED_SIGNATURES;

// ERC-20 / ERC-721 and Rome-specific method selectors seeded on startup.
// Canonical seed table — selectors that the decoder worker won't find on
// 4byte.directory (Rome precompiles + Rome-specific contracts) PLUS the
// standard EVM surface as defense in depth against 4byte purges.
//
// Sourced from rome-solidity/contracts/interface.sol +
// rome-uniswap-v2 + canonical OpenZeppelin ABIs.
// Selectors verified via `cast sig "<signature>"` (= keccak256(sig)[..4]).
//
// Layered by precompile address / contract for review-friendliness; the
// table's primary key is selector so order doesn't affect runtime.
const SEED_SIGNATURES: &[(&str, &str)] = &[
    // ─── ERC-20 standard ─────────────────────────────────────────────────
    ("0xa9059cbb", "transfer(address,uint256)"),
    ("0x095ea7b3", "approve(address,uint256)"),
    ("0x23b872dd", "transferFrom(address,address,uint256)"),
    ("0x70a08231", "balanceOf(address)"),
    ("0x18160ddd", "totalSupply()"),
    ("0xdd62ed3e", "allowance(address,address)"),
    ("0x06fdde03", "name()"),
    ("0x95d89b41", "symbol()"),
    ("0x313ce567", "decimals()"),
    ("0x40c10f19", "mint(address,uint256)"),
    ("0x9dc29fac", "burn(address,uint256)"),
    // ─── ERC-721 ─────────────────────────────────────────────────────────
    ("0x6352211e", "ownerOf(uint256)"),
    ("0x42842e0e", "safeTransferFrom(address,address,uint256)"),
    ("0xb88d4fde", "safeTransferFrom(address,address,uint256,bytes)"),
    // ─── Ownable ─────────────────────────────────────────────────────────
    ("0x8da5cb5b", "owner()"),
    ("0xf2fde38b", "transferOwnership(address)"),
    // ─── WETH9 / WSOL wrap-unwrap ────────────────────────────────────────
    ("0xd0e30db0", "deposit()"),
    ("0x2e1a7d4d", "withdraw(uint256)"),
    // ─── Oracle keeper (PriceBook) ───────────────────────────────────────
    // The current keeper reads every registered asset in one atomic tx per tick.
    // Seeding it makes the tx render "Oracle refresh" (via classify's refreshAll →
    // oracle_refresh) instead of a bare "call to 0x…(selector 0xae27861b)".
    ("0xae27861b", "refreshAll(bytes32[])"),
    // ─── UniswapV2 Router02 / Romeswap ──────────────────────────────────
    ("0x38ed1739", "swapExactTokensForTokens(uint256,uint256,address[],address,uint256)"),
    ("0x8803dbee", "swapTokensForExactTokens(uint256,uint256,address[],address,uint256)"),
    ("0x7ff36ab5", "swapExactETHForTokens(uint256,address[],address,uint256)"),
    ("0x4a25d94a", "swapTokensForExactETH(uint256,uint256,address[],address,uint256)"),
    ("0x18cbafe5", "swapExactTokensForETH(uint256,uint256,address[],address,uint256)"),
    ("0xfb3bdb41", "swapETHForExactTokens(uint256,address[],address,uint256)"),
    ("0xe8e33700", "addLiquidity(address,address,uint256,uint256,uint256,uint256,address,uint256)"),
    ("0xf305d719", "addLiquidityETH(address,uint256,uint256,uint256,address,uint256)"),
    ("0xbaa2abde", "removeLiquidity(address,address,uint256,uint256,uint256,address,uint256)"),
    ("0x02751cec", "removeLiquidityETH(address,uint256,uint256,uint256,address,uint256)"),
    ("0x2195995c", "removeLiquidityWithPermit(address,address,uint256,uint256,uint256,address,uint256,bool,uint8,bytes32,bytes32)"),
    ("0xad615dec", "quote(uint256,uint256,uint256)"),
    ("0x054d50d4", "getAmountOut(uint256,uint256,uint256)"),
    ("0x85f8c259", "getAmountIn(uint256,uint256,uint256)"),
    ("0xd06ca61f", "getAmountsOut(uint256,address[])"),
    ("0x1f00ca74", "getAmountsIn(uint256,address[])"),
    // ─── Romeswap MinimalRouter ──────────────────────────────────────────
    ("0x7a00ee9b", "addLiq(address,address,uint256,uint256,address)"),
    ("0xf24b25b6", "burnLPFor(address,address,address)"),
    // ─── ERC20SPLFactory ─────────────────────────────────────────────────
    ("0xffe575ae", "add_spl_token(string,string,uint8)"),
    ("0xbed62659", "create_token(string,string,uint8)"),
    ("0x81de150c", "add_spl_token_no_metadata(bytes32,uint8)"),
    // ─── ISystemProgram (0xFF..07) ───────────────────────────────────────
    ("0x77764881", "program_id()"),
    ("0xb76fd45b", "rome_evm_program_id()"),
    ("0xfa2b1a5f", "bytes32_to_base58(bytes32)"),
    ("0x5df01b72", "base58_to_bytes32(bytes)"),
    ("0x570ca735", "operator()"),
    ("0xe132a122", "mint_id()"),
    // ─── ICrossProgramInvocation (0xFF..08) — CpiProgram ─────────────────
    ("0x7480cb86", "invoke(bytes32,(bytes32,bool,bool)[],bytes)"),
    ("0xb94f3733", "invoke_signed(bytes32,(bytes32,bool,bool)[],bytes,bytes32[])"),
    ("0xc13465d9", "account_info(bytes32)"),
    ("0x593762e8", "account_data_at(bytes32,uint16,uint16)"),
    ("0xb317d4c1", "account_u64_at(bytes32,uint16)"),
    ("0xde79ed54", "account_lamports(bytes32)"),
    ("0x944336f8", "pdas_batch_derive(bytes[][],bytes32)"),
    // ─── IHelperProgram (0xFF..09) ───────────────────────────────────────
    ("0x5a7c3259", "create_ata(address)"),
    ("0x3de2251a", "create_ata(address,bytes32)"),
    ("0xd258a69d", "create_ata_for_key(bytes32,bytes32)"),
    ("0xff3556ca", "create_pda(address)"),
    ("0x58e88298", "create_pda(address,uint64)"),
    ("0x6e3f24e0", "swap_gas_to_lamports(uint64)"),
    ("0x5fe71665", "transfer_lamports(address,uint64)"),
    ("0xb12be5ba", "transfer_spl(address,uint64)"),
    ("0xba3a5eac", "transfer_spl(bytes32,uint64)"),
    ("0x53b505e0", "transfer_spl(address,uint64,bytes32)"),
    ("0xb6977879", "transfer_spl(bytes32,uint64,bytes32)"),
    ("0x766b362a", "transfer_spl(bytes32,bytes32,uint64,bytes32)"),
    ("0xe479df56", "transfer_spl(address,address,uint64,bytes32)"),
    ("0xabf6f675", "approve_spl(address,uint64,bytes32)"),
    ("0x7881d453", "approve_spl_raw_delegate(bytes32,bytes32,uint64,bytes32,uint8)"),
    ("0xd795522b", "mint_spl(address,uint64,bytes32)"),
    ("0xe97d3291", "create_mint_account(bytes32)"),
    ("0x4f75e987", "init_spl_mint(bytes32,uint8,bytes32,bool,bytes32)"),
    ("0x20972d0f", "create_and_init_mint(uint8,bytes32,bool,bytes32,bytes32)"),
    ("0x8854a299", "pda(address)"),
    ("0x5c6d04b3", "pda_with_salt(address,bytes32)"),
    ("0x31db4f82", "ata(address)"),
    ("0xfeb1c647", "ata(address,bytes32)"),
    ("0xdd0119c8", "user_balance(address,bytes32)"),
    ("0xed72dbc8", "allowance_of(address,address,bytes32)"),
    ("0x4479b709", "deposit_from_ata(uint256)"),
    // ─── IEd25519 (0xFF..0a) ─────────────────────────────────────────────
    ("0x5cff850e", "verify_from_allowlist(bytes32[],bytes,uint8,uint8)"),
    // ─── IWithdraw (0x42..16) — legacy uncached ──────────────────────────
    ("0x4d8b0ea4", "withdrawal(bytes32)"),
    ("0x7f3124a0", "withdraw_to_pda(uint256)"),
    ("0x8059abc0", "withdraw_to_ata(uint256)"),
    // ─── ISystemCached (0xFF..04) ────────────────────────────────────────
    ("0xe0402a8d", "create_pda()"),
    ("0x4ceab657", "create_pda(uint64)"),
    ("0x48e2bb86", "create_pda(uint64,bytes32)"),
    ("0xcc258bbf", "create_pda(bytes32,uint64,bytes32)"),
    ("0x93225c9f", "allocate(uint64,bytes32)"),
    ("0x8ac00bdc", "assign(bytes32,bytes32)"),
    ("0x5d359fbd", "transfer(address,uint64)"),
    ("0xfd54d1ea", "transfer(bytes32,uint64)"),
    ("0x875abfc0", "transfer(bytes32,uint64,bytes32)"),
    // ─── ISplCached (0xFF..05) ───────────────────────────────────────────
    // transfer(address,uint256) shares selector 0xa9059cbb with ERC-20 above.
    ("0x6a467394", "transfer(bytes32,uint256)"),
    ("0x57cfeeee", "transfer(address,uint256,bytes32)"),
    ("0x7db527f9", "transfer(bytes32,uint256,bytes32)"),
    ("0x401e3367", "transferFrom(address,address,uint256,bytes32)"),
    ("0x8180f2fc", "approve(address,uint256,bytes32)"),
    ("0x1e458bee", "mint(address,uint256,bytes32)"),
    ("0x0b0ad508", "init(bytes32,bytes32,bytes32)"),
    ("0x73b9aa91", "account(address)"),
    ("0x882358ae", "account(bytes32)"),
    // ─── IAssociatedSplCached (0xFF..06) ─────────────────────────────────
    // create_ata(address) shares 0x5a7c3259 with IHelperProgram (above).
    // create_ata(address,bytes32) shares 0x3de2251a likewise.
    ("0xb6d336ed", "create_ata()"),
    ("0x81972e35", "create_ata(bytes32)"),
    // ─── IWithdrawCached (0xFF..0b) ──────────────────────────────────────
    // withdrawal(bytes32) / withdraw_to_pda / withdraw_to_ata share selectors
    // with the legacy IWithdraw (0x42..16) — address dispatches, not selector.
    ("0xb6b55f25", "deposit(uint256)"),
    // ─── SPL_ERC20_cached wrapper ────────────────────────────────────────
    ("0x5e094743", "ensure_token_account(address)"),
    ("0x4796feb3", "get_token_account(address)"),
    ("0xe3e091cd", "create_token_account(address)"),
    ("0x833d6907", "mint_to(address,uint256)"),
];

/// Seed hardcoded selectors into method_signatures.
///
/// Two tables are seeded, both `source = 'seed'`: the hand-typed
/// `SEED_SIGNATURES` (Rome precompiles + defense-in-depth EVM standard
/// surface), then `GENERATED_SIGNATURES` (harvested from our own compiled
/// contract ABIs — see `rome-via-enrich/scripts/harvest_abi_selectors.py`).
/// The harvester excludes any selector already in `SEED_SIGNATURES`, so the
/// two tables never race to overwrite each other's row.
///
/// Uses `ON CONFLICT DO UPDATE` so the curated seed wins over earlier
/// `source='4byte'` rows that the runtime decoder may have inserted with
/// useless signatures (e.g. signature = selector itself when 4byte.directory
/// couldn't resolve a Rome-specific selector). Safe to call repeatedly; idempotent.
/// The exact rows `seed_static` upserts, in order. Pulled out so tests can
/// pin this without a live DB (`seed_static` itself just walks this and
/// executes the upsert per row).
fn seed_rows() -> impl Iterator<Item = &'static (&'static str, &'static str)> {
    SEED_SIGNATURES.iter().chain(GENERATED_SIGNATURES.iter())
}

pub async fn seed_static(pool: &PgPool) -> anyhow::Result<()> {
    for (selector, signature) in seed_rows() {
        sqlx::query(
            "INSERT INTO rome_via.method_signatures (selector, signature, source)
             VALUES ($1, $2, 'seed')
             ON CONFLICT (selector) DO UPDATE
             SET signature = EXCLUDED.signature,
                 source    = EXCLUDED.source",
        )
        .bind(selector)
        .bind(signature)
        .execute(pool)
        .await?;
    }
    info!(
        hand_seed = SEED_SIGNATURES.len(),
        generated_seed = GENERATED_SIGNATURES.len(),
        "seeded static method signatures (upsert)"
    );
    Ok(())
}

/// How long a raw-placeholder row (`source = '4byte' AND signature = selector`
/// — 4byte had no match, or an older bug once cached a raw selector after a
/// transient failure) stays "settled" before the decoder reconsiders it.
/// Resolved rows (`signature <> selector`) and `source = 'seed'` rows are
/// never subject to this TTL — they're excluded from `unknown_selectors_sql`
/// unconditionally, regardless of `added_at`.
const METHOD_SIG_RECHECK_TTL: &str = "30 days";

/// Runtime decode loop — finds unknown selectors and resolves via 4byte.directory.
/// Distinct method selectors present in `evm_tx` but not yet in
/// `method_signatures`, found by walking the index rather than scanning the table.
///
/// The previous form was:
///
/// ```sql
/// SELECT DISTINCT method_id FROM rome_via.evm_tx
/// WHERE method_id IS NOT NULL AND method_id <> '0x'
///   AND method_id NOT IN (SELECT selector FROM rome_via.method_signatures)
/// LIMIT $1
/// ```
///
/// Measured on hadrian-lt 2026-07-27: **4,262 ms and 1,275,483 disk block reads
/// (~10 GB) per execution, returning zero rows.** Against a 5s poll it ran
/// back-to-back — roughly 2 GB/s of continuous waste, and the dominant load on a
/// managed Postgres instance shared with three other chains.
///
/// `LIMIT` does not rescue it. The limit short-circuits only when unknown selectors
/// EXIST; in the steady state — every selector already resolved, which is the normal
/// case — the planner must read every row to prove the result is empty. So the query
/// is cheapest exactly when there is work and most expensive when there is none.
///
/// This is a loose index scan (a "skip scan"): seed with the smallest selector, then
/// repeatedly seek strictly past the last one. Each step is one index probe, so the
/// cost is O(distinct selectors) — 222 on this chain — instead of O(rows), and it
/// stays flat as the chain grows. Requires the (chain_id, method_id) index added in
/// migration 0228.
///
/// A selector is excluded from the result (i.e. treated as already known) only
/// when it has a REAL row — resolved (`signature <> selector`), curated
/// (`source = 'seed'`), or a still-fresh raw placeholder. A stale raw
/// placeholder (older than `METHOD_SIG_RECHECK_TTL`) does NOT count as real,
/// so it's re-included here and `run()` revisits it — this is what lets a
/// selector poisoned by a transient 4byte failure self-heal instead of being
/// cached wrong forever.
pub fn unknown_selectors_sql() -> String {
    format!(
        r#"
    WITH RECURSIVE walk AS (
        (SELECT method_id
           FROM rome_via.evm_tx
          WHERE chain_id = $1 AND method_id IS NOT NULL
          ORDER BY method_id
          LIMIT 1)
        UNION ALL
        SELECT (SELECT method_id
                  FROM rome_via.evm_tx
                 WHERE chain_id = $1
                   AND method_id > walk.method_id
                 ORDER BY method_id
                 LIMIT 1)
          FROM walk
         WHERE walk.method_id IS NOT NULL
    )
    SELECT method_id FROM walk
     WHERE method_id IS NOT NULL
       AND method_id <> '0x'
       AND NOT EXISTS (
             SELECT 1 FROM rome_via.method_signatures ms
              WHERE ms.selector = walk.method_id
                AND (
                      ms.signature <> ms.selector
                   OR ms.source = 'seed'
                   OR ms.added_at >= NOW() - INTERVAL '{ttl}'
                )
           )
     LIMIT $2
    "#,
        ttl = METHOD_SIG_RECHECK_TTL
    )
}

/// Upsert for a selector 4byte RESOLVED to a real signature (`Ok(Some)`).
/// Refreshes signature/source/added_at on conflict.
///
/// `method_signatures` is shared across every per-chain enrich process (the
/// table has no `chain_id` column), so a sibling process running an OLDER
/// image can be mid-batch — it SELECTed unknowns before its own `seed_static`
/// re-ran — and try to write here. `WHERE method_signatures.source = '4byte'`
/// makes the update touch ONLY rows 4byte itself authored: curated `seed` rows
/// and operator-applied `manual` rows (both preserved per migration 0207) are
/// left alone, so no interleave can demote them. Allow-list (not `<> 'seed'`)
/// so future curated sources stay protected by default. Near no-op on the
/// normal path — `unknown_selectors_sql` excludes `seed` rows explicitly and
/// every resolved row (`signature <> selector`, which covers `manual`) via the
/// TTL predicate, so this upsert normally fires only for a genuinely-4byte or
/// brand-new selector; the guard is what makes it safe under the race anyway.
pub const UPSERT_RESOLVED_SQL: &str = "\
INSERT INTO rome_via.method_signatures (selector, signature, source)
 VALUES ($1, $2, '4byte')
 ON CONFLICT (selector) DO UPDATE
     SET signature = EXCLUDED.signature,
         source    = EXCLUDED.source,
         added_at  = NOW()
   WHERE method_signatures.source = '4byte'";

/// Upsert for a GENUINE MISS (`Ok(None)`) — record the raw selector as its own
/// signature so the API shows `0xdeadbeef` rather than `unknown()`, and start
/// the recheck TTL via `added_at = NOW()`. Same allow-list guard as
/// `UPSERT_RESOLVED_SQL`: the Ok(None) path is the LIKELIER demote for Rome
/// precompile selectors (4byte doesn't know them), so guarding only the
/// resolved path would miss the common case.
pub const UPSERT_RAW_PLACEHOLDER_SQL: &str = "\
INSERT INTO rome_via.method_signatures (selector, signature, source, added_at)
 VALUES ($1, $2, '4byte', NOW())
 ON CONFLICT (selector) DO UPDATE
     SET signature = EXCLUDED.signature,
         source    = EXCLUDED.source,
         added_at  = NOW()
   WHERE method_signatures.source = '4byte'";

pub async fn run(
    pool: PgPool,
    chain_id: i64,
    fourbyte_enabled: bool,
    poll_interval: Duration,
    batch_size: i64,
) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .user_agent("rome-via-enrich/0.1")
        .build()?;

    // Token-bucket: 2 requests per second to 4byte.directory
    let mut last_fourbyte_req = std::time::Instant::now();
    let fourbyte_min_gap = Duration::from_millis(500);

    loop {
        // Find method_ids in evm_tx that have no entry in method_signatures.
        let sql = unknown_selectors_sql();
        let rows: Vec<(String,)> = match sqlx::query_as(&sql)
            .bind(chain_id)
            .bind(batch_size)
            .fetch_all(&pool)
            .await
        {
            Ok(rows) => rows,
            Err(e) => {
                // Don't let a failing query masquerade as "no unknown
                // selectors": the cursor below still stamps last_processed_at =
                // NOW() with count 0, so a broken query (dropped index, schema
                // regression) would otherwise look perfectly healthy forever.
                warn!(chain_id, error = %e, "unknown-selector query failed; skipping this poll");
                Vec::new()
            }
        };

        let unknown_count = rows.len();
        if unknown_count > 0 {
            debug!(count = unknown_count, "resolving unknown method selectors");
        }

        for (selector,) in rows {
            if !fourbyte_enabled {
                // Insert a placeholder so we don't keep re-querying.
                let _ = sqlx::query(
                    "INSERT INTO rome_via.method_signatures (selector, signature, source)
                     VALUES ($1, $2, 'manual')
                     ON CONFLICT (selector) DO NOTHING",
                )
                .bind(&selector)
                .bind("unknown()")
                .execute(&pool)
                .await;
                continue;
            }

            // Rate-limit: ensure we respect ≤2 req/s.
            let elapsed = last_fourbyte_req.elapsed();
            if elapsed < fourbyte_min_gap {
                tokio::time::sleep(fourbyte_min_gap - elapsed).await;
            }
            last_fourbyte_req = std::time::Instant::now();

            match fetch_fourbyte(&client, &selector).await {
                Ok(Some(signature)) => {
                    let _ = sqlx::query(UPSERT_RESOLVED_SQL)
                        .bind(&selector)
                        .bind(&signature)
                        .execute(&pool)
                        .await;
                    debug!(%selector, %signature, "resolved via 4byte.directory");
                }
                Ok(None) => {
                    // Genuine miss (200 with empty results) — record the raw selector
                    // so the API shows "0xdeadbeef" rather than an inscrutable
                    // "unknown()". `added_at = NOW()` starts the recheck TTL
                    // (`METHOD_SIG_RECHECK_TTL`); `unknown_selectors_sql` re-includes
                    // it once that expires, so this stays a settled answer for a
                    // while rather than being re-queried every poll, but still
                    // eventually revisited in case 4byte gains a submission later.
                    let _ = sqlx::query(UPSERT_RAW_PLACEHOLDER_SQL)
                        .bind(&selector)
                        .bind(&selector)
                        .execute(&pool)
                        .await;
                }
                Err(e) => {
                    warn!(%selector, error = %e, "4byte.directory lookup failed, skipping");
                }
            }
        }

        // Update cursor (last_processed = max block seen, keyed by worker name).
        // For method_decoder we track processed count rather than a slot cursor.
        let _ = sqlx::query(
            "INSERT INTO rome_via.enrich_cursors (chain_id, worker, last_processed, last_processed_at)
             VALUES ($1, 'method_decoder', $2, NOW())
             ON CONFLICT (chain_id, worker) DO UPDATE
                 SET last_processed    = EXCLUDED.last_processed,
                     last_processed_at = NOW()",
        )
        .bind(chain_id)
        .bind(unknown_count as i64)
        .execute(&pool)
        .await;

        tokio::time::sleep(poll_interval).await;
    }
}

/// Query 4byte.directory for a hex selector. Returns the **oldest** sane
/// signature for the selector, or None if not found.
///
/// Why oldest-first: 4byte is open-submission, so any selector with even one
/// collision-bait submission will return multiple entries. Real signatures for
/// common selectors (ERC-20, Uniswap, OpenZeppelin) were registered in
/// 2017–2020 when 4byte launched. Collision-bait spam started in 2022+ and is
/// always newer than the legit entry it tries to displace. Picking the lowest
/// `id` therefore selects the legit entry without any pattern matching, and
/// self-corrects against future spam patterns we'd otherwise have to enumerate.
///
/// Concrete example: selector 0x18cbafe5 returns
///   • id 171807 `swapExactTokensForETH(…)` (2020) — the real Uniswap router method
///   • id 844309 `join_tg_invmru_haha_617eab6(…)` (2022) — collision-bait spam
async fn fetch_fourbyte(
    client: &reqwest::Client,
    selector: &str,
) -> anyhow::Result<Option<String>> {
    let url = format!(
        "https://www.4byte.directory/api/v1/signatures/?hex_signature={}",
        selector
    );
    let resp = client.get(&url).send().await?;
    let status = resp.status();
    let body = resp.text().await?;
    classify_fourbyte(status, &body)
}

/// Turn a 4byte.directory HTTP status + body into a resolution decision.
/// Pure so the status→result mapping is unit-testable without a network dep.
///
/// A non-2xx (403 from their Cloudflare rate-limit, 429, 500, ...) is an
/// outage on 4byte's end, NOT evidence the selector has no signature — it
/// must be `Err` (retryable by the caller) rather than `Ok(None)`, or a
/// transient rate-limit permanently poisons the selector the moment it's
/// cached as a miss. Only a 200 with an empty `results` array is a genuine
/// "nobody has registered this selector" answer.
fn classify_fourbyte(status: reqwest::StatusCode, body: &str) -> anyhow::Result<Option<String>> {
    if !status.is_success() {
        anyhow::bail!("4byte.directory returned HTTP {status}");
    }

    #[derive(serde::Deserialize)]
    struct FourByteResponse {
        results: Vec<FourByteResult>,
    }
    #[derive(serde::Deserialize)]
    struct FourByteResult {
        id: i64,
        text_signature: String,
    }

    let parsed: FourByteResponse = serde_json::from_str(body)?;
    Ok(pick_oldest_sane(
        parsed.results.into_iter().map(|r| (r.id, r.text_signature)),
    ))
}

/// Pick the oldest (lowest 4byte id) signature that passes a basic sanity
/// filter. ASCII-only and length-capped — keeps DB rows clean against
/// occasional malformed submissions, but does not try to detect spam by
/// content; the id-ordering does that.
fn pick_oldest_sane(items: impl IntoIterator<Item = (i64, String)>) -> Option<String> {
    items
        .into_iter()
        .filter(|(_, s)| s.is_ascii() && s.len() <= 512)
        .min_by_key(|(id, _)| *id)
        .map(|(_, s)| s)
}

// ─────────────────────────────────────────────────────────────
// Unit tests
// ─────────────────────────────────────────────────────────────
#[cfg(test)]
mod tests {
    /// Finding the DISTINCT values of a low-cardinality column must not scan the
    /// table. Measured on hadrian-lt 2026-07-27, the anti-join form cost 4,262 ms and
    /// 1,275,483 disk block reads (~10 GB) PER EXECUTION and returned zero rows —
    /// running back-to-back against a 5s poll, it was ~2 GB/s of pure waste and the
    /// dominant load on a database shared with three other chains.
    ///
    /// `LIMIT` does not save it: it short-circuits only when unknowns EXIST. In the
    /// steady state — every selector already known, which is the normal case — the
    /// planner must scan everything to prove the result is empty.
    ///
    /// The recursive form walks the index one distinct value at a time, so the cost
    /// is O(distinct selectors) — 222 here — not O(rows).
    #[test]
    fn unknown_selector_query_is_a_loose_index_scan_not_a_table_scan() {
        let q = super::unknown_selectors_sql();
        assert!(
            q.contains("WITH RECURSIVE"),
            "must walk distinct values via the index, not scan the table: {q}"
        );
        assert!(
            !q.contains("SELECT DISTINCT"),
            "SELECT DISTINCT over evm_tx is the 10 GB scan this replaces: {q}"
        );
        // Each step must seek strictly past the previous value, or the walk either
        // stalls on one selector or silently skips others.
        assert!(
            q.contains("method_id >"),
            "each probe must seek past the previous selector: {q}"
        );
        // The whole point is bounded probes; an unbounded inner query would scan.
        assert!(
            q.matches("LIMIT 1").count() >= 2,
            "both the seed and the step must fetch exactly one value: {q}"
        );
    }


    use super::*;

    #[test]
    fn seed_signatures_have_correct_format() {
        for (sel, sig) in SEED_SIGNATURES {
            assert!(sel.starts_with("0x"), "selector must start with 0x: {sel}");
            assert_eq!(sel.len(), 10, "selector must be 10 chars: {sel}");
            assert!(!sig.is_empty(), "signature must be non-empty: {sig}");
        }
    }

    #[test]
    fn seed_selectors_match_keccak256_of_signature() {
        // For every (selector, signature) row in the seed, verify
        // `selector == "0x" + hex(keccak256(signature.bytes)[..4])`.
        // Catches typos and stale comments from the underlying solidity ABI.
        use sha3::{Digest, Keccak256};
        for (sel, sig) in SEED_SIGNATURES {
            let hash = Keccak256::digest(sig.as_bytes());
            let computed = format!("0x{:02x}{:02x}{:02x}{:02x}", hash[0], hash[1], hash[2], hash[3]);
            assert_eq!(
                *sel, computed.as_str(),
                "selector mismatch for signature {sig:?}: hardcoded={sel}, computed={computed}",
            );
        }
    }

    #[test]
    fn no_duplicate_selectors_in_seed() {
        // The same selector mapped to two different signatures would mean
        // we picked the wrong canonical signature for one of them.
        // (Address-dispatched aliases — e.g. ISplCached and ERC-20 both using
        //  0xa9059cbb for transfer(address,uint256) — share the SAME signature
        //  string, so this dedup catches genuine bugs without flagging those.)
        let mut seen: std::collections::HashMap<&str, &str> = std::collections::HashMap::new();
        for (sel, sig) in SEED_SIGNATURES {
            if let Some(prev) = seen.insert(sel, sig) {
                assert_eq!(prev, *sig, "selector {sel} mapped to two different signatures: {prev:?} vs {sig:?}");
            }
        }
    }

    // ── GENERATED_SIGNATURES: harvested-from-ABI seed sanity ───────────────

    #[test]
    fn generated_signatures_non_empty() {
        // Regression guard for the harvester silently producing nothing
        // (e.g. all artifact dirs missing) and shipping an empty seed.
        assert!(
            GENERATED_SIGNATURES.len() > 500,
            "expected 500+ harvested selectors, got {}",
            GENERATED_SIGNATURES.len()
        );
    }

    #[test]
    fn generated_signatures_well_formed() {
        for (sel, sig) in GENERATED_SIGNATURES {
            assert!(sel.starts_with("0x"), "selector must start with 0x: {sel}");
            assert_eq!(sel.len(), 10, "selector must be 10 chars: {sel}");
            assert!(!sig.is_empty(), "signature must be non-empty for {sel}");
        }
    }

    #[test]
    fn generated_signatures_no_duplicate_selectors() {
        let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for (sel, _) in GENERATED_SIGNATURES {
            assert!(seen.insert(sel), "duplicate selector in GENERATED_SIGNATURES: {sel}");
        }
    }

    #[test]
    fn generated_signatures_dont_overlap_hand_seed() {
        // The harvester excludes hand-seeded selectors so seed_static's two
        // upsert passes never race to overwrite each other's row.
        for (sel, _) in GENERATED_SIGNATURES {
            assert!(
                SEED_SIGNATURES.iter().all(|(hand_sel, _)| hand_sel != sel),
                "selector {sel} is in both SEED_SIGNATURES and GENERATED_SIGNATURES"
            );
        }
    }

    #[test]
    fn generated_signatures_match_keccak256() {
        use sha3::{Digest, Keccak256};
        for (sel, sig) in GENERATED_SIGNATURES {
            let hash = Keccak256::digest(sig.as_bytes());
            let computed = format!("0x{:02x}{:02x}{:02x}{:02x}", hash[0], hash[1], hash[2], hash[3]);
            assert_eq!(*sel, computed.as_str(), "selector mismatch for {sig:?}");
        }
    }

    #[test]
    fn generated_signatures_spot_check_compound_supply() {
        // Comet's supply(address,uint256) is not in the hand seed — this
        // selector must come from the ABI harvest, not the curated list.
        assert!(
            SEED_SIGNATURES.iter().all(|(sel, _)| *sel != "0xf2b9fdb8"),
            "0xf2b9fdb8 should not be hand-seeded (this test would be vacuous)"
        );
        let found = GENERATED_SIGNATURES.iter().find(|(sel, _)| *sel == "0xf2b9fdb8");
        assert_eq!(found, Some(&("0xf2b9fdb8", "supply(address,uint256)")));
    }

    #[test]
    fn seed_static_rows_include_generated_only_selector() {
        // Exercises the actual `seed_rows()` helper `seed_static` upserts
        // from (no live DB in this crate's test suite to gate on) — not a
        // re-derived copy of its logic, so a regression in the real chain()
        // call is caught here.
        let rows: Vec<&(&str, &str)> = seed_rows().collect();
        assert!(
            rows.contains(&&("0xf2b9fdb8", "supply(address,uint256)")),
            "seed_static's rows must include generated-only selectors"
        );
        // And every hand-seeded row still comes through untouched.
        assert!(rows.contains(&&("0xa9059cbb", "transfer(address,uint256)")));
    }

    #[test]
    fn transfer_selector_present() {
        let transfer = SEED_SIGNATURES
            .iter()
            .find(|(sel, _)| *sel == "0xa9059cbb");
        assert!(transfer.is_some(), "transfer(address,uint256) must be seeded");
        assert_eq!(transfer.unwrap().1, "transfer(address,uint256)");
    }

    #[test]
    fn approve_selector_present() {
        let approve = SEED_SIGNATURES
            .iter()
            .find(|(sel, _)| *sel == "0x095ea7b3");
        assert!(approve.is_some(), "approve(address,uint256) must be seeded");
    }

    #[test]
    fn spl_erc20_cached_selectors_present() {
        // SPL_ERC20_cached wrapper methods — decode to human signatures rather
        // than raw hex / generic "Call". Each pair is also checked by
        // `seed_selectors_match_keccak256_of_signature`.
        for (sel, sig) in [
            ("0x5e094743", "ensure_token_account(address)"),
            ("0x4796feb3", "get_token_account(address)"),
            ("0xe3e091cd", "create_token_account(address)"),
            ("0x833d6907", "mint_to(address,uint256)"),
        ] {
            let found = SEED_SIGNATURES.iter().find(|(s, _)| *s == sel);
            assert!(found.is_some(), "{sig} ({sel}) must be seeded");
            assert_eq!(found.unwrap().1, sig, "wrong signature for {sel}");
        }
    }

    #[test]
    fn picks_oldest_signature_displaces_collision_spam() {
        // Real 4byte.directory response for 0x18cbafe5 (Cassius tx
        // 0x2f3b916d…7149b). Spam was submitted in 2022 (id 844309) and
        // outranks the 2020 legit entry (id 171807) in default API ordering;
        // sorting by id ascending picks the legit one without any blacklist.
        let items = vec![
            (
                844309,
                "join_tg_invmru_haha_617eab6(address,uint256,bool)".to_string(),
            ),
            (
                171807,
                "swapExactTokensForETH(uint256,uint256,address[],address,uint256)"
                    .to_string(),
            ),
        ];
        assert_eq!(
            pick_oldest_sane(items).as_deref(),
            Some("swapExactTokensForETH(uint256,uint256,address[],address,uint256)"),
        );
    }

    #[test]
    fn picks_lowest_id_in_unordered_list() {
        let items = vec![
            (5, "newerCandidate()".to_string()),
            (1, "transfer(address,uint256)".to_string()),
            (3, "middleCandidate()".to_string()),
        ];
        assert_eq!(
            pick_oldest_sane(items).as_deref(),
            Some("transfer(address,uint256)"),
        );
    }

    #[test]
    fn rejects_non_ascii_and_oversized_signatures() {
        let huge = "x".repeat(513);
        let items = vec![
            (1, "naïve(uint256)".to_string()),
            (2, huge),
            (3, "legit(address)".to_string()),
        ];
        assert_eq!(pick_oldest_sane(items).as_deref(), Some("legit(address)"));
    }

    #[test]
    fn returns_none_for_empty_results() {
        let items: Vec<(i64, String)> = vec![];
        assert_eq!(pick_oldest_sane(items), None);
    }

    // ── classify_fourbyte: status/body → resolution decision ──────────────
    //
    // A transient outage on 4byte's end (403 from their Cloudflare rate-limit,
    // 429, 5xx) must never be treated the same as a genuine "no signature
    // registered for this selector" — the former is retryable, the latter is
    // a real answer worth caching. Only a 200 with an empty `results` array
    // is a genuine miss.

    #[test]
    fn non_2xx_status_is_err_not_ok_none() {
        for code in [403u16, 429, 500, 502, 503] {
            let status = reqwest::StatusCode::from_u16(code).unwrap();
            let result = classify_fourbyte(status, r#"{"results":[]}"#);
            assert!(
                result.is_err(),
                "status {code} must be Err (retryable), not Ok(None) (permanent miss)"
            );
        }
    }

    #[test]
    fn ok_200_with_empty_results_is_genuine_miss() {
        let status = reqwest::StatusCode::OK;
        let result = classify_fourbyte(status, r#"{"results":[]}"#);
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn ok_200_with_results_resolves_oldest() {
        let status = reqwest::StatusCode::OK;
        let body = r#"{"results":[
            {"id": 844309, "text_signature": "join_tg_invmru_haha_617eab6(address,uint256,bool)"},
            {"id": 171807, "text_signature": "swapExactTokensForETH(uint256,uint256,address[],address,uint256)"}
        ]}"#;
        let result = classify_fourbyte(status, body).unwrap();
        assert_eq!(
            result.as_deref(),
            Some("swapExactTokensForETH(uint256,uint256,address[],address,uint256)")
        );
    }

    #[test]
    fn malformed_body_on_200_is_err() {
        let status = reqwest::StatusCode::OK;
        let result = classify_fourbyte(status, "not json");
        assert!(result.is_err(), "unparseable 200 body must be Err, not a silent miss");
    }
}
