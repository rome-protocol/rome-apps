//! Contract-label enrichment worker.
//!
//! Resolves the **on-chain identity** of each distinct `evm_tx.to_addr` exactly
//! once and caches it in `rome_via.contract_labels`, so the API (Layer 2) can
//! JOIN a clean protocol label onto every transaction without re-probing the
//! chain per request.
//!
//! ## Two label sources: registry map FIRST, then on-chain reads
//!
//! Resolution consults two tiers, in order:
//!   1. **Registry-projected map** (the FIRST tier) — an `address → label` map
//!      projected from `rome-protocol/registry` into the deployment config and
//!      consulted by [`registry_label`] before any chain read. Protocol infra
//!      that *reverts* `name()`/`symbol()` (routers, pools, factories,
//!      Multicall3, the faucet, all of V3/V4) gets a clean label here with no
//!      on-chain read.
//!   2. **On-chain reads** (the fallback, when the registry map misses) —
//!      `name()` (`0x06fdde03`) and `symbol()` (`0x95d89b41`) eth_calls, plus
//!      runtime-bytecode selector fingerprints (`eth_getCode`) when `name()` is
//!      empty.
//!
//! Example (on-chain tier): the Compound v3 Comet at
//! `0x771d2f213b4c23f70fa884d441a405f41f51ab50` returns
//! `name() = "Compound cached 9-asset"`, `symbol() = "cwUSDC-9"`, which the
//! normalization map collapses to the display label **"Compound"**.
//!
//! ## Resolution is per-contract, `getCode`-first, contracts-only
//!
//! Each distinct `to_addr` is resolved with [`resolve_one`], which calls
//! `eth_getCode` **first** and branches on the result:
//!   * **RPC error** (`None`) — return the all-`None` tuple. [`should_upsert`]
//!     returns false, so it's not cached and gets re-probed next poll (the
//!     RPC-failure semantics; see [`should_upsert`]).
//!   * **empty code** (`""` / `0x` — definitively an EOA) — cache it as
//!     "seen, not a contract" (`raw_name = Some("")`, label `NULL`) so it isn't
//!     re-probed every poll, but skip `name()`/`symbol()`. This is the common
//!     case: most `to_addr` are EOAs (plain transfers), so this is **1 eth_call**
//!     instead of 3, cutting proxy load ~10×.
//!   * **bytecode** (a contract) — only now call `name()` + `symbol()` and
//!     normalize (the already-fetched `code` feeds the bytecode-fingerprint
//!     path). That's 3 eth_calls, but only for actual contracts.
//!
//! A Multicall3 `aggregate3` batch was tried but is **broken on Rome** —
//! `aggregate3` comes back all-failed for every contract, leaving
//! `contract_labels` empty even though the per-contract reads work fine. Rome
//! eth_calls are ~1s each, so resolution happens once per contract and is then
//! cached; the unbatched calls only fire for not-yet-seen addresses.
//!
//! ## Incremental upsert (rows appear immediately, restart-safe)
//!
//! Each address is upserted **as soon as it's resolved**, not after the whole
//! polled batch. Previously the entire `batch_size`-row slice was resolved
//! before any row was written, so on a fresh deploy nothing appeared for many
//! minutes (each contract is ~3 eth_calls × ~1s). Now a labeled row lands within
//! ~1–3s of its resolution, and a restart mid-batch loses no progress.
//!
//! ## Trigger (idempotent, auto-backfills)
//!
//! Each poll selects distinct `evm_tx.to_addr` for this `chain_id` that are
//! `NOT NULL` and absent from `contract_labels`, then resolves + upserts each.
//! "Absent from `contract_labels`" as the trigger means the first deploy
//! auto-backfills every already-indexed address, and re-running is safe.
//!
//! ## Startup registry re-assert (heals pre-provenance-column rows)
//!
//! Migration 0236 added `provenance` with `DEFAULT 'onchain'` — which means
//! every row that existed before the migration, **including rows this
//! worker's own registry tier already wrote**, silently defaulted to the
//! floor tier. This worker never re-resolves an address once it has a row
//! (the poll trigger is "absent from `contract_labels`"; the only backfill
//! is `has_code IS NULL`, and registry rows already have `has_code = true`),
//! so those mis-tiered rows would stay `'onchain'` forever — making them
//! visible to `verified_labels`' candidate query (`provenance NOT IN
//! ('verified', 'registry')`) and clobberable by a lower-confidence Sourcify
//! write. [`reassert_registry_labels`] runs once at worker startup (see
//! [`run`]) and re-upserts every registry-map entry at `provenance =
//! "registry"` through the same rank-guarded upsert every writer uses —
//! rank 3 always satisfies the `>=` guard, so this heals a mis-tiered row and
//! is a no-op once every row is healed (safe to run on every restart).

use sqlx::PgPool;
use std::collections::HashMap;
use std::time::Duration;
use tracing::{debug, info, warn};

use super::label_provenance::{self, PROVENANCE_ONCHAIN, PROVENANCE_REGISTRY};
use super::metadata;
use crate::config::ContractLabel;

/// `name()` ERC-20 selector.
const SEL_NAME: &str = "0x06fdde03";
/// `symbol()` ERC-20 selector.
const SEL_SYMBOL: &str = "0x95d89b41";
/// `description()` selector — the Chainlink `AggregatorV3Interface` surface,
/// `keccak256("description()")[0..4]`. Oracle price-feed adapters (Rome OG-V2 /
/// Pyth-Pull adapter clones, factory-deployed as EIP-1167 minimal proxies)
/// revert `name()`/`symbol()` but return a human feed name here (e.g.
/// "ETH / USD", "JITOSOL/USD cached"). Verified on Hadrian 2026-06-05: the 9
/// most-called unlabeled contracts are all such clones of one adapter impl.
const SEL_DESCRIPTION: &str = "0x7284e416";

// ─────────────────────────────────────────────────────────────────────────────
// Normalization map — the load-bearing display step
// ─────────────────────────────────────────────────────────────────────────────

/// Substring rules over the on-chain `name()` string → clean display label.
///
/// Order matters: the first matching rule wins. The Comet contract reports
/// `name() = "Compound cached 9-asset"`, so the "compound" rule must precede
/// the "comet" rule, otherwise a literal-"Comet"-named contract and the real
/// Comet would diverge. (The real Comet says "Compound …", so it resolves to
/// "Compound" — exactly the intended display.)
///
/// Each entry: (lowercased needle to search for in `name()`, display label).
const NAME_FINGERPRINTS: &[(&str, &str)] = &[
    ("compound", "Compound"),
    ("comet", "Comet"),
    ("aave", "Aave V3"),
    ("uniswap v3", "Uniswap V3"),
    ("uniswap v2", "Uniswap V2"),
    ("uniswap", "Uniswap"),
];

/// Bytecode selector fingerprints → display label, scanned only when `name()`
/// is empty. Mirrors `hook_metadata::HOOK_SELECTOR_FINGERPRINTS`.
///
/// Each entry: (display label, &[needle selectors present in runtime code]).
/// A match on any needle identifies the contract.
const CODE_FINGERPRINTS: &[(&str, &[&str])] = &[
    // Uniswap V3 pool: `swap(address,bool,int256,uint160,bytes)` selector.
    ("Uniswap V3", &["128acb08"]),
];

/// Normalize raw on-chain reads into `(display_label, display_label_detail)`.
///
/// * `name` — the contract's `name()` return (may be empty).
/// * `symbol` — the contract's `symbol()` return (may be empty).
/// * `code_hex` — runtime bytecode (0x-hex), consulted only when `name` is
///   empty for a selector fingerprint. Pass `""` to skip bytecode matching.
///
/// Returns:
/// * `display_label` — `Some(clean label)`, or `None` when nothing on-chain
///   identifies the contract (both `name` and `symbol` empty and no fingerprint).
/// * `display_label_detail` — a secondary string for the UI tooltip: the raw
///   symbol when present, else the raw name when it differs from the label,
///   else `None`.
///
/// Pure function — fully unit-testable, no I/O.
pub fn normalize_label(
    name: &str,
    symbol: &str,
    code_hex: &str,
) -> (Option<String>, Option<String>) {
    let name_trim = name.trim();
    let symbol_trim = symbol.trim();
    let name_lc = name_trim.to_lowercase();

    // 1. name() substring rules (first match wins; order is load-bearing).
    for (needle, label) in NAME_FINGERPRINTS {
        if name_lc.contains(needle) {
            let detail = label_detail(label, name_trim, symbol_trim);
            return (Some((*label).to_string()), detail);
        }
    }

    // 2. name() empty → bytecode selector fingerprint fallback.
    if name_trim.is_empty() && !code_hex.is_empty() {
        if let Some(label) = fingerprint_code(code_hex) {
            let detail = label_detail(label, name_trim, symbol_trim);
            return (Some(label.to_string()), detail);
        }
    }

    // 3. Token-ish fallback: short non-empty symbol → display the symbol.
    if !symbol_trim.is_empty() && symbol_trim.len() <= 16 {
        return (Some(symbol_trim.to_string()), None);
    }

    // 4. Otherwise the raw name(), if any; else nothing.
    if !name_trim.is_empty() {
        let detail = if symbol_trim.is_empty() {
            None
        } else {
            Some(symbol_trim.to_string())
        };
        return (Some(name_trim.to_string()), detail);
    }

    (None, None)
}

/// Compute the secondary detail string for a normalized label: prefer the raw
/// symbol; else the raw name when it isn't already the label; else `None`.
fn label_detail(label: &str, name: &str, symbol: &str) -> Option<String> {
    if !symbol.is_empty() {
        Some(symbol.to_string())
    } else if !name.is_empty() && name != label {
        Some(name.to_string())
    } else {
        None
    }
}

/// Scan runtime bytecode (0x-hex) for a known protocol selector fingerprint.
pub fn fingerprint_code(code_hex: &str) -> Option<&'static str> {
    let code = code_hex.trim_start_matches("0x").to_lowercase();
    for (label, needles) in CODE_FINGERPRINTS {
        if needles.iter().any(|n| code.contains(&n.to_lowercase())) {
            return Some(label);
        }
    }
    None
}

/// Look up an address in the registry-backed label map (the FIRST tier).
///
/// The map (`address → label`) is projected from `rome-protocol/registry`
/// into the deployment config and built once at worker start. Protocol infra
/// that reverts `name()`/`symbol()` — UniswapV2Router02, AavePool, Multicall3,
/// factories, the faucet, all of V3/V4 — is labeled here with no on-chain read.
///
/// Addresses in the map are already lowercased by the projector; we lowercase
/// the query for safety. Returns `Some(label)` on a hit, `None` on a miss (the
/// caller then falls through to the on-chain `name()`/`symbol()`/bytecode
/// heuristic).
///
/// Pure function — fully unit-testable, no I/O.
pub fn registry_label(map: &HashMap<String, String>, addr: &str) -> Option<String> {
    map.get(&addr.to_lowercase()).cloned()
}

// ─────────────────────────────────────────────────────────────────────────────
// Worker loop
// ─────────────────────────────────────────────────────────────────────────────

/// Distinct recipients in `evm_tx` that have no `contract_labels` row, found by
/// walking the index rather than scanning the table.
///
/// The previous form was `SELECT DISTINCT t.to_addr … LEFT JOIN contract_labels …
/// WHERE c.address IS NULL LIMIT $2`. Measured on hadrian-lt 2026-07-27:
/// **5,084,364 buffer hits** to produce an answer set of **1,350** distinct
/// recipients, against an `evm_tx` of 33M rows.
///
/// Same trap as `method_decoder::unknown_selectors_sql`: the `LIMIT` short-circuits
/// only while UNLABELLED addresses remain. Once everything is labelled — the steady
/// state — the planner must read every row to prove the result is empty, so the
/// query is cheapest when there is work and most expensive when there is none.
///
/// This is a loose index scan over the existing `ix_evm_tx_to (chain_id, to_addr)`:
/// one probe per distinct recipient, O(distinct) rather than O(rows), and flat as
/// the chain grows. No migration required.
pub(crate) fn unlabelled_recipients_sql() -> &'static str {
    r#"
    WITH RECURSIVE walk AS (
        (SELECT to_addr
           FROM rome_via.evm_tx
          WHERE chain_id = $1 AND to_addr IS NOT NULL
          ORDER BY to_addr
          LIMIT 1)
        UNION ALL
        SELECT (SELECT to_addr
                  FROM rome_via.evm_tx
                 WHERE chain_id = $1
                   AND to_addr > walk.to_addr
                 ORDER BY to_addr
                 LIMIT 1)
          FROM walk
         WHERE walk.to_addr IS NOT NULL
    )
    SELECT to_addr FROM walk
     WHERE to_addr IS NOT NULL
       AND NOT EXISTS (
             SELECT 1 FROM rome_via.contract_labels c
              WHERE c.chain_id = $1 AND c.address = walk.to_addr
           )
     LIMIT $2
    "#
}

pub async fn run(
    pool: PgPool,
    chain_id: i64,
    proxy_url: String,
    contract_labels: Vec<ContractLabel>,
    poll_interval: Duration,
    batch_size: i64,
) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .user_agent("rome-via-enrich/0.1")
        .build()?;

    // Build the registry-backed label map once at worker start (address → label),
    // mirroring the infra-set build in `cross_chain::run`. Lowercase the address
    // on insert for safety (the projector already lowercases, but be defensive).
    let registry: HashMap<String, String> = contract_labels
        .into_iter()
        .map(|c| (c.address.to_lowercase(), c.label))
        .collect();
    info!(
        worker = "contract_labels",
        registry_count = registry.len(),
        "registry-backed label map built"
    );

    // Heal any pre-migration-0236 registry row that defaulted to
    // provenance='onchain' before this worker's candidate-selection
    // downstream (verified_labels) can select and clobber it. See the module
    // doc "Startup registry re-assert" section. Cheap (dozens of entries),
    // runs once per boot.
    reassert_registry_labels(&pool, chain_id, &registry).await;

    loop {
        // Distinct recipients not yet labeled. NULL to_addr (contract creation)
        // is excluded. Absent-from-contract_labels is the trigger, so first
        // deploy auto-backfills and re-runs are no-ops. `LIMIT batch_size`
        // bounds how many we resolve per poll; each is upserted as it resolves.
        let mut rows: Vec<(String,)> = sqlx::query_as(unlabelled_recipients_sql())
        .bind(chain_id)
        .bind(batch_size)
        .fetch_all(&pool)
        .await
        .unwrap_or_default();

        // Backfill has_code onto rows that predate the column (has_code IS NULL)
        // by re-resolving them, so is_contract can key on ground-truth code
        // presence instead of label presence. Bounded per poll; the NULL set
        // drains to empty and this becomes a no-op. Every new row is written
        // with has_code from the start, so only the pre-column backlog needs it.
        let backfill: Vec<(String,)> = sqlx::query_as(
            "SELECT address FROM rome_via.contract_labels
              WHERE chain_id = $1 AND has_code IS NULL
              LIMIT $2",
        )
        .bind(chain_id)
        .bind(batch_size)
        .fetch_all(&pool)
        .await
        .unwrap_or_default();
        rows.extend(backfill);

        if rows.is_empty() {
            tokio::time::sleep(poll_interval).await;
            continue;
        }

        // Resolve and upsert each address incrementally, so rows appear as soon
        // as they're resolved (not after the whole polled slice) and a restart
        // mid-batch loses no progress.
        for (address,) in rows {
            let (raw_name, raw_symbol, label, detail, has_code, provenance) =
                resolve_one(&client, &proxy_url, &registry, &address).await;
            upsert_label(
                &pool, chain_id, &address, &raw_name, &raw_symbol, &label, &detail, has_code,
                provenance,
            )
            .await;
        }

        tokio::time::sleep(poll_interval).await;
    }
}

/// One-time (per-boot) pass that re-asserts every registry-map entry at
/// `provenance = "registry"` through the rank-guarded upsert. See the module
/// doc "Startup registry re-assert" section for why this exists: migration
/// 0236's `provenance DEFAULT 'onchain'` silently demoted every pre-existing
/// row, including ones this same registry tier already wrote, and this
/// worker never revisits an address once it has a row. Mirrors the registry
/// tier's own shape in [`resolve_one`] (`raw_name = None`, `has_code =
/// Some(true)`) so a healed row is indistinguishable from one written fresh
/// by the registry tier today.
///
/// Idempotent — rank 3 (`registry`) always satisfies the `>=` guard against
/// any existing tier, so re-running on every restart is a cheap no-op once
/// every row is healed.
pub async fn reassert_registry_labels(
    pool: &PgPool,
    chain_id: i64,
    registry: &HashMap<String, String>,
) {
    for (address, label) in registry {
        upsert_label(
            pool,
            chain_id,
            address,
            &None,
            &None,
            &Some(label.clone()),
            &None,
            Some(true),
            PROVENANCE_REGISTRY,
        )
        .await;
    }
}

/// Upsert a single resolved address into `contract_labels`, guarded by
/// [`should_upsert`] so an RPC-failure (all-`None`) shape is left absent for
/// re-probe rather than cached as a NULL-label row.
///
/// The write itself is rank-guarded (see [`label_provenance`]): a `registry`
/// or `verified` label already on the row survives a subsequent `onchain`
/// resolution — this worker's own registry tier writes `provenance =
/// "registry"`; its on-chain tier writes `"onchain"` (the floor), so it never
/// clobbers a higher tier written by another worker (e.g. `verified_labels`).
async fn upsert_label(
    pool: &PgPool,
    chain_id: i64,
    address: &str,
    raw_name: &Option<String>,
    raw_symbol: &Option<String>,
    label: &Option<String>,
    detail: &Option<String>,
    has_code: Option<bool>,
    provenance: &str,
) {
    if !should_upsert(raw_name, raw_symbol, label) {
        // RPC failure (getCode returned nothing) — leave the address absent so
        // the next poll re-probes it after the proxy recovers. Caching a
        // NULL-label row here would permanently block re-probing.
        debug!(%address, "skipping upsert: no on-chain reads (RPC failure?)");
        return;
    }
    let sql = format!(
        r#"
        INSERT INTO rome_via.contract_labels
            (chain_id, address, display_label, display_label_detail,
             raw_name, raw_symbol, has_code, provenance, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
        ON CONFLICT (chain_id, address) DO UPDATE
          SET display_label        = EXCLUDED.display_label,
              display_label_detail = EXCLUDED.display_label_detail,
              raw_name             = EXCLUDED.raw_name,
              raw_symbol           = EXCLUDED.raw_symbol,
              has_code             = EXCLUDED.has_code,
              provenance           = EXCLUDED.provenance,
              updated_at           = NOW()
        WHERE {}
        "#,
        label_provenance::GUARD_WHERE_CLAUSE
    );
    let res = sqlx::query(&sql)
        .bind(chain_id)
        .bind(address)
        .bind(label.as_deref())
        .bind(detail.as_deref())
        .bind(raw_name.as_deref())
        .bind(raw_symbol.as_deref())
        .bind(has_code)
        .bind(provenance)
        .execute(pool)
        .await;

    match res {
        // A non-null label is the interesting signal (an identified contract);
        // surface it at info so progress is visible on a fresh backfill. EOAs
        // (NULL label) stay at debug to avoid drowning the log.
        Ok(_) => match label {
            Some(l) => info!(%address, label = %l, "resolved contract label"),
            None => debug!(%address, "cached address (no on-chain label)"),
        },
        Err(e) => warn!(%address, error = %e, "failed to store contract label"),
    }
}

/// Decide whether a resolved tuple should be cached in `contract_labels`.
///
/// Returns `false` only for the all-`None` shape, which means **no on-chain
/// read succeeded** — the per-contract `name()`/`symbol()` eth_calls both
/// returned nothing (a proxy/RPC outage). Because the poll query triggers on
/// "absent from `contract_labels`", caching a `display_label = NULL` row in that
/// case would permanently block re-probing (a cold-start trap when the worker
/// races the proxy). Skipping leaves the address absent so the next poll retries
/// after recovery.
///
/// A contract that was *reachable* but whose reads don't identify it — e.g.
/// `name()`/`symbol()` returned empty strings (`raw_name = Some("")`) so
/// `label` is `None` — is a real observation and IS cached, so a known-unnamed
/// contract isn't re-probed every poll.
///
/// Pure function — fully unit-testable.
fn should_upsert(
    raw_name: &Option<String>,
    raw_symbol: &Option<String>,
    label: &Option<String>,
) -> bool {
    raw_name.is_some() || raw_symbol.is_some() || label.is_some()
}

/// What a `getCode` result tells us about an address, before any `name()` /
/// `symbol()` calls. Lets the I/O-free decision be unit-tested.
#[derive(Debug, PartialEq, Eq)]
enum CodeKind {
    /// `eth_getCode` itself failed (RPC error) — nothing observed.
    RpcFailure,
    /// `getCode` returned empty (`""` / `0x`): definitively an EOA, not a
    /// contract. Cache as seen-but-unlabeled; do NOT read `name()`/`symbol()`.
    Eoa,
    /// `getCode` returned bytecode: a real contract. Carries the 0x-hex code so
    /// the bytecode-fingerprint path can reuse it (no second `getCode`).
    Contract(String),
}

/// Classify an `eth_getCode` result. `None` = RPC error; `Some(code)` where the
/// code is empty / `"0x"` = EOA; otherwise a contract carrying its bytecode.
///
/// Pure function — fully unit-testable.
fn classify_code(code: Option<String>) -> CodeKind {
    match code {
        None => CodeKind::RpcFailure,
        Some(c) => {
            let trimmed = c.trim();
            if trimmed.is_empty() || trimmed == "0x" {
                CodeKind::Eoa
            } else {
                CodeKind::Contract(c)
            }
        }
    }
}

/// The `has_code` truth a getCode classification implies, persisted so the
/// address-page kind is a ground truth rather than a label-presence guess:
/// `None` for an RPC failure (unknown — don't cache), `Some(false)` for an EOA,
/// `Some(true)` for a contract. A nameless contract (no `name()`/`symbol()`)
/// is otherwise stored identically to an EOA (`raw_name = ""`), so without this
/// bit the two are indistinguishable — the residual behind is_contract false
/// negatives. Pure — unit-tested.
fn has_code_of(kind: &CodeKind) -> Option<bool> {
    match kind {
        CodeKind::RpcFailure => None,
        CodeKind::Eoa => Some(false),
        CodeKind::Contract(_) => Some(true),
    }
}

/// Resolve one address. Returns
/// `(raw_name, raw_symbol, display_label, display_label_detail)`.
///
/// Strategy:
///   0. **Registry tier FIRST.** If `addr` is in the registry-backed label map
///      (projected from `rome-protocol/registry`), use that label directly —
///      `(None, None, Some(label), None)` — and skip all on-chain reads. This
///      is the only tier that labels protocol infra which reverts
///      `name()`/`symbol()` (routers, pools, factories, the faucet, Multicall3,
///      V3/V4). **0 eth_calls.**
///   1. Otherwise `eth_getCode` FIRST, then [`classify_code`] (per-contract;
///      Multicall3 `aggregate3` is broken on Rome):
///      * RPC failure → all-`None` tuple ([`should_upsert`] → not cached →
///        re-probed next poll).
///      * EOA (empty code) → `(Some(""), None, None, None)`: cached as seen but
///        unlabeled (NULL `display_label`), so it isn't re-probed every poll —
///        and we DON'T spend `name()`/`symbol()` calls on it. **1 eth_call.**
///      * contract → fall through to step 2. **3 eth_calls total.**
///   2. For a contract: `name()` + `symbol()` eth_calls, then normalize with the
///      already-fetched bytecode feeding the fingerprint fallback.
async fn resolve_one(
    client: &reqwest::Client,
    proxy_url: &str,
    registry: &HashMap<String, String>,
    addr: &str,
) -> (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<bool>,
    &'static str,
) {
    // Tier 0: registry-backed label. Consulted FIRST, before any on-chain read,
    // so protocol infra that reverts name()/symbol() still gets a clean label.
    // raw_name = None so should_upsert is driven by the Some(label). Registry
    // infra is a contract by construction -> has_code = Some(true). This is
    // the top provenance tier — see label_provenance.
    if let Some(label) = registry_label(registry, addr) {
        return (None, None, Some(label), None, Some(true), PROVENANCE_REGISTRY);
    }

    // getCode first — most to_addr are EOAs, and this is the one call that
    // tells us so without two wasted name()/symbol() reads. Its result also IS
    // the has_code truth we persist (already computed here — no extra call).
    let kind = classify_code(eth_get_code(client, proxy_url, addr).await);
    let has_code = has_code_of(&kind);
    let code = match kind {
        // RPC error: leave absent (not cached) so the next poll re-probes.
        CodeKind::RpcFailure => return (None, None, None, None, has_code, PROVENANCE_ONCHAIN),
        // EOA: cache as seen-but-unlabeled. raw_name = Some("") makes
        // should_upsert true with a NULL display_label; no name()/symbol() call.
        CodeKind::Eoa => return (Some(String::new()), None, None, None, has_code, PROVENANCE_ONCHAIN),
        CodeKind::Contract(code) => code,
    };

    // Real contract — now (and only now) spend the two label reads.
    let raw_name = metadata::eth_call_string_public(client, proxy_url, addr, SEL_NAME).await;
    let raw_symbol = metadata::eth_call_string_public(client, proxy_url, addr, SEL_SYMBOL).await;

    // Oracle-adapter fallback: when name() AND symbol() are both empty/reverted,
    // try description() (the Chainlink AggregatorV3 surface). Rome's OG-V2 price
    // feeds are factory-deployed EIP-1167 clones that revert name()/symbol() but
    // expose a human feed name here ("ETH / USD"). Gated on name+symbol being
    // empty so a normal token (which has a name()) never spends this extra call —
    // and it's resolved once per contract then cached, never per-poll.
    let name_empty = raw_name.as_deref().unwrap_or("").trim().is_empty();
    let symbol_empty = raw_symbol.as_deref().unwrap_or("").trim().is_empty();
    let raw_description = if name_empty && symbol_empty {
        metadata::eth_call_string_public(client, proxy_url, addr, SEL_DESCRIPTION).await
    } else {
        None
    };

    let (rn, rs, l, d) = contract_tuple(&code, raw_name, raw_symbol, raw_description);
    (rn, rs, l, d, has_code, PROVENANCE_ONCHAIN)
}

/// Build the `contract_labels` cache tuple for an address whose `eth_getCode`
/// returned bytecode — i.e. a CONFIRMED contract — from its `name()`/`symbol()`
/// reads (`None` when the call reverted). Normalizes a display label (name
/// fingerprints first, then the already-fetched bytecode's selector fingerprint).
///
/// **Why a confirmed contract is always cached:** getCode already proved the
/// contract exists, so one that yields no identifying read (both reads revert,
/// no fingerprint, not in the registry tier) is still cached — `raw_name` is
/// coerced to `Some("")` (mirroring the EOA branch in [`resolve_one`]) so
/// [`should_upsert`] is true and the row lands with a NULL `display_label`.
/// Without this it returned the all-`None` shape that [`should_upsert`] reserves
/// for a genuine getCode RPC failure, so the address stayed absent from
/// `contract_labels` and the poll query ("absent from contract_labels")
/// re-selected it every poll — re-firing 3 reverting eth_calls (getCode +
/// name() + symbol()) against the proxy forever (the flat-24/7 revert storm).
/// The all-`None` no-cache path is now reachable ONLY for a getCode RPC failure,
/// which is handled (early return) before this point in [`resolve_one`].
///
/// `raw_description` is the `description()` read (the Chainlink AggregatorV3
/// surface), used as a label fallback for oracle price-feed adapters whose
/// `name()`/`symbol()` revert — see [`SEL_DESCRIPTION`]. Pass `None` when not
/// fetched (a contract already identified by name/symbol/bytecode).
///
/// Pure function — fully unit-tested.
fn contract_tuple(
    code: &str,
    raw_name: Option<String>,
    raw_symbol: Option<String>,
    raw_description: Option<String>,
) -> (Option<String>, Option<String>, Option<String>, Option<String>) {
    let name_str = raw_name.clone().unwrap_or_default();
    let symbol_str = raw_symbol.clone().unwrap_or_default();
    let (mut label, mut detail) = normalize_label(&name_str, &symbol_str, code);

    // Oracle-adapter fallback: name()/symbol()/bytecode didn't identify it, but
    // description() returned a non-empty feed name (e.g. "ETH / USD"). Use it as
    // the display label, with "Price Feed" as the secondary detail so the kind is
    // explicit. (Only consulted when normalize produced no label, so a real
    // name/symbol/bytecode identity always wins.)
    if label.is_none() {
        if let Some(desc) = raw_description.as_deref().map(str::trim).filter(|d| !d.is_empty()) {
            label = Some(desc.to_string());
            detail = Some("Price Feed".to_string());
        }
    }

    // Confirmed contract with nothing identifying it → cache as seen-but-
    // unlabeled (`Some("")` ⇒ should_upsert true, NULL display_label) instead of
    // leaving it absent (which re-probes forever). Mirrors the EOA branch.
    // `raw_description` is intentionally excluded from this all-None test: a
    // contract with only a description IS labeled above, so it never reaches here
    // unlabeled; and a description-less contract should still cache as seen.
    let raw_name = match (&raw_name, &raw_symbol, &label) {
        (None, None, None) => Some(String::new()),
        _ => raw_name,
    };
    (raw_name, raw_symbol, label, detail)
}

/// eth_getCode for the bytecode-fingerprint fallback.
async fn eth_get_code(client: &reqwest::Client, proxy_url: &str, address: &str) -> Option<String> {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getCode",
        "params": [address, "latest"]
    });
    let resp = client.post(proxy_url).json(&body).send().await.ok()?;
    let json: serde_json::Value = resp.json().await.ok()?;
    json.get("result")?.as_str().map(String::from)
}

// ─────────────────────────────────────────────────────────────────────────────
// Unit tests
// ─────────────────────────────────────────────────────────────────────────────
#[cfg(test)]
mod tests {
    /// Same class as method_decoder: "which distinct values are unprocessed?" written
    /// as a whole-table anti-join. Measured on hadrian-lt 2026-07-27 at **5,084,364
    /// buffer hits** to find an answer set of **1,350** distinct recipients, over an
    /// evm_tx that had already grown to 33M rows.
    ///
    /// The LIMIT does not bound it in the steady state: it short-circuits only when
    /// UNLABELLED addresses exist, so once everything is labelled the planner must
    /// read every row to prove the result empty — cheapest when there is work,
    /// most expensive when there is none.
    ///
    /// Uses the existing ix_evm_tx_to (chain_id, to_addr) index; no migration needed.
    #[test]
    fn unlabelled_recipients_query_walks_the_index() {
        let q = super::unlabelled_recipients_sql();
        assert!(
            q.contains("WITH RECURSIVE"),
            "must walk distinct recipients via the index: {q}"
        );
        assert!(
            !q.contains("SELECT DISTINCT"),
            "SELECT DISTINCT over evm_tx is the 5M-buffer scan this replaces: {q}"
        );
        assert!(
            q.contains("to_addr >"),
            "each probe must seek past the previous recipient: {q}"
        );
        assert!(
            q.matches("LIMIT 1").count() >= 2,
            "seed and step must each fetch exactly one value: {q}"
        );
        // Labelled addresses must still be excluded, or the worker re-resolves
        // everything it already knows on every poll.
        assert!(
            q.contains("NOT EXISTS"),
            "must still exclude already-labelled recipients: {q}"
        );
    }


    use super::*;
    use super::super::metadata::decode_string_result;
    use std::collections::HashMap;

    // ── registry_label: registry-backed tier (consulted FIRST) ───────────────

    /// An address present in the registry map resolves to its label,
    /// case-insensitively (the map is lowercased; the lookup lowercases the
    /// query). Protocol infra that reverts name()/symbol() (routers, pools,
    /// factories, the faucet, Multicall3, V3/V4) gets a label here with no
    /// on-chain read.
    #[test]
    fn registry_label_hits_case_insensitive() {
        let mut map: HashMap<String, String> = HashMap::new();
        map.insert(
            "0xb342f70d56855f11b0721fcbe2804a200d0f0533".to_string(),
            "Uniswap V2".to_string(),
        );
        // Mixed-case query against a lowercased map still hits.
        assert_eq!(
            registry_label(&map, "0xB342F70D56855F11B0721FCBE2804A200D0F0533"),
            Some("Uniswap V2".to_string())
        );
        // Exact lowercase also hits.
        assert_eq!(
            registry_label(&map, "0xb342f70d56855f11b0721fcbe2804a200d0f0533"),
            Some("Uniswap V2".to_string())
        );
    }

    /// An address absent from the registry map returns None, so the caller
    /// falls through to the on-chain name()/symbol()/bytecode heuristic.
    #[test]
    fn registry_label_miss_falls_through() {
        let mut map: HashMap<String, String> = HashMap::new();
        map.insert(
            "0xb342f70d56855f11b0721fcbe2804a200d0f0533".to_string(),
            "Uniswap V2".to_string(),
        );
        assert_eq!(
            registry_label(&map, "0x0000000000000000000000000000000000000001"),
            None
        );
    }

    /// An empty registry map (chain not re-rolled with the tier) always misses.
    #[test]
    fn registry_label_empty_map_misses() {
        let map: HashMap<String, String> = HashMap::new();
        assert_eq!(
            registry_label(&map, "0xb342f70d56855f11b0721fcbe2804a200d0f0533"),
            None
        );
    }

    // ── normalize_label ─────────────────────────────────────────────────────

    /// The load-bearing case from the brief: the Comet at
    /// 0x771d2f… returns name()="Compound cached 9-asset", symbol()="cwUSDC-9".
    /// It must collapse to ("Compound", Some("cwUSDC-9")).
    #[test]
    fn normalize_comet_collapses_to_compound() {
        let (label, detail) = normalize_label("Compound cached 9-asset", "cwUSDC-9", "");
        assert_eq!(label, Some("Compound".to_string()));
        assert_eq!(detail, Some("cwUSDC-9".to_string()));
    }

    /// "compound" must win over "comet" when both could theoretically match —
    /// order in NAME_FINGERPRINTS is load-bearing.
    #[test]
    fn normalize_compound_wins_over_comet_order() {
        // A name containing both tokens still resolves to Compound (listed first).
        let (label, _) = normalize_label("Compound Comet base", "x", "");
        assert_eq!(label, Some("Compound".to_string()));
    }

    /// A contract literally named "Comet …" with no "compound" substring maps
    /// to "Comet".
    #[test]
    fn normalize_bare_comet() {
        let (label, _) = normalize_label("Comet implementation", "", "");
        assert_eq!(label, Some("Comet".to_string()));
    }

    #[test]
    fn normalize_aave() {
        let (label, _) = normalize_label("Aave Pool", "", "");
        assert_eq!(label, Some("Aave V3".to_string()));
    }

    #[test]
    fn normalize_uniswap_v3_by_name() {
        let (label, _) = normalize_label("Uniswap V3 Pool", "", "");
        assert_eq!(label, Some("Uniswap V3".to_string()));
    }

    #[test]
    fn normalize_uniswap_v2_by_name() {
        let (label, _) = normalize_label("Uniswap V2", "UNI-V2", "");
        assert_eq!(label, Some("Uniswap V2".to_string()));
    }

    /// name() empty but bytecode carries the UV3-pool `swap` selector 0x128acb08.
    #[test]
    fn normalize_uniswap_v3_by_bytecode() {
        let code = "0x6080604052 128acb08 00".replace(' ', "");
        let (label, _) = normalize_label("", "", &code);
        assert_eq!(label, Some("Uniswap V3".to_string()));
    }

    /// No name, has a short symbol → display the symbol (a plain token).
    #[test]
    fn normalize_token_symbol_fallback() {
        let (label, detail) = normalize_label("", "wUSDC", "");
        assert_eq!(label, Some("wUSDC".to_string()));
        assert_eq!(detail, None);
    }

    /// A name that matches no protocol rule, with no symbol → raw name passes
    /// through as the label.
    #[test]
    fn normalize_unknown_name_passthrough() {
        let (label, detail) = normalize_label("Some Random Vault", "", "");
        assert_eq!(label, Some("Some Random Vault".to_string()));
        assert_eq!(detail, None);
    }

    /// Both empty, no bytecode match → no label.
    #[test]
    fn normalize_empty_is_none() {
        let (label, detail) = normalize_label("", "", "");
        assert_eq!(label, None);
        assert_eq!(detail, None);
    }

    /// A protocol-named token still surfaces its symbol as the detail.
    #[test]
    fn normalize_detail_prefers_symbol() {
        let (label, detail) = normalize_label("Uniswap V2", "UNI-V2", "");
        assert_eq!(label, Some("Uniswap V2".to_string()));
        assert_eq!(detail, Some("UNI-V2".to_string()));
    }

    // ── upsert guard: don't cache a NULL label on RPC failure ────────────────

    /// All-None resolved tuple (proxy/RPC outage — the per-contract reads
    /// returned nothing) must NOT be upserted. The poll query triggers on
    /// "absent from contract_labels", so caching a NULL-label row here would
    /// permanently block re-probing (cold-start trap). Skipping leaves the
    /// address absent for re-probe after recovery.
    #[test]
    fn should_not_upsert_all_none() {
        assert!(!should_upsert(&None, &None, &None));
    }

    /// A genuinely resolved label (the Comet case) IS upserted.
    #[test]
    fn should_upsert_real_label() {
        assert!(should_upsert(
            &Some("Compound cached 9-asset".to_string()),
            &Some("cwUSDC-9".to_string()),
            &Some("Compound".to_string()),
        ));
    }

    /// A contract that was reachable but whose on-chain reads simply don't
    /// identify it (e.g. name() and symbol() both returned empty strings, so
    /// raw_name/raw_symbol are Some("") and label is None) is still a real
    /// observation — it IS cached so we don't re-probe a known-unnamed
    /// contract every poll. Only the all-None (RPC-failure) shape is skipped.
    #[test]
    fn should_upsert_reachable_but_unnamed() {
        assert!(should_upsert(
            &Some(String::new()),
            &Some(String::new()),
            &None,
        ));
    }

    // ── classify_code: getCode-first EOA-vs-contract decision ───────────────

    /// getCode RPC error (None) → RpcFailure (→ all-None tuple → not cached →
    /// re-probed next poll).
    #[test]
    fn classify_code_rpc_failure() {
        assert_eq!(classify_code(None), CodeKind::RpcFailure);
    }

    /// Empty getCode result is an EOA — definitively not a contract.
    #[test]
    fn has_code_of_maps_getcode_classification() {
        assert_eq!(has_code_of(&CodeKind::RpcFailure), None);
        assert_eq!(has_code_of(&CodeKind::Eoa), Some(false));
        assert_eq!(has_code_of(&CodeKind::Contract("0x60ff".to_string())), Some(true));
    }

    #[test]
    fn classify_code_empty_is_eoa() {
        assert_eq!(classify_code(Some(String::new())), CodeKind::Eoa);
        assert_eq!(classify_code(Some("0x".to_string())), CodeKind::Eoa);
        assert_eq!(classify_code(Some("  0x  ".to_string())), CodeKind::Eoa);
    }

    /// Non-empty bytecode is a contract, and carries the code through for the
    /// fingerprint path (no second getCode).
    #[test]
    fn classify_code_bytecode_is_contract() {
        assert_eq!(
            classify_code(Some("0x6080604052".to_string())),
            CodeKind::Contract("0x6080604052".to_string())
        );
    }

    // ── contract_tuple: a CONFIRMED contract is always cached ────────────────

    /// THE re-probe-storm fix (rome-via-enrich hammering the proxy with
    /// name()/symbol() that revert, flat 24/7). getCode returned bytecode (a
    /// confirmed contract), but name() AND symbol() both reverted (→ None) and
    /// the bytecode matches no fingerprint. The contract MUST still be cached —
    /// raw_name coerced to Some("") so should_upsert() is true — otherwise the
    /// poll query ("absent from contract_labels") re-selects it every poll and
    /// re-fires the reverting calls forever. The all-None shape is reserved for a
    /// genuine getCode RPC failure, handled earlier in resolve_one.
    #[test]
    fn contract_tuple_caches_unidentified_contract() {
        let (raw_name, raw_symbol, label, detail) =
            contract_tuple("0x6080604052", None, None, None);
        assert_eq!(raw_name, Some(String::new()));
        assert_eq!(raw_symbol, None);
        assert_eq!(label, None);
        assert_eq!(detail, None);
        assert!(
            should_upsert(&raw_name, &raw_symbol, &label),
            "a confirmed contract with no readable name must be cached, not re-probed"
        );
    }

    /// name() reverts but the bytecode carries the UV3-pool `swap` selector
    /// 0x128acb08 → labeled by fingerprint. The label carries the cache (raw_name
    /// stays the real read, None — not coerced).
    #[test]
    fn contract_tuple_fingerprint_only_is_labeled() {
        let code = "0x6080 128acb08 00".replace(' ', "");
        let (raw_name, raw_symbol, label, _detail) = contract_tuple(&code, None, None, None);
        assert_eq!(label, Some("Uniswap V3".to_string()));
        assert_eq!(raw_name, None);
        assert!(should_upsert(&raw_name, &raw_symbol, &label));
    }

    /// A normally-named contract (the Comet) keeps its reads and resolves its
    /// label — unchanged behavior, no coercion.
    #[test]
    fn contract_tuple_named_contract_unchanged() {
        let (raw_name, raw_symbol, label, detail) = contract_tuple(
            "0x00",
            Some("Compound cached 9-asset".to_string()),
            Some("cwUSDC-9".to_string()),
            None,
        );
        assert_eq!(raw_name, Some("Compound cached 9-asset".to_string()));
        assert_eq!(raw_symbol, Some("cwUSDC-9".to_string()));
        assert_eq!(label, Some("Compound".to_string()));
        assert_eq!(detail, Some("cwUSDC-9".to_string()));
        assert!(should_upsert(&raw_name, &raw_symbol, &label));
    }

    /// Oracle price-feed adapter: getCode is a contract, name()/symbol() revert
    /// (→ None), but description() returns a feed name. The feed name becomes the
    /// display label with "Price Feed" as the detail — labels the OG-V2 adapter
    /// clones (the 9 most-called unlabeled contracts on Hadrian) and any future
    /// clone, no per-address registry enumeration. (raw_name NOT coerced to "" —
    /// the label carries should_upsert.)
    #[test]
    fn contract_tuple_labels_oracle_adapter_by_description() {
        let (raw_name, raw_symbol, label, detail) =
            contract_tuple("0x6080604052", None, None, Some("ETH / USD".to_string()));
        assert_eq!(label, Some("ETH / USD".to_string()));
        assert_eq!(detail, Some("Price Feed".to_string()));
        assert_eq!(raw_name, None);
        assert!(
            should_upsert(&raw_name, &raw_symbol, &label),
            "an oracle adapter identified by description() must be cached + labeled"
        );
    }

    /// A real name()/symbol() identity always wins over description() — the
    /// fallback is consulted only when normalize produced no label.
    #[test]
    fn contract_tuple_name_wins_over_description() {
        let (_raw_name, _raw_symbol, label, detail) = contract_tuple(
            "0x00",
            Some("Aave Pool".to_string()),
            None,
            Some("ETH / USD".to_string()),
        );
        assert_eq!(label, Some("Aave V3".to_string()));
        assert_ne!(detail, Some("Price Feed".to_string()));
    }

    /// An empty/whitespace description() is ignored (not used as a blank label) —
    /// the contract still caches as seen-but-unlabeled.
    #[test]
    fn contract_tuple_empty_description_ignored() {
        let (raw_name, _raw_symbol, label, detail) =
            contract_tuple("0x6080604052", None, None, Some("   ".to_string()));
        assert_eq!(label, None);
        assert_eq!(detail, None);
        assert_eq!(raw_name, Some(String::new()), "still cached as seen");
    }

    // ── decode_string_result reuse ──────────────────────────────────────────

    /// Confirm the reused metadata::decode_string_result handles a known blob:
    /// the exact ABI-`string` shape a `name()`/`symbol()` eth_call returns.
    #[test]
    fn decode_string_result_reuse() {
        let blob = abi_encode_string("Compound");
        assert_eq!(
            decode_string_result(&hex_str(&blob)),
            Some("Compound".to_string())
        );
    }

    // ── test-only ABI builders ──────────────────────────────────────────────

    /// Left-zero-padded 32-byte big-endian word for a small integer.
    fn word_u(v: u128) -> [u8; 32] {
        let mut w = [0u8; 32];
        w[16..32].copy_from_slice(&v.to_be_bytes());
        w
    }

    /// Right-pad bytes with zeros up to the next 32-byte boundary (ABI tail).
    fn pad32(data: &[u8]) -> Vec<u8> {
        let mut padded = data.to_vec();
        let rem = padded.len() % 32;
        if rem != 0 {
            padded.resize(padded.len() + (32 - rem), 0);
        }
        padded
    }

    /// Render bytes to a 0x-hex string (so it can be fed to `decode_string_result`).
    fn hex_str(b: &[u8]) -> String {
        let mut s = String::with_capacity(2 + b.len() * 2);
        s.push_str("0x");
        for byte in b {
            s.push_str(&format!("{byte:02x}"));
        }
        s
    }

    /// ABI-encode a bare `string` (offset + length + padded data) — the shape a
    /// `name()`/`symbol()` eth_call returns.
    fn abi_encode_string(s: &str) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&word_u(0x20)); // offset
        out.extend_from_slice(&word_u(s.len() as u128)); // length
        let mut data = pad32(s.as_bytes());
        if data.is_empty() {
            data.extend_from_slice(&[0u8; 32]);
        }
        out.extend_from_slice(&data);
        out
    }
}
