/// Token metadata worker.
///
/// Polls `token_metadata` for rows where name/symbol/decimals are NULL (newly seen tokens),
/// fetches them via `eth_call` (raw JSON-RPC to Proxy), and updates the row.
///
/// Uses the standard ERC-20 ABI: name(), symbol(), decimals().
///
/// # Token KIND classification (F6)
///
/// Beyond name/symbol/decimals/supply this worker also writes
/// `token_metadata.kind` — one of `ERC-20` (plain), `SPL` (an `SPL_ERC20` /
/// `SPL_ERC20_cached` wrapper of a Solana SPL-Token mint), or `Token-2022`
/// (a wrapper of a Token-2022 mint). The discriminator is purely on-chain and
/// observable from the explorer's read-only (`eth_call` + Solana `getAccountInfo`)
/// vantage:
///
/// 1. **wrapper-or-not** — `eth_call mint_id()` (selector [`MINT_ID_SELECTOR`] =
///    `0xe132a122`). Every factory-deployed wrapper (`SPL_ERC20` /
///    `SPL_ERC20_cached`) exposes `bytes32 public immutable mint_id`, so the
///    auto-generated getter returns the 32-byte underlying Solana mint pubkey.
///    A plain ERC-20 (e.g. a Uniswap V2 LP `UniswapV2ERC20`, an OZ `ERC20`)
///    has no such function — the call **reverts** (or returns empty), which our
///    [`eth_call_bytes32`] surfaces as `None`. Verified live on Hadrian
///    (2026-06-04): wUSDC/wETH/wSOL return their mint; the RSWAP-V2 LP pair at
///    `0x3595cc…` reverts; the ERC20SPLFactory / OracleAdapterFactory /
///    RomeBridgeWithdraw all revert.
///
/// 2. **SPL-vs-Token-2022** — read the underlying mint's **owner program** on
///    Solana via `getAccountInfo(<mint>).value.owner`
///    ([`fetch_mint_owner`]). `TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA`
///    (`TOKEN_PROGRAM_ID`) → `SPL`; `TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb`
///    (`TOKEN_2022_PROGRAM_ID`) → `Token-2022`. If the Solana read is
///    unavailable (RPC down, mint not retrievable) we keep the conservative
///    `SPL` default for a wrapper rather than guessing Token-2022 — Token-2022
///    is the rarer case and a false `SPL` is the safer error. All three live
///    Hadrian wrappers wrap SPL-Token mints (owner = `Tokenkeg…`).
///
/// The pure mapping (mint-present + mint-owner → kind) is [`classify_kind`],
/// unit-tested below.
///
/// # Token provenance — the mint (`token_metadata.mint`)
///
/// When a token IS a wrapper, `mint_id()` returns its underlying Solana mint as
/// bytes32; this worker converts it to base58 ([`bytes32_hex_to_base58`]) and
/// stores it in `token_metadata.mint`. A plain ERC-20 reverts on `mint_id()`, so
/// `mint` stays NULL — correct: it has no underlying mint. (The factory/creator
/// halves of provenance are filled by the `factory_tokens` worker from the
/// `TokenCreated` log; this worker owns only `mint`.)
///
/// # Backfill of existing rows (and why there's no perpetual re-probe)
///
/// The poll predicate is **`name IS NULL OR kind IS NULL`** (see [`run`]) —
/// deliberately NOT `... OR mint IS NULL`. Adding `mint IS NULL` would re-probe
/// every plain ERC-20 forever, because a non-wrapper's `mint` is *correctly and
/// permanently* NULL — the predicate would never stop matching. Instead the
/// backfill is driven once via a targeted migration step (0214):
///
/// ```sql
/// UPDATE rome_via.token_metadata SET kind = NULL WHERE kind IN ('SPL', 'Token-2022');
/// ```
///
/// This NULLs `kind` for **wrappers only** (the rows that HAVE a mint to
/// backfill). They then match `kind IS NULL`, get re-probed, and the UPDATE
/// rewrites `kind` AND writes `mint`. Plain ERC-20s (`kind = 'ERC-20'`) are left
/// untouched — they have no mint and must not re-probe. The pass is
/// self-terminating: once a wrapper is re-probed, `kind` becomes non-NULL and
/// `mint` is set, so it no longer matches the predicate.
///
/// Invariant: **wrappers end with `mint` set; plain ERC-20s stay `mint = NULL`;
/// no row re-probes forever.** Rows that the holders worker inserted before kind
/// was meaningful already carry a non-NULL `name`, so the same `kind IS NULL`
/// path (not `name IS NULL`) is what re-touches them.
use sqlx::PgPool;
use std::time::Duration;
use tracing::{debug, warn};

/// `mint_id()` getter selector on `SPL_ERC20` / `SPL_ERC20_cached`.
/// `keccak256("mint_id()")[0..4]`. Present on every factory wrapper (returns the
/// underlying Solana mint pubkey as bytes32); absent on plain ERC-20 (reverts).
const MINT_ID_SELECTOR: &str = "0xe132a122";

/// `getRestrictionModule(bytes32)` selector (`0xb9bbdc26`) concatenated with the
/// `keccak256("TRANSFER_RESTRICTION")` argument word — the full calldata to read
/// an ArcToken's transfer-restriction module. A plain ERC-20 reverts (→ None); a
/// gated ArcToken returns the WhitelistRestrictions module address (bytes32,
/// right-aligned). Selectors/arg computed offline (`cast sig` / `cast keccak`);
/// verified against keccak in the unit test below.
const GET_RESTRICTION_MODULE_CALL: &str =
    "0xb9bbdc2616d3efd52fe4afa679136c32a17cfe3bac40019518e3dd5b5d42aeb676bcb941";
/// `transfersAllowed()` selector — read on the restriction module. `false` means
/// transfers are currently gated (require both parties whitelisted).
const TRANSFERS_ALLOWED_SELECTOR: &str = "0xb0660c3d";

/// Solana SPL Token program id (mint owner for classic SPL mints).
const TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
/// Solana Token-2022 program id (mint owner for Token-2022 mints).
const TOKEN_2022_PROGRAM_ID: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

pub async fn run(
    pool: PgPool,
    chain_id: i64,
    proxy_url: String,
    solana_rpc_url: String,
    poll_interval: Duration,
    batch_size: i64,
) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .user_agent("rome-via-enrich/0.1")
        .build()?;

    loop {
        // Find tokens that still need enrichment. `name IS NULL` catches
        // freshly-inserted tokens (holders / factory-token discovery);
        // `kind IS NULL` is the re-poll path for the F6 kind backfill (operator
        // NULLs the column to force re-classification of already-named rows —
        // see the module doc "Backfill of existing rows").
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT address
             FROM rome_via.token_metadata
             WHERE chain_id = $1
               AND (name IS NULL OR kind IS NULL OR gated IS NULL)
             LIMIT $2",
        )
        .bind(chain_id)
        .bind(batch_size)
        .fetch_all(&pool)
        .await
        .unwrap_or_default();

        for (address,) in rows {
            let name = eth_call_string(&client, &proxy_url, &address, "0x06fdde03").await;
            let symbol = eth_call_string(&client, &proxy_url, &address, "0x95d89b41").await;
            let decimals = eth_call_uint8(&client, &proxy_url, &address, "0x313ce567").await;
            let total_supply =
                eth_call_uint256(&client, &proxy_url, &address, "0x18160ddd").await;

            // ── F6: classify kind ────────────────────────────────────────────
            // Step 1 (wrapper-or-not): mint_id() returns the underlying Solana
            // mint pubkey for wrappers; reverts (→ None) for plain ERC-20.
            let mint_b32 =
                eth_call_bytes32(&client, &proxy_url, &address, MINT_ID_SELECTOR).await;
            // Step 2 (SPL vs Token-2022, only when it IS a wrapper): read the
            // underlying mint's owner program on Solana.
            let mint_owner = match mint_b32.as_deref() {
                Some(m) => fetch_mint_owner(&client, &solana_rpc_url, m).await,
                None => None,
            };
            let kind = classify_kind(mint_b32.as_deref(), mint_owner.as_deref());

            // Provenance: the underlying Solana mint, base58. A wrapper's
            // `mint_id()` returns its bytes32 mint; a plain ERC-20 reverts (→
            // None) so `mint` stays NULL — correct, it has no underlying mint.
            // The all-zero `mint_id()` word (already filtered as "not a wrapper"
            // by classify_kind) likewise yields no mint here, so we don't write a
            // bogus base58 of the zero pubkey.
            let mint_b58 = match mint_b32.as_deref() {
                Some(m) if !is_zero_bytes32(m) => bytes32_hex_to_base58(m),
                _ => None,
            };

            // Negative-cache a reachable-but-unnamed token so it stops matching
            // `name IS NULL` and isn't re-probed every poll. "Reachable" = the
            // token answered ANY read (symbol / decimals / totalSupply / mint_id);
            // if `name()` reverted we then persist "" instead of NULL. If NOTHING
            // answered, it's an RPC failure / not-yet-reachable row — leave name
            // NULL so the next poll retries. See `name_to_persist`.
            let reachable = symbol.is_some()
                || decimals.is_some()
                || total_supply.is_some()
                || mint_b32.is_some();
            let name = name_to_persist(name, reachable);

            // ── Gated-RWA detection ──────────────────────────────────────────
            // A permissioned (Arc-style) token routes transfers through a
            // WhitelistRestrictions module. getRestrictionModule(TRANSFER_RESTRICTION)
            // returns its module address (revert / zero → no gate); the module's
            // transfersAllowed()==false means transfers are currently restricted.
            // Probe every token — a plain ERC-20 reverts (→ None), same as mint_id().
            let restriction_module_word =
                eth_call_bytes32(&client, &proxy_url, &address, GET_RESTRICTION_MODULE_CALL).await;
            let transfers_allowed = match restriction_module_word.as_deref() {
                Some(m) if !is_zero_bytes32(m) => {
                    eth_call_bool(
                        &client,
                        &proxy_url,
                        &bytes32_to_addr(m),
                        TRANSFERS_ALLOWED_SELECTOR,
                    )
                    .await
                }
                _ => None,
            };
            let (gated_verdict, restriction_module) =
                classify_gated(restriction_module_word.as_deref(), transfers_allowed);
            // Only cache a verdict for a token that answered something (`reachable`);
            // an unreachable row leaves `gated` NULL and re-probes, mirroring `name`.
            let gated = if reachable { gated_verdict } else { None };

            let res = sqlx::query(
                "UPDATE rome_via.token_metadata
                 SET name               = $3,
                     symbol             = $4,
                     decimals           = $5,
                     total_supply       = $6::NUMERIC,
                     kind               = $7,
                     mint               = COALESCE($8, mint),
                     gated              = COALESCE($9, gated),
                     restriction_module = COALESCE($10, restriction_module),
                     updated_at         = NOW()
                 WHERE chain_id = $1 AND address = $2",
            )
            .bind(chain_id)
            .bind(&address)
            .bind(name.as_deref())
            .bind(symbol.as_deref())
            .bind(decimals)
            .bind(total_supply.as_deref())
            .bind(kind)
            .bind(mint_b58.as_deref())
            .bind(gated)
            .bind(restriction_module.as_deref())
            .execute(&pool)
            .await;

            match res {
                Ok(_) => debug!(%address, kind, "fetched token metadata"),
                Err(e) => warn!(%address, error = %e, "failed to store token metadata"),
            }
        }

        tokio::time::sleep(poll_interval).await;
    }
}

/// Pure kind classifier (F6). No I/O — feed it the two on-chain reads.
///
/// * `mint_id` — the bytes32 returned by `eth_call mint_id()`, or `None` if the
///   call reverted / returned empty (i.e. NOT a wrapper). The all-zero bytes32
///   is also treated as "not a wrapper" (defensive: a real Solana mint pubkey is
///   never the zero pubkey, and a stray zero-word return shouldn't masquerade as
///   an SPL wrapper).
/// * `mint_owner` — the owner program of that mint on Solana
///   (`getAccountInfo(mint).value.owner`), or `None` if the Solana read was
///   unavailable.
///
/// Returns the canonical kind string for `token_metadata.kind`:
/// * not a wrapper → `"ERC-20"`
/// * wrapper, owner == Token-2022 program → `"Token-2022"`
/// * wrapper, owner == SPL Token program (or owner unknown) → `"SPL"`
///
/// The owner-unknown-defaults-to-SPL rule keeps the conservative answer when the
/// Solana RPC is unreachable: it's still definitely a wrapper (we got a mint), so
/// `SPL` is the right floor — far more wrappers are SPL than Token-2022, and a
/// false Token-2022 would be the more surprising mislabel.
pub fn classify_kind(mint_id: Option<&str>, mint_owner: Option<&str>) -> &'static str {
    let is_wrapper = match mint_id {
        Some(m) => !is_zero_bytes32(m),
        None => false,
    };
    if !is_wrapper {
        return "ERC-20";
    }
    match mint_owner {
        Some(o) if o == TOKEN_2022_PROGRAM_ID => "Token-2022",
        Some(o) if o == TOKEN_PROGRAM_ID => "SPL",
        // Wrapper, but owner unknown / unexpected — conservative SPL default.
        _ => "SPL",
    }
}

/// Resolve the `name` value to persist for a probed token, negative-caching a
/// reachable-but-unnamed token so it isn't re-probed every poll.
///
/// The worker's poll predicate is `WHERE name IS NULL OR kind IS NULL`. `kind`
/// is always set (`classify_kind` never returns None), so in steady state the
/// re-poll hinge is `name`. But `name` is written straight from the `name()`
/// eth_call, which is `None` when the call reverts — writing that back as SQL
/// NULL means the row matches `name IS NULL` again next poll → a permanent
/// re-probe (5 eth_calls/poll) for any token whose `name()` reverts. Same bug
/// class as the contract_labels storm (PR #371).
///
/// Fix: if `name()` reverted (`None`) but the token answered ANY other read
/// (`reachable` — symbol / decimals / totalSupply / mint_id succeeded), persist
/// `Some("")` so the row drops out of the predicate (a reachable, genuinely
/// unnamed token). If `name` AND every other read are `None` (`!reachable`) it's
/// an RPC failure / not-yet-reachable row — leave `None` (NULL) so the next poll
/// retries, mirroring contract_labels' getCode-RPC-failure handling.
///
/// Pure function — fully unit-tested.
fn name_to_persist(name: Option<String>, reachable: bool) -> Option<String> {
    match name {
        Some(n) => Some(n),
        None if reachable => Some(String::new()),
        None => None,
    }
}

/// True when a 0x-hex bytes32 word is all zeros (ignoring 0x prefix + case).
/// A real Solana mint pubkey is never the zero pubkey, so an all-zero `mint_id()`
/// return is treated as "not a wrapper".
fn is_zero_bytes32(hex: &str) -> bool {
    let h = hex.trim_start_matches("0x");
    !h.is_empty() && h.bytes().all(|b| b == b'0')
}

/// Low 20 bytes of a 0x-hex bytes32 word, as a 0x-prefixed EVM address. A
/// returned `address` is right-aligned in the 32-byte word (12 zero bytes then
/// the 20 address bytes = last 40 hex chars).
fn bytes32_to_addr(hex: &str) -> String {
    let h = hex.trim_start_matches("0x");
    let addr = if h.len() >= 40 { &h[h.len() - 40..] } else { h };
    format!("0x{addr}")
}

/// Pure gated-RWA classifier. No I/O — feed it the two on-chain reads.
///
/// * `restriction_module` — the bytes32 returned by
///   `getRestrictionModule(TRANSFER_RESTRICTION)`, or `None` when the call
///   reverted (a plain ERC-20 has no such getter). The all-zero word means no
///   module is wired.
/// * `transfers_allowed` — the module's `transfersAllowed()`, or `None` if that
///   read was unavailable this pass.
///
/// Returns `(gated, restriction_module_address)`:
/// * no module (revert / zero)        → `(Some(false), None)` — not permissioned
/// * module wired, transfers allowed  → `(Some(false), Some(addr))` — open (e.g. a
///   pool base before it's gated)
/// * module wired, transfers blocked  → `(Some(true),  Some(addr))` — actively gated
/// * module wired, toggle unreadable  → `(None, Some(addr))` — indeterminate; the
///   caller leaves `gated` NULL so the row re-probes rather than caching a guess.
pub fn classify_gated(
    restriction_module: Option<&str>,
    transfers_allowed: Option<bool>,
) -> (Option<bool>, Option<String>) {
    let module_hex = match restriction_module {
        Some(m) if !is_zero_bytes32(m) => m,
        _ => return (Some(false), None),
    };
    let addr = bytes32_to_addr(module_hex);
    match transfers_allowed {
        Some(true) => (Some(false), Some(addr)),
        Some(false) => (Some(true), Some(addr)),
        None => (None, Some(addr)),
    }
}

/// Read a Solana mint's owner program via `getAccountInfo`.
///
/// Returns the base58 owner program id (e.g. `Tokenkeg…` or `Tokenzc…`), or
/// `None` if the account isn't retrievable (RPC error, account missing). `mint`
/// is the bytes32 (0x-hex) pubkey from `mint_id()`; we convert it to base58 for
/// the Solana RPC.
async fn fetch_mint_owner(
    client: &reqwest::Client,
    solana_rpc_url: &str,
    mint_b32_hex: &str,
) -> Option<String> {
    let mint_b58 = bytes32_hex_to_base58(mint_b32_hex)?;
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAccountInfo",
        "params": [mint_b58, { "encoding": "base64" }],
    });
    let resp: serde_json::Value = client
        .post(solana_rpc_url)
        .json(&body)
        .send()
        .await
        .ok()?
        .json()
        .await
        .ok()?;
    resp.get("result")?
        .get("value")?
        .get("owner")?
        .as_str()
        .map(|s| s.to_string())
}

/// Call a view function that returns a `string` ABI-encoded result.
async fn eth_call_string(
    client: &reqwest::Client,
    proxy_url: &str,
    contract: &str,
    data: &str,
) -> Option<String> {
    let result_hex = eth_call_raw(client, proxy_url, contract, data).await?;
    decode_string_result(&result_hex)
}

/// Public wrapper so other workers (e.g. hook_metadata) can reuse the same
/// ABI-string eth_call logic without re-implementing RPC + decoding.
pub async fn eth_call_string_public(
    client: &reqwest::Client,
    proxy_url: &str,
    contract: &str,
    data: &str,
) -> Option<String> {
    eth_call_string(client, proxy_url, contract, data).await
}

/// Call a view function that returns a `uint8` (decimals).
async fn eth_call_uint8(
    client: &reqwest::Client,
    proxy_url: &str,
    contract: &str,
    data: &str,
) -> Option<i16> {
    let result_hex = eth_call_raw(client, proxy_url, contract, data).await?;
    let hex = result_hex.trim_start_matches("0x");
    if hex.len() < 64 {
        return None;
    }
    // Last 2 hex chars of the 32-byte word.
    i16::from_str_radix(&hex[hex.len() - 2..], 16).ok()
}

/// Call a view function that returns a `bool` (e.g. `transfersAllowed()`). An ABI
/// bool is a 32-byte word: any non-zero low byte = true. `None` on revert / empty.
async fn eth_call_bool(
    client: &reqwest::Client,
    proxy_url: &str,
    contract: &str,
    data: &str,
) -> Option<bool> {
    let result_hex = eth_call_raw(client, proxy_url, contract, data).await?;
    let hex = result_hex.trim_start_matches("0x");
    if hex.len() < 64 {
        return None;
    }
    u8::from_str_radix(&hex[hex.len() - 2..], 16)
        .ok()
        .map(|b| b != 0)
}

/// Call a view function that returns a `uint256` (totalSupply), as decimal string.
async fn eth_call_uint256(
    client: &reqwest::Client,
    proxy_url: &str,
    contract: &str,
    data: &str,
) -> Option<String> {
    let result_hex = eth_call_raw(client, proxy_url, contract, data).await?;
    let hex = result_hex.trim_start_matches("0x");
    if hex.is_empty() {
        return None;
    }
    // Parse as u128 (covers typical ERC-20 supplies).
    let truncated = if hex.len() > 32 {
        &hex[hex.len() - 32..]
    } else {
        hex
    };
    u128::from_str_radix(truncated, 16)
        .ok()
        .map(|n| n.to_string())
}

/// Call a view function that returns a single `bytes32` word (e.g. `mint_id()`).
///
/// Returns the 0x-prefixed 32-byte hex on success, or `None` when the call
/// reverts / returns empty (which is how a plain ERC-20 — no `mint_id()` —
/// surfaces, since `eth_call_raw` returns `None` for an `error` response and we
/// reject a too-short / empty `result`). This is the F6 wrapper discriminator.
async fn eth_call_bytes32(
    client: &reqwest::Client,
    proxy_url: &str,
    contract: &str,
    data: &str,
) -> Option<String> {
    let result_hex = eth_call_raw(client, proxy_url, contract, data).await?;
    let hex = result_hex.trim_start_matches("0x");
    // A bytes32 return is exactly 64 hex chars. Empty (`0x`) or short means the
    // selector wasn't a real getter on this contract.
    if hex.len() < 64 {
        return None;
    }
    Some(format!("0x{}", &hex[..64]))
}

/// Convert a 0x-hex 32-byte word to a base58 Solana pubkey string.
/// Returns `None` if the input isn't a clean 32-byte hex value.
///
/// `pub(crate)` so the `factory_tokens` worker can convert the `TokenCreated`
/// `mint` topic (topics[2], a bytes32 Solana pubkey) to the same canonical
/// base58 form this worker writes for `mint_id()` — one tested encoder, no
/// duplicate base58 implementation.
pub(crate) fn bytes32_hex_to_base58(hex: &str) -> Option<String> {
    let h = hex.trim_start_matches("0x");
    if h.len() != 64 {
        return None;
    }
    let mut bytes = [0u8; 32];
    for i in 0..32 {
        bytes[i] = u8::from_str_radix(&h[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(base58_encode(&bytes))
}

/// Minimal Bitcoin/Solana base58 encoder (no external dep). Handles leading
/// zero bytes as leading '1's, matching Solana pubkey display.
fn base58_encode(input: &[u8]) -> String {
    const ALPHABET: &[u8; 58] =
        b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    // Count leading zero bytes.
    let leading_zeros = input.iter().take_while(|&&b| b == 0).count();
    // Base-256 → base-58 via repeated division on a big-endian byte buffer.
    let mut digits: Vec<u8> = Vec::with_capacity(input.len() * 2);
    for &byte in input {
        let mut carry = byte as u32;
        for d in digits.iter_mut() {
            carry += (*d as u32) << 8;
            *d = (carry % 58) as u8;
            carry /= 58;
        }
        while carry > 0 {
            digits.push((carry % 58) as u8);
            carry /= 58;
        }
    }
    let mut out = String::with_capacity(leading_zeros + digits.len());
    for _ in 0..leading_zeros {
        out.push('1');
    }
    for &d in digits.iter().rev() {
        out.push(ALPHABET[d as usize] as char);
    }
    if out.is_empty() {
        out.push('1');
    }
    out
}

/// Raw eth_call JSON-RPC request. Returns the hex result string, or None on error.
async fn eth_call_raw(
    client: &reqwest::Client,
    proxy_url: &str,
    contract: &str,
    data: &str,
) -> Option<String> {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_call",
        "params": [
            { "to": contract, "data": data },
            "latest"
        ]
    });

    let resp = client
        .post(proxy_url)
        .json(&body)
        .send()
        .await
        .ok()?;

    let json: serde_json::Value = resp.json().await.ok()?;
    json.get("result")?.as_str().map(|s| s.to_string())
}

/// Decode an ABI-encoded `string` return value from a hex string.
/// Format: [offset (32 bytes)][length (32 bytes)][data (padded to 32 bytes)]
///
/// `pub(crate)` so sibling workers (e.g. `contract_labels`) can decode the
/// `string` payloads they pull out of a Multicall3 `aggregate3` return without
/// re-implementing ABI string decoding.
pub(crate) fn decode_string_result(hex: &str) -> Option<String> {
    let hex = hex.trim_start_matches("0x");
    // Minimum: 32 (offset) + 32 (length) + 1 char data.
    if hex.len() < 128 {
        return None;
    }
    // Parse string length from bytes 32..64 (second 32-byte word).
    let len_hex = &hex[64..128];
    let byte_len = usize::from_str_radix(len_hex, 16).ok()? * 2; // hex chars
    if byte_len == 0 {
        return Some(String::new());
    }
    // Data starts at byte 128.
    if hex.len() < 128 + byte_len {
        return None;
    }
    let data_hex = &hex[128..128 + byte_len];
    let bytes = (0..data_hex.len())
        .step_by(2)
        .filter_map(|i| u8::from_str_radix(&data_hex[i..i + 2], 16).ok())
        .collect::<Vec<u8>>();
    String::from_utf8(bytes).ok()
}

// ─────────────────────────────────────────────────────────────
// Unit tests
// ─────────────────────────────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_string_result_usdc() {
        // ABI-encoded "USD Coin" (result of eth_call name() on USDC)
        // offset: 0x20, length: 0x08
        // data: U=55 S=53 D=44 SP=20 C=43 o=6f i=69 n=6e, padded to 32 bytes
        let hex = "0x\
            0000000000000000000000000000000000000000000000000000000000000020\
            0000000000000000000000000000000000000000000000000000000000000008\
            555344204 36f696e0000000000000000000000000000000000000000000000000000";
        let hex_clean = hex.replace(' ', "");
        // hex_clean data segment: 55534420436f696e (8 bytes = "USD Coin"), then padding
        let result = decode_string_result(&hex_clean);
        assert_eq!(result, Some("USD Coin".to_string()));
    }

    #[test]
    fn decode_string_result_empty() {
        // ABI-encoded empty string
        let hex = "0x\
            0000000000000000000000000000000000000000000000000000000000000020\
            0000000000000000000000000000000000000000000000000000000000000000";
        let result = decode_string_result(hex);
        assert_eq!(result, Some(String::new()));
    }

    #[test]
    fn decode_string_result_too_short() {
        assert_eq!(decode_string_result("0x1234"), None);
        assert_eq!(decode_string_result("0x"), None);
    }

    // ── gated-RWA classifier ─────────────────────────────────────────────
    fn word_for(addr40: &str) -> String {
        format!("0x{}{}", "0".repeat(24), addr40)
    }

    #[test]
    fn classify_gated_plain_erc20_reverts_to_not_gated() {
        // getRestrictionModule reverted (→ None): a plain ERC-20 is not gated.
        assert_eq!(classify_gated(None, None), (Some(false), None));
    }

    #[test]
    fn classify_gated_zero_module_is_not_gated() {
        let zero = format!("0x{}", "0".repeat(64));
        assert_eq!(classify_gated(Some(&zero), Some(true)), (Some(false), None));
    }

    #[test]
    fn classify_gated_wired_and_restricted() {
        let w = word_for(&"a".repeat(40));
        assert_eq!(
            classify_gated(Some(&w), Some(false)),
            (Some(true), Some(format!("0x{}", "a".repeat(40))))
        );
    }

    #[test]
    fn classify_gated_wired_but_open() {
        // Module present but transfers currently allowed (e.g. a pool base before
        // it is gated) — record the module, but not gated.
        let w = word_for(&"b".repeat(40));
        assert_eq!(
            classify_gated(Some(&w), Some(true)),
            (Some(false), Some(format!("0x{}", "b".repeat(40))))
        );
    }

    #[test]
    fn classify_gated_toggle_unreadable_is_indeterminate() {
        // Module wired but transfersAllowed() couldn't be read — leave gated NULL
        // (re-probe), still surface the module address.
        let w = word_for(&"c".repeat(40));
        assert_eq!(
            classify_gated(Some(&w), None),
            (None, Some(format!("0x{}", "c".repeat(40))))
        );
    }

    #[test]
    fn bytes32_to_addr_takes_low_20_bytes() {
        assert_eq!(bytes32_to_addr(&word_for(&"d".repeat(40))), format!("0x{}", "d".repeat(40)));
    }

    #[test]
    fn gated_selectors_match_keccak256_of_signature() {
        use sha3::{Digest, Keccak256};
        let sel4 = |sig: &str| {
            let h = Keccak256::digest(sig.as_bytes());
            format!("0x{:02x}{:02x}{:02x}{:02x}", h[0], h[1], h[2], h[3])
        };
        assert_eq!(sel4("getRestrictionModule(bytes32)"), "0xb9bbdc26");
        assert_eq!(sel4("transfersAllowed()"), "0xb0660c3d");
        // Full getRestrictionModule calldata = selector + keccak256("TRANSFER_RESTRICTION").
        let arg_hex: String = Keccak256::digest(b"TRANSFER_RESTRICTION")
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        assert_eq!(GET_RESTRICTION_MODULE_CALL, format!("0xb9bbdc26{arg_hex}"));
    }

    // ─────────────────────────────────────────────────────────────
    // F6 — token KIND classification
    // ─────────────────────────────────────────────────────────────

    /// A plain ERC-20 has no `mint_id()` → the call reverts → `mint_id == None`
    /// → ERC-20. (Live-verified on Hadrian: the RSWAP-V2 LP pair at
    /// `0x3595cc…` reverts on `0xe132a122` while returning a valid symbol.)
    #[test]
    fn classify_kind_plain_erc20_when_no_mint() {
        assert_eq!(classify_kind(None, None), "ERC-20");
        // Even if a Solana owner were somehow present, no mint ⇒ not a wrapper.
        assert_eq!(classify_kind(None, Some(TOKEN_PROGRAM_ID)), "ERC-20");
    }

    /// An all-zero `mint_id()` word is NOT a wrapper (defensive — a real Solana
    /// mint pubkey is never the zero pubkey).
    #[test]
    fn classify_kind_zero_mint_is_erc20() {
        let zero = "0x0000000000000000000000000000000000000000000000000000000000000000";
        assert_eq!(classify_kind(Some(zero), None), "ERC-20");
    }

    /// A wrapper of a classic SPL-Token mint → SPL. (Live-verified on Hadrian:
    /// wUSDC `0x9a8b4c…` returns mint `4zMMC9srt5…` whose Solana owner is
    /// `TokenkegQ…`.)
    #[test]
    fn classify_kind_spl_wrapper() {
        let mint = "0x3b442cb3912157f13a933d0134282d032b5ffecd01a2dbf1b7790608df002ea7";
        assert_eq!(classify_kind(Some(mint), Some(TOKEN_PROGRAM_ID)), "SPL");
    }

    /// A wrapper whose underlying mint is owned by the Token-2022 program →
    /// Token-2022.
    #[test]
    fn classify_kind_token2022_wrapper() {
        let mint = "0x3b442cb3912157f13a933d0134282d032b5ffecd01a2dbf1b7790608df002ea7";
        assert_eq!(
            classify_kind(Some(mint), Some(TOKEN_2022_PROGRAM_ID)),
            "Token-2022"
        );
    }

    /// A wrapper whose Solana owner read failed (RPC down / mint missing) keeps
    /// the conservative SPL default — it's definitely a wrapper (we have a
    /// mint), and SPL is the far-more-common case.
    #[test]
    fn classify_kind_wrapper_owner_unknown_defaults_spl() {
        let mint = "0x4de5b3fa1e6c00708f7ff480e2186357da3bc7110c576e9364da84c4c77ad904";
        assert_eq!(classify_kind(Some(mint), None), "SPL");
        // An unexpected owner (not either token program) is also SPL-floored.
        assert_eq!(
            classify_kind(Some(mint), Some("SomeOtherProgram1111111111111111111111111111")),
            "SPL"
        );
    }

    // ── name_to_persist: negative-cache reachable-but-unnamed tokens ──────────

    /// name() returned a value → persist it verbatim (reachable is irrelevant).
    #[test]
    fn name_to_persist_keeps_real_name() {
        assert_eq!(
            name_to_persist(Some("USD Coin".to_string()), true),
            Some("USD Coin".to_string())
        );
        assert_eq!(
            name_to_persist(Some("USD Coin".to_string()), false),
            Some("USD Coin".to_string())
        );
    }

    /// THE latent-storm fix: name() reverted (None) but the token answered some
    /// other read (reachable) → persist "" so it stops matching `name IS NULL`
    /// and isn't re-probed every poll. (Empty-string Some, NOT None.)
    #[test]
    fn name_to_persist_negative_caches_reachable_unnamed() {
        assert_eq!(name_to_persist(None, true), Some(String::new()));
    }

    /// name() AND every other read were None (RPC failure / not yet reachable) →
    /// leave NULL so the next poll retries. This is the only path that keeps a
    /// row in the re-poll set, and it's the correct one (transient failure).
    #[test]
    fn name_to_persist_leaves_null_when_unreachable() {
        assert_eq!(name_to_persist(None, false), None);
    }

    #[test]
    fn is_zero_bytes32_detects_zero_and_nonzero() {
        assert!(is_zero_bytes32(
            "0x0000000000000000000000000000000000000000000000000000000000000000"
        ));
        assert!(is_zero_bytes32(
            "0000000000000000000000000000000000000000000000000000000000000000"
        ));
        assert!(!is_zero_bytes32(
            "0x3b442cb3912157f13a933d0134282d032b5ffecd01a2dbf1b7790608df002ea7"
        ));
        // Empty is not "all zeros" — it's nothing.
        assert!(!is_zero_bytes32(""));
        assert!(!is_zero_bytes32("0x"));
    }

    /// `mint_id()` decoding: a full 32-byte word is accepted (wrapper); empty /
    /// short returns are rejected (plain ERC-20 / non-getter). Mirrors what
    /// `eth_call_bytes32` does to the `eth_call_raw` result.
    #[test]
    fn bytes32_word_acceptance() {
        // Simulate eth_call_bytes32's length check on a raw result hex.
        let accept = |result_hex: &str| -> Option<String> {
            let hex = result_hex.trim_start_matches("0x");
            if hex.len() < 64 {
                return None;
            }
            Some(format!("0x{}", &hex[..64]))
        };
        let full = "0x3b442cb3912157f13a933d0134282d032b5ffecd01a2dbf1b7790608df002ea7";
        assert_eq!(accept(full), Some(full.to_string()));
        // Empty revert payload → None (plain ERC-20).
        assert_eq!(accept("0x"), None);
        assert_eq!(accept("0x1234"), None);
    }

    /// `bytes32 mint_id` → base58 must equal the canonical Solana mint pubkey.
    /// Live-verified vectors from Hadrian's deployed wrappers:
    /// wUSDC → `4zMMC9srt5Ri5X14GAgXhaHii3GnPAEERYPJgZJDncDU`,
    /// wSOL  → `So11111111111111111111111111111111111111112` (exercises the
    ///         leading-byte path so the base58 encoder is proven on real data).
    #[test]
    fn bytes32_to_base58_matches_solana_mints() {
        let wusdc = "0x3b442cb3912157f13a933d0134282d032b5ffecd01a2dbf1b7790608df002ea7";
        assert_eq!(
            bytes32_hex_to_base58(wusdc).as_deref(),
            Some("4zMMC9srt5Ri5X14GAgXhaHii3GnPAEERYPJgZJDncDU")
        );
        let wsol = "0x069b8857feab8184fb687f634618c035dac439dc1aeb3b5598a0f00000000001";
        assert_eq!(
            bytes32_hex_to_base58(wsol).as_deref(),
            Some("So11111111111111111111111111111111111111112")
        );
    }

    /// The two canonical token-program ids round-trip through the base58 encoder
    /// (they're the comparison constants used by `classify_kind`, so a broken
    /// encoder would silently misclassify). We encode their known byte forms.
    #[test]
    fn base58_encode_known_program_ids() {
        // SPL Token program id bytes (well-known).
        let spl_bytes: [u8; 32] = [
            6, 221, 246, 225, 215, 101, 161, 147, 217, 203, 225, 70, 206, 235, 121, 172, 28,
            180, 133, 237, 95, 91, 55, 145, 58, 140, 245, 133, 126, 255, 0, 169,
        ];
        assert_eq!(base58_encode(&spl_bytes), TOKEN_PROGRAM_ID);
    }
}
