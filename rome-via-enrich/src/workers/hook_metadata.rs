/// Hook metadata worker.
///
/// For every EVM hook in `hooks_registry` whose `name` is still NULL:
///   1. Try the ERC-20-style `name()` call — works for hooks that expose a
///      human-readable string there.
///   2. Fall back to a selector fingerprint against the deployed runtime
///      bytecode. Rome Protocol ships a small set of reference hooks
///      (KYCHook, AMLHook, SanctionsHook) whose unique function selectors
///      identify them deterministically.
///   3. If neither works, persist `''` as a "queried, unnamed" sentinel so we
///      don't re-probe every poll cycle. Clearing that value manually re-runs
///      resolution.
///
/// Non-EVM (native Solana) hook programs are skipped — eth_call cannot resolve
/// them. Their display falls back to the raw Solana pubkey.
use sqlx::PgPool;
use std::time::Duration;
use tracing::{debug, warn};

use super::metadata;

/// Well-known reference hook contracts shipped under `tests/solidity/`.
/// Each entry: (display name, list of unique selectors — present only in that
/// contract's runtime bytecode). Any match on the needle set identifies the
/// contract. Keep the needles distinctive — if two hooks share a selector,
/// the first match wins.
const HOOK_SELECTOR_FINGERPRINTS: &[(&str, &[&str])] = &[
    // KYCHook: distinctive `approveAddress(bytes32)` + `revokeAddress(bytes32)`.
    ("KYCHook", &["14038b10"]),
    // AMLHook: has exemption/volume-limit selectors.
    ("AMLHook", &["f440c502", "a9e75723"]),
    // SanctionsHook: explicit sanction() admin surface.
    ("SanctionsHook", &["e063125c"]),
];

pub async fn run(
    pool: PgPool,
    chain_id: i64,
    proxy_url: String,
    poll_interval: Duration,
    batch_size: i64,
) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .user_agent("rome-via-enrich/0.1")
        .build()?;

    loop {
        let rows: Vec<(String,)> = sqlx::query_as(
            r#"
            SELECT DISTINCT hook_address
            FROM rome_via.hooks_registry
            WHERE chain_id = $1
              AND name IS NULL
              AND hook_address LIKE '0x%'
              AND length(hook_address) = 42
            LIMIT $2
            "#,
        )
        .bind(chain_id)
        .bind(batch_size)
        .fetch_all(&pool)
        .await
        .unwrap_or_default();

        if rows.is_empty() {
            tokio::time::sleep(poll_interval).await;
            continue;
        }

        for (address,) in rows {
            let resolved = resolve_name(&client, &proxy_url, &address).await;
            let res = sqlx::query(
                "UPDATE rome_via.hooks_registry
                 SET name = $3
                 WHERE chain_id = $1 AND hook_address = $2",
            )
            .bind(chain_id)
            .bind(&address)
            .bind(&resolved)
            .execute(&pool)
            .await;
            match res {
                Ok(r) if r.rows_affected() > 0 => {
                    debug!(%address, name = %resolved, "hook name resolved");
                }
                Ok(_) => {}
                Err(e) => warn!(%address, error = %e, "hook name update failed"),
            }
        }

        tokio::time::sleep(poll_interval).await;
    }
}

/// Resolve a display name for an EVM hook contract. Returns `""` when
/// neither `name()` nor the selector fingerprint matches — the caller stores
/// that as a "queried, unnamed" sentinel.
async fn resolve_name(client: &reqwest::Client, proxy_url: &str, address: &str) -> String {
    if let Some(s) = metadata::eth_call_string_public(client, proxy_url, address, "0x06fdde03").await
    {
        if !s.is_empty() {
            return s;
        }
    }
    if let Some(code) = eth_get_code(client, proxy_url, address).await {
        if let Some(name) = fingerprint_hook(&code) {
            return name.to_string();
        }
    }
    String::new()
}

/// Scan a deployed runtime bytecode (0x-prefixed hex string) for known hook
/// selector fingerprints. Returns the matched display name or None.
pub fn fingerprint_hook(code_hex: &str) -> Option<&'static str> {
    let code = code_hex.trim_start_matches("0x").to_lowercase();
    for (name, needles) in HOOK_SELECTOR_FINGERPRINTS {
        if needles.iter().any(|n| code.contains(&n.to_lowercase())) {
            return Some(name);
        }
    }
    None
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_kyc_hook_by_approve_address_selector() {
        // Minimal bytecode-like blob containing the approveAddress(bytes32) selector.
        let code = "0x608060405263 14038b10 000000".replace(' ', "");
        assert_eq!(fingerprint_hook(&code), Some("KYCHook"));
    }

    #[test]
    fn fingerprint_sanctions_hook_by_sanction_address_selector() {
        let code = "0xdeadbeef e063125c 00".replace(' ', "");
        assert_eq!(fingerprint_hook(&code), Some("SanctionsHook"));
    }

    #[test]
    fn fingerprint_aml_hook_by_exempt_address_selector() {
        let code = "0x6080 f440c502 00".replace(' ', "");
        assert_eq!(fingerprint_hook(&code), Some("AMLHook"));
    }

    #[test]
    fn fingerprint_unknown_bytecode_returns_none() {
        let code = "0x60806040deadbeef";
        assert!(fingerprint_hook(code).is_none());
    }
}
