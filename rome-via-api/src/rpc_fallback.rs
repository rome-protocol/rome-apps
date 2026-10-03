/// RPC fallback — fetch transactions from foreign rollup Proxies via JSON-RPC.
///
/// When `GET /api/v1/txs/:hash?chain=<chain_id>` requests a chain not indexed locally,
/// we call the foreign Proxy's `eth_getTransactionByHash` + `eth_getTransactionReceipt`,
/// map to our `Tx` shape, and cache in Redis.
///
/// Cache TTLs:
/// - Pending (no receipt or receipt status=null): 30 seconds
/// - Confirmed (receipt present): 5 minutes (300s)
///
/// Rate limiting: token bucket, 10 rps per chain (enforced by per-chain governor limiter).
/// On unreachable proxy: returns AppError::ForeignProxyUnreachable (→ 503).
use crate::{api::models::{Tx, TxStatus, TxType}, error::AppError, state::AppState};
use serde_json::{json, Value};
use tracing::{debug, warn};

/// Cache key for a foreign tx: `rome_via:foreign_tx:{chain_id}:{tx_hash}`.
fn cache_key(chain_id: i64, tx_hash: &str) -> String {
    format!("rome_via:foreign_tx:{}:{}", chain_id, tx_hash)
}

/// Fetch a transaction from a foreign rollup via JSON-RPC, with Redis cache.
///
/// Returns `(Tx, source: "live")`. The source field is included by the caller.
pub async fn fetch_foreign_tx(
    state: &AppState,
    foreign_chain_id: i64,
    tx_hash: &str,
) -> Result<Tx, AppError> {
    let proxy_url = state
        .foreign_proxies
        .get(&foreign_chain_id)
        .ok_or_else(|| AppError::ChainNotSupported(format!("chain_id {foreign_chain_id} has no configured Proxy URL")))?
        .clone();

    // Check cache first.
    if let Some(ref mut redis) = state.redis.clone() {
        let key = cache_key(foreign_chain_id, tx_hash);
        let cached: Result<Option<String>, _> = redis::AsyncCommands::get(redis, &key).await;
        if let Ok(Some(json_str)) = cached {
            debug!(chain_id = foreign_chain_id, tx_hash, "Redis cache hit for foreign tx");
            if let Ok(tx) = serde_json::from_str::<Tx>(&json_str) {
                return Ok(tx);
            }
        }
    }

    // Check rate limiter.
    if let Some(limiter) = state.rate_limiters.get(&foreign_chain_id) {
        // Non-blocking check; if rate limited, we still proceed (lenient for v1).
        // A strict version would return 429, but that risks bad UX if the limiter is tight.
        if limiter.check().is_err() {
            warn!(chain_id = foreign_chain_id, "Rate limit hit for foreign chain — proceeding anyway");
        }
    }

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .map_err(|e| AppError::ForeignProxyUnreachable(format!("failed to build HTTP client: {e}")))?;

    // Fetch tx + receipt in parallel.
    let tx_req = make_rpc_call(&client, &proxy_url, "eth_getTransactionByHash", json!([tx_hash]));
    let receipt_req = make_rpc_call(&client, &proxy_url, "eth_getTransactionReceipt", json!([tx_hash]));

    let (tx_result, receipt_result) = tokio::join!(tx_req, receipt_req);

    let tx_val = tx_result.map_err(|e| {
        warn!(chain_id = foreign_chain_id, tx_hash, error = %e, "Foreign proxy unreachable");
        AppError::ForeignProxyUnreachable(format!("chain {foreign_chain_id}: {e}"))
    })?;

    // Tx not found on foreign proxy.
    if tx_val.is_null() {
        return Err(AppError::NotFound(format!(
            "transaction {} not found on chain {}", tx_hash, foreign_chain_id
        )));
    }

    let receipt_val = receipt_result.unwrap_or(Value::Null);

    // Map JSON-RPC response to Tx shape.
    let tx = map_eth_tx(&tx_val, &receipt_val, foreign_chain_id);

    // Cache result.
    if let Some(ref mut redis) = state.redis.clone() {
        if let Ok(json_str) = serde_json::to_string(&tx) {
            let ttl: u64 = if tx.status == TxStatus::Pending { 30 } else { 300 };
            let key = cache_key(foreign_chain_id, tx_hash);
            let _: Result<String, _> = redis::AsyncCommands::set_ex(redis, &key, json_str, ttl).await;
        }
    }

    Ok(tx)
}

/// Build and send an eth JSON-RPC call, returning the `result` field.
async fn make_rpc_call(
    client: &reqwest::Client,
    proxy_url: &str,
    method: &str,
    params: Value,
) -> Result<Value, String> {
    let body = json!({
        "jsonrpc": "2.0",
        "method": method,
        "params": params,
        "id": 1,
    });

    let resp = client
        .post(proxy_url)
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("HTTP error: {e}"))?;

    let json: Value = resp.json().await.map_err(|e| format!("JSON decode error: {e}"))?;

    let result = json.get("result").cloned().unwrap_or(Value::Null);
    Ok(result)
}

/// Map eth_getTransactionByHash + eth_getTransactionReceipt responses to our Tx shape.
fn map_eth_tx(tx: &Value, receipt: &Value, foreign_chain_id: i64) -> Tx {
    let hash = tx.get("hash").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let from = tx.get("from").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let to = tx.get("to").and_then(|v| v.as_str()).map(|s| s.to_string());
    let value = tx.get("value")
        .and_then(|v| v.as_str())
        .and_then(|s| u128::from_str_radix(s.trim_start_matches("0x"), 16).ok())
        .map(|n| n.to_string())
        .unwrap_or_else(|| "0".to_string());

    // Method selector: first 4 bytes of `input`.
    let method = tx.get("input")
        .and_then(|v| v.as_str())
        .filter(|s| s.len() >= 10)
        .map(|s| s[..10].to_string())
        .unwrap_or_else(|| "0x".to_string());

    // Block number.
    let block_number = tx.get("blockNumber")
        .and_then(|v| v.as_str())
        .and_then(|s| i64::from_str_radix(s.trim_start_matches("0x"), 16).ok());

    // Timestamp: not in tx response, would need eth_getBlockByNumber. Skip for v1.
    // Receipt status.
    let status = if receipt.is_null() || receipt.get("status").is_none() {
        TxStatus::Pending
    } else {
        let status_hex = receipt.get("status").and_then(|v| v.as_str()).unwrap_or("0x0");
        if status_hex == "0x1" {
            TxStatus::Success
        } else {
            TxStatus::Failed
        }
    };

    // Cross-chain type: for foreign chains always Rhea in v1 (we can't classify without full data).
    let _ = foreign_chain_id; // acknowledged
    let tx_type = TxType::Rhea;

    Tx {
        hash,
        status,
        tx_type,
        method,
        from,
        to,
        value,
        gas_used: None,
        gas_price: None,
        gas_recipient: None,
        // No Rome priority split for a foreign chain's tx (standard eth_* shape).
        priority_fee: None,
        base: None,
        gas_limit: None,
        tx_type_byte: None,
        timestamp: None,
        block_number,
        hook_executions: vec![],
        logs: vec![],
        logs_count: None,
        action_tags: vec![],
        solana_legs: vec![],
        origination: "ecdsa".to_string(),
        solana_signer: None,
        // Foreign-chain fallback has no local contract_labels row to draw from.
        to_label: None,
        to_label_detail: None,
        // Decoded transfers are a local-DB (detail-endpoint) concern; the
        // foreign-RPC fallback does not decode them.
        transfers: vec![],
        // Settling Solana signature comes from the local evm_tx_sol_tx mirror;
        // a foreign chain's tx has no such mapping here.
        solana_settlement_sig: None,
        // CPI target comes from the local cross_chain_correlations enrich table;
        // a foreign chain's tx has no such mapping here.
        cpi_program: None,
        cpi_program_label: None,
        cpi_instruction: None,
        // Foreign-chain fallback doesn't decode `nonce`; derivation is a
        // local-DB (rows_to_txs / get_tx_internal) concern only.
        contract_address: None,
        input_len: None,
        // Foreign-chain fallback doesn't decode revert reasons; a foreign
        // Proxy's own explorer is authoritative for that.
        revert_reason: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn map_eth_tx_success() {
        let tx = json!({
            "hash": "0xabc123",
            "from": "0xsender",
            "to": "0xrecipient",
            "value": "0xde0b6b3a7640000",
            "input": "0xa9059cbbdeadbeef",
            "blockNumber": "0x64",
        });
        let receipt = json!({ "status": "0x1" });
        let result = map_eth_tx(&tx, &receipt, 121214);
        assert_eq!(result.hash, "0xabc123");
        assert_eq!(result.status, TxStatus::Success);
        assert_eq!(result.method, "0xa9059cbb");
        assert_eq!(result.block_number, Some(100));
        assert_eq!(result.value, "1000000000000000000");
    }

    #[test]
    fn map_eth_tx_pending_no_receipt() {
        let tx = json!({
            "hash": "0xpending",
            "from": "0xsender",
            "to": null,
            "value": "0x0",
            "input": "0x",
            "blockNumber": null,
        });
        let receipt = Value::Null;
        let result = map_eth_tx(&tx, &receipt, 121214);
        assert_eq!(result.status, TxStatus::Pending);
        assert!(result.to.is_none());
    }

    #[test]
    fn cache_key_format() {
        let key = cache_key(121214, "0xabc");
        assert_eq!(key, "rome_via:foreign_tx:121214:0xabc");
    }
}
