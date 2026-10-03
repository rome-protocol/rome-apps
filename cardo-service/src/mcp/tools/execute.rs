// SECURITY INVARIANT: this function returns ONLY unsigned tx data.
// It MUST NOT accept a private key. It MUST NOT sign.
// Reviewer: if you see any signing logic added here, reject the PR.
//
//! MCP `execute` top-level tool — the generic unsigned-tx path.
//!
//! Symmetric with `quote`: same arg layout, same resolve-then-dispatch flow,
//! but returns the flat `UnsignedTxResponse` shape REST emits:
//!
//! ```json
//! {
//!   "to": "0x…",
//!   "data": "0x…",
//!   "value": "<dec>",
//!   "gas_limit": "<dec>",
//!   "chain_id": <u64>,
//!   "nonce": <u64>,
//!   "max_fee_per_gas": "<dec>",
//!   "max_priority_fee_per_gas": "<dec>"
//! }
//! ```
//!
//! No field contains any signing material. The
//! `execute_tool_response_has_no_signing_keywords` test in
//! `tests/mcp_handler_test.rs` grep-checks this on every CI run.

use serde_json::{json, Value};

use crate::mcp::error::McpError;
use crate::mcp::tools::capability;
use crate::rest::encoding::encode_inputs_as_hex;
use crate::rest::router::AppState;

/// Parsed + validated argument bundle.
#[derive(Debug)]
pub struct ExecuteArgs {
    pub app_id: String,
    pub capability: String,
    pub from: String,
    pub inputs: Value,
    pub gas_limit: Option<String>,
}

/// Parse + validate the arg object.
pub fn validate_args(args: &Value) -> Result<ExecuteArgs, McpError> {
    let app_id = args
        .get("app_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| McpError::InvalidParams("missing `app_id`".into()))?
        .to_string();
    let capability = args
        .get("capability")
        .and_then(|v| v.as_str())
        .ok_or_else(|| McpError::InvalidParams("missing `capability`".into()))?
        .to_string();
    let from = args
        .get("from")
        .and_then(|v| v.as_str())
        .ok_or_else(|| McpError::InvalidParams("missing `from` (caller address)".into()))?
        .to_string();
    let inputs = args.get("inputs").cloned().unwrap_or_else(|| json!({}));
    let gas_limit = args
        .get("gas_limit")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    Ok(ExecuteArgs {
        app_id,
        capability,
        from,
        inputs,
        gas_limit,
    })
}

/// Shared shape-builder for the unsigned-tx response.
///
/// Callers pass **already-rendered** string/hex fields (rather than raw
/// ethers types) so the grep test at `tests/mcp_handler_test.rs` can assert
/// the on-wire JSON shape without needing a live stack.
#[allow(clippy::too_many_arguments)]
pub fn build_unsigned_response_shape(
    to_hex_0x: &str,
    data: &[u8],
    value_dec: &str,
    gas_limit_dec: &str,
    chain_id: u64,
    nonce: u64,
    max_fee_per_gas_dec: &str,
    max_priority_fee_per_gas_dec: &str,
) -> Value {
    json!({
        "to": to_hex_0x,
        "data": format!("0x{}", hex::encode(data)),
        "value": value_dec,
        "gas_limit": gas_limit_dec,
        "chain_id": chain_id,
        "nonce": nonce,
        "max_fee_per_gas": max_fee_per_gas_dec,
        "max_priority_fee_per_gas": max_priority_fee_per_gas_dec,
    })
}

/// Full dispatch. Validates args, resolves the contract, builds an unsigned
/// EIP-1559 tx via `TxBuilder`, returns the response shape.
pub async fn call(state: &AppState, args: &Value) -> Result<Value, McpError> {
    let a = validate_args(args)?;
    let resolved = capability::resolve(&state.pool, &a.app_id, &a.capability).await?;
    let calldata_hex = encode_inputs_as_hex(&a.inputs)
        .map_err(|e| McpError::InvalidParams(format!("input encoding: {e}")))?;

    let ut = state
        .tx_builder
        .build_unsigned(
            &a.from,
            &resolved.contract_addr,
            &calldata_hex,
            "0",
            a.gas_limit.as_deref(),
        )
        .await
        .map_err(|e| McpError::Internal(anyhow::anyhow!("build_unsigned: {e}")))?;

    tracing::info!(
        client_kind = "agent",
        tool = "execute",
        app_id = %a.app_id,
        capability = %a.capability,
        "tools/call"
    );

    Ok(build_unsigned_response_shape(
        &format!("0x{:x}", ut.to),
        &ut.data,
        &ut.value.to_string(),
        &ut.gas_limit.to_string(),
        ut.chain_id,
        ut.nonce,
        &ut.max_fee_per_gas.to_string(),
        &ut.max_priority_fee_per_gas.to_string(),
    ))
}
