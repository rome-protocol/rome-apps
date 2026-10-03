//! MCP `quote` top-level tool — the generic simulate path.
//!
//! Agents that want a uniform entrypoint pass `app_id` + `capability` as
//! explicit arguments (as opposed to calling the per-capability tool
//! `<app_id>.<capability>`). Same dispatch underneath: `TxBuilder::simulate`.
//!
//! Response shape:
//!
//! ```json
//! {
//!   "outputs": {"return_data": "0x..."},
//!   "gas_used": "<decimal>",
//!   "cu_used": null | <u64>
//! }
//! ```

use serde_json::{json, Value};

use crate::mcp::error::McpError;
use crate::mcp::tools::capability;
use crate::rest::encoding::encode_inputs_as_hex;
use crate::rest::router::AppState;

/// Parsed + validated argument bundle.
#[derive(Debug)]
pub struct QuoteArgs {
    pub app_id: String,
    pub capability: String,
    pub from: String,
    pub inputs: Value,
}

/// Parse + validate the arg object. Pulled out of `call` so tests can exercise
/// the validation path without constructing an `AppState`.
pub fn validate_args(args: &Value) -> Result<QuoteArgs, McpError> {
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
    Ok(QuoteArgs {
        app_id,
        capability,
        from,
        inputs,
    })
}

/// Build the simulation response object — extracted so the shape is testable
/// from the response grep test without a live stack.
pub fn build_simulation_response_shape(
    return_data: &[u8],
    gas_used_dec: &str,
    cu_used: Option<u64>,
) -> Value {
    json!({
        "outputs": {
            "return_data": format!("0x{}", hex::encode(return_data))
        },
        "gas_used": gas_used_dec,
        "cu_used": cu_used,
    })
}

/// Full dispatch. Validates args, resolves the contract, simulates via
/// `TxBuilder`, returns `{outputs, gas_used, cu_used}`.
pub async fn call(state: &AppState, args: &Value) -> Result<Value, McpError> {
    let a = validate_args(args)?;
    let resolved = capability::resolve(&state.pool, &a.app_id, &a.capability).await?;
    let calldata_hex = encode_inputs_as_hex(&a.inputs)
        .map_err(|e| McpError::InvalidParams(format!("input encoding: {e}")))?;

    let sim = state
        .tx_builder
        .simulate(&a.from, &resolved.contract_addr, &calldata_hex, "0")
        .await
        .map_err(|e| McpError::SimulationFailed(e.to_string()))?;

    tracing::info!(
        client_kind = "agent",
        tool = "quote",
        app_id = %a.app_id,
        capability = %a.capability,
        "tools/call"
    );

    Ok(build_simulation_response_shape(
        &sim.return_data,
        &sim.gas_used.to_string(),
        sim.cu_used,
    ))
}
