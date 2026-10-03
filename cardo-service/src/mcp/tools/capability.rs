//! MCP dynamic per-capability tool dispatch.
//!
//! Tool names are `<app_id>.<capability_name>`. The capability's `kind`
//! (`query` | `quote` | `execute`) picks the TxBuilder entry point:
//! read-only simulate for query/quote, unsigned tx for execute.
//!
//! # Security invariant (inherited)
//!
//! `kind=execute` returns ONLY unsigned tx data — same invariant REST
//! `/execute` enforces. The response is the `UnsignedTxResponse` shape from
//! `rest::types` so there's no place to hide signing bytes.

use serde_json::{json, Value};
use sqlx::{PgPool, Row};

use crate::mcp::error::McpError;
use crate::rest::encoding::encode_inputs_as_hex;
use crate::rest::router::AppState;

/// Resolved capability row — contract + ABI + kind.
#[derive(Debug)]
pub struct Resolved {
    pub app_id: String,
    pub name: String,
    pub contract_addr: String,
    pub kind: String,
}

/// Split a dotted tool name into `(app_id, capability_name)`. Returns
/// `ToolNotFound` if the name has no dot (e.g. a static tool was dispatched
/// here by mistake).
pub fn parse_tool_name(tool_name: &str) -> Result<(String, String), McpError> {
    match tool_name.split_once('.') {
        Some((a, c)) if !a.is_empty() && !c.is_empty() => Ok((a.to_string(), c.to_string())),
        _ => Err(McpError::ToolNotFound(tool_name.to_string())),
    }
}

/// Look up the capability's contract address + kind. Errors if the row isn't
/// in `app_capabilities`.
pub async fn resolve(pool: &PgPool, app_id: &str, cap: &str) -> Result<Resolved, McpError> {
    let row = sqlx::query(
        "SELECT a.contract_addr AS contract_addr, c.kind AS kind \
         FROM app_capabilities c JOIN apps a ON a.id = c.app_id \
         WHERE c.app_id = $1 AND c.name = $2",
    )
    .bind(app_id)
    .bind(cap)
    .fetch_optional(pool)
    .await
    .map_err(|e| McpError::Internal(anyhow::anyhow!("resolve capability: {e}")))?;
    let row = row.ok_or_else(|| McpError::CapabilityNotFound {
        app_id: app_id.to_string(),
        name: cap.to_string(),
    })?;
    Ok(Resolved {
        app_id: app_id.to_string(),
        name: cap.to_string(),
        contract_addr: row.get("contract_addr"),
        kind: row.get("kind"),
    })
}

/// Full dispatch for a per-capability tool. Looks up the row, encodes
/// calldata, calls `TxBuilder::simulate` or `build_unsigned` based on
/// `kind`, returns the right response shape.
pub async fn call(
    state: &AppState,
    tool_name: &str,
    args: &Value,
) -> Result<Value, McpError> {
    let (app_id, cap) = parse_tool_name(tool_name)?;
    let resolved = resolve(&state.pool, &app_id, &cap).await?;

    let from = args
        .get("from")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            McpError::InvalidParams("missing or non-string `from` (caller address)".into())
        })?
        .to_string();
    let inputs = args.get("inputs").cloned().unwrap_or_else(|| json!({}));
    let gas_limit_opt = args
        .get("gas_limit")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let calldata_hex = encode_inputs_as_hex(&inputs)
        .map_err(|e| McpError::InvalidParams(format!("input encoding: {e}")))?;

    tracing::info!(
        client_kind = "agent",
        tool = %tool_name,
        kind = %resolved.kind,
        "tools/call"
    );

    match resolved.kind.as_str() {
        "query" | "quote" => {
            let sim = state
                .tx_builder
                .simulate(&from, &resolved.contract_addr, &calldata_hex, "0")
                .await
                .map_err(|e| McpError::SimulationFailed(e.to_string()))?;
            Ok(json!({
                "return_data": format!("0x{}", hex::encode(&sim.return_data)),
                "gas_used": sim.gas_used.to_string(),
                "cu_used": sim.cu_used,
            }))
        }
        "execute" => {
            let ut = state
                .tx_builder
                .build_unsigned(
                    &from,
                    &resolved.contract_addr,
                    &calldata_hex,
                    "0",
                    gas_limit_opt.as_deref(),
                )
                .await
                .map_err(|e| McpError::Internal(anyhow::anyhow!("build_unsigned: {e}")))?;
            // Emit the same flat UnsignedTxResponse shape REST emits. No
            // signature keywords can appear — see grep test in Task 6.
            Ok(json!({
                "to": format!("0x{:x}", ut.to),
                "data": format!("0x{}", hex::encode(&ut.data)),
                "value": ut.value.to_string(),
                "gas_limit": ut.gas_limit.to_string(),
                "chain_id": ut.chain_id,
                "nonce": ut.nonce,
                "max_fee_per_gas": ut.max_fee_per_gas.to_string(),
                "max_priority_fee_per_gas": ut.max_priority_fee_per_gas.to_string(),
            }))
        }
        other => Err(McpError::Internal(anyhow::anyhow!(
            "unknown capability kind `{other}` (expected query | quote | execute)"
        ))),
    }
}
