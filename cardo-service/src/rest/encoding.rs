//! Shared input-encoding helpers.
//!
//! Extracted from `rest::handlers` so MCP tools consume the same path — M3B.1
//! will replace the v1 minimum (empty-input-only) with real ABI encoding, and
//! both surfaces pick that up for free.
//!
//! # v1 limitation
//!
//! Until M3B.1 wires `ethers::abi::Abi` parsing, capability calls with any
//! arguments return `AppError::BadRequest` ("capability-argument encoding not
//! yet wired in v1"). Live Rome manifests' no-arg reads (e.g. `health_factor`-
//! style views) go through; anything with args fails cleanly.

use serde_json::Value;

use super::error::AppError;

/// ABI-encode the capability call from the manifest's JSON-schema `inputs`
/// structure.
///
/// v1 keeps this minimal: empty `{}` ⇒ `0x` (selector-only calldata); any
/// non-empty `{}` ⇒ `AppError::BadRequest`.
pub fn encode_inputs_as_hex(inputs: &Value) -> Result<String, AppError> {
    let obj = inputs
        .as_object()
        .ok_or_else(|| AppError::BadRequest("inputs must be object".into()))?;
    if !obj.is_empty() {
        return Err(AppError::BadRequest(
            "capability-argument encoding not yet wired in v1 — pass inputs: {} and the handler will return the raw function selector. Track in M3B.1.".into(),
        ));
    }
    Ok("0x".to_string())
}
