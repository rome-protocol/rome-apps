//! Transaction success/failure derivation.
//!
//! Shared so the persisted classification (rome-via-enrich) and the read path
//! (rome-via-api) can never disagree on whether a transaction succeeded.
use serde_json::Value;

pub const SUCCESS: &str = "success";
pub const FAILED: &str = "failed";
pub const PENDING: &str = "pending";

/// `tx_result.exit_reason` carries `{ code, reason }`: code 0 is success, any other
/// code is a failure, and a missing result row means the tx has not settled yet.
pub fn derive_status(tx_result: Option<&Value>) -> &'static str {
    let Some(v) = tx_result else { return PENDING };
    let Some(exit) = v.get("exit_reason") else { return FAILED };
    if let Some(code) = exit.get("code").and_then(|c| c.as_i64()) {
        return if code == 0 { SUCCESS } else { FAILED };
    }
    if let Some(reason) = exit.get("reason").and_then(|r| r.as_str()) {
        return if reason.starts_with("Succeed") { SUCCESS } else { FAILED };
    }
    FAILED
}

/// The revert cause for a FAILED tx, from `tx_result.exit_reason.reason`
/// (e.g. "Revert(0x…)" or a named error). `None` for success / pending / when
/// no reason string is present — the reason is currently read only to derive
/// status and then discarded, so the explorer's failed-tx page can't show a cause.
pub fn revert_reason(tx_result: Option<&Value>) -> Option<String> {
    if derive_status(tx_result) != FAILED {
        return None;
    }
    let reason = tx_result?.get("exit_reason")?.get("reason")?.as_str()?;
    if reason.is_empty() {
        return None;
    }
    Some(reason.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn revert_reason_extracts_cause_for_failed_txs_only() {
        // Failed (code != 0) with a reason → the reason string.
        assert_eq!(
            revert_reason(Some(&json!({"exit_reason":{"code":1,"reason":"Revert(0xdead)"}}))),
            Some("Revert(0xdead)".to_string())
        );
        // Failed via the reason-string fallback (no code) → the reason.
        assert_eq!(
            revert_reason(Some(&json!({"exit_reason":{"reason":"OutOfGas"}}))),
            Some("OutOfGas".to_string())
        );
        // Success → no revert reason.
        assert_eq!(
            revert_reason(Some(&json!({"exit_reason":{"code":0,"reason":"Succeed(Stopped)"}}))),
            None
        );
        // Pending / missing result → none.
        assert_eq!(revert_reason(None), None);
        // Failed but no reason string → none (nothing to show).
        assert_eq!(revert_reason(Some(&json!({"exit_reason":{"code":1}}))), None);
    }

    #[test]
    fn code_zero_is_success_any_other_code_fails() {
        assert_eq!(derive_status(Some(&json!({"exit_reason":{"code":0}}))), SUCCESS);
        assert_eq!(derive_status(Some(&json!({"exit_reason":{"code":1}}))), FAILED);
    }
    #[test]
    fn falls_back_to_the_reason_string_when_code_is_absent() {
        assert_eq!(derive_status(Some(&json!({"exit_reason":{"reason":"Succeed(Stopped)"}}))), SUCCESS);
        assert_eq!(derive_status(Some(&json!({"exit_reason":{"reason":"Revert(x)"}}))), FAILED);
    }
    #[test]
    fn missing_result_is_pending_missing_exit_reason_is_failed() {
        assert_eq!(derive_status(None), PENDING);
        assert_eq!(derive_status(Some(&json!({}))), FAILED);
    }
}
