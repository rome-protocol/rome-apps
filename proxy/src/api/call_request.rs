//! Wrapper around [`ethers::types::TransactionRequest`] that accepts the
//! call/transaction shape used by modern EVM clients.
//!
//! ## Why this exists
//!
//! The Ethereum JSON-RPC API spec / EIP-1474 names the calldata field
//! `input`. Many older nodes (and the original `eth_call`/`eth_sendTransaction`
//! examples) called it `data`. To stay compatible with both, modern clients —
//! `viem`, `ethers v6`, `cast`/`foundry` — populate **both** `input` and
//! `data` with the same value when sending an `eth_call` or
//! `eth_sendTransaction` payload.
//!
//! Upstream `ethers-core` 2.x models `TransactionRequest` with a single
//! `data: Option<Bytes>` field aliased to `input` via `#[serde(alias = "input")]`.
//! Serde's `alias` only adds a second accepted name for the field — it does
//! **not** merge two keys when both appear. When a client sends a payload
//! with both `data` and `input`, Serde rejects the request with
//! `duplicate field 'data'` and the proxy returns
//! `error code -32602: Invalid params`.
//!
//! That is a degradation of standard JSON-RPC behavior — `cast send`,
//! `cast call`, and any viem/ethers client cannot talk to the proxy without
//! reaching for raw `curl`. Per Rome's parity rule ("Ethereum-equivalent,
//! not Ethereum-lite"), we have to
//! accept the canonical shape Ethereum tooling produces.
//!
//! ## How the fix works
//!
//! [`CallRequest`] is a transparent newtype around [`TransactionRequest`]
//! with a custom [`Deserialize`] impl. It first deserializes the payload
//! into a generic [`serde_json::Map`], normalizes any `data` / `input`
//! keys into a single `data` entry (the field name `ethers-core` actually
//! uses internally), then hands the normalized map to
//! `TransactionRequest`'s derived `Deserialize`.
//!
//! Per EIP-1474 the canonical field is `input`. When both keys are present
//! and identical, behavior is identical regardless of which we keep. When
//! they conflict (rare; only seen in malformed clients), we prefer `input`
//! to match the spec.

use {
    ethers::types::TransactionRequest,
    serde::{Deserialize, Deserializer},
    serde_json::Value,
    std::ops::Deref,
};

/// Newtype wrapper around [`TransactionRequest`] that accepts payloads
/// containing either / both `data` and `input` fields.
///
/// See the module docs for rationale.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct CallRequest(pub TransactionRequest);

impl Deref for CallRequest {
    type Target = TransactionRequest;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<CallRequest> for TransactionRequest {
    fn from(call: CallRequest) -> Self {
        call.0
    }
}

impl<'de> Deserialize<'de> for CallRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Deserialize into a generic JSON value so we can normalize the
        // `data` / `input` aliases before handing it to ethers-core.
        let mut value = Value::deserialize(deserializer)?;

        if let Some(obj) = value.as_object_mut() {
            let data = obj.remove("data");
            let input = obj.remove("input");

            // Per EIP-1474 the canonical field is `input`; prefer it when
            // both are present. ethers-core expects the merged value under
            // `data` (its struct field name), so we always re-insert there.
            let merged = match (input, data) {
                (Some(input), _) => Some(input),
                (None, Some(data)) => Some(data),
                (None, None) => None,
            };

            if let Some(v) = merged {
                obj.insert("data".to_string(), v);
            }
        }

        let inner = TransactionRequest::deserialize(value).map_err(serde::de::Error::custom)?;
        Ok(CallRequest(inner))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(json: &str) -> Result<CallRequest, serde_json::Error> {
        serde_json::from_str::<CallRequest>(json)
    }

    /// Regression test for the `cast send` / `cast call` failure observed
    /// during a chain bring-up rehearsal: a payload containing both `data`
    /// and `input` (identical values) was rejected with
    /// `duplicate field 'data'`.
    #[test]
    fn accepts_both_data_and_input_when_identical() {
        let json = r#"{
            "to":"0x60ab3dce24acc3bab0d858edecbdecc721b71114",
            "data":"0x2ef0576863157dead8b63d86c5f2376cc3e4c333228c439195a59fcefd01eae40e8dbc64",
            "input":"0x2ef0576863157dead8b63d86c5f2376cc3e4c333228c439195a59fcefd01eae40e8dbc64"
        }"#;
        let call = parse(json).expect("must parse with both data and input");
        let expected =
            "0x2ef0576863157dead8b63d86c5f2376cc3e4c333228c439195a59fcefd01eae40e8dbc64"
                .parse::<ethers::types::Bytes>()
                .unwrap();
        assert_eq!(call.0.data, Some(expected));
    }

    /// Pins the upstream behavior we are working around: ethers-core 2.x
    /// `TransactionRequest` rejects payloads that include both `data` and
    /// `input` even when their values are identical, because Serde's
    /// `#[serde(alias = "input")]` does not merge duplicate keys. If this
    /// ever changes upstream (and the wrapper becomes redundant), this
    /// test will fail and the wrapper can be removed.
    #[test]
    fn upstream_transaction_request_still_rejects_both_keys() {
        let json = r#"{
            "to":"0x60ab3dce24acc3bab0d858edecbdecc721b71114",
            "data":"0xdeadbeef",
            "input":"0xdeadbeef"
        }"#;
        let result: Result<TransactionRequest, _> = serde_json::from_str(json);
        let err = result.expect_err("upstream still rejects both keys");
        assert!(
            err.to_string().contains("duplicate field"),
            "expected duplicate-field error, got: {err}"
        );
    }

    #[test]
    fn accepts_only_data() {
        let json = r#"{
            "to":"0x60ab3dce24acc3bab0d858edecbdecc721b71114",
            "data":"0xdeadbeef"
        }"#;
        let call = parse(json).expect("must parse with only data");
        let expected = "0xdeadbeef".parse::<ethers::types::Bytes>().unwrap();
        assert_eq!(call.0.data, Some(expected));
    }

    #[test]
    fn accepts_only_input() {
        let json = r#"{
            "to":"0x60ab3dce24acc3bab0d858edecbdecc721b71114",
            "input":"0xdeadbeef"
        }"#;
        let call = parse(json).expect("must parse with only input");
        let expected = "0xdeadbeef".parse::<ethers::types::Bytes>().unwrap();
        assert_eq!(call.0.data, Some(expected));
    }

    /// When `data` and `input` disagree, EIP-1474 says `input` is canonical.
    #[test]
    fn prefers_input_when_data_and_input_disagree() {
        let json = r#"{
            "to":"0x60ab3dce24acc3bab0d858edecbdecc721b71114",
            "data":"0xaaaaaaaa",
            "input":"0xbbbbbbbb"
        }"#;
        let call = parse(json).expect("must parse with conflicting data/input");
        let expected = "0xbbbbbbbb".parse::<ethers::types::Bytes>().unwrap();
        assert_eq!(call.0.data, Some(expected));
    }

    #[test]
    fn accepts_neither_data_nor_input() {
        let json = r#"{"to":"0x60ab3dce24acc3bab0d858edecbdecc721b71114"}"#;
        let call = parse(json).expect("must parse without data or input");
        assert_eq!(call.0.data, None);
    }

    #[test]
    fn preserves_other_fields() {
        let json = r#"{
            "from":"0x768b73ee6ca9e0a1bc32868ca65db89e44696dd8",
            "to":"0x60ab3dce24acc3bab0d858edecbdecc721b71114",
            "gas":"0x5208",
            "gasPrice":"0x3b9aca00",
            "value":"0x0",
            "nonce":"0x1",
            "data":"0xdeadbeef",
            "input":"0xdeadbeef"
        }"#;
        let call = parse(json).expect("must parse full payload");
        let inner = call.0;
        assert!(inner.from.is_some());
        assert!(inner.to.is_some());
        assert_eq!(inner.gas, Some(0x5208u64.into()));
        assert_eq!(inner.gas_price, Some(0x3b9aca00u64.into()));
        assert_eq!(inner.value, Some(0u64.into()));
        assert_eq!(inner.nonce, Some(1u64.into()));
        assert!(inner.data.is_some());
    }
}
