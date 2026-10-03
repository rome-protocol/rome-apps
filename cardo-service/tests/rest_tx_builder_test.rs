//! Security-invariant tests for `TxBuilder` / `UnsignedTx`.
//!
//! These tests don't need a live Rome EVM stack — they construct the domain
//! types directly and round-trip them through the REST DTO. The assertion is
//! that nothing signature-shaped ever appears on the wire.

use ethers::types::{Address, U256};
use cardo_service::rest::types::UnsignedTxResponse;
use cardo_service::tx_builder::UnsignedTx;

#[tokio::test]
async fn unsigned_tx_response_never_contains_signing_keywords() {
    let ut = UnsignedTx {
        to: Address::zero(),
        data: vec![],
        value: U256::zero(),
        gas_limit: U256::from(21000u64),
        chain_id: 200200,
        nonce: 0,
        max_fee_per_gas: U256::from(1_000_000_000u64),
        max_priority_fee_per_gas: U256::from(1u64),
    };
    // Roundtrip via the REST DTO shape the handler actually emits.
    let dto = UnsignedTxResponse {
        to: format!("0x{:x}", ut.to),
        data: format!("0x{}", hex::encode(&ut.data)),
        value: ut.value.to_string(),
        gas_limit: ut.gas_limit.to_string(),
        chain_id: ut.chain_id,
        nonce: ut.nonce,
        max_fee_per_gas: ut.max_fee_per_gas.to_string(),
        max_priority_fee_per_gas: ut.max_priority_fee_per_gas.to_string(),
    };
    let json = serde_json::to_string(&dto).unwrap();
    let lower = json.to_lowercase();
    assert!(
        !lower.contains("signature"),
        "response must not contain 'signature': {json}"
    );
    assert!(
        !lower.contains("private"),
        "response must not contain 'private': {json}"
    );
    assert!(
        !lower.contains("secret"),
        "response must not contain 'secret': {json}"
    );
    assert!(
        !lower.contains("mnemonic"),
        "response must not contain 'mnemonic': {json}"
    );
}
