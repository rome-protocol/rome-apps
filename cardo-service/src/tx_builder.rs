//! TxBuilder for cardo-service.
//!
//! Composes `simulate` and `build_unsigned` on top of `rome-evm-client`'s
//! existing primitives (`call`, `estimate_gas`, `transaction_count`,
//! `gas_price`).
//!
//! # Security invariant
//!
//! This module NEVER signs anything. It NEVER accepts a private key. The only
//! outputs are (a) simulation results and (b) unsigned tx data for the caller
//! to sign. If a PR reviewer sees a signing helper or private-key import added
//! here, reject it.

use anyhow::{Context, Result};
use ethers::types::{
    Address, Bytes, NameOrAddress, TransactionRequest, U256, U64,
};
use rome_sdk::rome_evm_client::RomeEVMClient;
use std::str::FromStr;
use std::sync::Arc;

/// Result of a simulation via `RomeEVMClient::call` + `estimate_gas`.
///
/// Note: `cu_used` is always `None` in v1. The `eth_call` path uses the
/// in-process Rust emulator which doesn't execute SBF, so real Solana CU is
/// not measured here. A future `SolanaEmulator` (Mollusk) integration in the
/// quote path can populate this.
pub struct SimulationOutcome {
    pub return_data: Vec<u8>,
    pub gas_used: U256,
    pub cu_used: Option<u64>,
}

/// An unsigned EIP-1559 transaction assembled for the caller to sign.
///
/// No signature material is carried on this struct — callers must sign it
/// externally (e.g. via their wallet).
pub struct UnsignedTx {
    pub to: Address,
    pub data: Vec<u8>,
    pub value: U256,
    pub gas_limit: U256,
    pub chain_id: u64,
    pub nonce: u64,
    pub max_fee_per_gas: U256,
    pub max_priority_fee_per_gas: U256,
}

/// Wraps a `RomeEVMClient` to provide simulate + unsigned-tx composition.
pub struct TxBuilder {
    client: Arc<RomeEVMClient>,
    chain_id: u64,
}

impl TxBuilder {
    /// Build a `TxBuilder` from an already-constructed `RomeEVMClient`.
    ///
    /// Keeping this constructor `Arc<RomeEVMClient>`-shaped keeps this module
    /// agnostic of whatever constructor shape the client crate needs. `main.rs`
    /// handles `RomeEVMClient::new` wiring.
    pub fn new(client: Arc<RomeEVMClient>, chain_id: u64) -> Self {
        Self { client, chain_id }
    }

    /// Simulate a call via `RomeEVMClient::call` + `estimate_gas`.
    ///
    /// Returns `return_data` (ABI-encoded outputs), `gas_used` (EVM gas), and
    /// `cu_used` (always `None` in v1 — see struct docs).
    pub async fn simulate(
        &self,
        from_hex: &str,
        to_hex: &str,
        calldata_hex: &str,
        value_dec: &str,
    ) -> Result<SimulationOutcome> {
        let req = build_tx_request(from_hex, to_hex, calldata_hex, value_dec)?;
        let return_data = self
            .client
            .call(&req)
            .await
            .map_err(|e| anyhow::anyhow!("call failed: {:?}", e))?;
        let gas_used = self
            .client
            .estimate_gas(&req)
            .await
            .map_err(|e| anyhow::anyhow!("estimate_gas failed: {:?}", e))?;
        Ok(SimulationOutcome {
            return_data: return_data.to_vec(),
            gas_used,
            cu_used: None,
        })
    }

    /// Build an unsigned EIP-1559 transaction ready for the caller to sign.
    ///
    /// If `gas_limit_dec` is `None`, gas is estimated via `estimate_gas`.
    pub async fn build_unsigned(
        &self,
        from_hex: &str,
        to_hex: &str,
        calldata_hex: &str,
        value_dec: &str,
        gas_limit_dec: Option<&str>,
    ) -> Result<UnsignedTx> {
        let from = parse_address(from_hex).context("invalid `from` address")?;
        let to = parse_address(to_hex).context("invalid `to` address")?;
        let data = parse_hex_bytes(calldata_hex).context("invalid calldata hex")?;
        let value = parse_u256_dec(value_dec).context("invalid `value` decimal")?;

        // Nonce.
        let nonce_u64: U64 = self
            .client
            .transaction_count(from)
            .await
            .map_err(|e| anyhow::anyhow!("transaction_count failed: {:?}", e))?;
        let nonce = nonce_u64.as_u64();

        // Gas price — used for `max_fee_per_gas`. Rome EVM doesn't currently
        // differentiate base fee vs priority fee, but keeping both fields on
        // the response lets clients treat it as a standard EIP-1559 tx.
        let gas_price = self
            .client
            .gas_price()
            .await
            .map_err(|e| anyhow::anyhow!("gas_price failed: {:?}", e))?;

        // Gas limit — caller-provided, else estimate.
        let gas_limit = match gas_limit_dec {
            Some(s) => parse_u256_dec(s).context("invalid `gas_limit` decimal")?,
            None => {
                let req = build_tx_request(from_hex, to_hex, calldata_hex, value_dec)?;
                self.client
                    .estimate_gas(&req)
                    .await
                    .map_err(|e| anyhow::anyhow!("estimate_gas failed: {:?}", e))?
            }
        };

        Ok(UnsignedTx {
            to,
            data: data.to_vec(),
            value,
            gas_limit,
            chain_id: self.chain_id,
            nonce,
            max_fee_per_gas: gas_price,
            // Minimum priority fee. Rome EVM ignores this, but we emit a
            // non-zero value so clients constructing EIP-1559 txs don't see
            // a fee quirk.
            max_priority_fee_per_gas: U256::from(1u64),
        })
    }
}

fn parse_address(s: &str) -> Result<Address> {
    Address::from_str(s.trim_start_matches("0x")).context("parse address")
}

fn parse_hex_bytes(s: &str) -> Result<Bytes> {
    let trimmed = s.trim_start_matches("0x");
    let bytes = hex::decode(trimmed).context("decode hex")?;
    Ok(Bytes::from(bytes))
}

fn parse_u256_dec(s: &str) -> Result<U256> {
    U256::from_dec_str(s).context("parse uint256 decimal")
}

fn build_tx_request(
    from_hex: &str,
    to_hex: &str,
    calldata_hex: &str,
    value_dec: &str,
) -> Result<TransactionRequest> {
    let from = parse_address(from_hex).context("invalid `from` address")?;
    let to = parse_address(to_hex).context("invalid `to` address")?;
    let data = parse_hex_bytes(calldata_hex).context("invalid calldata hex")?;
    let value = parse_u256_dec(value_dec).context("invalid `value` decimal")?;
    Ok(TransactionRequest {
        from: Some(from),
        to: Some(NameOrAddress::Address(to)),
        data: Some(data),
        value: Some(value),
        ..Default::default()
    })
}

/// Trip-wire constant asserting this module's security invariant.
///
/// If this string needs updating because a signing code path was added here,
/// that change must be rejected at code review. The `/execute` endpoint
/// returns an unsigned tx — signing happens client-side, never here.
#[allow(dead_code)]
const _SECURITY_INVARIANT: &str = "tx_builder: unsigned only, no keys";
