//! Production [`ResolverRpc`] — backed by a real `ethers::providers::
//! Middleware` (D5's "no real implementation exists" gap, closed for P3b).
//! Every method here issues a real `eth_call`/`eth_getStorageAt`/
//! `eth_getLogs`; nothing here is a fake. This module's own tests exercise
//! it via `ethers::providers::{Provider, MockProvider}` — a canned-response
//! transport, never a live network socket from this crate's test suite (the
//! task's no-network-in-unit-tests rule).
//!
//! **Selectors/topics are 4-byte function selectors computed from the real
//! Solidity signature** (`cast sig "…"` / `cast keccak "…"`, same provenance
//! discipline as every `_TOPIC0` constant in `src/abi/*.rs`) — never
//! invented. See each method for its signature.
//!
//! **`logs_for` is NOT on the live log-walk path.** Its `eth_getLogs`
//! queries `[Earliest, Latest]` in one call, and the production proxy caps
//! `eth_getLogs` at 12,000 blocks (`-32005 block range too wide`), so this
//! path can never complete a real resolver log walk against it. The live
//! service therefore serves log history from the Hercules source DB via
//! [`super::hercules_rpc::HerculesResolverRpc`], which delegates only its
//! STATE reads here; this `logs_for` survives as that delegate's unused
//! log method and for standalone `MockProvider` tests (which have no range
//! cap).

use std::sync::Arc;

use ethers::providers::Middleware;
use ethers::types::{Address, BlockNumber, Bytes, Filter, TransactionRequest, H256};

use super::router_slot::{decode_address_from_storage_word, restrictions_router_slot};
use super::rpc::{LogEntry, MarketParams, ResolverRpc, RpcError};

/// `getTokenImplementation(address)`.
const GET_TOKEN_IMPLEMENTATION_SELECTOR: [u8; 4] = [0x3c, 0x15, 0xa7, 0xba];
/// `getRestrictionModule(bytes32)`.
const GET_RESTRICTION_MODULE_SELECTOR: [u8; 4] = [0xb9, 0xbb, 0xdc, 0x26];
/// `getGlobalModuleAddress(bytes32)`.
const GET_GLOBAL_MODULE_ADDRESS_SELECTOR: [u8; 4] = [0xee, 0xb9, 0x73, 0xdd];
/// `token0()`.
const TOKEN0_SELECTOR: [u8; 4] = [0x0d, 0xfe, 0x16, 0x81];
/// `token1()`.
const TOKEN1_SELECTOR: [u8; 4] = [0xd2, 0x12, 0x20, 0xa7];
/// `idToMarketParams(bytes32)`.
const ID_TO_MARKET_PARAMS_SELECTOR: [u8; 4] = [0x2c, 0x3c, 0x91, 0x57];

pub struct EthersResolverRpc<M> {
    client: Arc<M>,
}

impl<M> EthersResolverRpc<M> {
    pub fn new(client: Arc<M>) -> Self {
        Self { client }
    }
}

fn encode_call_address(selector: [u8; 4], addr: [u8; 20]) -> Bytes {
    let mut data = Vec::with_capacity(4 + 32);
    data.extend_from_slice(&selector);
    data.extend_from_slice(&[0u8; 12]);
    data.extend_from_slice(&addr);
    Bytes::from(data)
}

fn encode_call_bytes32(selector: [u8; 4], word: [u8; 32]) -> Bytes {
    let mut data = Vec::with_capacity(4 + 32);
    data.extend_from_slice(&selector);
    data.extend_from_slice(&word);
    Bytes::from(data)
}

fn encode_call_no_args(selector: [u8; 4]) -> Bytes {
    Bytes::from(selector.to_vec())
}

/// Standard ABI single-`address`-return decode: the low 20 bytes of the
/// (padded-to-32-byte) return word.
fn decode_address_return(ret: &[u8]) -> Result<[u8; 20], RpcError> {
    if ret.len() < 32 {
        return Err(RpcError::CallFailed(format!(
            "eth_call return too short for an address (need 32 bytes, got {})",
            ret.len()
        )));
    }
    let mut addr = [0u8; 20];
    addr.copy_from_slice(&ret[ret.len() - 20..]);
    Ok(addr)
}

fn call_data(to: [u8; 20], data: Bytes) -> ethers::types::transaction::eip2718::TypedTransaction {
    TransactionRequest::new()
        .to(Address::from(to))
        .data(data)
        .into()
}

#[async_trait::async_trait]
impl<M: Middleware + 'static> ResolverRpc for EthersResolverRpc<M> {
    async fn get_token_implementation(
        &self,
        factory: [u8; 20],
        token: [u8; 20],
    ) -> Result<[u8; 20], RpcError> {
        let tx = call_data(
            factory,
            encode_call_address(GET_TOKEN_IMPLEMENTATION_SELECTOR, token),
        );
        let ret = self
            .client
            .call(&tx, None)
            .await
            .map_err(|e| RpcError::CallFailed(e.to_string()))?;
        decode_address_return(&ret)
    }

    /// **Storage read, not an `eth_call`** (capture §1.2, corrected
    /// 2026-08-17) — see `router_slot`'s module doc.
    async fn restrictions_router(&self, token: [u8; 20]) -> Result<[u8; 20], RpcError> {
        let slot = restrictions_router_slot();
        let word = self
            .client
            .get_storage_at(Address::from(token), H256::from(slot), None)
            .await
            .map_err(|e| RpcError::CallFailed(e.to_string()))?;
        Ok(decode_address_from_storage_word(word.0))
    }

    async fn get_restriction_module(
        &self,
        token: [u8; 20],
        type_id: [u8; 32],
    ) -> Result<[u8; 20], RpcError> {
        let tx = call_data(
            token,
            encode_call_bytes32(GET_RESTRICTION_MODULE_SELECTOR, type_id),
        );
        let ret = self
            .client
            .call(&tx, None)
            .await
            .map_err(|e| RpcError::CallFailed(e.to_string()))?;
        decode_address_return(&ret)
    }

    async fn logs_for(
        &self,
        address: [u8; 20],
        topic0: [u8; 32],
    ) -> Result<Vec<LogEntry>, RpcError> {
        let filter = Filter::new()
            .address(Address::from(address))
            .topic0(H256::from(topic0))
            .from_block(BlockNumber::Earliest)
            .to_block(BlockNumber::Latest);
        let logs = self
            .client
            .get_logs(&filter)
            .await
            .map_err(|e| RpcError::CallFailed(e.to_string()))?;

        logs.into_iter()
            .map(|l| {
                Ok(LogEntry {
                    block_number: l
                        .block_number
                        .ok_or_else(|| RpcError::CallFailed("log missing block_number".into()))?
                        .as_u64() as i64,
                    tx_index: l
                        .transaction_index
                        .ok_or_else(|| {
                            RpcError::CallFailed("log missing transaction_index".into())
                        })?
                        .as_u32() as i32,
                    log_index: l
                        .log_index
                        .ok_or_else(|| RpcError::CallFailed("log missing log_index".into()))?
                        .as_u32() as i32,
                    address: l.address.0,
                    topics: l.topics.iter().map(|t| t.0).collect(),
                    data: l.data.to_vec(),
                })
            })
            .collect()
    }

    async fn get_global_module_address(
        &self,
        router: [u8; 20],
        type_id: [u8; 32],
    ) -> Result<[u8; 20], RpcError> {
        let tx = call_data(
            router,
            encode_call_bytes32(GET_GLOBAL_MODULE_ADDRESS_SELECTOR, type_id),
        );
        let ret = self
            .client
            .call(&tx, None)
            .await
            .map_err(|e| RpcError::CallFailed(e.to_string()))?;
        decode_address_return(&ret)
    }

    async fn token0(&self, pair: [u8; 20]) -> Result<[u8; 20], RpcError> {
        let tx = call_data(pair, encode_call_no_args(TOKEN0_SELECTOR));
        let ret = self
            .client
            .call(&tx, None)
            .await
            .map_err(|e| RpcError::CallFailed(e.to_string()))?;
        decode_address_return(&ret)
    }

    async fn token1(&self, pair: [u8; 20]) -> Result<[u8; 20], RpcError> {
        let tx = call_data(pair, encode_call_no_args(TOKEN1_SELECTOR));
        let ret = self
            .client
            .call(&tx, None)
            .await
            .map_err(|e| RpcError::CallFailed(e.to_string()))?;
        decode_address_return(&ret)
    }

    async fn id_to_market_params(
        &self,
        morpho: [u8; 20],
        market_id: [u8; 32],
    ) -> Result<MarketParams, RpcError> {
        let tx = call_data(
            morpho,
            encode_call_bytes32(ID_TO_MARKET_PARAMS_SELECTOR, market_id),
        );
        let ret = self
            .client
            .call(&tx, None)
            .await
            .map_err(|e| RpcError::CallFailed(e.to_string()))?;
        // MarketParams = (address loanToken, address collateralToken,
        // address oracle, address irm, uint256 lltv) — 5 consecutive
        // 32-byte words, all static (no head/tail indirection). Only the
        // first two matter to this crate's discovery decision.
        if ret.len() < 64 {
            return Err(RpcError::CallFailed(format!(
                "idToMarketParams return too short (need >= 64 bytes, got {})",
                ret.len()
            )));
        }
        let loan_token = decode_address_return(&ret[0..32])?;
        let collateral_token = decode_address_return(&ret[32..64])?;
        Ok(MarketParams {
            loan_token,
            collateral_token,
        })
    }
}

#[cfg(test)]
mod tests {
    //! Every test here goes through `ethers::providers::Provider::mocked()`
    //! — a canned JSON-RPC response queue, zero network sockets. This is
    //! what "real production code, exercised without a live network call"
    //! means for this module (task requirement).

    use ethers::providers::Provider;
    use ethers::types::{Bytes as EthersBytes, H256 as EthersH256, U256 as EthersU256};

    use super::*;

    fn addr(b: u8) -> [u8; 20] {
        [b; 20]
    }

    fn word_from_address(a: [u8; 20]) -> [u8; 32] {
        let mut word = [0u8; 32];
        word[12..].copy_from_slice(&a);
        word
    }

    #[tokio::test]
    async fn restrictions_router_reads_storage_at_the_derived_slot() {
        let (provider, mock) = Provider::mocked();
        let rpc = EthersResolverRpc::new(Arc::new(provider));

        let router = addr(0x42);
        let mut word = [0u8; 32];
        word[12..].copy_from_slice(&router);
        // `eth_getStorageAt` returns a hex-string-quoted H256 over JSON-RPC.
        mock.push(EthersH256::from(word)).unwrap();

        let got = rpc.restrictions_router(addr(0xA0)).await.unwrap();
        assert_eq!(got, router);
        // Slot-correctness itself is `router_slot`'s own self-check test
        // (pure, no mock needed — comparing against the vendored constant);
        // this test's job is the WIRING: a real `Middleware::get_storage_at`
        // call, whose returned word this crate decodes into an address.
    }

    #[tokio::test]
    async fn token0_and_token1_decode_the_returned_address() {
        let (provider, mock) = Provider::mocked();
        let rpc = EthersResolverRpc::new(Arc::new(provider));

        let token0_addr = addr(0x11);
        let ret = word_from_address(token0_addr);
        mock.push::<EthersBytes, _>(EthersBytes::from(ret.to_vec()))
            .unwrap();

        let got = rpc.token0(addr(0x99)).await.unwrap();
        assert_eq!(got, token0_addr);
    }

    #[tokio::test]
    async fn token0_propagates_a_reverted_call_as_an_error() {
        let (provider, mock) = Provider::mocked();
        let rpc = EthersResolverRpc::new(Arc::new(provider));

        // A JSON-RPC error response (the shape a revert surfaces as).
        mock.push_response(ethers::providers::MockResponse::Error(
            ethers::providers::JsonRpcError {
                code: 3,
                message: "execution reverted".to_string(),
                data: None,
            },
        ));

        let err = rpc.token0(addr(0x99)).await.unwrap_err();
        assert!(matches!(err, RpcError::CallFailed(_)));
    }

    #[tokio::test]
    async fn get_global_module_address_decodes_the_returned_address() {
        let (provider, mock) = Provider::mocked();
        let rpc = EthersResolverRpc::new(Arc::new(provider));

        let module = addr(0x77);
        let ret = word_from_address(module);
        mock.push::<EthersBytes, _>(EthersBytes::from(ret.to_vec()))
            .unwrap();

        let got = rpc
            .get_global_module_address(addr(0xB0), [0x7bu8; 32])
            .await
            .unwrap();
        assert_eq!(got, module);
    }

    #[tokio::test]
    async fn id_to_market_params_decodes_loan_and_collateral_from_the_first_two_words() {
        let (provider, mock) = Provider::mocked();
        let rpc = EthersResolverRpc::new(Arc::new(provider));

        let loan = addr(0x01);
        let collateral = addr(0x02);
        let oracle = addr(0x03);
        let irm = addr(0x04);
        let mut ret = Vec::new();
        for a in [loan, collateral, oracle, irm] {
            ret.extend_from_slice(&word_from_address(a));
        }
        ret.extend_from_slice(&[0u8; 32]); // lltv, unused by this decode
        mock.push::<EthersBytes, _>(EthersBytes::from(ret)).unwrap();

        let params = rpc
            .id_to_market_params(addr(0x55), [0xABu8; 32])
            .await
            .unwrap();
        assert_eq!(params.loan_token, loan);
        assert_eq!(params.collateral_token, collateral);
    }

    #[tokio::test]
    async fn logs_for_maps_a_real_shaped_log_response() {
        let (provider, mock) = Provider::mocked();
        let rpc = EthersResolverRpc::new(Arc::new(provider));

        let contract = addr(0x9c);
        let topic0 = [0x11u8; 32];
        let log = ethers::types::Log {
            address: Address::from(contract),
            topics: vec![
                EthersH256::from(topic0),
                EthersH256::from(word_from_address(addr(0xAA))),
            ],
            data: EthersBytes::from(vec![0u8; 32]),
            block_number: Some(ethers::types::U64::from(123u64)),
            transaction_index: Some(ethers::types::U64::from(2u64)),
            log_index: Some(EthersU256::from(5u64)),
            ..Default::default()
        };
        mock.push::<Vec<ethers::types::Log>, _>(vec![log]).unwrap();

        let logs = rpc.logs_for(contract, topic0).await.unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].block_number, 123);
        assert_eq!(logs[0].tx_index, 2);
        assert_eq!(logs[0].log_index, 5);
        assert_eq!(logs[0].address, contract);
        assert_eq!(logs[0].topics[0], topic0);
    }
}
