mod call_request;
mod eth;
mod rome;

pub use call_request::CallRequest;

use {
    crate::error::Result,
    ethers::types::{
        Address, BlockId, Bytes, FeeHistory, Log, Transaction, TxHash, H256, U256, U64,
    },
    jsonrpsee::proc_macros::rpc,
    rome::{
        AccountMetaB58, AtomicUnsignedV1Plan, B58Pubkey, B58Signature, DepositV1Plan, Resource,
        UnsignedV1Plan, VersionInfo,
    },
    serde_json::Value,
    solana_sdk::{pubkey::Pubkey, signature::Signature},
};

#[rpc(server)]
pub trait Eth {
    #[method(name = "eth_getBalance")]
    async fn eth_get_balance(&self, address: Address, block: String) -> Result<U256>;
    #[method(name = "eth_chainId")]
    async fn eth_chain_id(&self) -> Result<U64>;
    #[method(name = "eth_blockNumber")]
    async fn eth_block_number(&self) -> Result<U64>;
    #[method(name = "eth_gasPrice")]
    async fn eth_gas_price(&self) -> Result<U256>;
    #[method(name = "eth_getBlockByNumber")]
    async fn eth_get_block_by_number(
        &self,
        block_number: BlockId,
        flag: bool,
    ) -> Result<Option<Value>>;
    #[method(name = "eth_getBlockByHash")]
    async fn eth_get_block_by_hash(&self, block_hash: H256, flag: bool) -> Result<Option<Value>>;
    #[method(name = "eth_call")]
    async fn eth_call(&self, call: CallRequest, block: String) -> Result<Bytes>;
    #[method(name = "eth_getTransactionCount")]
    async fn eth_get_transaction_count(&self, address: Address, block: String) -> Result<U64>;
    #[method(name = "eth_estimateGas")]
    async fn eth_estimate_gas(&self, call: CallRequest) -> Result<U256>;
    #[method(name = "eth_getCode")]
    async fn eth_get_code(&self, address: Address, block: String) -> Result<Bytes>;
    #[method(name = "eth_sendRawTransaction")]
    async fn eth_send_raw_transaction(&self, rlp: Bytes) -> Result<TxHash>;
    #[method(name = "net_version")]
    async fn net_version(&self) -> Result<String>;
    #[method(name = "eth_getTransactionReceipt")]
    async fn eth_get_transaction_receipt(&self, hash: H256) -> Result<Option<Value>>;
    #[method(name = "eth_getTransactionByHash")]
    async fn eth_get_transaction_by_hash(&self, hash: H256) -> Result<Option<Transaction>>;
    #[method(name = "eth_feeHistory")]
    async fn eth_fee_history(
        &self,
        count: U64,
        block_number: BlockId,
        reward_percentiles: Vec<f64>,
    ) -> Result<FeeHistory>;
    #[method(name = "txpool_status")]
    async fn txpool_status(&self) -> Result<Value>;
    #[method(name = "rpc_modules")]
    async fn rpc_modules(&self) -> Result<Value>;
    #[method(name = "web3_clientVersion")]
    async fn web3_client_version(&self) -> Result<String>;
    #[method(name = "eth_getStorageAt")]
    async fn eth_get_storage_at(
        &self,
        address: Address,
        slot: U256,
        block: String,
    ) -> Result<String>;
    #[method(name = "eth_maxPriorityFeePerGas")]
    async fn eth_max_priority_fee_per_gas(&self) -> Result<U256>;
    #[method(name = "eth_syncing")]
    async fn eth_syncing(&self) -> Result<Value>;
    #[method(name = "eth_accounts")]
    async fn eth_accounts(&self) -> Result<Vec<Address>>;
    #[method(name = "net_listening")]
    async fn net_listening(&self) -> Result<bool>;
    #[method(name = "net_peerCount")]
    async fn net_peer_count(&self) -> Result<U64>;
    #[method(name = "eth_getUncleCountByBlockNumber")]
    async fn eth_get_uncle_count_by_block_number(&self, block: BlockId) -> Result<U64>;
    #[method(name = "eth_getUncleCountByBlockHash")]
    async fn eth_get_uncle_count_by_block_hash(&self, hash: H256) -> Result<U64>;
    #[method(name = "eth_getUncleByBlockHashAndIndex")]
    async fn eth_get_uncle_by_block_hash_and_index(
        &self,
        hash: H256,
        index: U64,
    ) -> Result<Option<Value>>;
    #[method(name = "eth_getUncleByBlockNumberAndIndex")]
    async fn eth_get_uncle_by_block_number_and_index(
        &self,
        block: BlockId,
        index: U64,
    ) -> Result<Option<Value>>;
    #[method(name = "eth_getLogs")]
    async fn eth_get_logs(&self, filter: Value) -> Result<Vec<Log>>;
    // --- Step 4: block/tx index methods ---
    #[method(name = "eth_getBlockTransactionCountByNumber")]
    async fn eth_get_block_transaction_count_by_number(
        &self,
        block: BlockId,
    ) -> Result<Option<U64>>;
    #[method(name = "eth_getBlockTransactionCountByHash")]
    async fn eth_get_block_transaction_count_by_hash(&self, hash: H256) -> Result<Option<U64>>;
    #[method(name = "eth_getTransactionByBlockNumberAndIndex")]
    async fn eth_get_transaction_by_block_number_and_index(
        &self,
        block: BlockId,
        index: U64,
    ) -> Result<Option<Transaction>>;
    #[method(name = "eth_getTransactionByBlockHashAndIndex")]
    async fn eth_get_transaction_by_block_hash_and_index(
        &self,
        hash: H256,
        index: U64,
    ) -> Result<Option<Transaction>>;
    #[method(name = "eth_getBlockReceipts")]
    async fn eth_get_block_receipts(&self, block: BlockId) -> Result<Option<Vec<Value>>>;
    // --- Step 5: filter/polling methods ---
    #[method(name = "eth_newFilter")]
    async fn eth_new_filter(&self, filter: Value) -> Result<String>;
    #[method(name = "eth_newBlockFilter")]
    async fn eth_new_block_filter(&self) -> Result<String>;
    #[method(name = "eth_newPendingTransactionFilter")]
    async fn eth_new_pending_transaction_filter(&self) -> Result<String>;
    #[method(name = "eth_getFilterChanges")]
    async fn eth_get_filter_changes(&self, id: String) -> Result<Value>;
    #[method(name = "eth_getFilterLogs")]
    async fn eth_get_filter_logs(&self, id: String) -> Result<Vec<Log>>;
    #[method(name = "eth_uninstallFilter")]
    async fn eth_uninstall_filter(&self, id: String) -> Result<bool>;

    // --- Step 6: WebSocket push subscriptions (eth_subscribe / eth_unsubscribe) ---
    // `kind` is "newHeads" | "logs" | "newPendingTransactions"; `filter` is the
    // optional log-filter object for the "logs" kind. Poll-backed (Rome has no
    // internal block/log broadcast stream) — the handler pushes new blocks/logs
    // to the WS sink on an interval. Notifications use the "eth_subscription"
    // method name per the Ethereum convention.
    #[subscription(name = "eth_subscribe" => "eth_subscription", unsubscribe = "eth_unsubscribe", item = Value)]
    async fn eth_subscribe(
        &self,
        kind: String,
        filter: Option<Value>,
    ) -> jsonrpsee::core::SubscriptionResult;
}

#[rpc(server)]
pub trait Rome {
    #[method(name = "rome_emulateTxWithPayer")]
    async fn emulate_with_payer(&self, rlp: Bytes, pkey: B58Pubkey) -> Result<Vec<AccountMetaB58>>;

    /// Account discovery for a Solana-native (synthetic-sender) call. Takes the
    /// call by explicit `from` (no signature) — set `call.from` to the synthetic
    /// EVM address — and returns the Solana accounts the call touches, so the
    /// client can assemble the DoTxUnsigned tx and submit it to Solana directly.
    #[method(name = "rome_emulateCallAccounts")]
    async fn emulate_call_accounts(
        &self,
        call: CallRequest,
        payer: B58Pubkey,
    ) -> Result<Vec<AccountMetaB58>>;

    #[method(name = "rome_emulateTx")]
    async fn emulate_tx(&self, rlp: Bytes) -> Result<()>;
    #[method(name = "rome_mintId")]
    async fn mint_id(&self) -> Result<Option<Pubkey>>;
    #[method(name = "rome_emulateRegRollup")]
    async fn emulate_reg_rollup(
        &self,
        chain_id: u64,
        registry_authority_key: Pubkey,
        mint: Option<Pubkey>,
        single_state: bool,
    ) -> Result<Vec<AccountMetaB58>>;
    #[method(name = "rome_buildInfo")]
    async fn build_info(&self) -> Result<VersionInfo>;
    #[method(name = "rome_isCompatible")]
    async fn is_compatible(&self) -> Result<bool>;
    #[method(name = "rome_getResources")]
    async fn get_resources(&self) -> Result<Vec<Resource>>;
    /// Solana transaction signatures that executed a given EVM tx.
    /// Iterative EVM txs span multiple Solana txs, so the response is
    /// an array ordered by Solana slot. Empty array = not indexed yet.
    /// Backed by Hercules's evm_tx_sol_tx mapping.
    #[method(name = "rome_solanaTxForEvmTx")]
    async fn solana_tx_for_evm_tx(&self, tx_hash: H256) -> Result<Vec<String>>;

    /// Per-sub-call + per-CPI breakdown for a `DoTxBatch` transaction.
    /// Returns `None` if the tx is not a batch (or not yet indexed).
    /// Calls `parse_batch_trace` from rome-evm-client against the
    /// resolved Solana tx's `meta.log_messages`.
    #[method(name = "debug_traceRomeTransaction")]
    async fn debug_trace_rome_transaction(
        &self,
        tx_hash: H256,
    ) -> Result<Option<serde_json::Value>>;

    /// Submit an unsigned EVM transaction whose authority is proven by a
    /// Solana Ed25519 signature instead of an Ethereum secp256k1 signature.
    /// The Solana runtime verifies the signature via an Ed25519SigVerify
    /// instruction emitted ahead of the Rome `DoTx` ix; on success the EVM
    /// `msg.sender` is derived from `solana_pubkey`.
    ///
    /// Routes to `RomeEVMClient::send_transaction_ed25519`. Returns the EVM
    /// tx hash (`keccak256(rlp)`) after Solana confirmation.
    #[method(name = "rome_sendUnsignedTransaction")]
    async fn rome_send_unsigned_transaction(
        &self,
        rlp: Bytes,
        solana_pubkey: B58Pubkey,
        solana_signature: B58Signature,
    ) -> Result<TxHash>;

    /// Compose the wallet-signable V1 execution plan. The server emits one
    /// atomic `DoTx` transaction when possible, otherwise the iterative
    /// continuation sequence. Every response transaction has an empty wallet
    /// signature slot.
    #[method(name = "rome_composeUnsignedIterativeV1")]
    async fn compose_unsigned_iterative_v1(
        &self,
        rlp: Bytes,
        solana_pubkey: B58Pubkey,
        solana_signature: B58Signature,
    ) -> Result<UnsignedV1Plan>;

    /// Compose the complete wallet-signable V1 atomic sequence. The wallet
    /// supplies an Ed25519 signature over the typed unsigned EIP-1559 payload;
    /// the response contains its verifier prefix followed by generic `DoTx`.
    #[method(name = "rome_composeUnsignedAtomicV1")]
    async fn compose_unsigned_atomic_v1(
        &self,
        rlp: Bytes,
        solana_pubkey: B58Pubkey,
        solana_signature: B58Signature,
    ) -> Result<AtomicUnsignedV1Plan>;

    /// Compose the wallet-signed V1 deposit that funds this wallet's own
    /// synthetic EVM balance for proxy-relayed execution.
    #[method(name = "rome_composeExternalDepositV1")]
    async fn compose_external_deposit_v1(
        &self,
        solana_pubkey: B58Pubkey,
        amount: U256,
    ) -> Result<DepositV1Plan>;

    /// Broadcast a wallet-signed V1 execution plan. Each message remains paid
    /// for by `solana_pubkey`; the proxy only provides transport/confirmation.
    #[method(name = "rome_submitSignedV1Plan")]
    async fn submit_signed_v1_plan(
        &self,
        solana_pubkey: B58Pubkey,
        transactions: Vec<String>,
    ) -> Result<Vec<Signature>>;

    /// Relay a bare unsigned EIP-1559 transaction through a proxy payer.
    /// This is additive; `rome_sendUnsignedTransaction` remains unchanged.
    #[method(name = "rome_relayUnsignedV1")]
    async fn relay_unsigned_v1(
        &self,
        rlp: Bytes,
        solana_pubkey: B58Pubkey,
        solana_signature: B58Signature,
    ) -> Result<TxHash>;
}
