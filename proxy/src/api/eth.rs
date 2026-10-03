use {
    super::{CallRequest, EthServer},
    crate::{
        proxy::{Filter, FilterState, Proxy},
        error::{ApiError, Result, LOGS_LIMIT_EXCEEDED_CODE},
    },
    jsonrpsee::types::ErrorObjectOwned,
    jsonrpsee::{PendingSubscriptionSink, SubscriptionMessage},
    async_trait::async_trait,
    ethers::types::{
        transaction::eip2718::TypedTransaction, Address, BlockId, BlockNumber, Bytes, FeeHistory,
        Log, Transaction, TxHash, H256, U256, U64,
    },
    serde_json::{json, Value},
    std::{str::FromStr, time::Duration},
};

/// WebSocket subscription poll interval. Rome has no internal block/log
/// broadcast stream, so `eth_subscribe` is poll-backed: the handler checks for
/// new blocks at this cadence and pushes to the sink. ~2s keeps push latency
/// low without hammering the indexer (block time is well above this).
const SUBSCRIPTION_POLL: Duration = Duration::from_millis(2000);

/// Parse a JSON log filter's `address` + `topics` into the typed shapes the
/// indexer's `get_logs` expects. Shared by `eth_newFilter` semantics and the
/// `logs` WebSocket subscription so both filter identically.
fn parse_log_filter(filter: &Value) -> (Vec<Address>, Vec<Option<Vec<H256>>>) {
    let addresses: Vec<Address> = match filter.get("address") {
        None | Some(Value::Null) => vec![],
        Some(v) => {
            if let Some(s) = v.as_str() {
                Address::from_str(s).ok().into_iter().collect()
            } else if let Some(arr) = v.as_array() {
                arr.iter()
                    .filter_map(|a| a.as_str().and_then(|s| Address::from_str(s).ok()))
                    .collect()
            } else {
                vec![]
            }
        }
    };
    let topics: Vec<Option<Vec<H256>>> = match filter.get("topics").and_then(|t| t.as_array()) {
        None => vec![],
        Some(arr) => arr
            .iter()
            .map(|pos| {
                if pos.is_null() {
                    None
                } else if let Some(s) = pos.as_str() {
                    H256::from_str(s).ok().map(|h| vec![h])
                } else if let Some(inner) = pos.as_array() {
                    let hashes: Vec<H256> = inner
                        .iter()
                        .filter_map(|h| h.as_str().and_then(|s| H256::from_str(s).ok()))
                        .collect();
                    if hashes.is_empty() {
                        None
                    } else {
                        Some(hashes)
                    }
                } else {
                    None
                }
            })
            .collect(),
    };
    (addresses, topics)
}

impl Proxy {
    /// Parse a hex filter ID string ("0x1") into its u64 map key.
    fn parse_filter_id(id: &str) -> Option<u64> {
        u64::from_str_radix(id.trim_start_matches("0x"), 16).ok()
    }

    /// Allocate a new filter ID, store `state`, and return the hex ID string.
    /// Fail-closed at [`crate::proxy::MAX_FILTERS`]: refuses new filters once the
    /// registry is full (after evicting anything idle past [`crate::proxy::FILTER_TTL`]),
    /// so a client looping `eth_newFilter` can't grow the map without bound.
    async fn alloc_filter(&self, state: FilterState) -> Result<String> {
        let now = std::time::Instant::now();
        let mut filters = self.filters.write().await;
        match crate::proxy::insert_bounded_filter(
            &mut filters,
            &self.filter_counter,
            state,
            now,
            crate::proxy::MAX_FILTERS,
            crate::proxy::FILTER_TTL,
        ) {
            Some(id) => Ok(format!("0x{:x}", id)),
            None => Err(ApiError::custom(format!(
                "filter registry at capacity ({}); uninstall unused filters",
                crate::proxy::MAX_FILTERS
            ))),
        }
    }

    /// Enforce the configured eth_getLogs block-span cap (no-op when the
    /// `get_logs_max_block_range` config is omitted). Violations surface as
    /// the standard -32005 "limit exceeded" so clients paginate.
    fn check_get_logs_range(&self, from: U64, to: U64) -> Result<()> {
        Proxy::validate_get_logs_range(self.get_logs_max_block_range, from.as_u64(), to.as_u64())
            .map_err(|msg| {
                ApiError::ResponseFailed(ErrorObjectOwned::owned(
                    LOGS_LIMIT_EXCEEDED_CODE,
                    msg,
                    None::<String>,
                ))
            })
    }
}

/// The namespaces this proxy dispatches, for `rpc_modules` (geth's
/// node-introspection convention — not in the execution-apis spec, but served
/// by geth/erigon/nethermind/besu; also advertises `rome_*` to
/// capability-probing tools). Keep in lockstep with the `#[method(name=…)]`
/// prefixes in `api/mod.rs` — the unit test counts them.
/// Geth-style client identifier (`Name/vVersion/language`) so version-sniffing
/// tools see a real client string instead of a placeholder.
fn client_version_string() -> String {
    format!("Rome-Proxy/v{}/rust", env!("CARGO_PKG_VERSION"))
}

/// Single-state Rome executes synchronously — there is no mempool. Honest
/// empty counts (geth hex-string format) rather than -32601.
fn txpool_status_map() -> Value {
    serde_json::json!({"pending": "0x0", "queued": "0x0"})
}

fn rpc_modules_map() -> Value {
    serde_json::json!({
        "debug": "1.0", "eth": "1.0", "net": "1.0",
        "rome": "1.0", "rpc": "1.0", "txpool": "1.0", "web3": "1.0",
    })
}



fn decode_priority_fee_wei(rlp: &Bytes) -> u128 {
    let vec = rlp.to_vec();
    let rlp_ = rlp::Rlp::new(&vec);
    match TypedTransaction::decode_signed(&rlp_) {
        Ok((TypedTransaction::Eip1559(tx), _)) => match tx.max_priority_fee_per_gas {
            Some(fee) => {
                if fee.bits() > 128 {
                    u128::MAX
                } else {
                    fee.as_u128()
                }
            }
            None => 0,
        },
        _ => 0,
    }
}


/// Normalize a block Value for Blockscout compatibility:
/// - Remap type 0x7e (Rome deposit) transactions to type 0x0
/// - Remove OP-stack-specific fields (sourceHash, mint, isSystemTx)
fn normalize_block(mut v: Value) -> Value {
    if let Some(txs) = v.get_mut("transactions") {
        if let Some(arr) = txs.as_array_mut() {
            for tx in arr.iter_mut() {
                if let Some(obj) = tx.as_object_mut() {
                    if obj.get("type").and_then(|t| t.as_str()) == Some("0x7e") {
                        obj.insert("type".to_string(), json!("0x0"));
                        obj.remove("sourceHash");
                        obj.remove("mint");
                        obj.remove("isSystemTx");
                    }
                }
            }
        }
    }
    v
}

/// `net_version` returns the chain id as a DECIMAL string, per the JSON-RPC
/// convention. Returning ethers `U64` here serializes as hex ("0x30d4a"),
/// which web3.js and legacy MetaMask network-detection paths mis-parse
/// (`parseInt("0x30d4a")` is wrong). `eth_chainId` stays hex `U64`; only
/// `net_version` is decimal.
fn net_version_decimal(chain_id: u64) -> String {
    chain_id.to_string()
}

/// Alias the `pending` block tag to `latest`. Rome has no mempool/pending
/// state (`eth_newPendingTransactionFilter` returns empty), and the indexer's
/// `get_block_number` rejects any tag other than `latest`/numeric — so a
/// `pending` query (issued by ethers, MetaMask, and many health-checks) would
/// otherwise fail with an error. Aliasing to `latest` is the conventional
/// behavior for chains without a distinct pending block.
fn normalize_block_id(id: BlockId) -> BlockId {
    match id {
        BlockId::Number(BlockNumber::Pending) => BlockId::Number(BlockNumber::Latest),
        other => other,
    }
}

/// Overlay the served block's `baseFeePerGas` with the live pool gas price so
/// ethers/viem `getFeeData()` (which reads `block.baseFeePerGas` from
/// `getBlock('latest')`) computes a sane maxFee; the program then charges
/// `min(maxFee, base)`. Rome has no real per-block base fee (the indexer stores
/// 0), so advertising the current price on every served block is correct. `base
/// == None` (price read failed) leaves the block untouched (graceful).
fn overlay_base_fee(mut v: Value, base: Option<U256>) -> Value {
    if let (Some(obj), Some(base)) = (v.as_object_mut(), base) {
        obj.insert("baseFeePerGas".to_string(), json!(format!("0x{:x}", base)));
    }
    v
}

#[async_trait]
impl EthServer for Proxy {
    #[tracing::instrument(name = "proxy::eth_chain_id", skip(self))]
    async fn eth_chain_id(&self) -> Result<U64> {
        let result = self.rome_evm_client.chain_id();
        tracing::info!("eth_chain_id: {:?}", result);
        Ok(result.into())
    }

    #[tracing::instrument(name = "proxy::eth_block_number", skip(self))]
    async fn eth_block_number(&self) -> Result<U64> {
        let result = self
            .rome_evm_client
            .block_number()
            .await
            .map_err(ApiError::RomeEvmError);
        tracing::info!("eth_block_number: {:?}", result);
        result
    }

    #[tracing::instrument(name = "proxy::eth_get_balance", skip(self), fields(address = ?address))]
    async fn eth_get_balance(&self, address: Address, _block: String) -> Result<U256> {
        let result = self
            .rome_evm_client
            .get_balance(address)
            .await
            .map_err(ApiError::RomeEvmError)?;

        tracing::info!("eth_get_balance: {:?} {:?}", address, result);
        Ok(result)
    }

    #[tracing::instrument(name = "proxy::eth_gas_price", skip(self))]
    async fn eth_gas_price(&self) -> Result<U256> {
        let result = self
            .rome_evm_client
            .gas_price()
            .await
            .map_err(ApiError::RomeEvmError);
        tracing::info!("eth_gas_price: {:?}", result);
        result
    }

    #[tracing::instrument(name = "proxy::eth_get_block_by_number", skip(self), fields(block_number = ?block_number))]
    async fn eth_get_block_by_number(
        &self,
        block_number: BlockId,
        full_transactions: bool,
    ) -> Result<Option<Value>> {
        let block = self
            .rome_evm_client
            .get_block(normalize_block_id(block_number), full_transactions)
            .await
            .map_err(ApiError::RomeEvmError)?;
        let base = self.rome_evm_client.gas_price().await.ok();
        let result = block.map(|b| overlay_base_fee(normalize_block(serde_json::to_value(b).unwrap_or(Value::Null)), base));
        tracing::info!("eth_get_block_by_number {:?}", result);
        Ok(result)
    }

    #[tracing::instrument(name = "proxy::eth_get_block_by_hash", skip(self), fields(block_hash = %block_hash))]
    async fn eth_get_block_by_hash(
        &self,
        block_hash: H256,
        full_transactions: bool,
    ) -> Result<Option<Value>> {
        let block = self
            .rome_evm_client
            .get_block(BlockId::Hash(block_hash), full_transactions)
            .await
            .map_err(ApiError::RomeEvmError)?;
        let base = self.rome_evm_client.gas_price().await.ok();
        let result = block.map(|b| overlay_base_fee(normalize_block(serde_json::to_value(b).unwrap_or(Value::Null)), base));
        tracing::info!("eth_get_block_by_hash {:?}", block_hash);
        Ok(result)
    }

    #[tracing::instrument(name = "proxy::eth_call", skip(self))]
    async fn eth_call(&self, call: CallRequest, _block: String) -> Result<Bytes> {
        // rome-sdk's `call_blocking` became async when the EmulatorBackend
        // trait split landed. Its internal `spawn_blocking` already moves the
        // sync emulator work off the async worker, so bypassing the read
        // pool here is safe for correctness; the tradeoff is that read-pool
        // backpressure ("read pool at capacity") no longer gates eth_call.
        // TODO(read-pool-async): extend `offload`/`read_pool.run` to accept
        // async closures so we can preserve the intake bound here.
        let result = self
            .rome_evm_client
            .call_blocking(&call)
            .await
            .map_err(ApiError::from);
        tracing::info!("eth_call: {:?}", result);
        result
    }

    #[tracing::instrument(name = "proxy::eth_get_transaction_count", skip(self), fields(address = ?address))]
    async fn eth_get_transaction_count(&self, address: Address, block: String) -> Result<U64> {
        // Always read the per-address nonce from on-chain `BalancePda.nonce`,
        // regardless of block tag. The historical-by-block_num path used to
        // route to the indexed `evm_tx` table, but that count diverges from
        // on-chain truth in two scenarios we've hit in production:
        //
        //   1. Indexer over-count (Aurelius 2026-05-11): hercules indexed
        //      duplicate evm_tx rows for the same tx_hash after a stack
        //      restart while talking to a rate-limited Solana RPC. Wallets
        //      querying eth_getTransactionCount(addr, 0xN) saw an inflated
        //      count, submitted with too-high nonce, the chain rejected as
        //      InvalidTxNonce(addr, got, expected).
        //
        //   2. Indexer under-count (Aurelius 2026-05-12): the Rome-operated
        //      Solana RPC bootstrapped from a snapshot at slot N, so
        //      pre-snapshot user txs aren't reachable from any peer (Solana
        //      testnet retains snapshots <24h). The indexed view is
        //      structurally incomplete — there's no way to recover those
        //      tx receipts. Numeric-block nonce queries return 0 for a
        //      user whose on-chain BalancePda.nonce is N > 0, the wallet
        //      submits nonce 0, the chain rejects.
        //
        // The right invariant for Rome: per-address nonce lives in a single
        // non-versioned counter on the user's BalancePda. There's no per-EVM-
        // block historical state to query, so the block tag is functionally
        // meaningless for this RPC method. Matches geth's effective behavior
        // for non-archive nodes (any non-historical block resolves to
        // current state).
        //
        // The `block` arg is preserved in the tracing line for diagnostic
        // completeness; it's intentionally not used for storage routing.
        let result = self
            .rome_evm_client
            .transaction_count(address)
            .await
            .map_err(ApiError::RomeEvmError);
        tracing::info!("eth_get_transaction_count: {:?}, block={:?}, {:?}", address, block, result);
        result
    }

    #[tracing::instrument(name = "proxy::eth_estimate_gas", skip(self, call))]
    async fn eth_estimate_gas(&self, call: CallRequest) -> Result<U256> {
        let client = self.rome_evm_client.clone();
        let result = client.estimate_gas(&call).await.map_err(|e| e.into());
        tracing::info!("eth_estimate_gas: {:?}", result);
        result
    }

    #[tracing::instrument(name = "proxy::eth_get_code", skip(self), fields(address = ?address))]
    async fn eth_get_code(&self, address: Address, _block: String) -> Result<Bytes> {
        let result = self
            .rome_evm_client
            .get_code(address)
            .await
            .map_err(ApiError::RomeEvmError);
        tracing::info!("eth_get_code: {:?} {:?}", address, result);
        result
    }

    #[tracing::instrument(name = "proxy::eth_send_raw_transaction", skip(self, rlp))]
    async fn eth_send_raw_transaction(&self, rlp: Bytes) -> Result<TxHash> {
        // JB6.2 routing: when operator has configured a `JitoBundleClient`
        // AND set `evm_priority_fee_threshold_wei` AND the inbound tx clears
        // the threshold, dispatch via the Jito bundle path. Otherwise, the
        // existing tower path. Default-off invariant: with no `jito_bundler`
        // section in the proxy config, `bundler.is_none()` and every tx
        // takes the original path — byte-identical to today.
        // Batching (default-off) takes precedence when wired: coalesce concurrent
        // submits via the Batcher (→ RomeEVMClient::send_pack). Absent `batching`
        // config ⇒ batcher.is_none() ⇒ the bundle/tower path below, unchanged.
        let priority_fee_wei = decode_priority_fee_wei(&rlp);
        let result = if Proxy::should_batch(self.batcher.as_ref()) {
            // INVARIANT: should_batch is true iff batcher.is_some() — safe to unwrap.
            self.batcher
                .as_ref()
                .expect("should_batch=true implies batcher.is_some()")
                .submit(rlp)
                .await
                .map_err(|e| e.into())
        } else if Proxy::should_auto_promote(self.bundler.as_ref(), priority_fee_wei) {
            // INVARIANT: predicate returns true iff `bundler.is_some()` —
            // safe to unwrap.
            let bundler = self
                .bundler
                .as_ref()
                .expect("should_auto_promote=true implies bundler.is_some()");
            self.rome_evm_client
                .send_transaction_via_bundle(rlp, &bundler.client, &bundler.config)
                .await
                .map_err(|e| e.into())
        } else {
            self.rome_evm_client
                .send_transaction(rlp)
                .await
                .map_err(|e| e.into())
        };

        tracing::info!("eth_send_raw_transaction: {:?}", result);
        result
    }

    #[tracing::instrument(name = "proxy::net_version", skip(self))]
    async fn net_version(&self) -> Result<String> {
        let result = net_version_decimal(self.rome_evm_client.chain_id());
        tracing::info!("net_version: {result}");
        Ok(result)
    }

    #[tracing::instrument(name = "proxy::eth_get_transaction_receipt", skip(self), fields(tx_hash = ?tx_hash))]
    async fn eth_get_transaction_receipt(
        &self,
        tx_hash: H256,
    ) -> Result<Option<Value>> {
        let receipt = self.rome_evm_client
            .get_transaction_receipt(&tx_hash)
            .await
            .map_err(ApiError::RomeEvmError)?;
        Ok(receipt.map(|r| {
            let mut v = serde_json::to_value(r).unwrap_or(Value::Null);
            if let Value::Object(ref mut map) = v {
                map.remove("deposit_nonce");
                map.remove("deposit_receipt_version");
            }
            v
        }))
    }

    #[tracing::instrument(name = "proxy::eth_get_transaction_by_hash", skip(self), fields(tx_hash = ?tx_hash))]
    async fn eth_get_transaction_by_hash(&self, tx_hash: H256) -> Result<Option<Transaction>> {
        self.rome_evm_client
            .get_transaction(&tx_hash)
            .await
            .map_err(|err| err.into())
    }

    async fn rpc_modules(&self) -> Result<Value> {
        Ok(rpc_modules_map())
    }

    #[tracing::instrument(name = "proxy::eth_fee_history", skip(self), fields(block_number = ?block_number))]
    async fn eth_fee_history(
        &self,
        count: U64,
        block_number: BlockId,
        reward_percentiles: Vec<f64>,
    ) -> Result<FeeHistory> {
        let result = self
            .rome_evm_client
            .fee_history(count.as_u64(), block_number, reward_percentiles)
            .await
            .map_err(ApiError::from);

        tracing::info!("eth_fee_history({:?}): {:?}", block_number, result);
        result
    }

    #[tracing::instrument(name = "proxy::web3_client_version", skip(self))]
    async fn web3_client_version(&self) -> Result<String> {
        Ok(client_version_string())
    }

    async fn txpool_status(&self) -> Result<Value> {
        Ok(txpool_status_map())
    }

    #[tracing::instrument(name = "proxy::eth_get_storage_at", skip(self, slot, _block), fields(address = ?address))]
    async fn eth_get_storage_at(
        &self,
        address: Address,
        slot: U256,
        _block: String,
    ) -> Result<String> {
        let value = self
            .rome_evm_client
            .eth_get_storage_at(address, slot)
            .await
            .map_err(ApiError::from)?;
        let mut buf = [0_u8; 32];
        value.to_big_endian(&mut buf);
        let hex = format!("0x{}", hex::encode(buf));

        Ok(hex)
    }

    #[tracing::instrument(name = "proxy::eth_max_priority_fee_per_gas", skip(self))]
    async fn eth_max_priority_fee_per_gas(&self) -> Result<U256> {
        Ok(U256::zero())
    }

    #[tracing::instrument(name = "proxy::eth_syncing", skip(self))]
    async fn eth_syncing(&self) -> Result<Value> {
        Ok(json!(false))
    }

    #[tracing::instrument(name = "proxy::eth_accounts", skip(self))]
    async fn eth_accounts(&self) -> Result<Vec<Address>> {
        Ok(vec![])
    }

    #[tracing::instrument(name = "proxy::net_listening", skip(self))]
    async fn net_listening(&self) -> Result<bool> {
        Ok(true)
    }

    #[tracing::instrument(name = "proxy::net_peer_count", skip(self))]
    async fn net_peer_count(&self) -> Result<U64> {
        Ok(U64::zero())
    }

    #[tracing::instrument(name = "proxy::eth_get_uncle_count_by_block_number", skip(self))]
    async fn eth_get_uncle_count_by_block_number(&self, _block: BlockId) -> Result<U64> {
        Ok(U64::zero())
    }

    #[tracing::instrument(name = "proxy::eth_get_uncle_count_by_block_hash", skip(self))]
    async fn eth_get_uncle_count_by_block_hash(&self, _hash: H256) -> Result<U64> {
        Ok(U64::zero())
    }

    #[tracing::instrument(name = "proxy::eth_get_uncle_by_block_hash_and_index", skip(self))]
    async fn eth_get_uncle_by_block_hash_and_index(
        &self,
        _hash: H256,
        _index: U64,
    ) -> Result<Option<Value>> {
        Ok(None)
    }

    #[tracing::instrument(name = "proxy::eth_get_uncle_by_block_number_and_index", skip(self))]
    async fn eth_get_uncle_by_block_number_and_index(
        &self,
        _block: BlockId,
        _index: U64,
    ) -> Result<Option<Value>> {
        Ok(None)
    }

    #[tracing::instrument(name = "proxy::eth_get_logs", skip(self, filter))]
    async fn eth_get_logs(&self, filter: Value) -> Result<Vec<Log>> {
        // Resolve "latest" to the current block number
        let latest = self
            .rome_evm_client
            .block_number()
            .await
            .map_err(ApiError::RomeEvmError)?
            .as_u64();

        let resolve_block = |v: Option<&Value>| -> u64 {
            match v.and_then(|v| v.as_str()) {
                Some("latest") | Some("pending") | None => latest,
                Some("earliest") => 0,
                Some(s) if s.starts_with("0x") => {
                    u64::from_str_radix(s.trim_start_matches("0x"), 16).unwrap_or(latest)
                }
                _ => latest,
            }
        };

        let from_block = U64::from(resolve_block(filter.get("fromBlock")));
        let to_block = U64::from(resolve_block(filter.get("toBlock")));
        self.check_get_logs_range(from_block, to_block)?;

        // Parse address filter: single address or array
        let addresses: Vec<Address> = match filter.get("address") {
            None => vec![],
            Some(v) if v.is_null() => vec![],
            Some(v) => {
                if let Some(s) = v.as_str() {
                    Address::from_str(s).ok().into_iter().collect()
                } else if let Some(arr) = v.as_array() {
                    arr.iter()
                        .filter_map(|a| a.as_str().and_then(|s| Address::from_str(s).ok()))
                        .collect()
                } else {
                    vec![]
                }
            }
        };

        // Parse topics: array of up to 4 positions, each null | single hash | array of hashes
        let topics: Vec<Option<Vec<H256>>> = match filter.get("topics").and_then(|t| t.as_array()) {
            None => vec![],
            Some(arr) => arr
                .iter()
                .map(|pos| {
                    if pos.is_null() {
                        None
                    } else if let Some(s) = pos.as_str() {
                        H256::from_str(s).ok().map(|h| vec![h])
                    } else if let Some(inner) = pos.as_array() {
                        let hashes: Vec<H256> = inner
                            .iter()
                            .filter_map(|h| h.as_str().and_then(|s| H256::from_str(s).ok()))
                            .collect();
                        if hashes.is_empty() { None } else { Some(hashes) }
                    } else {
                        None
                    }
                })
                .collect(),
        };

        let logs = self
            .rome_evm_client
            .get_logs(from_block, to_block, &addresses, &topics)
            .await
            .map_err(ApiError::RomeEvmError)?;

        tracing::info!(
            "eth_get_logs(from={}, to={}) -> {} logs",
            from_block,
            to_block,
            logs.len()
        );
        Ok(logs)
    }

    // -------------------------------------------------------------------------
    // Step 4 — Block / tx index methods
    // -------------------------------------------------------------------------

    #[tracing::instrument(name = "proxy::eth_get_block_transaction_count_by_number", skip(self))]
    async fn eth_get_block_transaction_count_by_number(
        &self,
        block: BlockId,
    ) -> Result<Option<U64>> {
        let number = match self
            .rome_evm_client
            .resolve_block_number(block)
            .await
            .map_err(ApiError::RomeEvmError)?
        {
            Some(n) => n,
            None => return Ok(None),
        };
        self.rome_evm_client
            .get_block_tx_count_by_number(number)
            .await
            .map_err(ApiError::RomeEvmError)
    }

    #[tracing::instrument(name = "proxy::eth_get_block_transaction_count_by_hash", skip(self))]
    async fn eth_get_block_transaction_count_by_hash(
        &self,
        hash: H256,
    ) -> Result<Option<U64>> {
        self.rome_evm_client
            .get_block_tx_count_by_hash(&hash)
            .await
            .map_err(ApiError::RomeEvmError)
    }

    #[tracing::instrument(name = "proxy::eth_get_transaction_by_block_number_and_index", skip(self))]
    async fn eth_get_transaction_by_block_number_and_index(
        &self,
        block: BlockId,
        index: U64,
    ) -> Result<Option<Transaction>> {
        let number = match self
            .rome_evm_client
            .resolve_block_number(block)
            .await
            .map_err(ApiError::RomeEvmError)?
        {
            Some(n) => n,
            None => return Ok(None),
        };
        self.rome_evm_client
            .get_tx_by_block_number_and_index(number, index)
            .await
            .map_err(ApiError::RomeEvmError)
    }

    #[tracing::instrument(name = "proxy::eth_get_transaction_by_block_hash_and_index", skip(self))]
    async fn eth_get_transaction_by_block_hash_and_index(
        &self,
        hash: H256,
        index: U64,
    ) -> Result<Option<Transaction>> {
        self.rome_evm_client
            .get_tx_by_block_hash_and_index(&hash, index)
            .await
            .map_err(ApiError::RomeEvmError)
    }

    #[tracing::instrument(name = "proxy::eth_get_block_receipts", skip(self))]
    async fn eth_get_block_receipts(&self, block: BlockId) -> Result<Option<Vec<Value>>> {
        let number = match self
            .rome_evm_client
            .resolve_block_number(block)
            .await
            .map_err(ApiError::RomeEvmError)?
        {
            Some(n) => n,
            None => return Ok(None),
        };
        let receipts = self
            .rome_evm_client
            .get_block_receipts(number)
            .await
            .map_err(ApiError::RomeEvmError)?;

        Ok(receipts.map(|rs| {
            rs.into_iter()
                .map(|r| {
                    let mut v = serde_json::to_value(r).unwrap_or(Value::Null);
                    if let Value::Object(ref mut map) = v {
                        map.remove("deposit_nonce");
                        map.remove("deposit_receipt_version");
                    }
                    v
                })
                .collect()
        }))
    }

    // -------------------------------------------------------------------------
    // Step 5 — Filter / polling API
    // -------------------------------------------------------------------------

    #[tracing::instrument(name = "proxy::eth_new_filter", skip(self, filter))]
    async fn eth_new_filter(&self, filter: Value) -> Result<String> {
        let latest = self
            .rome_evm_client
            .block_number()
            .await
            .map_err(ApiError::RomeEvmError)?
            .as_u64();

        let resolve_block = |v: Option<&Value>| -> u64 {
            match v.and_then(|v| v.as_str()) {
                Some("latest") | Some("pending") | None => latest,
                Some("earliest") => 0,
                Some(s) if s.starts_with("0x") => {
                    u64::from_str_radix(s.trim_start_matches("0x"), 16).unwrap_or(latest)
                }
                _ => latest,
            }
        };

        let from_block = resolve_block(filter.get("fromBlock"));
        let to_block = resolve_block(filter.get("toBlock"));
        let last_block = from_block.saturating_sub(1).max(latest.saturating_sub(1));
        let _ = to_block; // stored in filter for eth_getFilterLogs

        let addresses: Vec<Address> = match filter.get("address") {
            None | Some(Value::Null) => vec![],
            Some(v) => {
                if let Some(s) = v.as_str() {
                    Address::from_str(s).ok().into_iter().collect()
                } else if let Some(arr) = v.as_array() {
                    arr.iter()
                        .filter_map(|a| a.as_str().and_then(|s| Address::from_str(s).ok()))
                        .collect()
                } else {
                    vec![]
                }
            }
        };

        let topics: Vec<Option<Vec<H256>>> =
            match filter.get("topics").and_then(|t| t.as_array()) {
                None => vec![],
                Some(arr) => arr
                    .iter()
                    .map(|pos| {
                        if pos.is_null() {
                            None
                        } else if let Some(s) = pos.as_str() {
                            H256::from_str(s).ok().map(|h| vec![h])
                        } else if let Some(inner) = pos.as_array() {
                            let hashes: Vec<H256> = inner
                                .iter()
                                .filter_map(|h| h.as_str().and_then(|s| H256::from_str(s).ok()))
                                .collect();
                            if hashes.is_empty() {
                                None
                            } else {
                                Some(hashes)
                            }
                        } else {
                            None
                        }
                    })
                    .collect(),
            };

        self.alloc_filter(FilterState::Logs {
            from_block,
            last_block,
            addresses,
            topics,
        })
        .await
    }

    #[tracing::instrument(name = "proxy::eth_new_block_filter", skip(self))]
    async fn eth_new_block_filter(&self) -> Result<String> {
        let latest = self
            .rome_evm_client
            .block_number()
            .await
            .map_err(ApiError::RomeEvmError)?
            .as_u64();
        self.alloc_filter(FilterState::NewBlocks {
            last_block: latest,
        })
        .await
    }

    #[tracing::instrument(name = "proxy::eth_new_pending_transaction_filter", skip(self))]
    async fn eth_new_pending_transaction_filter(&self) -> Result<String> {
        self.alloc_filter(FilterState::PendingTransactions).await
    }

    #[tracing::instrument(name = "proxy::eth_get_filter_changes", skip(self))]
    async fn eth_get_filter_changes(&self, id: String) -> Result<Value> {
        let key = Self::parse_filter_id(&id)
            .ok_or_else(|| ApiError::custom(format!("invalid filter id: {id}")))?;

        let current_block = self
            .rome_evm_client
            .block_number()
            .await
            .map_err(ApiError::RomeEvmError)?
            .as_u64();

        let mut filters = self.filters.write().await;
        let f = filters
            .get_mut(&key)
            .ok_or_else(|| ApiError::custom(format!("filter not found: {id}")))?;
        // Polling keeps the filter alive against the idle-TTL eviction.
        f.last_touched = std::time::Instant::now();

        match &mut f.state {
            FilterState::PendingTransactions => Ok(json!([])),

            FilterState::NewBlocks { last_block } => {
                if current_block <= *last_block {
                    *last_block = current_block;
                    return Ok(json!([]));
                }
                let from = *last_block + 1;
                *last_block = current_block;
                drop(filters);

                // Collect block hashes for each new block
                let mut hashes = Vec::new();
                for n in from..=current_block {
                    let block_id = BlockId::Number(BlockNumber::Number(U64::from(n)));
                    if let Ok(Some(block)) =
                        self.rome_evm_client.get_block(block_id, false).await
                    {
                        let hash = match &block {
                            rome_sdk::rome_evm_client::indexer::BlockType::BlockWithHashes(b) => {
                                b.hash
                            }
                            rome_sdk::rome_evm_client::indexer::BlockType::BlockWithTransactions(
                                b,
                            ) => b.hash,
                        };
                        if let Some(h) = hash {
                            hashes.push(format!("0x{:x}", h));
                        }
                    }
                }
                Ok(json!(hashes))
            }

            FilterState::Logs {
                last_block,
                addresses,
                topics,
                ..
            } => {
                if current_block <= *last_block {
                    *last_block = current_block;
                    return Ok(json!([]));
                }
                let from = U64::from(*last_block + 1);
                let to = U64::from(current_block);
                let addresses = addresses.clone();
                let topics = topics.clone();
                *last_block = current_block;
                drop(filters);

                let logs = self
                    .rome_evm_client
                    .get_logs(from, to, &addresses, &topics)
                    .await
                    .map_err(ApiError::RomeEvmError)?;
                Ok(json!(logs))
            }
        }
    }

    #[tracing::instrument(name = "proxy::eth_get_filter_logs", skip(self))]
    async fn eth_get_filter_logs(&self, id: String) -> Result<Vec<Log>> {
        let key = Self::parse_filter_id(&id)
            .ok_or_else(|| ApiError::custom(format!("invalid filter id: {id}")))?;

        let current_block = self
            .rome_evm_client
            .block_number()
            .await
            .map_err(ApiError::RomeEvmError)?
            .as_u64();

        let (from_block, addresses, topics) = {
            let filters = self.filters.read().await;
            match filters.get(&key) {
                None => {
                    return Err(ApiError::custom(format!("filter not found: {id}")));
                }
                Some(Filter {
                    state:
                        FilterState::Logs {
                            from_block,
                            addresses,
                            topics,
                            ..
                        },
                    ..
                }) => (U64::from(*from_block), addresses.clone(), topics.clone()),
                Some(_) => {
                    return Err(ApiError::custom(
                        "eth_getFilterLogs only valid for log filters",
                    ));
                }
            }
        };

        self.check_get_logs_range(from_block, U64::from(current_block))?;
        self.rome_evm_client
            .get_logs(from_block, U64::from(current_block), &addresses, &topics)
            .await
            .map_err(ApiError::RomeEvmError)
    }

    #[tracing::instrument(name = "proxy::eth_uninstall_filter", skip(self))]
    async fn eth_uninstall_filter(&self, id: String) -> Result<bool> {
        let key = Self::parse_filter_id(&id)
            .ok_or_else(|| ApiError::custom(format!("invalid filter id: {id}")))?;
        let removed = self.filters.write().await.remove(&key).is_some();
        Ok(removed)
    }

    #[tracing::instrument(name = "proxy::eth_subscribe", skip(self, pending), fields(kind = %kind))]
    async fn eth_subscribe(
        &self,
        pending: PendingSubscriptionSink,
        kind: String,
        filter: Option<Value>,
    ) -> jsonrpsee::core::SubscriptionResult {
        // Reject unknown kinds before accepting, so the client gets a clean error.
        match kind.as_str() {
            "newHeads" | "logs" | "newPendingTransactions" => {}
            other => {
                pending
                    .reject(ErrorObjectOwned::owned(
                        jsonrpsee::types::error::INVALID_PARAMS_CODE,
                        format!("unsupported subscription kind: {other}"),
                        None::<String>,
                    ))
                    .await;
                return Ok(());
            }
        }

        let sink = pending.accept().await?;

        // Rome has no public mempool — accept the subscription (so clients don't
        // error) but never emit, matching Neon. Stay open until the client leaves.
        if kind == "newPendingTransactions" {
            sink.closed().await;
            return Ok(());
        }

        let (addresses, topics) = match kind.as_str() {
            "logs" => parse_log_filter(&filter.unwrap_or(Value::Null)),
            _ => (vec![], vec![]),
        };

        // Seed at the current head so we only push blocks/logs that arrive AFTER
        // the subscription is established (standard eth_subscribe semantics).
        let mut last_block = self
            .rome_evm_client
            .block_number()
            .await
            .map(|b| b.as_u64())
            .unwrap_or(0);

        loop {
            // Exit promptly when the client unsubscribes / disconnects.
            tokio::select! {
                _ = sink.closed() => return Ok(()),
                _ = tokio::time::sleep(SUBSCRIPTION_POLL) => {}
            }

            let current = match self.rome_evm_client.block_number().await {
                Ok(b) => b.as_u64(),
                Err(_) => continue, // transient indexer hiccup; retry next tick
            };
            if current <= last_block {
                continue;
            }
            let from = last_block + 1;
            last_block = current;

            if kind == "newHeads" {
                for n in from..=current {
                    let block_id = BlockId::Number(BlockNumber::Number(U64::from(n)));
                    if let Ok(Some(block)) = self.rome_evm_client.get_block(block_id, false).await {
                        let v = normalize_block(serde_json::to_value(&block).unwrap_or(Value::Null));
                        let msg = match SubscriptionMessage::from_json(&v) {
                            Ok(m) => m,
                            Err(_) => continue,
                        };
                        if sink.send(msg).await.is_err() {
                            return Ok(()); // client gone
                        }
                    }
                }
            } else {
                // logs
                match self
                    .rome_evm_client
                    .get_logs(U64::from(from), U64::from(current), &addresses, &topics)
                    .await
                {
                    Ok(logs) => {
                        for log in logs {
                            let msg = match SubscriptionMessage::from_json(&log) {
                                Ok(m) => m,
                                Err(_) => continue,
                            };
                            if sink.send(msg).await.is_err() {
                                return Ok(());
                            }
                        }
                    }
                    Err(_) => continue,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    //! Proxy-local `eth_*` helper tests. The `maxPriorityFeePerGas` RLP decoder
    //! (`decode_priority_fee_wei`) was lifted into the SDK
    //! (`rome_sdk::rome_evm_client::tx::priority`) so both the submit-path tx
    //! structs and this proxy's Jito-promotion predicate share one decoder; its
    //! contract tests live alongside it there.
    use super::*;

    /// Tools sniff `web3_clientVersion` for client-specific behavior — the
    /// placeholder string identified nothing. Geth-style: Name/vVersion/lang.
    #[test]
    fn client_version_identifies_rome() {
        let v = client_version_string();
        assert!(v.starts_with("Rome-Proxy/v"), "got {v}");
        assert!(!v.contains("proxy-version"));
    }

    /// Single-state Rome has no mempool; an honest empty txpool_status unblocks
    /// tools that only require the method to exist.
    #[test]
    fn txpool_status_is_honestly_empty() {
        let t = txpool_status_map();
        assert_eq!(t["pending"], "0x0");
        assert_eq!(t["queued"], "0x0");
    }

    /// `rpc_modules` (geth node-introspection convention, served by every major
    /// client) must advertise exactly the namespaces this proxy dispatches.
    #[test]
    fn rpc_modules_lists_served_namespaces() {
        let m = rpc_modules_map();
        for ns in ["debug", "eth", "net", "rome", "rpc", "txpool", "web3"] {
            assert_eq!(m[ns], "1.0", "namespace {ns} missing");
        }
        assert_eq!(m.as_object().unwrap().len(), 7, "unadvertised or stale namespace");
    }

    #[test]
    fn overlay_base_fee_sets_pool_price_and_is_graceful() {
        // Sets baseFeePerGas to the live pool price (overwriting the indexer's 0x0).
        let v = json!({"number": "0x1", "baseFeePerGas": "0x0"});
        let out = overlay_base_fee(v, Some(U256::from(1_000_000_000u64)));
        assert_eq!(out["baseFeePerGas"], json!("0x3b9aca00")); // 1e9
        // None (price read failed) leaves the block untouched — no error, no change.
        let v2 = json!({"number": "0x1", "baseFeePerGas": "0x0"});
        assert_eq!(overlay_base_fee(v2.clone(), None), v2);
        // Null block (missing) is passed through unharmed.
        assert_eq!(overlay_base_fee(Value::Null, Some(U256::from(5))), Value::Null);
    }

    /// `net_version` must be a DECIMAL string (JSON-RPC convention). ethers
    /// `U64` serializes as hex ("0x30d4a"), which web3.js / legacy MetaMask
    /// network-detection mis-parse. Pin decimal output + guard against a hex
    /// regression.
    #[test]
    fn net_version_is_decimal_not_hex() {
        assert_eq!(net_version_decimal(200010), "200010");
        assert!(!net_version_decimal(200010).starts_with("0x"));
        assert_eq!(net_version_decimal(1), "1");
    }

    /// `pending` must alias to `latest` (Rome has no pending block); other
    /// tags (latest, numeric, hash) pass through unchanged.
    #[test]
    fn pending_block_tag_is_aliased_to_latest() {
        let latest = BlockId::Number(BlockNumber::Latest);
        assert_eq!(normalize_block_id(BlockId::Number(BlockNumber::Pending)), latest);
        assert_eq!(normalize_block_id(latest), latest);
        let numeric = BlockId::Number(BlockNumber::Number(U64::from(123u64)));
        assert_eq!(normalize_block_id(numeric), numeric);
    }
}
