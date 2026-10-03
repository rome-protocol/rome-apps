use {
    super::{CallRequest, RomeServer},
    crate::{
        error::{ApiError, Result},
        proxy::Proxy,
    },
    async_trait::async_trait,
    ed25519_dalek::ed25519::Signature as EdSignature,
    ethers::types::{Address, Bytes, TxHash, H256},
    rome_sdk::rome_evm_client::{
        emulator,
        error::RomeEvmError,
        rome_evm::{state::pda::Pda, H160 as RomeH160},
        tx::TxBuilder,
        util::RomeEvmUtil,
        BuildInfo, RomeEVMClient,
    },
    serde::{Deserialize, Deserializer, Serialize, Serializer},
    solana_sdk::{
        ed25519_program, instruction::AccountMeta, message::VersionedMessage, pubkey::Pubkey,
        signature::Signature as SolSignature, transaction::VersionedTransaction,
    },
    spl_associated_token_account_interface::address::get_associated_token_address_with_program_id,
    std::str::FromStr,
};

/// SPL Token program ID (spl-token v3 / legacy).
const TOKEN_PROGRAM_ID: Pubkey =
    Pubkey::from_str_const("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
const COMPUTE_BUDGET_PROGRAM_ID: Pubkey =
    Pubkey::from_str_const("ComputeBudget111111111111111111111111111111");

/// `transfer_spl(bytes32 to_ata, uint64 tokens, bytes32 mint)` selector.
/// Verified: keccak256("transfer_spl(bytes32,uint64,bytes32)")[..4] = 0xb6977879
const TRANSFER_SPL_SEL: [u8; 4] = [0xb6, 0x97, 0x78, 0x79];

/// HelperProgram precompile address (20 raw bytes).
/// `0xff00000000000000000000000000000000000009`
const HELPER_PROGRAM_BYTES: [u8; 20] = [
    0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x09,
];

/// A wallet plan is composed from compute-budget, Ed25519 verification, and
/// Rome instructions only. Keeping this bound at the proxy prevents the
/// broadcast endpoint becoming a general-purpose free transaction relay.
const MAX_SIGNED_V1_PLAN_LEGS: usize = 64;

fn decode_wallet_signed_v1(
    encoded: &str,
    expected_payer: &Pubkey,
    rome_program: &Pubkey,
) -> Result<VersionedTransaction> {
    use base64::{engine::general_purpose::STANDARD, Engine as _};

    let bytes = STANDARD
        .decode(encoded)
        .map_err(|e| ApiError::custom(format!("invalid signed V1 base64: {e}")))?;
    let tx: VersionedTransaction = wincode::deserialize(&bytes)
        .map_err(|e| ApiError::custom(format!("invalid signed V1 wire transaction: {e}")))?;
    let VersionedMessage::V1(message) = &tx.message else {
        return Err(ApiError::custom("signed plan transaction must use V1"));
    };
    if message.account_keys.first() != Some(expected_payer) {
        return Err(ApiError::custom(
            "signed plan fee payer does not match solana_pubkey",
        ));
    }
    if message.header.num_required_signatures != 1 || tx.signatures.len() != 1 {
        return Err(ApiError::custom(
            "signed plan must require exactly the wallet signature",
        ));
    }
    tx.verify_and_hash_message()
        .map_err(|e| ApiError::custom(format!("invalid wallet signature: {e}")))?;

    let has_rome = message
        .instructions
        .iter()
        .any(|ix| message.account_keys.get(ix.program_id_index as usize) == Some(rome_program));
    if !has_rome {
        return Err(ApiError::custom(
            "signed plan does not contain a Rome instruction",
        ));
    }
    for ix in &message.instructions {
        let Some(program) = message.account_keys.get(ix.program_id_index as usize) else {
            return Err(ApiError::custom(
                "signed plan has invalid instruction program index",
            ));
        };
        if program != rome_program
            && program != &COMPUTE_BUDGET_PROGRAM_ID
            && program != &ed25519_program::id()
        {
            return Err(ApiError::custom(
                "signed plan contains a non-Rome instruction",
            ));
        }
    }
    Ok(tx)
}

/// Decode a `transfer_spl(bytes32 to_ata, uint64 tokens, bytes32 mint)` calldata blob
/// and derive the three Solana accounts the proxy must append when the dest ATA is missing:
///   - `synth_pda`  = EXTERNAL_AUTHORITY PDA of the synthetic `from` address
///   - `synth_ata`  = ATA of (owner=synth_pda, mint) under the TOKEN program
///   - `dst`        = the explicit `to_ata` from arg0 (raw 32-byte Solana pubkey)
///
/// Returns `None` if calldata is too short or the selector doesn't match.
fn decode_transfer_spl_accounts(
    program_id: &Pubkey,
    from: &Address,
    data: &[u8],
) -> Option<(Pubkey, Pubkey, Pubkey)> {
    // Calldata layout:
    //   [0..4]   selector (4 bytes)
    //   [4..36]  to_ata   (bytes32, arg0 — raw Solana pubkey)
    //   [36..68] tokens   (uint256 ABI-encoded uint64, arg1 — ignored)
    //   [68..100] mint    (bytes32, arg2 — raw Solana pubkey)
    if data.len() < 100 {
        return None;
    }
    if &data[..4] != TRANSFER_SPL_SEL {
        return None;
    }

    let dst = Pubkey::from(<[u8; 32]>::try_from(&data[4..36]).ok()?);
    let mint = Pubkey::from(<[u8; 32]>::try_from(&data[68..100]).ok()?);

    let from_h160 = RomeH160::from_slice(from.as_bytes());
    let pda = Pda::new_(
        program_id, 0, /* chain_id unused for external_auth seed */
    );
    // external_auth seed = [EXTERNAL_AUTHORITY, from_h160.as_bytes()]
    let (synth_pda, _) = pda.external_auth(&from_h160);

    let synth_ata =
        get_associated_token_address_with_program_id(&synth_pda, &mint, &TOKEN_PROGRAM_ID);

    Some((synth_pda, synth_ata, dst))
}

pub const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const CARGO_CFG_FEATURE: &str = env!("CARGO_CFG_FEATURE");
pub const GIT_HASH: &str = env!("GIT_HASH");
pub const RUSTC_VERSION: &str = env!("RUSTC_VERSION");
pub const COMPILE_DATETIME: &str = env!("COMPILE_DATETIME");

#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub struct B58Pubkey(pub Pubkey);

impl<'de> Deserialize<'de> for B58Pubkey {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // JSON value must be a string
        let s = String::deserialize(deserializer)?;
        Pubkey::from_str(&s)
            .map(Self)
            .map_err(serde::de::Error::custom)
    }
}

impl Serialize for B58Pubkey {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0.to_string())
    }
}

/// Base58-encoded Solana ed25519 signature (64 bytes). Mirrors [`B58Pubkey`].
/// JSON wire shape matches what `@solana/web3.js` and the Solana CLI emit by
/// default for `Signature` values.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub struct B58Signature(pub EdSignature);

impl<'de> Deserialize<'de> for B58Signature {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let sol = SolSignature::from_str(&s).map_err(serde::de::Error::custom)?;
        Ok(Self(EdSignature::from_bytes(&sol.into())))
    }
}

impl Serialize for B58Signature {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&SolSignature::from(self.0.to_bytes()).to_string())
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountMetaB58 {
    pub pubkey: B58Pubkey,
    pub is_signer: bool,
    pub is_writable: bool,
}

/// V1 wire templates for a Solana-native iterative EVM execution. Each
/// transaction is base64-encoded and has an empty wallet signature slot.
#[derive(Debug, Clone, Serialize)]
pub struct UnsignedV1Plan {
    #[serde(rename = "recentBlockhash")]
    pub recent_blockhash: String,
    pub transactions: Vec<String>,
}

/// A complete atomic generic-`DoTx` V1 wire message, prefixed with the
/// Ed25519 verifier that establishes the synthetic EVM sender.
#[derive(Debug, Clone, Serialize)]
pub struct AtomicUnsignedV1Plan {
    #[serde(rename = "recentBlockhash")]
    pub recent_blockhash: String,
    pub transaction: String,
}

/// A wallet-signable V1 Deposit transaction. The `amount` is wei credited to
/// the caller's synthetic EVM balance while its SOL equivalent leaves the
/// Solana wallet when the transaction is signed and broadcast.
#[derive(Debug, Clone, Serialize)]
pub struct DepositV1Plan {
    #[serde(rename = "recentBlockhash")]
    pub recent_blockhash: String,
    pub transaction: String,
}

impl From<AccountMeta> for AccountMetaB58 {
    fn from(meta: AccountMeta) -> Self {
        Self {
            pubkey: B58Pubkey(meta.pubkey),
            is_signer: meta.is_signer,
            is_writable: meta.is_writable,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionInfo {
    program: BuildInfo,
    emulator: String,
    rome_evm_client: String,
    proxy: BuildInfo,
}

fn serialize_pubkey<S>(pubkey: &Pubkey, serializer: S) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&pubkey.to_string())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Resource {
    #[serde(serialize_with = "serialize_pubkey")]
    pub payer: Pubkey,
    pub fee_recipients: Vec<Address>,
}

/// Derive the two deterministic PDAs that `eth_estimate_gas` does not surface
/// in `Emulation.accounts` but that a real `DoTxUnsigned` for a synthetic
/// sender always touches: the chain's **treasure wallet** (index 0) and the
/// caller's **balance_key** (EVM-address → balance PDA). Returns
/// `(treasure_wallet_0, balance_key)`.
fn derive_treasure_and_balance(
    program_id: &Pubkey,
    chain_id: u64,
    from: &Address,
) -> (Pubkey, Pubkey) {
    let from_h160 = RomeH160::from_slice(from.as_bytes());
    let pda = Pda::new_(program_id, chain_id);
    let treasure = pda.treasure_wallet(0).0;
    let balance_key = pda.balance_key(&from_h160).0;
    (treasure, balance_key)
}

#[async_trait]
impl RomeServer for Proxy {
    #[tracing::instrument(name = "proxy::emulate_with_payer", skip(self))]
    async fn emulate_with_payer(&self, rlp: Bytes, pkey: B58Pubkey) -> Result<Vec<AccountMetaB58>> {
        let mut data = vec![emulator::Instruction::DoTx as u8, 0];
        data.extend_from_slice(&rlp);

        let emulation = RomeEVMClient::emulate(
            self.rome_evm_client.program_id(),
            &data,
            &pkey.0,
            self.rome_evm_client.rpc_client(),
            None,
        )?;
        let vec = emulation
            .accounts
            .iter()
            .map(|(pubkey, acc)| {
                let meta = AccountMeta {
                    pubkey: *pubkey,
                    is_signer: acc.account.signer,
                    is_writable: acc.account.writable,
                };
                AccountMetaB58::from(meta)
            })
            .collect::<Vec<_>>();

        tracing::info!("rome_emulate_tx: {:?}", vec);

        Ok(vec)
    }

    #[tracing::instrument(name = "proxy::emulate_call_accounts", skip(self))]
    async fn emulate_call_accounts(
        &self,
        call: CallRequest,
        payer: B58Pubkey,
    ) -> Result<Vec<AccountMetaB58>> {
        // Explicit-`from` emulation (no signature) — `call.from` is the
        // synthetic EVM address. Reuses the eth_estimateGas emulation path,
        // which resolves the account set the call touches, and returns those
        // accounts instead of the gas number. Offloaded to the read pool (when
        // enabled) so this heavy emulation can't starve the write/confirm path.
        let program_id = *self.rome_evm_client.program_id();
        let chain_id = self.rome_evm_client.chain_id();
        let storage = self.rome_evm_client.account_storage();
        let payer_key = payer.0;

        let vec = self
            .offload(
                move || -> std::result::Result<Vec<AccountMetaB58>, RomeEvmError> {
                    let emulation = emulator::emulate_call(
                        &program_id,
                        RomeEvmUtil::cast_transaction_request(&call, chain_id),
                        storage,
                        &payer_key,
                    )
                    .map_err(RomeEvmError::from)?;

                    let mut vec = emulation
                        .accounts
                        .iter()
                        .map(|(pubkey, acc)| {
                            AccountMetaB58::from(AccountMeta {
                                pubkey: *pubkey,
                                is_signer: acc.account.signer,
                                is_writable: acc.account.writable,
                            })
                        })
                        .collect::<Vec<_>>();

                    // Append the two deterministic PDAs the emulation omits but a real
                    // DoTxUnsigned for this synthetic sender always touches. Deduped.
                    let from = call.from.unwrap_or_default();
                    let (treasure, balance_key) =
                        derive_treasure_and_balance(&program_id, chain_id, &from);
                    for pubkey in [treasure, balance_key] {
                        if !vec.iter().any(|m| m.pubkey.0 == pubkey) {
                            vec.push(AccountMetaB58 {
                                pubkey: B58Pubkey(pubkey),
                                is_signer: false,
                                is_writable: true,
                            });
                        }
                    }

                    // When the call targets HelperProgram with the transfer_spl(bytes32,uint64,bytes32)
                    // selector, the emulator truncates on a missing dest ATA — dropping the source ATA,
                    // the external-auth PDA, AND the SPL Token program (the transfer CPI target the
                    // emulation never reaches when the dest read aborts early).  Append all of them
                    // unconditionally (deduped) so the client never needs to pre-create the dest ATA in
                    // a separate tx or hand-append accounts: the returned set lands a dst-missing
                    // transfer_spl in one bundle (with the idempotent ATA-create folded in by the client).
                    let is_helper = call
                        .to
                        .as_ref()
                        .map(|a| match a {
                            ethers::types::NameOrAddress::Address(addr) => {
                                addr.as_bytes() == HELPER_PROGRAM_BYTES
                            }
                            ethers::types::NameOrAddress::Name(_) => false,
                        })
                        .unwrap_or(false);
                    if is_helper {
                        let data = call.data.as_deref().unwrap_or(&[]);
                        if let Some((synth_pda, synth_ata, dst)) =
                            decode_transfer_spl_accounts(&program_id, &from, data)
                        {
                            // The two ATAs + the authority PDA carry/spend tokens → writable.
                            for pubkey in [synth_pda, synth_ata, dst] {
                                if !vec.iter().any(|m| m.pubkey.0 == pubkey) {
                                    vec.push(AccountMetaB58 {
                                        pubkey: B58Pubkey(pubkey),
                                        is_signer: false,
                                        is_writable: true,
                                    });
                                }
                            }
                            // The SPL Token program is the transfer CPI target — a read-only,
                            // executable program account (NEVER writable; Solana rejects a writable
                            // executable). The emulator surfaces it only when the dest ATA already
                            // exists; on a missing dest it aborts before the CPI, so restore it here —
                            // else the on-chain DoTx reverts "account not found: Token..".
                            if !vec.iter().any(|m| m.pubkey.0 == TOKEN_PROGRAM_ID) {
                                vec.push(AccountMetaB58 {
                                    pubkey: B58Pubkey(TOKEN_PROGRAM_ID),
                                    is_signer: false,
                                    is_writable: false,
                                });
                            }
                        }
                    }

                    Ok(vec)
                },
            )
            .await?;

        tracing::info!("rome_emulate_call_accounts: {:?}", vec);

        Ok(vec)
    }

    #[tracing::instrument(name = "proxy::emulate_tx", skip(self))]
    async fn emulate_tx(&self, rlp: Bytes) -> Result<()> {
        self.rome_evm_client
            .try_build_tx(rlp)
            .await
            .inspect_err(|err| tracing::warn!("emulate_tx error: {:?}", err))?;

        Ok(())
    }

    #[tracing::instrument(name = "proxy::mint_id", skip(self))]
    async fn mint_id(&self) -> Result<Option<Pubkey>> {
        let info = RomeEVMClient::get_rollup_info(
            self.rome_evm_client.program_id(),
            self.rome_evm_client.rpc_client(),
            self.rome_evm_client.chain_id(),
        )?;

        Ok(info.mint)
    }

    #[tracing::instrument(name = "proxy::emulate_reg_rollup", skip(self))]
    async fn emulate_reg_rollup(
        &self,
        chain_id: u64,
        registry_authority_key: Pubkey,
        mint: Option<Pubkey>,
        single_state: bool,
    ) -> Result<Vec<AccountMetaB58>> {
        // TODO: get solana tx from sdk and use its account list
        let mut data = vec![emulator::Instruction::RegOwner as u8];
        data.extend(chain_id.to_le_bytes());
        data.push(single_state as u8);
        if let Some(key) = mint {
            data.extend(key.as_ref())
        }

        let emulation = RomeEVMClient::emulate(
            self.rome_evm_client.program_id(),
            &data,
            &registry_authority_key,
            self.rome_evm_client.rpc_client(),
            None,
        )?;

        let ix = TxBuilder::build_ix(self.rome_evm_client.program_id(), &emulation, data);
        let accs = ix
            .accounts
            .into_iter()
            .map(AccountMetaB58::from)
            .collect::<Vec<_>>();

        Ok(accs)
    }

    #[tracing::instrument(name = "proxy::build_info", skip(self))]
    async fn build_info(&self) -> Result<VersionInfo> {
        let contract = match RomeEVMClient::program_build_info(
            self.rome_evm_client.program_id(),
            self.rome_evm_client.rpc_client(),
        )
        .await
        {
            Ok(info) => info,
            Err(e) => {
                tracing::warn!("error to fetch contract build-info: {e}");
                return Err(e.into());
            }
        };

        let proxy = BuildInfo {
            version: CARGO_PKG_VERSION.to_string(),
            compile_datetime: COMPILE_DATETIME.to_string(),
            git_hash: GIT_HASH.to_string(),
            cargo_cfg_feature: CARGO_CFG_FEATURE.to_string(),
            rustc_version: RUSTC_VERSION.to_string(),
        };

        let info = VersionInfo {
            program: contract,
            emulator: emulator::CARGO_PKG_VERSION.to_string(),
            rome_evm_client: rome_sdk::rome_evm_client::CARGO_PKG_VERSION.to_string(),
            proxy,
        };

        Ok(info)
    }

    #[tracing::instrument(name = "proxy::is_compatible", skip(self))]
    async fn is_compatible(&self) -> Result<bool> {
        RomeEVMClient::is_compatible(
            self.rome_evm_client.program_id(),
            self.rome_evm_client.rpc_client(),
        )
        .await
        .map_err(|e| e.into())
    }

    #[tracing::instrument(name = "proxy::get_resources", skip(self))]
    async fn get_resources(&self) -> Result<Vec<Resource>> {
        let resources = self
            .rome_evm_client
            .get_resources()
            .map_err(ApiError::from)?;
        Ok(resources
            .into_iter()
            .map(|(pubkey, fee_recipients)| Resource {
                payer: pubkey,
                fee_recipients,
            })
            .collect())
    }

    #[tracing::instrument(name = "proxy::solana_tx_for_evm_tx", skip(self), fields(tx_hash = ?tx_hash))]
    async fn solana_tx_for_evm_tx(&self, tx_hash: H256) -> Result<Vec<String>> {
        self.rome_evm_client
            .get_sol_sigs_for_evm_tx(&tx_hash)
            .await
            .map_err(ApiError::RomeEvmError)
    }

    #[tracing::instrument(
        name = "proxy::debug_trace_rome_transaction",
        skip(self),
        fields(tx_hash = ?tx_hash)
    )]
    async fn debug_trace_rome_transaction(
        &self,
        tx_hash: H256,
    ) -> Result<Option<serde_json::Value>> {
        use rome_sdk::rome_evm_client::indexer::parsers::parse_batch_trace;

        // Resolve EVM tx hash → Solana tx signature(s). For a DoTxBatch the
        // execution is atomic (one Solana tx); take the first sig.
        let sigs = self
            .rome_evm_client
            .get_sol_sigs_for_evm_tx(&tx_hash)
            .await
            .map_err(ApiError::RomeEvmError)?;

        let Some(sig_str) = sigs.into_iter().next() else {
            return Ok(None);
        };

        // Fetch Solana tx logs via raw JSON-RPC. Avoids pulling
        // solana-transaction-status / commitment_config into proxy's direct
        // dep set — those crates aren't workspace deps here and the
        // structured Solana types serde-deserialize fine into a slim
        // local struct that only reads meta.logMessages.
        let logs = self.fetch_solana_logs(&sig_str).await?;

        // Parse → BatchTrace (or None if logs don't contain the DoTxBatch
        // start marker — e.g., the tx_hash points to a non-batch tx).
        Ok(parse_batch_trace(&logs)
            .map(|trace| serde_json::to_value(trace).unwrap_or(serde_json::Value::Null)))
    }

    #[tracing::instrument(name = "proxy::rome_send_unsigned_transaction", skip(self, rlp))]
    async fn rome_send_unsigned_transaction(
        &self,
        rlp: Bytes,
        solana_pubkey: B58Pubkey,
        solana_signature: B58Signature,
    ) -> Result<TxHash> {
        let result = self
            .rome_evm_client
            .send_transaction_ed25519(rlp, Some((solana_pubkey.0, solana_signature.0)))
            .await
            .map_err(ApiError::from);
        tracing::info!("rome_send_unsigned_transaction: {:?}", result);
        result
    }

    #[tracing::instrument(name = "proxy::compose_unsigned_iterative_v1", skip(self, rlp))]
    async fn compose_unsigned_iterative_v1(
        &self,
        rlp: Bytes,
        solana_pubkey: B58Pubkey,
        solana_signature: B58Signature,
    ) -> Result<UnsignedV1Plan> {
        use base64::{engine::general_purpose::STANDARD, Engine as _};

        let recent_blockhash = self
            .rome_evm_client
            .rpc_client()
            .get_latest_blockhash()
            .await
            .map_err(|error| {
                ApiError::RomeEvmError(RomeEvmError::Custom(format!(
                    "get_latest_blockhash for external V1 composition: {error}"
                )))
            })?;
        let (_, transactions) = self
            .rome_evm_client
            .compose_external_v1(
                rlp,
                solana_pubkey.0,
                solana_signature.0,
                recent_blockhash,
            )
            .await
            .map_err(ApiError::from)?;
        let transactions = transactions
            .iter()
            .map(|tx| wincode::serialize(tx).map(|bytes| STANDARD.encode(bytes)))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| {
                ApiError::RomeEvmError(RomeEvmError::Custom(format!(
                    "encode external V1 transaction: {error}"
                )))
            })?;

        Ok(UnsignedV1Plan {
            recent_blockhash: recent_blockhash.to_string(),
            transactions,
        })
    }

    #[tracing::instrument(name = "proxy::compose_unsigned_atomic_v1", skip(self, rlp))]
    async fn compose_unsigned_atomic_v1(
        &self,
        rlp: Bytes,
        solana_pubkey: B58Pubkey,
        solana_signature: B58Signature,
    ) -> Result<AtomicUnsignedV1Plan> {
        use base64::{engine::general_purpose::STANDARD, Engine as _};

        let recent_blockhash = self
            .rome_evm_client
            .rpc_client()
            .get_latest_blockhash()
            .await
            .map_err(|error| {
                ApiError::RomeEvmError(RomeEvmError::Custom(format!(
                    "get_latest_blockhash for external V1 composition: {error}"
                )))
            })?;
        let transaction = self
            .rome_evm_client
            .compose_external_atomic_v1(rlp, solana_pubkey.0, solana_signature.0, recent_blockhash)
            .map_err(ApiError::from)?;
        let transaction = wincode::serialize(&transaction)
            .map(|bytes| STANDARD.encode(bytes))
            .map_err(|error| {
                ApiError::RomeEvmError(RomeEvmError::Custom(format!(
                    "encode external atomic V1 transaction: {error}"
                )))
            })?;
        Ok(AtomicUnsignedV1Plan {
            recent_blockhash: recent_blockhash.to_string(),
            transaction,
        })
    }

    #[tracing::instrument(name = "proxy::compose_external_deposit_v1", skip(self))]
    async fn compose_external_deposit_v1(
        &self,
        solana_pubkey: B58Pubkey,
        amount: ethers::types::U256,
    ) -> Result<DepositV1Plan> {
        use base64::{engine::general_purpose::STANDARD, Engine as _};

        let recent_blockhash = self
            .rome_evm_client
            .rpc_client()
            .get_latest_blockhash()
            .await
            .map_err(|error| {
                ApiError::RomeEvmError(RomeEvmError::Custom(format!(
                    "get_latest_blockhash for external V1 deposit: {error}"
                )))
            })?;
        let transaction = self
            .rome_evm_client
            .compose_external_deposit_v1(solana_pubkey.0, amount, recent_blockhash)
            .await
            .map_err(ApiError::from)?;
        let transaction = wincode::serialize(&transaction)
            .map(|bytes| STANDARD.encode(bytes))
            .map_err(|error| {
                ApiError::RomeEvmError(RomeEvmError::Custom(format!(
                    "encode external deposit V1 transaction: {error}"
                )))
            })?;
        Ok(DepositV1Plan {
            recent_blockhash: recent_blockhash.to_string(),
            transaction,
        })
    }

    #[tracing::instrument(name = "proxy::submit_signed_v1_plan", skip(self, transactions))]
    async fn submit_signed_v1_plan(
        &self,
        solana_pubkey: B58Pubkey,
        transactions: Vec<String>,
    ) -> Result<Vec<SolSignature>> {
        if transactions.is_empty() {
            return Err(ApiError::custom(
                "signed V1 plan must contain at least one transaction",
            ));
        }
        if transactions.len() > MAX_SIGNED_V1_PLAN_LEGS {
            return Err(ApiError::custom(format!(
                "signed V1 plan has {} legs; maximum is {MAX_SIGNED_V1_PLAN_LEGS}",
                transactions.len()
            )));
        }

        let program_id = self.rome_evm_client.program_id();
        let decoded = transactions
            .iter()
            .map(|wire| decode_wallet_signed_v1(wire, &solana_pubkey.0, program_id))
            .collect::<Result<Vec<_>>>()?;

        // Submit in plan order. The user's wallet remains fee payer and sole
        // signer on every transaction; this proxy only transports confirmed
        // user-authorized wire messages and never substitutes a payer lane.
        self.rome_evm_client
            .solana()
            .send_and_confirm_signed_plan(&decoded)
            .await
            .map_err(ApiError::from)
    }

    #[tracing::instrument(name = "proxy::relay_unsigned_v1", skip(self, rlp))]
    async fn relay_unsigned_v1(
        &self,
        rlp: Bytes,
        solana_pubkey: B58Pubkey,
        solana_signature: B58Signature,
    ) -> Result<TxHash> {
        self.rome_evm_client
            .relay_external_v1(rlp, solana_pubkey.0, solana_signature.0)
            .await
            .map_err(ApiError::from)
    }
}

#[derive(serde::Deserialize)]
struct GetTxResponse {
    result: Option<GetTxResult>,
}

#[derive(serde::Deserialize)]
struct GetTxResult {
    meta: Option<GetTxMeta>,
}

#[derive(serde::Deserialize)]
struct GetTxMeta {
    #[serde(rename = "logMessages")]
    log_messages: Option<Vec<String>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsigned_v1_plan_exposes_only_wallet_signable_wire_fields() {
        let plan = UnsignedV1Plan {
            recent_blockhash: "test-blockhash".to_owned(),
            transactions: vec!["AAE=".to_owned()],
        };

        let json = serde_json::to_value(plan).expect("serializes RPC response");
        assert_eq!(json["recentBlockhash"], "test-blockhash");
        assert_eq!(json["transactions"], serde_json::json!(["AAE="]));
        assert!(
            json.get("payer").is_none(),
            "wallet plan must not expose a proxy payer"
        );
    }

    #[test]
    fn signed_plan_rejects_non_v1_wire_before_broadcast() {
        use base64::{engine::general_purpose::STANDARD, Engine as _};
        use solana_sdk::transaction::Transaction;

        let payer = Pubkey::new_unique();
        let legacy = VersionedTransaction::from(Transaction::default());
        let wire = STANDARD.encode(wincode::serialize(&legacy).expect("legacy wire"));
        let err = decode_wallet_signed_v1(&wire, &payer, &Pubkey::new_unique())
            .expect_err("a legacy transaction must never enter the user-pays V1 path");
        assert!(err.to_string().contains("must use V1"));
    }

    /// Known inputs for the unit test — all values are deterministic (no RPC required).
    ///
    /// from  = 0x857534c27f4c0e8394921ad3b5b73cb4d7963633  (SYNTH addr in probe-collapse)
    /// program_id = RPTWwELXAY4KC9ZPHhaxp7Sq1hHtU3HNEgLbSegCcWf  (Hadrian program)
    /// mint  = some arbitrary 32-byte pubkey
    /// dst   = some arbitrary 32-byte pubkey (the to_ata arg)
    #[test]
    fn transfer_spl_decode_derive_accounts() {
        // --- Inputs ---
        let program_id: Pubkey = "RPTWwELXAY4KC9ZPHhaxp7Sq1hHtU3HNEgLbSegCcWf"
            .parse()
            .unwrap();
        let from: Address = "0x857534c27f4c0e8394921ad3b5b73cb4d7963633"
            .parse()
            .unwrap();

        // Arbitrary distinct 32-byte values for mint and dst (to_ata).
        let mint_pk: Pubkey = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
            .parse()
            .unwrap();
        // We use a fresh random-looking pubkey for dst
        let dst_pk: Pubkey = "9wJGNGWdFaotGrqBEuAkujhnRi94vyadDS4vz8YeiAds"
            .parse()
            .unwrap();

        // Build calldata: selector(4) + to_ata(32) + tokens/uint256(32) + mint(32) = 100 bytes
        let mut data = Vec::with_capacity(100);
        data.extend_from_slice(&TRANSFER_SPL_SEL); // [0..4]
        data.extend_from_slice(dst_pk.as_ref()); // [4..36]  to_ata (bytes32)
        data.extend_from_slice(&[0u8; 24]); // [36..60] high 24 bytes of uint256 = 0
        data.extend_from_slice(&1000u64.to_be_bytes()); // [60..68] low 8 bytes = 1000
        data.extend_from_slice(mint_pk.as_ref()); // [68..100] mint (bytes32)

        // --- Call the function under test ---
        let result = decode_transfer_spl_accounts(&program_id, &from, &data);
        assert!(result.is_some(), "should decode and derive accounts");
        let (synth_pda, synth_ata, dst_out) = result.unwrap();

        // --- Assert dst passthrough ---
        assert_eq!(dst_out, dst_pk, "dst must equal the to_ata arg0");

        // --- Assert synthPda = find_program_address([EXTERNAL_AUTHORITY, from_bytes], program_id) ---
        let from_h160 = RomeH160::from_slice(from.as_bytes());
        let expected_pda = Pda::new_(&program_id, 0).external_auth(&from_h160).0;
        assert_eq!(
            synth_pda, expected_pda,
            "synthPda must equal external_auth PDA"
        );

        // --- Assert synthAta = ATA(owner=synthPda, mint=mint_pk) ---
        let expected_ata =
            get_associated_token_address_with_program_id(&synth_pda, &mint_pk, &TOKEN_PROGRAM_ID);
        assert_eq!(
            synth_ata, expected_ata,
            "synthAta must equal ATA(synthPda, mint)"
        );

        // --- Assert all three are distinct (sanity) ---
        assert_ne!(synth_pda, synth_ata);
        assert_ne!(synth_pda, dst_out);
        assert_ne!(synth_ata, dst_out);
    }

    /// Wrong selector → returns None.
    #[test]
    fn transfer_spl_wrong_selector_returns_none() {
        let program_id: Pubkey = "RPTWwELXAY4KC9ZPHhaxp7Sq1hHtU3HNEgLbSegCcWf"
            .parse()
            .unwrap();
        let from: Address = "0x857534c27f4c0e8394921ad3b5b73cb4d7963633"
            .parse()
            .unwrap();
        let mut data = vec![0u8; 100];
        data[0..4].copy_from_slice(&[0xde, 0xad, 0xbe, 0xef]); // wrong selector
        assert!(decode_transfer_spl_accounts(&program_id, &from, &data).is_none());
    }

    /// Too-short calldata → returns None.
    #[test]
    fn transfer_spl_short_data_returns_none() {
        let program_id: Pubkey = "RPTWwELXAY4KC9ZPHhaxp7Sq1hHtU3HNEgLbSegCcWf"
            .parse()
            .unwrap();
        let from: Address = "0x857534c27f4c0e8394921ad3b5b73cb4d7963633"
            .parse()
            .unwrap();
        let data = vec![0xb6, 0x97, 0x78, 0x79, 0x00]; // only 5 bytes
        assert!(decode_transfer_spl_accounts(&program_id, &from, &data).is_none());
    }
}

impl Proxy {
    /// Fetch `meta.logMessages` for a Solana tx via raw JSON-RPC.
    ///
    /// Mirrors the same approach used by rome-via-enrich's `batch_trace`
    /// worker — sidesteps Solana SDK type plumbing in favor of a slim
    /// JSON deserializer that only reads the field we care about.
    async fn fetch_solana_logs(&self, sig: &str) -> Result<Vec<String>> {
        let url = self.rome_evm_client.rpc_client().url();
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .map_err(|e| ApiError::Custom(format!("http client init: {}", e)))?;

        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTransaction",
            "params": [sig, {
                "encoding": "json",
                "commitment": "confirmed",
                "maxSupportedTransactionVersion": 1
            }]
        });

        let res: GetTxResponse = http
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| ApiError::Custom(format!("solana RPC: {}", e)))?
            .json()
            .await
            .map_err(|e| ApiError::Custom(format!("solana RPC decode: {}", e)))?;

        Ok(res
            .result
            .and_then(|r| r.meta)
            .and_then(|m| m.log_messages)
            .unwrap_or_default())
    }
}
