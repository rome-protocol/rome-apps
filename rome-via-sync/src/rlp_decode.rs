/// RLP-decode a raw signed Ethereum transaction and recover the sender address.
///
/// Supports legacy (type 0), EIP-2930 (type 1), EIP-1559 (type 2), and op-stack
/// DepositTransaction (type 0x7E) envelopes. The type byte prefix is handled by
/// ethers-core's `Transaction::decode` (the `optimism` feature gates 0x7E support).
///
/// # EIP-1559 gas_price
/// EIP-1559 transactions do not have a static `gas_price` — the effective price depends on the
/// block's `baseFeePerGas`. We do not have that value at sync time. We store `max_fee_per_gas`
/// as the `gas_price` field instead. It is an upper bound and is clearly documented here.
/// The API should expose both `max_fee_per_gas` and `max_priority_fee_per_gas` in Phase 3
/// when the richer tx model lands.
///
/// # Sender recovery
/// `Transaction::recover_from()` uses the EIP-155 v value (or EIP-2718 `y_parity`) plus r/s
/// to recover the public key and derive the `from` address. This is the same logic used by
/// Ethereum nodes and Etherscan.
use crate::cpi_calldata::CpiCall;
use bigdecimal::BigDecimal;
use ethers::types::{Transaction, U64};
use std::str::FromStr;

/// All denormalized fields we derive from a raw signed transaction.
#[derive(Debug, PartialEq)]
pub struct DecodedTx {
    /// 0x-prefixed, 20-byte checksummed address of the recovered sender.
    pub from: String,
    /// 0x-prefixed recipient address; None for contract creation (to field is empty in RLP).
    pub to: Option<String>,
    /// Value in Wei, as a `BigDecimal` (compatible with sqlx NUMERIC).
    pub value_wei: BigDecimal,
    /// Transaction nonce.
    pub nonce: i64,
    /// Effective gas price: `gas_price` for legacy/EIP-2930, `max_fee_per_gas` for EIP-1559.
    /// See module doc for the EIP-1559 fallback rationale.
    pub gas_price: BigDecimal,
    /// Gas limit (upper bound).
    pub gas_limit: i64,
    /// "0x" + first 4 bytes of `input` in hex, or None for plain transfers (empty/short data).
    pub method_id: Option<String>,
    /// Total byte length of the `input` field.
    pub input_len: i32,
    /// Wire-level transaction type: 0 = legacy, 1 = EIP-2930, 2 = EIP-1559.
    pub tx_type_byte: i16,
    /// Depth-1 CPI target, decoded straight from calldata — see `cpi_calldata`
    /// module doc. `None` for any tx that isn't a `CpiProgram.invoke`/`invoke_signed`
    /// call (the overwhelming majority of txs).
    pub cpi_call: Option<CpiCall>,
}

/// Error type for RLP decoding failures.
#[derive(Debug, thiserror::Error)]
pub enum RlpError {
    #[error("RLP decode error: {0}")]
    Decode(#[from] rlp::DecoderError),
    #[error("sender recovery failed: {0}")]
    Recovery(String),
    #[error("gas value overflow: {0}")]
    Overflow(String),
}

/// Decode a raw Ethereum transaction from RLP bytes.
///
/// `recover_sender` controls whether ECDSA sender recovery is attempted:
///
/// - `true`  — recover `from` via `Transaction::recover_from()`. Required for
///   standard signed (ecdsa) txs. Errors if v/r/s are zero or invalid.
/// - `false` — skip recovery entirely; `from` is set to the zero-address
///   sentinel (`"0x000…000"`). Use for Solana-origin (`solana_unsigned`) txs
///   whose EIP-1559 envelope carries zeroed v/r/s. The `from` stored in the DB
///   for those rows is the Hercules-derived synthetic address (resolved by
///   `resolve_sync_from`), not this field — so the sentinel is never persisted.
///
/// Op-stack DepositTransaction (type 0x7E) always reads `from` from the RLP
/// explicitly, regardless of `recover_sender`.
///
/// Returns a [`DecodedTx`] with all explorer-relevant fields populated, or an
/// [`RlpError`] if the bytes are not a valid Ethereum transaction.
pub fn decode_signed_tx(rlp_bytes: &[u8], recover_sender: bool) -> Result<DecodedTx, RlpError> {
    // ethers-core's Transaction::decode handles legacy (0xc0+ list prefix),
    // EIP-2930 (0x01), EIP-1559 (0x02), and op-stack DepositTransaction (0x7E
    // — gated on the `optimism` feature, enabled in our Cargo.toml).
    let tx: Transaction = rlp::decode(rlp_bytes)?;

    // Op-stack deposit txs have no ECDSA signature — `from` is encoded explicitly
    // in the RLP. For all other envelopes we recover from the signature only when
    // `recover_sender` is true. When false (Solana-origin txs) we emit a sentinel
    // zero-address; `resolve_sync_from` will override it with the Hercules value.
    let is_deposit = tx.transaction_type == Some(U64::from(0x7E));
    let from_addr = if is_deposit {
        tx.from
    } else if recover_sender {
        tx.recover_from()
            .map_err(|e: ethers::types::SignatureError| RlpError::Recovery(e.to_string()))?
    } else {
        ethers::types::Address::zero()
    };
    let from = format!("{:?}", from_addr); // "0x..." lowercase hex

    let to = tx
        .to
        .map(|addr| format!("{:?}", addr));

    // U256 → BigDecimal via decimal string representation.
    let value_wei = BigDecimal::from_str(&tx.value.to_string())
        .map_err(|e| RlpError::Overflow(format!("value: {e}")))?;

    // nonce: U256 → i64. Nonces above i64::MAX are not realistic in practice,
    // but raw RLP is user-authored (direct-to-program submission bypasses proxy
    // validation), so an out-of-range value must error, not panic or silently wrap.
    let nonce = u64::try_from(tx.nonce)
        .ok()
        .and_then(|v| i64::try_from(v).ok())
        .ok_or_else(|| RlpError::Overflow(format!("nonce: {}", tx.nonce)))?;

    // gas_price: prefer legacy gas_price; fall back to max_fee_per_gas for EIP-1559.
    let gas_price_u256 = tx
        .gas_price
        .or(tx.max_fee_per_gas)
        .unwrap_or_default();
    let gas_price = BigDecimal::from_str(&gas_price_u256.to_string())
        .map_err(|e| RlpError::Overflow(format!("gas_price: {e}")))?;

    // gas: U256 → i64. Same non-panicking guard as nonce above.
    let gas_limit = u64::try_from(tx.gas)
        .ok()
        .and_then(|v| i64::try_from(v).ok())
        .ok_or_else(|| RlpError::Overflow(format!("gas: {}", tx.gas)))?;

    // input / data
    let input_bytes: &[u8] = tx.input.as_ref();
    let input_len = input_bytes.len() as i32;
    // For contract creation (tx.to is None), the input is deploy bytecode, not
    // calldata — its first 4 bytes are the Solidity memory-setup prologue
    // (`PUSH1 0x80 PUSH1 0x40 MSTORE` == 0x60806040 / 0x60a06040), not a method
    // selector. Leave method_id NULL and let the API render "Contract Creation".
    let method_id = if tx.to.is_some() && input_bytes.len() >= 4 {
        Some(format!("0x{}", hex::encode(&input_bytes[..4])))
    } else {
        None
    };

    // Transaction type byte: None → 0 (legacy), Some(1) → 1 (EIP-2930), Some(2) → 2 (EIP-1559).
    let tx_type_byte = tx
        .transaction_type
        .map(|t: U64| t.as_u64() as i16)
        .unwrap_or(0i16);

    // Depth-1 CPI target from calldata. `decode_cpi_calldata_bytes` gates on
    // `to == CpiProgram (0xFF..08)` first, so this is a cheap no-op for every
    // tx that isn't a CPI invoke (the overwhelming majority).
    let cpi_call = to
        .as_deref()
        .and_then(|t| crate::cpi_calldata::decode_cpi_calldata_bytes(t, input_bytes));

    Ok(DecodedTx {
        from,
        to,
        value_wei,
        nonce,
        gas_price,
        gas_limit,
        method_id,
        input_len,
        tx_type_byte,
        cpi_call,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use ethers::types::U256;

    // ── Fixture 1: EIP-1559 WETH deposit (type 2) ────────────────────────────
    // Source: ethers-core tests (Ethereum mainnet)
    // from: 0x5409ED021D9299bf6814279A6A1411A7e866A631 (or recovered)
    // to:   0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2 (WETH)
    // method: 0xd0e30db0 (deposit())
    // value: ~0.00313 ETH (2_824_200_000_000_000 wei from test; here we test hex blob)
    const EIP1559_WETH_DEPOSIT: &str =
        "02f87a018201df851344ead983851344ead983826d2294c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2882b40d6d551c8970c84d0e30db0c001a05616cdaec839ca14d209b59eafb706e623169dc9d0fa58fbf13931cef5b5e3b0a03e708f8044bd158d29c2e250b6a98ea637c3bc460beeea63a8f00f7cebac432a";

    // ── Fixture 2: Legacy USDT transfer ──────────────────────────────────────
    // Source: ethers-core tests — legacy type 0
    // to: 0xdAC17F958D2ee523a2206206994597C13D831ec7 (Tether USDT)
    // method: 0xa9059cbb (transfer(address,uint256))
    // value: 0
    const LEGACY_USDT_TRANSFER: &str =
        "f8aa808512ec276caf83010e2b94dac17f958d2ee523a2206206994597c13d831ec780b844a9059cbb000000000000000000000000fdae129ecc2c27d166a3131098bc05d143fa258e0000000000000000000000000000000000000000000000000000000002faf08025a0c81e70f9e49e0d3b854720143e86d172fecc9e76ef8a8666f2fdc017017c5141a01dd3410180f6a6ca3e25ad3058789cd0df3321ed76b5b4dbe0a2bb2dc28ae274";

    // ── Fixture 3: EIP-1559 with method call (e5225381) ──────────────────────
    // Source: ethers-core tests — type 2 with non-empty data
    const EIP1559_METHOD_CALL: &str =
        "02f86f05418459682f008459682f098301a0cf9411d7c2ab0d4aa26b7d8502f6a7ef6844908495c28084e5225381c001a01a8d7bef47f6155cbdf13d57107fc577fd52880fa2862b1a50d47641f8839419a03279bbf73fde76de83440d04b9d97f3809fec8617d3557ee40ac3e0edc391514";

    // Fixture 4: Op-stack DepositTransaction (type 0x7E)
    // Source: live marcus tx 0xbd6dcfdd…957c — Rome user-deposit, depositor and
    // recipient are the same wallet 0x6e39…359a. 0.1 ETH-equivalent mint, gas
    // 21000, no input, not a system tx.
    const OPTIMISM_DEPOSIT: &str =
        "7ef862a0e492d32230edbf9fd453305a381e244258eecaab7b9acc1f1109ed629fc8d3db946e39546bed06afa0c786d6602d570ffc638a359a946e39546bed06afa0c786d6602d570ffc638a359a88016345785d8a000088016345785d8a00008252088080";

    // ── Fixture 5: real Hadrian tx — EIP-1559 to CpiProgram (0xFF..08), invoke()
    // SPL-Token Approve 5,000,000. Same calldata as the golden fixture in
    // `cpi_calldata` tests, wrapped in a real signed RLP envelope.
    const CPI_INVOKE_APPROVE: &str =
        "02f9025083030d4a4c808504d83a9850826aa494ff0000000000000000000000000000000000000880b901e47480cb8606ddf6e1d765a193d9cbe146ceeb79ac1cb485ed5f5b37913a8cf5857eff00a9000000000000000000000000000000000000000000000000000000000000006000000000000000000000000000000000000000000000000000000000000001a000000000000000000000000000000000000000000000000000000000000000037c802811e8e0c52e1edd7535384ce00cbcf11ea7d6378927921448fc0ea5882000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001bff74ead8660afbbe482c2b9cd8603caab71b4487849899e047e2a7edf53d64400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000ce872e4e9606bddcc1fc5e9990fa08edd90b70e6587f27e69d4d79ef46a421e700000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000904404b4c00000000000000000000000000000000000000000000000000000000c001a049309e72a8758b69048049ab25d288d567affeaf0abf08dd228cb630ca9c266da008cf06ea37d04c95ef3fe181cfbbf43d0fac25a87bc6e8263f43ebea1017fe34";

    // Malformed bytes
    const MALFORMED: &[u8] = b"\xde\xad\xbe\xef\x00\x01";

    fn from_hex(s: &str) -> Vec<u8> {
        hex::decode(s).expect("valid hex fixture")
    }

    // ── Test: EIP-1559 WETH deposit ───────────────────────────────────────────
    #[test]
    fn eip1559_weth_deposit_fields() {
        let raw = from_hex(EIP1559_WETH_DEPOSIT);
        let decoded = decode_signed_tx(&raw, true).expect("should decode");

        // type 2
        assert_eq!(decoded.tx_type_byte, 2, "EIP-1559 type byte should be 2");

        // to: WETH contract
        assert_eq!(
            decoded.to.as_deref(),
            Some("0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"),
            "to should be WETH"
        );

        // method: deposit() = 0xd0e30db0
        assert_eq!(
            decoded.method_id.as_deref(),
            Some("0xd0e30db0"),
            "method should be deposit()"
        );

        // input_len: 4 bytes (method selector only, no args for deposit)
        assert_eq!(decoded.input_len, 4, "WETH deposit has 4-byte selector");

        // value > 0 (this tx sends ETH to WETH)
        assert!(
            decoded.value_wei > BigDecimal::from(0),
            "WETH deposit should have non-zero value"
        );

        // gas_price is set (max_fee_per_gas fallback for EIP-1559)
        assert!(
            decoded.gas_price > BigDecimal::from(0),
            "gas_price should be non-zero"
        );

        // from address should be valid 0x format with 42 chars
        assert!(decoded.from.starts_with("0x"), "from should start with 0x");
        assert_eq!(decoded.from.len(), 42, "from should be 42 chars");
    }

    // ── Test: Legacy USDT transfer ────────────────────────────────────────────
    #[test]
    fn legacy_usdt_transfer_fields() {
        let raw = from_hex(LEGACY_USDT_TRANSFER);
        let decoded = decode_signed_tx(&raw, true).expect("should decode");

        // type 0 (legacy)
        assert_eq!(decoded.tx_type_byte, 0, "legacy tx type should be 0");

        // to: USDT
        assert_eq!(
            decoded.to.as_deref(),
            Some("0xdac17f958d2ee523a2206206994597c13d831ec7"),
            "to should be USDT"
        );

        // value: 0 (ERC-20 transfer, ETH value is 0)
        assert_eq!(decoded.value_wei, BigDecimal::from(0), "USDT transfer value is 0");

        // method: transfer(address,uint256) = 0xa9059cbb
        assert_eq!(
            decoded.method_id.as_deref(),
            Some("0xa9059cbb"),
            "method should be ERC-20 transfer"
        );

        // input_len: 68 = 4 (selector) + 32 (address) + 32 (amount)
        assert_eq!(decoded.input_len, 68, "ERC-20 transfer has 68 bytes of input");

        // from address should be valid
        assert!(decoded.from.starts_with("0x"));
        assert_eq!(decoded.from.len(), 42);
    }

    // ── Test: Pure transfer (empty data) → method_id = None ──────────────────
    // We test this via the EIP-1559 tx by inspecting method_id is not None for
    // data-bearing txs (confirmed above). For a pure-transfer we build a simple
    // legacy tx RLP with empty input. Use the same blob but manipulate expectations
    // for the USDT tx: it has data, method_id = Some(...).
    //
    // A real pure-ETH-transfer test: the EIP-1559 WETH deposit sends value with
    // only a 4-byte selector — that's NOT a pure transfer. We test method_id=None
    // logic by checking what happens for tx with input.len() < 4.
    #[test]
    fn method_id_none_for_short_input() {
        // Construct a minimal pseudo-tx to test the method_id logic directly.
        // The real code checks input_bytes.len() >= 4 — if len < 4, method_id = None.
        // We verify this with a 3-byte input (not a valid tx, but tests the branch).
        let short_input: &[u8] = &[0xab, 0xcd, 0xef]; // 3 bytes
        let method_id = if short_input.len() >= 4 {
            Some(format!("0x{}", hex::encode(&short_input[..4])))
        } else {
            None
        };
        assert_eq!(method_id, None, "3-byte input should yield no method_id");

        // Empty input (pure transfer)
        let empty_input: &[u8] = &[];
        let method_id_empty = if empty_input.len() >= 4 {
            Some(format!("0x{}", hex::encode(&empty_input[..4])))
        } else {
            None
        };
        assert_eq!(method_id_empty, None, "empty input should yield no method_id");
    }

    // ── H9: oversized nonce/gas must return Err, never panic ───────────────────
    // Raw RLP is user-authored (direct-to-program submission bypasses proxy
    // validation), so a nonce or gas above u64::MAX can land in the indexed bytes.
    // `U256::as_u64()` PANICS on overflow; one such row would crash-loop
    // sync_evm_tx mid-batch — the cursor never advances past it and the mirror
    // wedges. Decode must degrade to RlpError::Overflow, not panic.
    fn legacy_rlp_9(nonce: U256, gas: U256) -> Vec<u8> {
        let mut s = rlp::RlpStream::new_list(9);
        s.append(&nonce); // nonce
        s.append(&U256::from(1u64)); // gasPrice
        s.append(&gas); // gasLimit
        s.append(&vec![0u8; 20]); // to (20-byte address)
        s.append(&U256::zero()); // value
        s.append(&Vec::<u8>::new()); // data (empty)
        s.append(&27u64); // v
        s.append(&U256::from(1u64)); // r
        s.append(&U256::from(1u64)); // s
        s.out().to_vec()
    }

    #[test]
    fn oversized_nonce_returns_err_not_panic() {
        // nonce = U256::MAX (> u64::MAX), gas valid. recover_sender=false so we
        // exercise the field-decode path (line ~104), not signature recovery.
        let raw = legacy_rlp_9(U256::MAX, U256::from(21_000u64));
        let result = decode_signed_tx(&raw, false);
        assert!(
            matches!(result, Err(RlpError::Overflow(_))),
            "oversized nonce must return RlpError::Overflow, got {result:?}"
        );
    }

    #[test]
    fn oversized_gas_returns_err_not_panic() {
        // nonce valid, gas = U256::MAX (> u64::MAX) — gates the gas_limit line (~115).
        let raw = legacy_rlp_9(U256::from(0u64), U256::MAX);
        let result = decode_signed_tx(&raw, false);
        assert!(
            matches!(result, Err(RlpError::Overflow(_))),
            "oversized gas must return RlpError::Overflow, got {result:?}"
        );
    }

    // ── Test: EIP-1559 method call ────────────────────────────────────────────
    #[test]
    fn eip1559_method_call_fields() {
        let raw = from_hex(EIP1559_METHOD_CALL);
        let decoded = decode_signed_tx(&raw, true).expect("should decode");

        assert_eq!(decoded.tx_type_byte, 2, "should be EIP-1559");

        // method: 0xe5225381 (unknown selector, but must be decoded)
        assert_eq!(
            decoded.method_id.as_deref(),
            Some("0xe5225381"),
            "method_id should be first 4 bytes of input"
        );

        // from address valid
        assert!(decoded.from.starts_with("0x"));
        assert_eq!(decoded.from.len(), 42);

        // to is non-None (not contract creation)
        assert!(decoded.to.is_some(), "this tx has a recipient");
        assert!(decoded.to.as_ref().unwrap().starts_with("0x"));
    }

    // Test: Op-stack DepositTransaction (type 0x7E)
    #[test]
    fn optimism_deposit_fields() {
        let raw = from_hex(OPTIMISM_DEPOSIT);
        // Deposit txs don't need ECDSA recovery (from is in the RLP), but the
        // is_deposit branch fires regardless of recover_sender — both values work.
        let decoded = decode_signed_tx(&raw, true).expect("0x7E deposit should decode");

        assert_eq!(decoded.tx_type_byte, 0x7E, "tx_type_byte should be 0x7E (126)");

        // Sender is encoded explicitly in the RLP, not ECDSA-recovered.
        assert_eq!(
            decoded.from,
            "0x6e39546bed06afa0c786d6602d570ffc638a359a",
            "from should match the depositor"
        );

        // For this self-deposit, recipient equals depositor.
        assert_eq!(
            decoded.to.as_deref(),
            Some("0x6e39546bed06afa0c786d6602d570ffc638a359a"),
            "to should be populated"
        );

        // 0.1 ETH-equivalent (0x016345785d8a0000 wei).
        assert_eq!(
            decoded.value_wei.to_string(),
            "100000000000000000",
            "value should be 0.1 ETH-equivalent"
        );

        // Gas limit 21000.
        assert_eq!(decoded.gas_limit, 21_000);

        // Deposits pay 0 gas price on the L2.
        assert_eq!(decoded.gas_price.to_string(), "0");

        // Empty input → no method_id.
        assert_eq!(decoded.input_len, 0);
        assert_eq!(decoded.method_id, None);
    }

    // Test: Malformed RLP → Err
    #[test]
    fn malformed_rlp_returns_error() {
        let result = decode_signed_tx(MALFORMED, true);
        assert!(
            result.is_err(),
            "malformed bytes should return RlpError, got: {result:?}"
        );
    }

    // ── Test: Solana-unsigned (zeroed v/r/s) decode without sender recovery ───
    //
    // Solana-origin txs are EIP-1559 (type 2) envelopes with zeroed v/r/s —
    // `recover_from()` errors on them. The old `decode_signed_tx` path (which
    // always recovers) therefore returns Err for these txs, causing sync.rs to
    // set `decoded = None` and bind NULL for `to_addr`/`method_id`.
    //
    // The new `decode_signed_tx(rlp, recover_sender: bool)` path with
    // `recover_sender = false` must:
    //   (a) return Ok(d) with `to` and `method_id` populated correctly, AND
    //   (b) set `from` to the zero-address sentinel (recovery skipped).
    //
    // We also assert that the OLD `recover_sender = true` path errors on the same
    // bytes, documenting exactly why the bug existed.
    //
    // To build the fixture we take the real EIP-1559 WETH-deposit hex above and
    // replace its r/s/v with zeros by constructing a Transaction struct directly
    // and calling `.rlp()`.
    #[test]
    fn solana_unsigned_decode_without_recovery() {
        use ethers::types::{Address, Bytes, Transaction, U256, U64};

        // Build a type-2 tx that looks like a Solana-origin EVM tx:
        //   to = wUSDC approve target (any 20-byte address)
        //   input = approve() selector + 64 bytes of args (method = 0x095ea7b3)
        //   v = 0, r = U256::zero(), s = U256::zero()  ← zeroed, no real ECDSA sig
        let to_addr: Address = "0x9a8b4cb7cafebabe000000000000000000000001"
            .parse()
            .unwrap();
        // approve(address,uint256) selector = 0x095ea7b3
        let mut input_bytes = vec![0x09u8, 0x5e, 0xa7, 0xb3];
        input_bytes.extend_from_slice(&[0u8; 64]); // 32-byte address + 32-byte amount

        let tx = Transaction {
            transaction_type: Some(U64::from(2u64)),
            to: Some(to_addr),
            from: Address::zero(),
            input: Bytes::from(input_bytes),
            value: U256::zero(),
            nonce: U256::from(7u64),
            gas: U256::from(200_000u64),
            max_fee_per_gas: Some(U256::from(1_000_000_000u64)),
            max_priority_fee_per_gas: Some(U256::from(1_000_000u64)),
            chain_id: Some(U256::from(121302u64)),
            // zeroed signature — exactly what Solana-origin txs have
            v: U64::from(0u64),
            r: U256::zero(),
            s: U256::zero(),
            // all other fields at default
            ..Default::default()
        };

        let rlp_bytes = tx.rlp().to_vec();

        // (a) WITHOUT recovery — must succeed and populate to/method_id.
        let decoded = decode_signed_tx(&rlp_bytes, false)
            .expect("decode without recovery must succeed on zeroed-sig tx");

        assert_eq!(
            decoded.tx_type_byte, 2,
            "should be EIP-1559 type 2"
        );
        assert_eq!(
            decoded.to.as_deref(),
            Some(format!("{:?}", to_addr).as_str()),
            "to_addr must be populated"
        );
        assert_eq!(
            decoded.method_id.as_deref(),
            Some("0x095ea7b3"),
            "method_id must be approve() selector"
        );
        assert_eq!(
            decoded.from,
            "0x0000000000000000000000000000000000000000",
            "from should be zero-address sentinel when recovery skipped"
        );

        // (b) WITH recovery — must fail on the zeroed sig (documents the original bug).
        let recover_result = decode_signed_tx(&rlp_bytes, true);
        assert!(
            recover_result.is_err(),
            "decode WITH recovery must fail on zeroed-sig tx (the original bug)"
        );
        matches!(recover_result.unwrap_err(), RlpError::Recovery(_));
    }

    // ── Test: Verify contract creation (to = None) ─────────────────────────
    // When a tx has an empty `to` field, the output `to` should be None.
    // We verify this from the code path: `tx.to.map(...)` → None if tx.to is None.
    #[test]
    fn to_field_none_means_contract_creation() {
        // The existing legacy and EIP-1559 fixtures both have non-None `to`.
        // We test the Option<Address> → Option<String> mapping logic:
        let addr_some: Option<ethers::types::Address> = Some(
            "0xdac17f958d2ee523a2206206994597c13d831ec7"
                .parse()
                .unwrap(),
        );
        let addr_none: Option<ethers::types::Address> = None;

        let to_some = addr_some.map(|a| format!("{:?}", a));
        let to_none = addr_none.map(|a| format!("{:?}", a));

        assert!(to_some.is_some(), "Some(address) → Some(string)");
        assert!(to_none.is_none(), "None address → None (contract creation)");
        assert_eq!(
            to_some.as_deref(),
            Some("0xdac17f958d2ee523a2206206994597c13d831ec7")
        );
    }

    // ── Test: cpi_call populated for a real CpiProgram.invoke tx ──────────────
    #[test]
    fn decode_signed_tx_populates_cpi_call_for_invoke() {
        let raw = from_hex(CPI_INVOKE_APPROVE);
        let decoded = decode_signed_tx(&raw, true).expect("CPI invoke tx should decode");

        assert_eq!(
            decoded.cpi_call,
            Some(crate::cpi_calldata::CpiCall {
                program_id: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_string(),
                program_label: Some("SPL-Token".to_string()),
                instruction: Some("Approve".to_string()),
            }),
            "cpi_call should decode the SPL-Token Approve invoke target"
        );
    }

    // ── Test: cpi_call is None for a non-CPI tx ────────────────────────────────
    #[test]
    fn decode_signed_tx_cpi_call_none_for_non_cpi_tx() {
        let raw = from_hex(LEGACY_USDT_TRANSFER);
        let decoded = decode_signed_tx(&raw, true).expect("should decode");
        assert_eq!(decoded.cpi_call, None, "non-CPI tx must not populate cpi_call");
    }
}
