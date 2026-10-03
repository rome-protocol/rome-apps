/// Decode Rome `CpiProgram` (`0xFF..08`) `invoke`/`invoke_signed` calldata into the
/// CPI's target Solana program + instruction, using **only the calldata** — no RPC,
/// no DB lookup, no execution. This is depth-1: it decodes the single Solana call the
/// EVM tx's calldata directly requests. It does not (and cannot, from calldata alone)
/// see further CPIs a target program might issue at runtime.
///
/// Robust, log-independent source for the CPI target. Today `cross_chain.rs` classifies
/// by matching `Program X invoke [N]` lines in `meta.logMessages` and skips
/// "plumbing"/infra programs, so a direct user `invoke()` of a filtered (or otherwise
/// unmatched) program yields no target — the audit's live NULL case. Note: `invoke` /
/// `invoke_signed` ARE real CPIs and DO emit the `Program X invoke [N]` log — unlike the
/// five `0xFF..08` READ shortcuts (`account_info`, `account_data_at`, `account_u64_at`,
/// `account_lamports`, `pdas_batch_derive`), which dispatch as `CrossStateEthCall` and
/// emit no log (see the `CrossStateEthCall` dispatch in the upstream `rome-evm` program).
/// This decoder reads the CPI target deterministically out of the ABI-encoded calldata
/// instead of depending on the log heuristic.
///
/// # ABI shape
/// Both `invoke` and `invoke_signed` share a `(bytes32 program_id, AccountMeta[]
/// accounts, bytes data, ..)` argument head — `invoke_signed` appends further
/// arguments (PDA seeds) after `data`, which this decoder never needs to read.
/// Standard Solidity ABI encoding: the static `program_id` occupies args word 0
/// (calldata bytes `[4..36]`); `accounts` and `data` are dynamic, so args words 1
/// and 2 hold *offsets* (relative to the start of the args, i.e. immediately after
/// the 4-byte selector) into their own head/tail encoding elsewhere in the calldata.
///
/// This decoder does not need to walk `accounts` at all — it jumps straight from the
/// word-2 offset to the `data` argument's length-prefixed bytes.
///
/// # Wiring
/// `rlp_decode::decode_signed_tx` calls `decode_cpi_calldata_bytes` and surfaces the
/// result as `DecodedTx.cpi_call`; sync persists it to `evm_tx.cpi_program_calldata`
/// / `cpi_program_label_calldata` / `cpi_instruction_calldata`, and the API COALESCEs
/// those over the `cross_chain_correlations` log-scrape values (program id = calldata
/// primary; label/instruction = log-scrape primary until the Anchor IDL cache lands).
///
/// # Anchor instruction names — curated seed, not the general path
/// `ANCHOR_REGISTRY` below is a compiled-in static map (base58 program id → label +
/// 8-byte-discriminator → instruction-name map) for the small set of Anchor CPI
/// targets Rome has confirmed on-chain — seeded here with Meteora DAMM v1. This is
/// deliberately NOT a general Anchor IDL resolver: no DB, no Solana RPC, no on-chain
/// IDL account fetch. A program not in this map still gets the `"anchor:<disc>"` hex
/// fallback, same as before. The general path — fetching the on-chain `anchor:idl`
/// PDA and caching it with negative-result versioning (spec §2.7) — is a deferred
/// follow-up; this seed exists so the one confirmed Anchor target doesn't wait on it.
use std::collections::HashMap;
use std::sync::LazyLock;

/// The CPI's target Solana program and (best-effort) decoded instruction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CpiCall {
    /// Base58-encoded target Solana program id.
    pub program_id: String,
    /// Human label for well-known native programs (SPL-Token, Token-2022, ATA,
    /// System, ComputeBudget). `None` for anything else — labelling unknown
    /// programs is out of scope here (that's the CPI-target-labelling enrich path).
    pub program_label: Option<String>,
    /// Best-effort instruction name. For SPL-Token / Token-2022, the mapped
    /// `TokenInstruction` variant name. For any other program with a `data` arg of
    /// at least 8 bytes, `"anchor:<8-byte-discriminator-hex>"` — full IDL resolution
    /// is a later slice. `None` when the leading byte/discriminator can't be mapped
    /// or `data` is empty.
    pub instruction: Option<String>,
}

/// `CpiProgram` precompile address, lowercase hex, no `0x` prefix.
const CPI_PROGRAM_ADDR: &str = "ff00000000000000000000000000000000000008";

const INVOKE_SELECTOR: [u8; 4] = [0x74, 0x80, 0xcb, 0x86];
const INVOKE_SIGNED_SELECTOR: [u8; 4] = [0xb9, 0x4f, 0x37, 0x33];

const SPL_TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022_PROGRAM_ID: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

static PROGRAM_LABELS: LazyLock<HashMap<&'static str, &'static str>> = LazyLock::new(|| {
    HashMap::from([
        (SPL_TOKEN_PROGRAM_ID, "SPL-Token"),
        (TOKEN_2022_PROGRAM_ID, "Token-2022"),
        ("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL", "ATA"),
        ("11111111111111111111111111111111", "System"),
        ("ComputeBudget111111111111111111111111111111", "ComputeBudget"),
    ])
});

/// A curated Anchor program: display label + its instruction discriminator map
/// (8-byte-discriminator hex, lowercase → PascalCase instruction name, matching
/// Anchor's log-emitted "Instruction: <Name>" form — kept consistent with
/// `cross_chain_correlations.cpi_instruction`, which the API COALESCEs this
/// value against).
struct AnchorProgramInfo {
    label: &'static str,
    disc_map: HashMap<&'static str, &'static str>,
}

/// Curated Anchor CPI targets — seed-first, not a general resolver (see module
/// doc). Meteora DAMM v1 is deployed under two on-chain program ids that share
/// the same instruction set; both are seeded here.
static ANCHOR_REGISTRY: LazyLock<HashMap<&'static str, AnchorProgramInfo>> = LazyLock::new(|| {
    // Discriminators derived from the vendored `dynamic-amm` crate:
    // sha256(format!("global:{ix_name}"))[..8], hex-encoded.
    let meteora_damm_v1_discs: HashMap<&'static str, &'static str> = HashMap::from([
        ("a8e3323ebdab54b0", "AddBalanceLiquidity"),
        ("4f237a54ad0f5dbf", "AddImbalanceLiquidity"),
        ("04e4d747e1fd77ce", "BootstrapLiquidity"),
        ("a9204f8988e84689", "ClaimFee"),
        ("9109489d5f7d3d55", "CloseConfig"),
        ("c9cff3724b6f2fbd", "CreateConfig"),
        ("3657a51345e3dae0", "CreateLockEscrow"),
        ("0d46a829fa64945a", "CreateMintMetadata"),
        ("8006e48337a134a9", "EnableOrDisablePool"),
        ("9118acc2db7d03be", "InitializeCustomizablePermissionlessConstantProductPool"),
        ("4d55b29d3230d47e", "InitializePermissionedPool"),
        ("07a68aabceabecf4", "InitializePermissionlessConstantProductPoolWithConfig"),
        ("76ad299dad486167", "InitializePermissionlessPool"),
        ("06874493e552a971", "InitializePermissionlessPoolWithFeeTier"),
        ("1513d02bed3eff57", "Lock"),
        ("28d22235e95fe88f", "MoveLockedLp"),
        ("6256cc335e4745bb", "OverrideCurveParam"),
        ("3935b01e7b463440", "PartnerClaimFee"),
        ("856d2cb338ee7221", "RemoveBalanceLiquidity"),
        ("5454b142feb90afb", "RemoveLiquiditySingleSide"),
        ("662c9e36cd257e4e", "SetPoolFees"),
        ("f8c69e91e17587c8", "Swap"),
        ("963e7ddbabdc1aed", "UpdateActivationPoint"),
        ("0930dc6516f04ec8", "GetPoolInfo"),
    ]);
    HashMap::from([
        (
            "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB",
            AnchorProgramInfo { label: "Meteora DAMM v1", disc_map: meteora_damm_v1_discs.clone() },
        ),
        (
            "ammbh4CQztZ6txJ8AaQgPsWjd6o7GhmvopS2JAo5bCB",
            AnchorProgramInfo { label: "Meteora DAMM v1", disc_map: meteora_damm_v1_discs },
        ),
    ])
});

/// Resolve a program's display label: native (SPL/Token-2022/ATA/System/
/// ComputeBudget) first, then the curated Anchor registry, else `None`.
fn resolve_program_label(program_id: &str) -> Option<String> {
    PROGRAM_LABELS
        .get(program_id)
        .map(|s| s.to_string())
        .or_else(|| ANCHOR_REGISTRY.get(program_id).map(|info| info.label.to_string()))
}

/// Decode `CpiProgram.invoke`/`invoke_signed` calldata into its target program +
/// instruction. `to` = `tx.to` (any hex case, `0x`-prefixed or not); `input` = the
/// full calldata hex (`"0x7480cb86.."`).
///
/// Returns `None` — never panics — when `to` isn't the CPI precompile, the selector
/// isn't `invoke`/`invoke_signed`, or the calldata is malformed/truncated. This runs
/// in the enrich/sync path, which must survive arbitrary user-authored calldata
/// (direct-to-program submission bypasses proxy validation) without crash-looping.
pub fn decode_cpi_calldata(to: &str, input: &str) -> Option<CpiCall> {
    let bytes = decode_hex(input)?;
    decode_cpi_calldata_bytes(to, &bytes)
}

/// Bytes-core of [`decode_cpi_calldata`] — same contract, but takes already-decoded
/// calldata bytes instead of a hex string. Lets callers that already have raw bytes
/// (e.g. `rlp_decode::decode_signed_tx`, which decodes `tx.input` off the wire) skip
/// a redundant hex-encode/decode round trip.
pub fn decode_cpi_calldata_bytes(to: &str, bytes: &[u8]) -> Option<CpiCall> {
    if !is_cpi_program(to) {
        return None;
    }

    let selector = bytes.get(0..4)?;
    if selector != INVOKE_SELECTOR && selector != INVOKE_SIGNED_SELECTOR {
        return None;
    }
    let args = bytes.get(4..)?;

    // Arg 0 (static): program_id, bytes32.
    let program_id_word = args.get(0..32)?;
    let program_id = bs58::encode(program_id_word).into_string();

    // Arg 2 (dynamic): data. Its head word is the offset (relative to `args`) of
    // its length-prefixed tail encoding.
    let data_offset = read_offset(args.get(64..96)?)?;
    let len_word_start = data_offset;
    let len_word_end = len_word_start.checked_add(32)?;
    let data_len = read_offset(args.get(len_word_start..len_word_end)?)?;
    let data_start = len_word_end;
    let data_end = data_start.checked_add(data_len)?;
    let data = args.get(data_start..data_end)?;

    let program_label = resolve_program_label(&program_id);
    let instruction = decode_instruction(&program_id, data);

    Some(CpiCall {
        program_id,
        program_label,
        instruction,
    })
}

fn is_cpi_program(to: &str) -> bool {
    to.strip_prefix("0x").unwrap_or(to).to_lowercase() == CPI_PROGRAM_ADDR
}

fn decode_hex(input: &str) -> Option<Vec<u8>> {
    let s = input.strip_prefix("0x").unwrap_or(input);
    hex::decode(s).ok()
}

/// Read a 32-byte big-endian ABI word as a `usize` offset/length. The upper 24
/// bytes must be zero — real calldata offsets/lengths never approach `u64::MAX`,
/// so a nonzero high part means malformed/adversarial input, and we bail rather
/// than silently truncate.
fn read_offset(word: &[u8]) -> Option<usize> {
    if word.len() != 32 || word[..24].iter().any(|&b| b != 0) {
        return None;
    }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&word[24..32]);
    usize::try_from(u64::from_be_bytes(buf)).ok()
}

fn decode_instruction(program_id: &str, data: &[u8]) -> Option<String> {
    let &tag = data.first()?;

    // Native SPL/Token-2022 resolution always runs first and wins — a program
    // can't simultaneously be a native token program and a curated Anchor entry.
    if program_id == SPL_TOKEN_PROGRAM_ID || program_id == TOKEN_2022_PROGRAM_ID {
        return spl_token_instruction_name(tag).map(|s| s.to_string());
    }

    if data.len() < 8 {
        return None;
    }
    let disc_hex = hex::encode(&data[0..8]);

    // Curated Anchor program: look up the 8-byte discriminator. A hit yields the
    // real instruction name; a miss (an instruction not yet seeded) still falls
    // back to the raw discriminator hex, same as an uncurated program.
    if let Some(info) = ANCHOR_REGISTRY.get(program_id) {
        if let Some(name) = info.disc_map.get(disc_hex.as_str()) {
            return Some(name.to_string());
        }
    }

    Some(format!("anchor:{disc_hex}"))
}

/// SPL `TokenInstruction` discriminant → variant name (both legacy Token and
/// Token-2022 share this base set for the instructions decoded here).
fn spl_token_instruction_name(tag: u8) -> Option<&'static str> {
    match tag {
        0 => Some("InitializeMint"),
        1 => Some("InitializeAccount"),
        2 => Some("InitializeMultisig"),
        3 => Some("Transfer"),
        4 => Some("Approve"),
        5 => Some("Revoke"),
        6 => Some("SetAuthority"),
        7 => Some("MintTo"),
        8 => Some("Burn"),
        9 => Some("CloseAccount"),
        10 => Some("FreezeAccount"),
        11 => Some("ThawAccount"),
        12 => Some("TransferChecked"),
        13 => Some("ApproveChecked"),
        14 => Some("MintToChecked"),
        15 => Some("BurnChecked"),
        17 => Some("SyncNative"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CPI_PROGRAM_TO: &str = "0xff00000000000000000000000000000000000008";

    // ── Golden fixture: real Hadrian tx — SPL-Token Approve 5,000,000 ──────────
    // invoke(bytes32 program_id, AccountMeta[] accounts, bytes data):
    //   word0 = program_id = TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA (bytes32)
    //   word1 = 0x60  → accounts offset  → word[3] = accounts.len() == 3
    //   word2 = 0x1a0 → data offset      → word[13] = data.len() == 9,
    //                                      word[14][0..9] = 04 40 4b 4c 00 00 00 00 00
    //                                      (tag 4 = Approve, amount LE u64 = 5_000_000)
    const GOLDEN_APPROVE: &str = "0x7480cb8606ddf6e1d765a193d9cbe146ceeb79ac1cb485ed5f5b37913a8cf5857eff00a9000000000000000000000000000000000000000000000000000000000000006000000000000000000000000000000000000000000000000000000000000001a000000000000000000000000000000000000000000000000000000000000000037c802811e8e0c52e1edd7535384ce00cbcf11ea7d6378927921448fc0ea5882000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001bff74ead8660afbbe482c2b9cd8603caab71b4487849899e047e2a7edf53d64400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000ce872e4e9606bddcc1fc5e9990fa08edd90b70e6587f27e69d4d79ef46a421e700000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000904404b4c00000000000000000000000000000000000000000000000000000000";

    #[test]
    fn golden_invoke_spl_token_approve() {
        let call = decode_cpi_calldata(CPI_PROGRAM_TO, GOLDEN_APPROVE)
            .expect("golden invoke fixture must decode");
        assert_eq!(call.program_id, "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
        assert_eq!(call.program_label.as_deref(), Some("SPL-Token"));
        assert_eq!(call.instruction.as_deref(), Some("Approve"));
    }

    #[test]
    fn invoke_signed_selector_routes_the_same() {
        // Swap only the 4-byte selector on the golden fixture — invoke_signed's
        // shared (program_id, accounts, data, ..) head is byte-identical up to the
        // trailing seeds argument, which this decoder never reads.
        let input = format!("0xb94f3733{}", &GOLDEN_APPROVE[10..]);
        let call = decode_cpi_calldata(CPI_PROGRAM_TO, &input)
            .expect("invoke_signed must route through the same decode path");
        assert_eq!(call.program_id, "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
        assert_eq!(call.instruction.as_deref(), Some("Approve"));
    }

    /// Build minimal `invoke`-shaped calldata: `program_id` + an empty
    /// `accounts[]` + `data`. Mirrors the golden fixture's head/tail layout but
    /// with the accounts array collapsed to zero entries, so `data`'s tail sits
    /// immediately after the accounts length word.
    fn build_invoke_calldata(selector: [u8; 4], program_id: [u8; 32], data: &[u8]) -> String {
        let mut out = Vec::new();
        out.extend_from_slice(&selector);
        out.extend_from_slice(&program_id); // word0: program_id
        out.extend_from_slice(&word_offset(0x60)); // word1: accounts offset -> word3
        out.extend_from_slice(&word_offset(0x80)); // word2: data offset -> word4
        out.extend_from_slice(&word_offset(0)); // word3: accounts.len() == 0
        out.extend_from_slice(&word_offset(data.len())); // word4: data.len()
        let mut padded_data = data.to_vec();
        while padded_data.len() % 32 != 0 {
            padded_data.push(0);
        }
        out.extend_from_slice(&padded_data); // word5..: data bytes, zero-padded
        format!("0x{}", hex::encode(out))
    }

    fn word_offset(v: usize) -> [u8; 32] {
        let mut w = [0u8; 32];
        w[24..32].copy_from_slice(&(v as u64).to_be_bytes());
        w
    }

    fn anchor_program_id_bytes() -> [u8; 32] {
        // Arbitrary non-native program id (not any of the labelled constants).
        [0x42u8; 32]
    }

    #[test]
    fn anchor_program_with_8_byte_discriminator() {
        let data = [0xde, 0xad, 0xbe, 0xef, 0x01, 0x02, 0x03, 0x04];
        let input = build_invoke_calldata(INVOKE_SELECTOR, anchor_program_id_bytes(), &data);
        let call = decode_cpi_calldata(CPI_PROGRAM_TO, &input)
            .expect("well-formed anchor-shaped invoke must decode");
        assert_eq!(call.program_label, None, "unlabelled program id must have no label");
        assert_eq!(call.instruction.as_deref(), Some("anchor:deadbeef01020304"));
    }

    #[test]
    fn wrong_to_address_returns_none() {
        let not_cpi_program = "0xff00000000000000000000000000000000000009"; // HelperProgram, not CpiProgram
        assert_eq!(decode_cpi_calldata(not_cpi_program, GOLDEN_APPROVE), None);
    }

    #[test]
    fn wrong_selector_returns_none() {
        let input = format!("0xdeadbeef{}", &GOLDEN_APPROVE[10..]);
        assert_eq!(decode_cpi_calldata(CPI_PROGRAM_TO, &input), None);
    }

    #[test]
    fn malformed_input_returns_none_without_panic() {
        // Selector-only, no args at all.
        assert_eq!(decode_cpi_calldata(CPI_PROGRAM_TO, "0x7480cb86"), None);
        // Odd-length hex (undecodable).
        assert_eq!(decode_cpi_calldata(CPI_PROGRAM_TO, "0x7480cb860"), None);
        // Empty input.
        assert_eq!(decode_cpi_calldata(CPI_PROGRAM_TO, ""), None);
        assert_eq!(decode_cpi_calldata(CPI_PROGRAM_TO, "0x"), None);
        // Truncated right before the data offset word.
        let truncated = &GOLDEN_APPROVE[..(2 + 8 + 128)]; // selector + program_id + accounts-offset word only
        assert_eq!(decode_cpi_calldata(CPI_PROGRAM_TO, truncated), None);
        // data_len claims far more bytes than are actually present.
        let input = build_invoke_calldata(INVOKE_SELECTOR, anchor_program_id_bytes(), &[]);
        // Corrupt the data-length word (word4, immediately after the selector +
        // 4 head/accounts-len words = 4+32*4 = 132 bytes = 264 hex chars) to an
        // enormous value while leaving no bytes backing it.
        let mut corrupted = hex::decode(input.trim_start_matches("0x")).unwrap();
        let len_word_start = 4 + 32 * 4;
        corrupted[len_word_start..len_word_start + 32].copy_from_slice(&word_offset(usize::MAX / 2));
        let corrupted_hex = format!("0x{}", hex::encode(corrupted));
        assert_eq!(decode_cpi_calldata(CPI_PROGRAM_TO, &corrupted_hex), None);
    }

    #[test]
    fn empty_data_yields_no_instruction() {
        let input = build_invoke_calldata(INVOKE_SELECTOR, anchor_program_id_bytes(), &[]);
        let call = decode_cpi_calldata(CPI_PROGRAM_TO, &input).expect("empty-data invoke still decodes");
        assert_eq!(call.instruction, None);
    }

    // ── S3: curated Anchor instruction names (Meteora DAMM v1 seed) ────────────

    const METEORA_DAMM_V1_ID_A: &str = "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB";
    const METEORA_DAMM_V1_ID_B: &str = "ammbh4CQztZ6txJ8AaQgPsWjd6o7GhmvopS2JAo5bCB";

    fn program_id_bytes(base58: &str) -> [u8; 32] {
        let v = bs58::decode(base58).into_vec().expect("valid base58 fixture");
        v.try_into().expect("32-byte program id")
    }

    #[test]
    fn meteora_swap_resolves_by_curated_discriminator_id_a() {
        // Swap disc f8c69e91e17587c8 + a few trailing arg bytes (amount_in etc).
        let data = [0xf8, 0xc6, 0x9e, 0x91, 0xe1, 0x75, 0x87, 0xc8, 0x01, 0x02, 0x03, 0x04];
        let input = build_invoke_calldata(INVOKE_SELECTOR, program_id_bytes(METEORA_DAMM_V1_ID_A), &data);
        let call = decode_cpi_calldata(CPI_PROGRAM_TO, &input).expect("Meteora swap invoke must decode");
        assert_eq!(call.program_label.as_deref(), Some("Meteora DAMM v1"));
        assert_eq!(call.instruction.as_deref(), Some("Swap"));
    }

    #[test]
    fn meteora_swap_resolves_by_curated_discriminator_id_b() {
        // Second on-chain program id for the same Meteora DAMM v1 instruction set.
        let data = [0xf8, 0xc6, 0x9e, 0x91, 0xe1, 0x75, 0x87, 0xc8, 0x01, 0x02, 0x03, 0x04];
        let input = build_invoke_calldata(INVOKE_SELECTOR, program_id_bytes(METEORA_DAMM_V1_ID_B), &data);
        let call = decode_cpi_calldata(CPI_PROGRAM_TO, &input).expect("Meteora swap invoke must decode");
        assert_eq!(call.program_label.as_deref(), Some("Meteora DAMM v1"));
        assert_eq!(call.instruction.as_deref(), Some("Swap"));
    }

    #[test]
    fn meteora_unknown_discriminator_falls_back_to_anchor_hex_but_keeps_label() {
        let data = [0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef];
        let input = build_invoke_calldata(INVOKE_SELECTOR, program_id_bytes(METEORA_DAMM_V1_ID_A), &data);
        let call = decode_cpi_calldata(CPI_PROGRAM_TO, &input).expect("well-formed invoke must decode");
        // The program is curated (we know its label) even though this particular
        // instruction discriminator isn't in the seeded map.
        assert_eq!(call.program_label.as_deref(), Some("Meteora DAMM v1"));
        assert_eq!(call.instruction.as_deref(), Some("anchor:deadbeefdeadbeef"));
    }

    #[test]
    fn golden_spl_approve_regression_unaffected_by_anchor_registry() {
        // S2's golden fixture must be untouched by the curated-Anchor addition —
        // native SPL/Token-2022 resolution still runs first and wins.
        let call = decode_cpi_calldata(CPI_PROGRAM_TO, GOLDEN_APPROVE)
            .expect("golden invoke fixture must decode");
        assert_eq!(call.program_label.as_deref(), Some("SPL-Token"));
        assert_eq!(call.instruction.as_deref(), Some("Approve"));
    }
}
