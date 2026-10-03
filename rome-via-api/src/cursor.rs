/// HMAC-signed opaque cursor tokens for stable pagination.
///
/// Format: `base64url(JSON payload) . base64url(HMAC-SHA256)`
///
/// The payload JSON is compact (no spaces). The HMAC covers the *encoded* payload
/// so the signature is unambiguous regardless of JSON key ordering.
///
/// # Security
/// Cursors are signed with a service secret. Tampered or expired cursors are rejected
/// with `AppError::CursorInvalid`. Rotating `cursor_secret` in config invalidates all
/// outstanding cursors — this is accepted as per SPEC §Security.
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use crate::error::AppError;

type HmacSha256 = Hmac<Sha256>;

/// Cursor for block list pagination (keyed by block number descending).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BlockCursor {
    pub chain_id: i64,
    /// The `params_block_number` of the last block returned.
    pub last_number: i64,
}

/// Cursor for transaction list pagination (keyed by slot + tx_idx descending).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TxCursor {
    pub chain_id: i64,
    /// The `slot_number` of the last tx's block.
    pub last_slot: i64,
    /// The `tx_idx` of the last tx within that slot.
    pub last_tx_idx: i32,
}

/// Cursor for the gated audit-events feed (keyed by the total-order triple
/// `(block_number, tx_index, log_index)` descending, within one chain).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AuditCursor {
    pub chain_id: i64,
    /// `block_number` of the last event returned.
    pub last_block: i64,
    /// `tx_index` of the last event within that block.
    pub last_tx_idx: i32,
    /// `log_index` of the last event within that tx.
    pub last_log_idx: i32,
}

/// Encode a serializable payload as a signed cursor token.
pub fn encode<T: Serialize>(payload: &T, secret: &[u8]) -> anyhow::Result<String> {
    let json = serde_json::to_string(payload)
        .map_err(|e| anyhow::anyhow!("cursor serialize failed: {e}"))?;
    let encoded_payload = URL_SAFE_NO_PAD.encode(json.as_bytes());

    let mut mac = HmacSha256::new_from_slice(secret)
        .map_err(|e| anyhow::anyhow!("HMAC init failed: {e}"))?;
    mac.update(encoded_payload.as_bytes());
    let signature = mac.finalize().into_bytes();
    let encoded_sig = URL_SAFE_NO_PAD.encode(&signature[..]);

    Ok(format!("{}.{}", encoded_payload, encoded_sig))
}

/// Decode and verify a cursor token. Returns `AppError::CursorInvalid` on tamper/malform.
pub fn decode<T: for<'de> Deserialize<'de>>(token: &str, secret: &[u8]) -> Result<T, AppError> {
    let dot = token
        .rfind('.')
        .ok_or_else(|| AppError::CursorInvalid("cursor missing separator".to_string()))?;

    let encoded_payload = &token[..dot];
    let encoded_sig = &token[dot + 1..];

    // Verify HMAC before decoding payload.
    let mut mac = HmacSha256::new_from_slice(secret)
        .map_err(|e| AppError::CursorInvalid(format!("HMAC init: {e}")))?;
    mac.update(encoded_payload.as_bytes());

    let expected_sig = URL_SAFE_NO_PAD
        .decode(encoded_sig)
        .map_err(|_| AppError::CursorInvalid("cursor signature not valid base64".to_string()))?;

    mac.verify_slice(&expected_sig)
        .map_err(|_| AppError::CursorInvalid("cursor signature mismatch".to_string()))?;

    // Signature verified — safe to decode payload.
    let payload_bytes = URL_SAFE_NO_PAD
        .decode(encoded_payload)
        .map_err(|_| AppError::CursorInvalid("cursor payload not valid base64".to_string()))?;

    serde_json::from_slice(&payload_bytes)
        .map_err(|e| AppError::CursorInvalid(format!("cursor payload malformed: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &[u8] = b"test-secret-key";

    #[test]
    fn block_cursor_roundtrip() {
        let cursor = BlockCursor {
            chain_id: 121220,
            last_number: 12345,
        };
        let token = encode(&cursor, SECRET).expect("encode should succeed");
        let decoded: BlockCursor = decode(&token, SECRET).expect("decode should succeed");
        assert_eq!(cursor, decoded);
    }

    #[test]
    fn tx_cursor_roundtrip() {
        let cursor = TxCursor {
            chain_id: 121220,
            last_slot: 99887766,
            last_tx_idx: 42,
        };
        let token = encode(&cursor, SECRET).expect("encode should succeed");
        let decoded: TxCursor = decode(&token, SECRET).expect("decode should succeed");
        assert_eq!(cursor, decoded);
    }

    #[test]
    fn audit_cursor_roundtrip() {
        let cursor = AuditCursor {
            chain_id: 121220,
            last_block: 987654,
            last_tx_idx: 7,
            last_log_idx: 3,
        };
        let token = encode(&cursor, SECRET).expect("encode should succeed");
        let decoded: AuditCursor = decode(&token, SECRET).expect("decode should succeed");
        assert_eq!(cursor, decoded);
    }

    #[test]
    fn tampered_payload_rejected() {
        let cursor = BlockCursor {
            chain_id: 121220,
            last_number: 1,
        };
        let token = encode(&cursor, SECRET).expect("encode should succeed");
        // Flip a character in the payload segment to simulate tampering.
        let mut tampered = token.clone();
        let first_char = tampered.chars().next().unwrap();
        let replacement = if first_char == 'a' { 'b' } else { 'a' };
        tampered = replacement.to_string() + &tampered[first_char.len_utf8()..];

        let result: Result<BlockCursor, AppError> = decode(&tampered, SECRET);
        assert!(
            result.is_err(),
            "tampered cursor should be rejected: {tampered}"
        );
    }

    #[test]
    fn wrong_secret_rejected() {
        let cursor = BlockCursor {
            chain_id: 121220,
            last_number: 1,
        };
        let token = encode(&cursor, SECRET).expect("encode should succeed");
        let result: Result<BlockCursor, AppError> = decode(&token, b"wrong-secret");
        assert!(result.is_err(), "cursor signed with wrong secret should be rejected");
    }

    #[test]
    fn missing_separator_rejected() {
        let result: Result<BlockCursor, AppError> = decode("notasignedtoken", SECRET);
        assert!(result.is_err());
    }
}
