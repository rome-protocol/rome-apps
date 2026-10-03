//! HMAC verify-BEFORE-parse (P5a §5.1 auth gate). Handlers take raw
//! `axum::body::Bytes` and call [`verify`] BEFORE any `serde_json::from_slice`
//! — a bad signature must reject a malformed-JSON body with the SAME 401
//! response a well-formed-but-unsigned body gets, never let parsing run
//! first and surface a 400 (`unsigned_request_is_401_before_parse` in
//! `tests/overlay_db.rs` pins the ordering).

use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Header carrying the unix-seconds request timestamp.
pub const TS_HEADER: &str = "x-bloom-audit-ts";
/// Header carrying the lowercase-hex `HMAC-SHA256` signature.
pub const SIG_HEADER: &str = "x-bloom-audit-sig";

/// Max allowed clock skew between the request's timestamp and the
/// server's own clock, in seconds — inclusive both ends (§5.1: `now-300`
/// accepted, `now-301` rejected).
const MAX_SKEW_SECS: i64 = 300;

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("missing or malformed header: {0}")]
    MissingOrMalformedHeader(&'static str),
    #[error("request timestamp is outside the {MAX_SKEW_SECS}s skew window")]
    StaleTimestamp,
    #[error("signature verification failed")]
    BadSignature,
}

/// Verifies an incoming `/overlay/*` request's HMAC signature.
///
/// `sig = lowercase-hex(HMAC-SHA256(secret, "{ts}{METHOD}{path}{raw}"))`
/// where `{raw}` is the exact request body bytes and `{ts}` is the
/// timestamp header's raw string value (not a re-serialization of the
/// parsed integer). Constant-time compare via [`Mac::verify_slice`] —
/// NEVER a `==` on hex strings or decoded bytes.
///
/// MED-4 (contract-pinned, §5.1 — do NOT change this formula): the
/// concatenation has no delimiter between `{ts}`/`{METHOD}`/`{path}`/`{raw}`,
/// so it is unambiguous only as long as no overlay route is a path-PREFIX
/// of another (e.g. a future `/overlay/evidence-batch` alongside
/// `/overlay/evidence` would let a signature computed for one replay
/// against the other by shifting the boundary into `{raw}`). Verified
/// true today (`IDENTITY_PATH`/`EVIDENCE_PATH` in `routes.rs` are not
/// prefixes of each other) — any new `/overlay/*` route MUST be checked
/// against every existing one before being added.
pub fn verify(
    secret: &[u8],
    ts_header: Option<&str>,
    sig_header: Option<&str>,
    method: &str,
    path: &str,
    raw: &[u8],
    now: i64,
) -> Result<(), AuthError> {
    let ts_str = ts_header.ok_or(AuthError::MissingOrMalformedHeader(TS_HEADER))?;
    let ts: i64 = ts_str
        .trim()
        .parse()
        .map_err(|_| AuthError::MissingOrMalformedHeader(TS_HEADER))?;

    let sig_str = sig_header.ok_or(AuthError::MissingOrMalformedHeader(SIG_HEADER))?;
    let sig_bytes = hex::decode(sig_str.trim()).map_err(|_| AuthError::MissingOrMalformedHeader(SIG_HEADER))?;

    // MED-1: `now - ts` on a crafted extreme `ts` (i64::MIN/MAX) can
    // overflow the subtraction — `checked_sub` turns that into a clean
    // rejection (`is_none_or(...)`) instead of a debug-build panic that
    // would reset the connection rather than 401 it.
    if now.checked_sub(ts).is_none_or(|d| d.abs() > MAX_SKEW_SECS) {
        return Err(AuthError::StaleTimestamp);
    }

    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC-SHA256 accepts any key length");
    mac.update(ts_str.as_bytes());
    mac.update(method.as_bytes());
    mac.update(path.as_bytes());
    mac.update(raw);

    mac.verify_slice(&sig_bytes).map_err(|_| AuthError::BadSignature)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sig_for(secret: &[u8], ts: i64, method: &str, path: &str, raw: &[u8]) -> String {
        let mut mac = HmacSha256::new_from_slice(secret).unwrap();
        mac.update(ts.to_string().as_bytes());
        mac.update(method.as_bytes());
        mac.update(path.as_bytes());
        mac.update(raw);
        hex::encode(mac.finalize().into_bytes())
    }

    #[test]
    fn valid_signature_within_skew_is_accepted() {
        let secret = b"a-shared-secret-at-least-32-bytes";
        let ts = 1_700_000_000i64;
        let raw = br#"{"a":1}"#;
        let sig = sig_for(secret, ts, "POST", "/overlay/identity", raw);
        assert!(verify(secret, Some(&ts.to_string()), Some(&sig), "POST", "/overlay/identity", raw, ts).is_ok());
    }

    #[test]
    fn wrong_secret_is_rejected() {
        let ts = 1_700_000_000i64;
        let raw = br#"{"a":1}"#;
        let sig = sig_for(b"secret-one-at-least-32-bytes-longx", ts, "POST", "/overlay/identity", raw);
        let err = verify(
            b"secret-two-at-least-32-bytes-longx",
            Some(&ts.to_string()),
            Some(&sig),
            "POST",
            "/overlay/identity",
            raw,
            ts,
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::BadSignature));
    }

    #[test]
    fn skew_boundary_300_ok_301_rejected() {
        let secret = b"a-shared-secret-at-least-32-bytes";
        let ts = 1_700_000_000i64;
        let raw = b"";
        let sig = sig_for(secret, ts, "POST", "/overlay/evidence", raw);

        assert!(verify(secret, Some(&ts.to_string()), Some(&sig), "POST", "/overlay/evidence", raw, ts + 300).is_ok());
        assert!(verify(secret, Some(&ts.to_string()), Some(&sig), "POST", "/overlay/evidence", raw, ts + 301).is_err());
    }

    /// MED-1: a crafted `ts` at an i64 extreme must be REJECTED (an `Err`),
    /// never overflow-panic `(now - ts).abs()` mid-subtraction — a panic
    /// here is a connection reset, not a clean 401.
    #[test]
    fn extreme_timestamps_are_rejected_not_panicked() {
        let secret = b"a-shared-secret-at-least-32-bytes";
        let now = 1_700_000_000i64;

        let sig_min = sig_for(secret, i64::MIN, "POST", "/overlay/identity", b"");
        assert!(
            verify(secret, Some(&i64::MIN.to_string()), Some(&sig_min), "POST", "/overlay/identity", b"", now).is_err(),
            "ts = i64::MIN must reject cleanly, not panic"
        );

        let sig_max = sig_for(secret, i64::MAX, "POST", "/overlay/identity", b"");
        assert!(
            verify(secret, Some(&i64::MAX.to_string()), Some(&sig_max), "POST", "/overlay/identity", b"", now).is_err(),
            "ts = i64::MAX must reject cleanly, not panic"
        );
    }

    /// LOW-3: only PAST skew was pinned before — a feeder clock running
    /// FAST (ts ahead of the server) must reject past the same boundary.
    #[test]
    fn future_skew_boundary_300_ok_301_rejected() {
        let secret = b"a-shared-secret-at-least-32-bytes";
        let ts = 1_700_000_000i64;
        let raw = b"";
        let sig = sig_for(secret, ts, "POST", "/overlay/evidence", raw);

        assert!(verify(secret, Some(&ts.to_string()), Some(&sig), "POST", "/overlay/evidence", raw, ts - 300).is_ok());
        assert!(verify(secret, Some(&ts.to_string()), Some(&sig), "POST", "/overlay/evidence", raw, ts - 301).is_err());
    }

    #[test]
    fn missing_headers_are_rejected() {
        let secret = b"a-shared-secret-at-least-32-bytes";
        assert!(verify(secret, None, Some("aa"), "POST", "/overlay/identity", b"", 0).is_err());
        assert!(verify(secret, Some("0"), None, "POST", "/overlay/identity", b"", 0).is_err());
    }
}
