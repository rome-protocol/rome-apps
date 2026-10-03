//! P5a canonical JCS serializer for `evidence_hash` — SHA-256 over exactly
//! the 9 canonical `audit.evidence_record` fields (IMPL-PLAN §14.2's
//! "re-verifiable from the row"), EXCLUDING `evidence_id` (DB surrogate),
//! `source_ref` (dedup key only, never rendered/hashed), and `evidence_hash`
//! itself.
//!
//! Full RFC 8785 (JCS) handles floats, nested objects/arrays, and Unicode
//! normalization the ECMAScript way. This payload shape has none of
//! that — 8 flat scalar fields, no floats, no nesting — so a hand-rolled
//! `BTreeMap<&str, serde_json::Value>` serialized via `serde_json::to_string`
//! already satisfies both properties RFC 8785 requires here: (a)
//! lexicographic key order (`BTreeMap`'s iteration order) and (b) integers
//! rendered as bare JSON integers (`serde_json::Number` from an `i64`,
//! never `f64`). `jcs_is_key_sorted_and_int_stable` (below) pins that claim
//! against a hand-written vector rather than trusting it.

use sha2::{Digest, Sha256};

/// The 9 fields that enter `evidence_hash`.
#[derive(Debug, Clone)]
pub struct CanonicalEvidence<'a> {
    pub identity_id: &'a str,
    pub kind: &'a str,
    pub vendor: &'a str,
    /// Exactly 32 bytes — rendered as lowercase `0x`-hex in the canonical form.
    pub subject_ref: &'a [u8],
    pub subject_ref_version: &'a str,
    pub status: &'a str,
    /// GREEN-only policy window (unix seconds); `None` renders as JSON `null`.
    pub valid_through: Option<i64>,
    pub vendor_timestamp: i64,
    pub received_at: i64,
}

/// Serializes into the canonical JSON form: object keys in ascending byte
/// order, integers as bare JSON integers, `null` for an absent
/// `valid_through`.
pub fn canonical_json(e: &CanonicalEvidence<'_>) -> String {
    let mut map: std::collections::BTreeMap<&str, serde_json::Value> = std::collections::BTreeMap::new();
    map.insert("identity_id", serde_json::Value::String(e.identity_id.to_string()));
    map.insert("kind", serde_json::Value::String(e.kind.to_string()));
    map.insert(
        "received_at",
        serde_json::Value::Number(e.received_at.into()),
    );
    map.insert("status", serde_json::Value::String(e.status.to_string()));
    map.insert(
        "subject_ref",
        serde_json::Value::String(format!("0x{}", hex::encode(e.subject_ref))),
    );
    map.insert(
        "subject_ref_version",
        serde_json::Value::String(e.subject_ref_version.to_string()),
    );
    map.insert(
        "valid_through",
        match e.valid_through {
            Some(v) => serde_json::Value::Number(v.into()),
            None => serde_json::Value::Null,
        },
    );
    map.insert("vendor", serde_json::Value::String(e.vendor.to_string()));
    map.insert(
        "vendor_timestamp",
        serde_json::Value::Number(e.vendor_timestamp.into()),
    );

    serde_json::to_string(&map).expect("canonical evidence fields always serialize")
}

/// SHA-256 over [`canonical_json`]'s UTF-8 bytes — this IS `evidence_hash`.
pub fn evidence_hash(e: &CanonicalEvidence<'_>) -> [u8; 32] {
    let json = canonical_json(e);
    let mut hasher = Sha256::new();
    hasher.update(json.as_bytes());
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test #19 (P5a plan) — byte-equals a HAND-WRITTEN JCS vector,
    /// including the `valid_through: null` case. Guards against silently
    /// switching the backing map to a `HashMap` (unordered) or rendering
    /// integers as JSON floats.
    #[test]
    fn jcs_is_key_sorted_and_int_stable() {
        let e = CanonicalEvidence {
            identity_id: "id-1",
            kind: "KYC_STATUS",
            vendor: "sumsub",
            subject_ref: &[0xab; 32],
            subject_ref_version: "v1",
            status: "GREEN",
            valid_through: Some(1_234_567_890),
            vendor_timestamp: 1_700_000_000,
            received_at: 1_700_000_005,
        };

        let expected = format!(
            "{{\"identity_id\":\"id-1\",\"kind\":\"KYC_STATUS\",\"received_at\":1700000005,\"status\":\"GREEN\",\"subject_ref\":\"0x{}\",\"subject_ref_version\":\"v1\",\"valid_through\":1234567890,\"vendor\":\"sumsub\",\"vendor_timestamp\":1700000000}}",
            "ab".repeat(32)
        );

        assert_eq!(canonical_json(&e), expected);
    }

    #[test]
    fn jcs_renders_null_for_absent_valid_through() {
        let e = CanonicalEvidence {
            identity_id: "id-2",
            kind: "SANCTIONS_SCREEN",
            vendor: "chainalysis",
            subject_ref: &[0x00; 32],
            subject_ref_version: "v1",
            status: "RED",
            valid_through: None,
            vendor_timestamp: 1_700_000_000,
            received_at: 1_700_000_001,
        };
        let json = canonical_json(&e);
        assert!(json.contains("\"valid_through\":null"), "json was: {json}");
    }

    #[test]
    fn evidence_hash_is_sha256_of_canonical_json() {
        let e = CanonicalEvidence {
            identity_id: "id-3",
            kind: "KYC_STATUS",
            vendor: "sumsub",
            subject_ref: &[0x11; 32],
            subject_ref_version: "v1",
            status: "GREEN",
            valid_through: Some(1),
            vendor_timestamp: 2,
            received_at: 3,
        };
        let json = canonical_json(&e);
        let mut hasher = Sha256::new();
        hasher.update(json.as_bytes());
        let want: [u8; 32] = hasher.finalize().into();
        assert_eq!(evidence_hash(&e), want);
    }
}
