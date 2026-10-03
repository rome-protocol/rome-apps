//! Envelope + staleness decision for the stale-while-revalidate read cache.
//!
//! Pure functions split out of `cache.rs` so the freshness contract is
//! unit-testable without Redis. The cache stores `{"v": <payload>,
//! "fresh_until": <unix secs>}` under a PHYSICAL Redis expiry much longer than
//! the freshness window, so an aged entry is still servable: the reader
//! returns it immediately and refreshes in the background, instead of making
//! a user wait out the multi-second aggregate scan (measured 4–6s cold for
//! `throughput/cadence` + `timeseries?range=24h` on hadrian — the "TPS tile
//! is slow" report, 2026-07-30).

use serde_json::{json, Value};

/// How much longer an entry stays physically servable past its freshness
/// window. 12× a 5s TTL = a minute of stale-but-instant serving; floor keeps
/// rarely-hit endpoints from expiring between dashboard visits.
const PHYSICAL_FACTOR: u64 = 12;
const PHYSICAL_FLOOR_SECS: u64 = 300;

/// Physical Redis expiry for a freshness window.
pub fn physical_ttl(fresh_secs: u64) -> u64 {
    (fresh_secs * PHYSICAL_FACTOR).max(PHYSICAL_FLOOR_SECS)
}

/// Wrap a payload for storage.
pub fn envelope(value: &Value, now_unix: i64, fresh_secs: u64) -> String {
    json!({ "v": value, "fresh_until": now_unix + fresh_secs as i64 }).to_string()
}

/// Parse a cache entry. Returns `(payload, is_fresh)`.
///
/// Legacy entries (pre-envelope plain JSON) are served as STALE — valid to
/// show, refreshed in the background once, then re-written enveloped.
pub fn parse_envelope(raw: &str, now_unix: i64) -> Option<(Value, bool)> {
    let v: Value = serde_json::from_str(raw).ok()?;
    match (v.get("v"), v.get("fresh_until").and_then(Value::as_i64)) {
        (Some(payload), Some(fresh_until)) => Some((payload.clone(), now_unix < fresh_until)),
        _ => Some((v, false)), // legacy plain payload
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_entry_round_trips_and_reports_fresh() {
        let payload = json!({"tps": 27.1});
        let raw = envelope(&payload, 1_000, 5);
        let (got, fresh) = parse_envelope(&raw, 1_004).unwrap();
        assert_eq!(got, payload);
        assert!(fresh);
    }

    #[test]
    fn entry_past_fresh_until_is_stale_but_servable() {
        let payload = json!([1, 2, 3]);
        let raw = envelope(&payload, 1_000, 5);
        let (got, fresh) = parse_envelope(&raw, 1_005).unwrap();
        assert_eq!(got, payload);
        assert!(!fresh, "at fresh_until the entry must already count as stale");
    }

    #[test]
    fn legacy_plain_payload_is_served_stale() {
        // Entries written before the envelope existed are bare payloads.
        let (got, fresh) = parse_envelope(r#"{"tps": 1.0}"#, 0).unwrap();
        assert_eq!(got, json!({"tps": 1.0}));
        assert!(!fresh);
    }

    #[test]
    fn a_payload_that_itself_has_v_and_fresh_until_like_fields_is_unwrapped_correctly() {
        // The envelope test must not be fooled by payloads containing a "v"
        // key without the numeric fresh_until sibling.
        let (got, fresh) = parse_envelope(r#"{"v": "1.2.3"}"#, 0).unwrap();
        assert_eq!(got, json!({"v": "1.2.3"}));
        assert!(!fresh);
    }

    #[test]
    fn garbage_is_none() {
        assert!(parse_envelope("not json", 0).is_none());
    }

    #[test]
    fn physical_ttl_scales_with_floor() {
        assert_eq!(physical_ttl(5), 300); // floor wins
        assert_eq!(physical_ttl(60), 720); // 12× wins
    }
}
