//! Default-off proxy batching config (Phase 3 plumbing).
//!
//! Mirrors the `jito_bundler` staging: this slice adds the config type and its
//! mapping to the SDK pack limits. Absent from `PROXY_CONFIG` ⇒ `None` on
//! [`crate::config::ProxyConfig`] ⇒ behavior identical to today. The
//! concurrent-request coalescer and the `eth_sendRawTransaction` dispatch land
//! in follow-on slices.

use rome_sdk::rome_evm_client::tx::PackLimits;

/// Operator-tunable batching knobs. Present in `PROXY_CONFIG` ⇒ batching is
/// available; absent ⇒ off.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct BatchingConfig {
    /// Max EVM txs packed into one Solana tx.
    #[serde(default = "default_max_pack_size")]
    pub max_pack_size: usize,
    /// Max composed Solana-tx size in bytes; clamped to the 1232 protocol cap.
    #[serde(default = "default_max_pack_bytes")]
    pub max_pack_bytes: usize,
    /// Max total compute units per pack; clamped to the 1.4M protocol cap.
    #[serde(default = "default_max_pack_cu")]
    pub max_pack_cu: u32,
    /// Number of packs that may be in flight concurrently.
    #[serde(default = "default_pack_concurrency")]
    pub pack_concurrency: usize,
    /// How long a *backlogged* forming pack waits to gather more txs before
    /// submitting (milliseconds). `0` (default) = off: submit as soon as the
    /// queue drains. A lone tx never waits regardless of this value.
    #[serde(default = "default_pack_fill_timeout_ms")]
    pub pack_fill_timeout_ms: u64,
    /// Bounded intake-queue capacity (jobs awaiting a packer lane). The queue is
    /// bounded, so when full it **backpressures** the submit (never drops). Raise
    /// it to absorb deeper bursts before backpressure latency kicks in. Default 1024.
    #[serde(default = "default_intake_capacity")]
    pub intake_capacity: usize,
}

fn default_max_pack_size() -> usize {
    4
}
fn default_max_pack_bytes() -> usize {
    1232
}
fn default_max_pack_cu() -> u32 {
    1_400_000
}
fn default_pack_concurrency() -> usize {
    1
}
fn default_pack_fill_timeout_ms() -> u64 {
    0
}
fn default_intake_capacity() -> usize {
    1024
}

impl BatchingConfig {
    /// Map to the SDK pack limits, clamping bytes/CU to the Solana protocol
    /// hard caps so an operator misconfig can never exceed them.
    ///
    /// Staged plumbing: the caller is the coalescer slice. Until then it has no
    /// non-test caller, so the release build (`-D warnings`, no `--tests`) would
    /// flag it as dead code — `allow` is removed when the coalescer wires it.
    #[allow(dead_code)]
    pub fn to_pack_limits(&self) -> PackLimits {
        let caps = PackLimits::solana();
        PackLimits {
            max_count: self.max_pack_size,
            max_bytes: self.max_pack_bytes.min(caps.max_bytes),
            max_cu: self.max_pack_cu.min(caps.max_cu),
            // Union-gate account ceiling is a protocol constant (v1 composer),
            // not an operator knob — no config field for it.
            max_accounts: caps.max_accounts,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_deserialize() {
        let c: BatchingConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(c.max_pack_size, 4);
        assert_eq!(c.max_pack_bytes, 1232);
        assert_eq!(c.max_pack_cu, 1_400_000);
        assert_eq!(c.pack_concurrency, 1);
        assert_eq!(c.pack_fill_timeout_ms, 0);
        assert_eq!(c.intake_capacity, 1024);
    }

    #[test]
    fn absent_section_is_none() {
        #[derive(serde::Deserialize)]
        struct W {
            #[serde(default)]
            batching: Option<BatchingConfig>,
        }
        let w: W = serde_json::from_str("{}").unwrap();
        assert!(w.batching.is_none(), "absent batching section ⇒ default-off");
    }

    #[test]
    fn to_pack_limits_clamps_to_solana_caps() {
        let c = BatchingConfig {
            max_pack_size: 6,
            max_pack_bytes: 5000,   // > 1232
            max_pack_cu: 9_000_000, // > 1.4M
            pack_concurrency: 1,
            pack_fill_timeout_ms: 5,
            intake_capacity: 1024,
        };
        let l = c.to_pack_limits();
        let caps = PackLimits::solana();
        assert_eq!(l.max_count, 6);
        assert!(c.max_pack_bytes > caps.max_bytes, "config must exceed the cap for this test");
        assert_eq!(l.max_bytes, caps.max_bytes, "bytes clamped to protocol cap");
        assert_eq!(l.max_cu, caps.max_cu, "cu clamped to protocol cap");
    }

    #[test]
    fn to_pack_limits_passes_through_within_caps() {
        let c = BatchingConfig {
            max_pack_size: 3,
            max_pack_bytes: 800,
            max_pack_cu: 500_000,
            pack_concurrency: 2,
            pack_fill_timeout_ms: 10,
            intake_capacity: 2048,
        };
        let l = c.to_pack_limits();
        assert_eq!(l.max_count, 3);
        assert_eq!(l.max_bytes, 800);
        assert_eq!(l.max_cu, 500_000);
    }

    #[test]
    fn intake_capacity_configurable() {
        let c: BatchingConfig = serde_json::from_str(r#"{"intake_capacity": 2048}"#).unwrap();
        assert_eq!(c.intake_capacity, 2048);
    }

    /// `PackLimits::max_accounts` (v1 composer union-gate ceiling, B2) is a
    /// protocol constant, not an operator knob — `to_pack_limits` must carry
    /// `PackLimits::solana().max_accounts` through regardless of the config.
    #[test]
    fn to_pack_limits_carries_union_account_ceiling() {
        let c: BatchingConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(
            c.to_pack_limits().max_accounts,
            PackLimits::solana().max_accounts
        );
    }
}
