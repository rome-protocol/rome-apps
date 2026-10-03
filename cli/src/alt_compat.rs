//! Vendored subset of rome-sdk's (now-deleted) `alt_manager` — the read-side
//! primitives the retained `alt health` / `alt retire` CLI verbs and the
//! `resources` reclaim/enumerate path still need, now that
//! `rome_solana::alt_manager` / `alt_provision` (and the on-chain `AltSlots`
//! program struct) are gone from the v1-migrated program + SDK.
//!
//! `AltTier` / `AltRecord` / `AltsManifest` / `contents_hash` /
//! `TableHealth` / `check_contents` / `build_deactivate_ix` are copied
//! **verbatim** from rome-sdk git `359c46e:rome-solana/src/alt_manager.rs`.
//! Byte-identical output is load-bearing — registry `chains/<id>/alts.json`
//! manifests hold hashes produced by the original; any drift here raises
//! false DRIFT alarms against Hadrian/Martius's live tables. Do not
//! "improve" the hash algorithm, seed encoding, or field names — see the
//! golden-vector tests below.
//!
//! Deliberately NOT vendored (provisioning-only, dropped with the v1
//! migration's cli split): `create`/`extend` ix builders, `extend_batches`,
//! `MAX_ADDRESSES_PER_EXTEND`, `derive_p1_addresses` / `derive_dapp_addresses`.
//!
//! The `AltSlots` read-only parser + PDA derivation below is vendored from
//! the on-chain program at `8ceb53a` (`program/src/state/pda.rs` +
//! `program/src/accounts/{account_type,ver,alt_id,alt_slots}.rs`) — the
//! struct itself was deleted from the merged program, but its account layout
//! is unchanged for every AltSlots PDA that predates the v1 upgrade, and the
//! CLI still needs to read (never write) it to enumerate/report stranded rent.

use {
    serde::{Deserialize, Serialize},
    solana_address_lookup_table_interface::instruction::deactivate_lookup_table,
    solana_sdk::{hash::hash, instruction::Instruction, pubkey::Pubkey},
    std::fmt::Write as _,
};

/// Serde adapter encoding a `Pubkey` as its base58 string in JSON — the
/// registry's canonical pubkey form.
mod pubkey_base58 {
    use {
        serde::{Deserialize, Deserializer, Serializer},
        solana_sdk::pubkey::Pubkey,
        std::str::FromStr,
    };

    pub fn serialize<S: Serializer>(pubkey: &Pubkey, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&pubkey.to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Pubkey, D::Error> {
        let s = String::deserialize(deserializer)?;
        Pubkey::from_str(&s).map_err(serde::de::Error::custom)
    }
}

/// Coverage tier of a persistent table. `Chain` (P1) / `Dapp` (P2) — kept only
/// as the manifest's recorded shape; provisioning either tier is gone.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AltTier {
    Chain,
    Dapp,
}

/// One provisioned lookup table, as recorded in `chains/<id>/alts.json`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AltRecord {
    #[serde(with = "pubkey_base58")]
    pub pubkey: Pubkey,
    pub tier: AltTier,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dapp: Option<String>,
    pub authority_kind: String,
    pub frozen: bool,
    pub contents_hash: String,
}

/// The `chains/<id>/alts.json` manifest.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AltsManifest {
    pub chain_id: String,
    #[serde(default)]
    pub tables: Vec<AltRecord>,
}

impl AltsManifest {
    /// `save`/`upsert` have no production caller now that provisioning is
    /// gone from the CLI (only `health`'s read-only `load` runs live) — kept
    /// for the manifest round-trip guarantee (`manifest_roundtrip` test) and
    /// for a future operator tool that needs to write `alts.json`.
    #[allow(dead_code)]
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    pub fn from_json(s: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(s)
    }

    /// Register a table: replace in place if the pubkey already exists (order
    /// preserved), otherwise append.
    #[allow(dead_code)]
    pub fn upsert(&mut self, record: AltRecord) {
        match self.tables.iter_mut().find(|r| r.pubkey == record.pubkey) {
            Some(existing) => *existing = record,
            None => self.tables.push(record),
        }
    }

    pub fn load(path: &std::path::Path) -> anyhow::Result<Self> {
        let s = std::fs::read_to_string(path)?;
        Ok(Self::from_json(&s)?)
    }

    #[allow(dead_code)]
    pub fn save(&self, path: &std::path::Path) -> anyhow::Result<()> {
        std::fs::write(path, self.to_json()?)?;
        Ok(())
    }
}

/// sha256-hex over an ordered address list — stable, order-sensitive,
/// append-sensitive. Registry manifests hold hashes produced by this exact
/// algorithm; changing it invalidates every recorded `contents_hash`.
pub fn contents_hash(addresses: &[Pubkey]) -> String {
    let mut bytes = Vec::with_capacity(addresses.len() * 32);
    for a in addresses {
        bytes.extend_from_slice(a.as_ref());
    }
    let digest = hash(&bytes);
    let mut out = String::with_capacity(64);
    for b in digest.as_ref() {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// Drift verdict for a registered table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TableHealth {
    Ok,
    Drift { expected: String, actual: String },
}

/// Compare a table's live on-chain contents against the recorded hash.
pub fn check_contents(fetched_addresses: &[Pubkey], expected_hash: &str) -> TableHealth {
    let actual = contents_hash(fetched_addresses);
    if actual == expected_hash {
        TableHealth::Ok
    } else {
        TableHealth::Drift {
            expected: expected_hash.to_string(),
            actual,
        }
    }
}

/// Build the `DeactivateLookupTable` instruction — starts the ~512-slot
/// cooldown before the table can be closed. Authority-held tables only.
pub fn build_deactivate_ix(table: &Pubkey, authority: &Pubkey) -> Instruction {
    deactivate_lookup_table(*table, *authority)
}

// ---------------------------------------------------------------------------
// AltSlots (read-only, vendored from the pre-v1 program at 8ceb53a)
// ---------------------------------------------------------------------------

/// `AccountType::AltSlots` discriminator (`program/src/accounts/account_type.rs`
/// at `8ceb53a`) — byte 0 of every AltSlots account.
const ALT_SLOTS_ACCOUNT_TYPE: u8 = 7;

/// Header length before the packed `[u64 LE]` slot list: 1 (AccountType) + 1
/// (Ver) + 8 (AltId.session_id) = 10 bytes. `#[repr(C, packed)]` throughout
/// (`ver.rs`, `alt_id.rs`, `alt_slots.rs` at `8ceb53a`) — no padding.
const ALT_SLOTS_HEADER_LEN: usize = 10;

/// The `ALT_SLOTS` PDA seed salt (`program/src/config.rs` at `8ceb53a`).
const ALT_SLOTS_SEED: &[u8] = b"ALT_SLOTS";

/// Derive an AltSlots holder PDA — mirrors the deleted `Pda::alt_slots_key`
/// (`program/src/state/pda.rs::holder_key` at `8ceb53a`): seeds
/// `[chain_id LE (8B), b"ALT_SLOTS", base (32B), index LE (8B)]`.
pub fn alt_slots_key(program_id: &Pubkey, chain_id: u64, base: &Pubkey, index: u64) -> Pubkey {
    let seeds: [&[u8]; 4] = [
        &chain_id.to_le_bytes(),
        ALT_SLOTS_SEED,
        base.as_ref(),
        &index.to_le_bytes(),
    ];
    Pubkey::find_program_address(&seeds, program_id).0
}

/// Parse an AltSlots account's raw data into its packed `u64` slot list.
/// Read-only: the on-chain `AltSlots` struct itself is gone from the
/// v1-migrated program (`AltDealloc` is `unsupported`), so this exists only
/// to enumerate/report — never to mutate — pre-upgrade holder accounts.
pub fn parse_alt_slots(data: &[u8]) -> anyhow::Result<Vec<u64>> {
    anyhow::ensure!(!data.is_empty(), "empty AltSlots account data");
    anyhow::ensure!(
        data[0] == ALT_SLOTS_ACCOUNT_TYPE,
        "not an AltSlots account: type byte {} (expected {ALT_SLOTS_ACCOUNT_TYPE})",
        data[0]
    );
    anyhow::ensure!(
        data.len() >= ALT_SLOTS_HEADER_LEN,
        "AltSlots account too short: {} bytes (header needs {ALT_SLOTS_HEADER_LEN})",
        data.len()
    );
    let body = &data[ALT_SLOTS_HEADER_LEN..];
    anyhow::ensure!(
        body.len().is_multiple_of(8),
        "AltSlots slot-list length {} is not a multiple of 8",
        body.len()
    );
    Ok(body
        .chunks_exact(8)
        .map(|c| u64::from_le_bytes(c.try_into().expect("chunks_exact(8)")))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pk(n: u8) -> Pubkey {
        Pubkey::new_from_array([n; 32])
    }

    /// `contents_hash` golden vector: pinned to the sha256-hex this exact
    /// algorithm (sha256 over the concatenated 32-byte pubkeys, lowercase
    /// hex — verbatim from rome-sdk `alt_manager::contents_hash`) produces
    /// for a fixed 2-pubkey list, so a future edit can't silently drift from
    /// every hash already recorded in a live registry `alts.json`. Reversed
    /// order asserts positional sensitivity (table indices are positional).
    #[test]
    fn contents_hash_golden() {
        let forward = contents_hash(&[pk(1), pk(2)]);
        let reversed = contents_hash(&[pk(2), pk(1)]);
        assert_eq!(
            forward,
            "f818afd37a6dc3bc92fb44731011277006db4efa6e9023cd7468c02335d22a4d"
        );
        assert_ne!(reversed, forward, "order must be significant");
        assert_eq!(forward.len(), 64, "sha256 as lowercase hex = 64 chars");
    }

    /// `parse_alt_slots` round-trips a synthetic AltSlots layout: type=7,
    /// ver, 8-byte session_id, then the packed u64 slot list.
    #[test]
    fn parse_alt_slots_roundtrip() {
        let mut data = vec![7u8, 0u8];
        data.extend_from_slice(&99u64.to_le_bytes()); // session_id
        for slot in [42u64, 7u64, i64::MAX as u64] {
            data.extend_from_slice(&slot.to_le_bytes());
        }
        let got = parse_alt_slots(&data).expect("valid AltSlots layout must parse");
        assert_eq!(got, vec![42, 7, i64::MAX as u64]);
    }

    #[test]
    fn parse_alt_slots_rejects_wrong_type() {
        let mut data = vec![3u8, 0u8]; // 3 = TxHolder, not AltSlots
        data.extend_from_slice(&0u64.to_le_bytes());
        data.extend_from_slice(&42u64.to_le_bytes());
        assert!(parse_alt_slots(&data).is_err());
    }

    #[test]
    fn parse_alt_slots_rejects_ragged_length() {
        let mut data = vec![7u8, 0u8];
        data.extend_from_slice(&0u64.to_le_bytes());
        data.push(1); // 1 stray byte — not a multiple of 8
        assert!(parse_alt_slots(&data).is_err());
    }

    #[test]
    fn parse_alt_slots_empty_slot_list_is_ok() {
        let mut data = vec![7u8, 0u8];
        data.extend_from_slice(&0u64.to_le_bytes());
        assert_eq!(parse_alt_slots(&data).unwrap(), Vec::<u64>::new());
    }

    /// `manifest_roundtrip`: load → upsert → save → load on a temp file
    /// preserves every registry field name verbatim (base58 pubkey, tier,
    /// dapp, authority_kind, frozen, contents_hash).
    #[test]
    fn manifest_roundtrip() {
        let mut m = AltsManifest {
            chain_id: "200010-hadrian".to_string(),
            tables: vec![],
        };
        m.upsert(AltRecord {
            pubkey: pk(1),
            tier: AltTier::Chain,
            dapp: None,
            authority_kind: "squads-v4-multisig".to_string(),
            frozen: false,
            contents_hash: contents_hash(&[pk(10), pk(11)]),
        });
        m.upsert(AltRecord {
            pubkey: pk(2),
            tier: AltTier::Dapp,
            dapp: Some("comet".to_string()),
            authority_kind: "squads-v4-multisig".to_string(),
            frozen: false,
            contents_hash: contents_hash(&[pk(20)]),
        });

        let path =
            std::env::temp_dir().join(format!("rome_alt_compat_test_{}.json", std::process::id()));
        m.save(&path).unwrap();
        let back = AltsManifest::load(&path).unwrap();
        std::fs::remove_file(&path).ok();

        assert_eq!(m, back, "load(save(m)) == m");
        // Field-name pin: the JSON is what a human/tool reads in the registry.
        let json = m.to_json().unwrap();
        assert!(json.contains("\"pubkey\""));
        assert!(json.contains("\"tier\""));
        assert!(json.contains("\"dapp\""));
        assert!(json.contains("\"authority_kind\""));
        assert!(json.contains("\"frozen\""));
        assert!(json.contains("\"contents_hash\""));
    }

    #[test]
    fn check_contents_detects_match_and_drift() {
        let addrs: Vec<Pubkey> = vec![pk(1), pk(2), pk(3)];
        let hash = contents_hash(&addrs);
        assert_eq!(check_contents(&addrs, &hash), TableHealth::Ok);
        assert!(matches!(
            check_contents(&addrs, "deadbeef"),
            TableHealth::Drift { .. }
        ));
    }

    /// `alt_slots_pda_matches_legacy_derivation`: a fixed (program_id, chain,
    /// payer, index) tuple must derive the exact PDA the deleted
    /// `Pda::alt_slots_key` (`8ceb53a:program/src/state/pda.rs::holder_key`)
    /// produced. Pinned as a base58 literal computed once (below) and
    /// hardcoded here — comparing against a second inline call to
    /// `Pubkey::find_program_address` with the same seed slice would only
    /// prove this function is deterministic, not that the seed order/encoding
    /// matches the deleted program code; a literal is the actual guard.
    #[test]
    fn alt_slots_pda_matches_legacy_derivation() {
        let program_id = Pubkey::new_from_array([9u8; 32]);
        let base = Pubkey::new_from_array([5u8; 32]);
        let chain_id = 200010u64;
        let index = 3u64;

        let got = alt_slots_key(&program_id, chain_id, &base, index);

        assert_eq!(
            got.to_string(),
            "AnpvVVhVicCvEnVxaR7VeP6fXp3Jc7rZeuRS8gWFh2ob"
        );
    }
}
