use anyhow::{anyhow, Context, Result};
use ed25519_dalek::{Signature, VerifyingKey};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, HashSet};
use std::path::Path;

/// Recursively sort object keys so serialization is deterministic regardless of
/// input key insertion order. Arrays keep index order. Primitives pass through.
///
/// MUST be byte-identical to the JavaScript canonicalize() in
/// rome-app-registry/scripts/check-signature.ts. Any divergence breaks signatures
/// produced by the JS signer. The JS impl was patched in M1 (commit b5baf63) from
/// a buggy shallow sort to this recursive form — don't reintroduce the shallow
/// sort.
pub fn canonicalize(v: &Value) -> Value {
    match v {
        Value::Array(items) => Value::Array(items.iter().map(canonicalize).collect()),
        Value::Object(obj) => {
            let sorted: BTreeMap<&String, Value> =
                obj.iter().map(|(k, v)| (k, canonicalize(v))).collect();
            let mut out = Map::new();
            for (k, v) in sorted {
                out.insert(k.clone(), v);
            }
            Value::Object(out)
        }
        other => other.clone(),
    }
}

fn canonical_body(m: &Value) -> Result<String> {
    let obj = m.as_object().context("manifest is not an object")?;
    let mut filtered = obj.clone();
    filtered.remove("signature");
    filtered.remove("metrics_cache");
    let canonical = canonicalize(&Value::Object(filtered));
    Ok(serde_json::to_string(&canonical)?)
}

pub struct Verifier {
    registered: HashSet<[u8; 32]>,
}

impl Verifier {
    pub fn from_keys_dir(dir: &Path) -> Result<Self> {
        let mut set = HashSet::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            if entry.path().extension().map(|e| e == "pub").unwrap_or(false) {
                let raw = std::fs::read_to_string(entry.path())?;
                let bytes = hex::decode(raw.trim())?;
                let arr: [u8; 32] = bytes
                    .try_into()
                    .map_err(|_| anyhow!("pubkey not 32 bytes in {}", entry.path().display()))?;
                set.insert(arr);
            }
        }
        Ok(Self { registered: set })
    }
}

pub fn verify_manifest(v: &Verifier, manifest: &Value) -> Result<()> {
    let sig = manifest.get("signature").context("missing signature")?;
    let pub_hex = sig
        .get("pubkey")
        .and_then(|p| p.as_str())
        .context("missing signature.pubkey")?;
    let val_hex = sig
        .get("value")
        .and_then(|p| p.as_str())
        .context("missing signature.value")?;

    let pub_bytes: [u8; 32] = hex::decode(pub_hex)?
        .try_into()
        .map_err(|_| anyhow!("pubkey not 32 bytes"))?;
    if !v.registered.contains(&pub_bytes) {
        return Err(anyhow!("pubkey not registered"));
    }
    let sig_bytes: [u8; 64] = hex::decode(val_hex)?
        .try_into()
        .map_err(|_| anyhow!("signature not 64 bytes"))?;
    let verifying = VerifyingKey::from_bytes(&pub_bytes)?;
    let signature = Signature::from_bytes(&sig_bytes);
    let msg = canonical_body(manifest)?;
    verifying
        .verify_strict(msg.as_bytes(), &signature)
        .map_err(|e| anyhow!("signature verification failed: {}", e))?;
    Ok(())
}
