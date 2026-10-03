//! P5a overlay HTTP handlers — two routes, one transaction each.
//!
//! Order in every handler: [`crate::overlay::auth::verify`] against the RAW
//! body BEFORE any `serde_json::from_slice` — the PII canary
//! (`pii_canary_structural_reject` in `tests/overlay_db.rs`) depends on
//! `#[serde(deny_unknown_fields)]` running at all, which only happens once
//! the signature has already passed.

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;

use super::{auth, canonical, OverlayState};

pub const IDENTITY_PATH: &str = "/overlay/identity";
pub const EVIDENCE_PATH: &str = "/overlay/evidence";

pub fn router(state: OverlayState) -> Router {
    Router::new()
        .route(IDENTITY_PATH, post(identity_handler))
        .route(EVIDENCE_PATH, post(evidence_handler))
        .with_state(state)
}

// ---- shared response helpers -------------------------------------------

fn unauthorized() -> Response {
    (StatusCode::UNAUTHORIZED, "unauthorized").into_response()
}

fn bad_request(msg: impl Into<String>) -> Response {
    (StatusCode::BAD_REQUEST, Json(json!({ "error": msg.into() }))).into_response()
}

fn unprocessable(msg: impl Into<String>) -> Response {
    (StatusCode::UNPROCESSABLE_ENTITY, Json(json!({ "error": msg.into() }))).into_response()
}

fn conflict(msg: impl Into<String>) -> Response {
    (StatusCode::CONFLICT, Json(json!({ "error": msg.into() }))).into_response()
}

/// LOW-1: the caller gets a generic message; the detail (which can carry a
/// DSN fragment, a constraint name, or other internal shape) goes to the
/// trace log only, never into the HTTP response body.
fn internal_error(e: impl std::fmt::Display) -> Response {
    tracing::error!(error = %e, "overlay: internal error");
    (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": "internal error" }))).into_response()
}

/// NIT: an unbounded TEXT field landing in an append-only store is a
/// permanent bad row — same class of hole as MED-2/MED-3. Generous enough
/// for every legitimate value in this schema (identity ids, vendor names,
/// source refs, status strings, version tags), tight enough to reject a
/// pasted document.
const MAX_TEXT_FIELD_LEN: usize = 256;

fn check_len(field: &'static str, s: &str) -> Result<(), String> {
    if s.len() > MAX_TEXT_FIELD_LEN {
        return Err(format!("{field} exceeds the {MAX_TEXT_FIELD_LEN}-char limit"));
    }
    Ok(())
}

/// HMAC-verifies the request BEFORE any body parse. Returns the 401
/// response to short-circuit with on failure.
// Err is boxed (`clippy::result_large_err`): a full `axum::Response` is >128
// bytes, and this returns one purely to short-circuit the handler with.
fn verify_request(state: &OverlayState, headers: &HeaderMap, method: &str, path: &str, raw: &[u8]) -> Result<(), Box<Response>> {
    let ts = headers.get(auth::TS_HEADER).and_then(|v| v.to_str().ok());
    let sig = headers.get(auth::SIG_HEADER).and_then(|v| v.to_str().ok());
    let now = (state.now)();
    auth::verify(&state.secret, ts, sig, method, path, raw, now).map_err(|_| Box::new(unauthorized()))
}

fn is_foreign_key_violation(e: &sqlx::Error) -> bool {
    e.as_database_error()
        .and_then(|db| db.code())
        .map(|code| code == "23503")
        .unwrap_or(false)
}

// ---- shape checks (H1-style manual byte checks, no regex dep) ---------

/// `0x` + 40 lowercase hex chars — 20 bytes. Used for both EVM `root_key`
/// and `synthetic_evm_address`.
fn is_hex20_lowercase(s: &str) -> bool {
    match s.strip_prefix("0x") {
        Some(hex) => hex.len() == 40 && hex.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        None => false,
    }
}

/// `0x` + 64 lowercase hex chars — 32 bytes.
fn is_hex32_lowercase(s: &str) -> bool {
    match s.strip_prefix("0x") {
        Some(hex) => hex.len() == 64 && hex.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        None => false,
    }
}

fn decode_hex(s: &str) -> Vec<u8> {
    hex::decode(s.trim_start_matches("0x")).expect("caller has already shape-checked this string")
}

/// Base58 alphabet (Bitcoin/Solana) — excludes `0`, `O`, `I`, `l`.
const BASE58_ALPHABET: &[u8] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

fn is_solana_root_key_shape(s: &str) -> bool {
    (32..=44).contains(&s.len()) && s.bytes().all(|b| BASE58_ALPHABET.contains(&b))
}

// ---- POST /overlay/identity ---------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityKeyIn {
    root_key_type: String,
    root_key: String,
    derivation_fn_version: String,
    #[serde(default)]
    synthetic_evm_address: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityIn {
    identity_id: String,
    keys: Vec<IdentityKeyIn>,
}

/// Shape-checks one key per the P5a contract: EVM `root_key` is
/// `^0x[0-9a-f]{40}$` and MUST NOT carry a `synthetic_evm_address`; SOLANA
/// `root_key` is base58 32-44 chars and MUST carry a 20-byte
/// `synthetic_evm_address`.
fn validate_key_shape(k: &IdentityKeyIn) -> Result<(), &'static str> {
    match k.root_key_type.as_str() {
        "EVM" => {
            if !is_hex20_lowercase(&k.root_key) {
                return Err("EVM root_key must be 0x + 40 lowercase hex chars");
            }
            if k.synthetic_evm_address.is_some() {
                return Err("EVM keys must not carry a synthetic_evm_address");
            }
            Ok(())
        }
        "SOLANA" => {
            if !is_solana_root_key_shape(&k.root_key) {
                return Err("SOLANA root_key must be base58, 32-44 chars");
            }
            match &k.synthetic_evm_address {
                Some(addr) if is_hex20_lowercase(addr) => Ok(()),
                _ => Err("SOLANA keys require a 20-byte synthetic_evm_address"),
            }
        }
        _ => Err("root_key_type must be SOLANA or EVM"),
    }
}

async fn identity_handler(State(state): State<OverlayState>, headers: HeaderMap, body: Bytes) -> Response {
    if let Err(resp) = verify_request(&state, &headers, "POST", IDENTITY_PATH, &body) {
        return *resp;
    }

    let payload: IdentityIn = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => return bad_request(format!("invalid identity payload: {e}")),
    };

    if let Err(msg) = check_len("identity_id", &payload.identity_id) {
        return unprocessable(msg);
    }

    for k in &payload.keys {
        if let Err(msg) = check_len("derivation_fn_version", &k.derivation_fn_version) {
            return unprocessable(msg);
        }
        if let Err(msg) = validate_key_shape(k) {
            return unprocessable(msg);
        }
    }

    let now = (state.now)();
    let mut tx = match state.pool.begin().await {
        Ok(tx) => tx,
        Err(e) => return internal_error(e),
    };

    // ON CONFLICT DO NOTHING only — an existing identity row is NEVER
    // touched (the append-only trigger would reject an UPDATE anyway).
    if let Err(e) = sqlx::query(
        "INSERT INTO audit.identity (identity_id, created_at) VALUES ($1,$2) ON CONFLICT (identity_id) DO NOTHING",
    )
    .bind(&payload.identity_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    {
        return internal_error(e);
    }

    let mut keys_appended: i64 = 0;
    let mut keys_existing: i64 = 0;

    for k in &payload.keys {
        let synthetic: Option<Vec<u8>> = k.synthetic_evm_address.as_deref().map(decode_hex);

        let result = sqlx::query(
            "INSERT INTO audit.identity_key (identity_id, root_key_type, root_key, derivation_fn_version, synthetic_evm_address, added_at)
             VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (root_key, derivation_fn_version) DO NOTHING",
        )
        .bind(&payload.identity_id)
        .bind(&k.root_key_type)
        .bind(&k.root_key)
        .bind(&k.derivation_fn_version)
        .bind(&synthetic)
        .bind(now)
        .execute(&mut *tx)
        .await;

        let rows_affected = match result {
            Ok(r) => r.rows_affected(),
            Err(e) => return internal_error(e),
        };

        if rows_affected == 1 {
            keys_appended += 1;
            continue;
        }

        // Conflict on (root_key, derivation_fn_version): a replay of the
        // SAME identity's SAME key is idempotent (keys_existing++); a
        // different identity claiming an already-owned key is a foreign
        // ownership conflict — roll back the WHOLE request, never silently
        // DO NOTHING (that would let identity B silently "steal" identity
        // A's key with a 200).
        let existing: Option<(String, String, Option<Vec<u8>>)> = match sqlx::query_as(
            "SELECT identity_id, root_key_type, synthetic_evm_address FROM audit.identity_key
             WHERE root_key = $1 AND derivation_fn_version = $2",
        )
        .bind(&k.root_key)
        .bind(&k.derivation_fn_version)
        .fetch_optional(&mut *tx)
        .await
        {
            Ok(row) => row,
            Err(e) => return internal_error(e),
        };

        match existing {
            Some((eid, ekind, esynth))
                if eid == payload.identity_id && ekind == k.root_key_type && esynth == synthetic =>
            {
                keys_existing += 1;
            }
            _ => return conflict("key already bound to a different identity"),
        }
    }

    if let Err(e) = tx.commit().await {
        return internal_error(e);
    }

    Json(json!({ "keys_appended": keys_appended, "keys_existing": keys_existing })).into_response()
}

// ---- POST /overlay/evidence ----------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EvidenceIn {
    source_ref: String,
    identity_id: String,
    kind: String,
    vendor: String,
    subject_ref: String,
    subject_ref_version: String,
    status: String,
    #[serde(default)]
    valid_through: Option<i64>,
    vendor_timestamp: i64,
}

async fn evidence_handler(State(state): State<OverlayState>, headers: HeaderMap, body: Bytes) -> Response {
    if let Err(resp) = verify_request(&state, &headers, "POST", EVIDENCE_PATH, &body) {
        return *resp;
    }

    let payload: EvidenceIn = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => return bad_request(format!("invalid evidence payload: {e}")),
    };

    // MED-2: `status` is free text into an append-only store — a whitelist
    // failure here is a PERMANENT bad row, not a correctable one. Kept in
    // the handler (not a rigid SQL CHECK) because SANCTIONS_SCREEN's future
    // vocabulary shouldn't need a migration to grow the accepted set.
    match payload.kind.as_str() {
        "KYC_STATUS" => {
            if !matches!(payload.status.as_str(), "GREEN" | "RED") {
                return unprocessable("KYC_STATUS status must be exactly GREEN or RED");
            }
        }
        "SANCTIONS_SCREEN" => {
            return unprocessable("SANCTIONS_SCREEN evidence not yet accepted (feeder undefined)");
        }
        _ => return unprocessable("kind must be KYC_STATUS or SANCTIONS_SCREEN"),
    }
    for (field, value) in [
        ("identity_id", &payload.identity_id),
        ("vendor", &payload.vendor),
        ("source_ref", &payload.source_ref),
        ("status", &payload.status),
        ("subject_ref_version", &payload.subject_ref_version),
    ] {
        if let Err(msg) = check_len(field, value) {
            return unprocessable(msg);
        }
    }
    if !is_hex32_lowercase(&payload.subject_ref) {
        return unprocessable("subject_ref must be 0x + 64 lowercase hex chars (32 bytes)");
    }
    if payload.valid_through.is_some() && payload.status != "GREEN" {
        return unprocessable("valid_through is only legal alongside a GREEN status");
    }
    let subject_ref = decode_hex(&payload.subject_ref);

    let now = (state.now)();
    let hash = canonical::evidence_hash(&canonical::CanonicalEvidence {
        identity_id: &payload.identity_id,
        kind: &payload.kind,
        vendor: &payload.vendor,
        subject_ref: &subject_ref,
        subject_ref_version: &payload.subject_ref_version,
        status: &payload.status,
        valid_through: payload.valid_through,
        vendor_timestamp: payload.vendor_timestamp,
        received_at: now,
    });

    let inserted: Option<(i64,)> = match sqlx::query_as(
        "INSERT INTO audit.evidence_record
            (source_ref, identity_id, kind, vendor, subject_ref, subject_ref_version, status, valid_through, vendor_timestamp, received_at, evidence_hash)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
         ON CONFLICT (source_ref) DO NOTHING
         RETURNING evidence_id",
    )
    .bind(&payload.source_ref)
    .bind(&payload.identity_id)
    .bind(&payload.kind)
    .bind(&payload.vendor)
    .bind(&subject_ref)
    .bind(&payload.subject_ref_version)
    .bind(&payload.status)
    .bind(payload.valid_through)
    .bind(payload.vendor_timestamp)
    .bind(now)
    .bind(hash.to_vec())
    .fetch_optional(&state.pool)
    .await
    {
        Ok(row) => row,
        Err(e) => {
            if is_foreign_key_violation(&e) {
                return unprocessable("identity_id does not exist");
            }
            return internal_error(e);
        }
    };

    if inserted.is_some() {
        return Json(json!({ "result": "inserted", "evidence_hash": format!("0x{}", hex::encode(hash)) })).into_response();
    }

    // Conflict on source_ref — compare stored FED fields (never
    // received_at/evidence_id) against the incoming payload. `evidence_hash`
    // is fetched too (LOW-5) so a redelivering feeder gets back the SAME
    // hash a first-time insert would have returned, to reconcile against —
    // it is read-only here, never part of the equality comparison.
    #[allow(clippy::type_complexity)]
    let existing: Option<(String, String, String, Vec<u8>, String, String, Option<i64>, i64, Vec<u8>)> = match sqlx::query_as(
        "SELECT identity_id, kind, vendor, subject_ref, subject_ref_version, status, valid_through, vendor_timestamp, evidence_hash
         FROM audit.evidence_record WHERE source_ref = $1",
    )
    .bind(&payload.source_ref)
    .fetch_optional(&state.pool)
    .await
    {
        Ok(row) => row,
        Err(e) => return internal_error(e),
    };

    match existing {
        Some((eid, ekind, evendor, esubj, esubjv, estatus, evalid, evendorts, ehash))
            if eid == payload.identity_id
                && ekind == payload.kind
                && evendor == payload.vendor
                && esubj == subject_ref
                && esubjv == payload.subject_ref_version
                && estatus == payload.status
                && evalid == payload.valid_through
                && evendorts == payload.vendor_timestamp =>
        {
            Json(json!({ "result": "deduped", "evidence_hash": format!("0x{}", hex::encode(&ehash)) })).into_response()
        }
        _ => conflict("evidence with this source_ref already exists with different content"),
    }
}
