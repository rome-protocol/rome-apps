//! Verified-contract label worker.
//!
//! Any contract VERIFIED on this chain's Rome Sourcify instance gets its
//! verified compilation name promoted into `rome_via.contract_labels`
//! (`provenance = "verified"`), so it shows its name everywhere a label is
//! joined — the `/addresses` list, search, and a tx's `to` — not just on its
//! own contract page (today `verified` is a frontend-only concept; see
//! `rome-via/src/lib/verifier.ts`).
//!
//! ## Disabled by default
//!
//! `config.verifier_url` is `None` unless the deployment TOML sets it. When
//! `None`, [`run`] logs once and returns `Ok(())` — the supervisor treats a
//! clean `Ok(())` as "worker finished, don't restart" (see `supervisor.rs`),
//! so this is a graceful no-op, not a crash loop. Chains without a Sourcify
//! instance projected are unaffected.
//!
//! ## Candidate selection + TTL re-poll
//!
//! Each pass polls [`verified_candidates_sql`]: contract rows (`has_code =
//! true`) not already `provenance IN ('verified', 'registry')` — i.e. not
//! already curated or confirmed-verified — bounded per poll, ordered so
//! never-checked rows (`verified_checked_at IS NULL`) go first. A row that
//! comes back "not verified (yet)" is stamped `verified_checked_at = NOW()`
//! and only re-checked after `RECHECK_TTL_SECS`, so a contract verified
//! *after* first-seen is eventually picked up without re-hitting Sourcify for
//! every unverified contract on every poll.
//!
//! ## Rank-guarded write
//!
//! The upsert goes through [`label_provenance::GUARD_WHERE_CLAUSE`] like every
//! other `contract_labels` writer, so a `registry`-curated label (rank 3)
//! can't be downgraded by a `verified` write (rank 2) — though in practice the
//! candidate query already excludes `registry` rows.
//!
//! ## Outage vs. genuine "not verified" ([`VerifyOutcome`])
//!
//! A Sourcify response is classified into three outcomes, not two:
//! [`VerifyOutcome::Verified`] (2xx + a real match), [`VerifyOutcome::NotVerified`]
//! (2xx with `match: null`, or 404 — both are Sourcify **definitively**
//! answering "no"), and [`VerifyOutcome::Transient`] (transport error, 5xx,
//! 429, any other non-2xx/non-404 status, or a non-JSON body). Only
//! `NotVerified` stamps `verified_checked_at` (so it waits out the TTL);
//! `Transient` does **not** stamp, leaving the row a candidate for the next
//! poll. Conflating the two (as an earlier version of this worker did) means
//! a Sourcify outage stamps every in-flight candidate as "checked", so a
//! contract verified during that outage window would wait a full TTL to
//! surface — the same outage/success conflation flagged in the #519 review.

use std::time::Duration;

use sqlx::PgPool;
use tracing::{debug, info, warn};

use super::label_provenance;

/// How long to wait before re-checking a contract that was NOT verified on
/// its last check — long enough that we're not hammering Sourcify for every
/// unverified contract on every poll, short enough that a contract verified
/// shortly after deploy shows up within a day.
const RECHECK_TTL_SECS: i64 = 24 * 3600;

/// Minimum gap between Sourcify requests — a simple rate limit (~3 req/s),
/// mirroring `method_decoder`'s token-bucket-by-sleep pattern for
/// 4byte.directory.
const RATE_GAP: Duration = Duration::from_millis(300);

/// Candidate contracts for a verified-label check: has code, not already
/// `verified` or `registry` provenance, and either never checked
/// (`verified_checked_at IS NULL`) or checked more than `$2` seconds ago.
/// Backed by `ix_contract_labels_verified_candidates` (migration 0236).
pub fn verified_candidates_sql() -> &'static str {
    r#"
    SELECT address
      FROM rome_via.contract_labels
     WHERE chain_id = $1
       AND has_code = true
       AND provenance NOT IN ('verified', 'registry')
       AND (verified_checked_at IS NULL
            OR verified_checked_at < NOW() - ($2 * INTERVAL '1 second'))
     ORDER BY verified_checked_at ASC NULLS FIRST
     LIMIT $3
    "#
}

/// Map a Sourcify `GET /v2/contract/{chainId}/{address}?fields=compilation`
/// response body to a verified compilation name.
///
/// `match` must be present and non-null (`"exact_match"` or `"match"` both
/// count — Sourcify's two verification strengths; either means "verified" for
/// our purposes) AND `compilation.name` must be present and non-empty.
/// Anything else — `match` absent, `match` null (not verified), missing
/// `compilation`/`name`, or a malformed body — returns `None`.
///
/// Pure function — fully unit-testable, no I/O.
pub fn parse_verified_name(body: &serde_json::Value) -> Option<String> {
    let m = body.get("match")?;
    if m.is_null() {
        return None;
    }
    let name = body.get("compilation")?.get("name")?.as_str()?.trim();
    if name.is_empty() {
        return None;
    }
    Some(name.to_string())
}

/// Three-way classification of a Sourcify check, so a definitive "no" can be
/// distinguished from an outage (S1 fix — see module doc "Outage vs. genuine
/// 'not verified'"). Only `NotVerified` is safe to cache with a TTL;
/// `Transient` must be retried, not stamped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerifyOutcome {
    /// 2xx + a real match — the compilation name to promote.
    Verified(String),
    /// Sourcify definitively answered "not verified": 2xx with `match: null`,
    /// or 404 (no record for this address).
    NotVerified,
    /// Couldn't get a definitive answer this pass: transport failure, 5xx,
    /// 429, any other non-2xx/non-404 status, or an unparseable body.
    Transient,
}

/// Classify a Sourcify HTTP response (status + raw body) into a
/// [`VerifyOutcome`]. Pure function — fully unit-testable, no I/O.
///
/// * `404` → `NotVerified` (Sourcify's "no record for this address").
/// * any other non-2xx (5xx, 429, …) → `Transient` — don't cache a "no" for
///   what might just be Sourcify being down.
/// * 2xx with an unparseable body → `Transient` (same reasoning — a 2xx with
///   garbage in it is not a confident negative).
/// * 2xx with a parseable body → delegate to [`parse_verified_name`]:
///   `Some(name)` → `Verified(name)`, `None` (includes `match: null`) →
///   `NotVerified` (a clean 2xx answer IS definitive).
pub fn classify_response(status: reqwest::StatusCode, body: &str) -> VerifyOutcome {
    if status == reqwest::StatusCode::NOT_FOUND {
        return VerifyOutcome::NotVerified;
    }
    if !status.is_success() {
        return VerifyOutcome::Transient;
    }
    match serde_json::from_str::<serde_json::Value>(body) {
        Ok(v) => match parse_verified_name(&v) {
            Some(name) => VerifyOutcome::Verified(name),
            None => VerifyOutcome::NotVerified,
        },
        Err(e) => {
            debug!(error = %e, "sourcify 2xx response was not valid JSON");
            VerifyOutcome::Transient
        }
    }
}

/// `GET {verifier_url}/v2/contract/{chain_id}/{address}?fields=compilation`.
/// A transport failure (send error) is `Transient` — same reasoning as a
/// non-2xx status: don't cache a confident "no" for a network hiccup.
async fn fetch_verify_outcome(
    client: &reqwest::Client,
    verifier_url: &str,
    chain_id: i64,
    address: &str,
) -> VerifyOutcome {
    let url = format!(
        "{}/v2/contract/{}/{}?fields=compilation",
        verifier_url.trim_end_matches('/'),
        chain_id,
        address
    );
    let resp = match client.get(&url).send().await {
        Ok(r) => r,
        Err(e) => {
            debug!(%address, error = %e, "sourcify request failed");
            return VerifyOutcome::Transient;
        }
    };
    let status = resp.status();
    let body = match resp.text().await {
        Ok(b) => b,
        Err(e) => {
            debug!(%address, error = %e, "sourcify response body unreadable");
            return VerifyOutcome::Transient;
        }
    };
    classify_response(status, &body)
}

/// Rank-guarded promotion of a verified compilation name into
/// `contract_labels`. Only `display_label`, `has_code`, `provenance`, and
/// `verified_checked_at` are touched — `raw_name`/`raw_symbol`/
/// `display_label_detail` (the on-chain audit trail from `contract_labels.rs`)
/// are left as-is.
async fn upsert_verified(pool: &PgPool, chain_id: i64, address: &str, name: &str) {
    let sql = format!(
        r#"
        INSERT INTO rome_via.contract_labels
            (chain_id, address, display_label, has_code, provenance, verified_checked_at, updated_at)
        VALUES ($1, $2, $3, true, 'verified', NOW(), NOW())
        ON CONFLICT (chain_id, address) DO UPDATE
          SET display_label       = EXCLUDED.display_label,
              has_code            = EXCLUDED.has_code,
              provenance          = EXCLUDED.provenance,
              verified_checked_at = EXCLUDED.verified_checked_at,
              updated_at          = NOW()
        WHERE {}
        "#,
        label_provenance::GUARD_WHERE_CLAUSE
    );
    let res = sqlx::query(&sql)
        .bind(chain_id)
        .bind(address)
        .bind(name)
        .execute(pool)
        .await;
    match res {
        Ok(_) => info!(%address, name = %name, "labeled verified contract"),
        Err(e) => warn!(%address, error = %e, "failed to store verified label"),
    }
}

/// Stamp a candidate as checked (not verified this pass) without touching its
/// label/provenance — no guard needed, this never writes `display_label`.
async fn stamp_checked(pool: &PgPool, chain_id: i64, address: &str) {
    let res = sqlx::query(
        "UPDATE rome_via.contract_labels
            SET verified_checked_at = NOW(), updated_at = NOW()
          WHERE chain_id = $1 AND address = $2",
    )
    .bind(chain_id)
    .bind(address)
    .execute(pool)
    .await;
    if let Err(e) = res {
        warn!(%address, error = %e, "failed to stamp verified_checked_at");
    }
}

pub async fn run(
    pool: PgPool,
    chain_id: i64,
    verifier_url: Option<String>,
    poll_interval: Duration,
    batch_size: i64,
) -> anyhow::Result<()> {
    let Some(verifier_url) = verifier_url else {
        info!(
            worker = "verified_labels",
            "verifier_url not configured; worker disabled"
        );
        return Ok(());
    };

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .user_agent("rome-via-enrich/0.1")
        .build()?;

    info!(worker = "verified_labels", %verifier_url, "worker enabled");

    loop {
        // N1: a failing candidate query must not be silently indistinguishable
        // from "no candidates" — a broken query (dropped index, schema
        // regression) would otherwise look perfectly healthy forever, the
        // same trap method_decoder's N1 fix addressed.
        let rows: Vec<(String,)> = match sqlx::query_as(verified_candidates_sql())
            .bind(chain_id)
            .bind(RECHECK_TTL_SECS)
            .bind(batch_size)
            .fetch_all(&pool)
            .await
        {
            Ok(rows) => rows,
            Err(e) => {
                warn!(chain_id, error = %e, "verified-candidates query failed; skipping this poll");
                Vec::new()
            }
        };

        if rows.is_empty() {
            tokio::time::sleep(poll_interval).await;
            continue;
        }

        for (address,) in rows {
            match fetch_verify_outcome(&client, &verifier_url, chain_id, &address).await {
                VerifyOutcome::Verified(name) => {
                    upsert_verified(&pool, chain_id, &address, &name).await
                }
                // Definitive "no" — safe to cache with the TTL.
                VerifyOutcome::NotVerified => stamp_checked(&pool, chain_id, &address).await,
                // S1: don't stamp — an outage/5xx/429/non-JSON isn't a
                // confident "not verified". Leaving verified_checked_at
                // untouched keeps this address a candidate next poll, so a
                // Sourcify blip can't wedge a contract verified during the
                // outage behind a full TTL wait.
                VerifyOutcome::Transient => {
                    warn!(%address, "sourcify check transient failure; retrying next poll");
                }
            }
            tokio::time::sleep(RATE_GAP).await;
        }

        tokio::time::sleep(poll_interval).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── candidate SQL shape ──────────────────────────────────────────────

    #[test]
    fn candidates_sql_excludes_verified_and_registry_requires_code() {
        let q = verified_candidates_sql();
        assert!(q.contains("has_code = true"), "must require code: {q}");
        assert!(
            q.contains("provenance NOT IN ('verified', 'registry')"),
            "must exclude already-curated/verified rows: {q}"
        );
        assert!(q.contains("verified_checked_at"), "must key on staleness: {q}");
    }

    // ── S1: classify_response — status+body → outcome ───────────────────

    /// 2xx + a real match → Verified(name).
    #[test]
    fn classify_2xx_match_is_verified() {
        let body = r#"{"match":"exact_match","compilation":{"name":"ArcTokenFactoryV2"}}"#;
        assert_eq!(
            classify_response(reqwest::StatusCode::OK, body),
            VerifyOutcome::Verified("ArcTokenFactoryV2".to_string())
        );
    }

    /// 2xx but `match: null` (Sourcify successfully reports "not verified")
    /// is a DEFINITIVE not-verified — must be stamped so it isn't re-checked
    /// until the TTL.
    #[test]
    fn classify_2xx_null_match_is_not_verified() {
        let body = r#"{"match":null}"#;
        assert_eq!(classify_response(reqwest::StatusCode::OK, body), VerifyOutcome::NotVerified);
    }

    /// 404 (Sourcify's "no record for this address") is also definitive.
    #[test]
    fn classify_404_is_not_verified() {
        assert_eq!(
            classify_response(reqwest::StatusCode::NOT_FOUND, "not found"),
            VerifyOutcome::NotVerified
        );
    }

    /// 500 is a Sourcify-side failure — transient, must NOT be stamped (or a
    /// Sourcify outage falsely stamps every candidate "not verified" for a
    /// full TTL, the #519-shaped bug this fixes).
    #[test]
    fn classify_500_is_transient() {
        assert_eq!(
            classify_response(reqwest::StatusCode::INTERNAL_SERVER_ERROR, "oops"),
            VerifyOutcome::Transient
        );
    }

    /// 429 (rate-limited) is transient — retry next poll, don't stamp.
    #[test]
    fn classify_429_is_transient() {
        assert_eq!(
            classify_response(reqwest::StatusCode::TOO_MANY_REQUESTS, "slow down"),
            VerifyOutcome::Transient
        );
    }

    /// A 2xx body that isn't valid JSON (proxy error page, empty body, etc.)
    /// is transient, not a confident "not verified".
    #[test]
    fn classify_non_json_2xx_is_transient() {
        assert_eq!(
            classify_response(reqwest::StatusCode::OK, "<html>not json</html>"),
            VerifyOutcome::Transient
        );
    }

    // ── parse_verified_name ──────────────────────────────────────────────

    /// The confirmed-live shape: ArcTokenFactoryV2 exact_match.
    #[test]
    fn parse_exact_match_returns_name() {
        let body = serde_json::json!({
            "match": "exact_match",
            "compilation": { "name": "ArcTokenFactoryV2" }
        });
        assert_eq!(parse_verified_name(&body), Some("ArcTokenFactoryV2".to_string()));
    }

    /// A partial "match" (not "exact_match") still counts as verified.
    #[test]
    fn parse_partial_match_returns_name() {
        let body = serde_json::json!({
            "match": "match",
            "compilation": { "name": "SomeContract" }
        });
        assert_eq!(parse_verified_name(&body), Some("SomeContract".to_string()));
    }

    /// `match: null` — not verified.
    #[test]
    fn parse_null_match_is_none() {
        let body = serde_json::json!({
            "match": serde_json::Value::Null,
            "compilation": { "name": "SomeContract" }
        });
        assert_eq!(parse_verified_name(&body), None);
    }

    /// Verified but the compilation name is missing/absent.
    #[test]
    fn parse_missing_name_is_none() {
        let body = serde_json::json!({
            "match": "exact_match",
            "compilation": {}
        });
        assert_eq!(parse_verified_name(&body), None);

        let body2 = serde_json::json!({ "match": "exact_match" });
        assert_eq!(parse_verified_name(&body2), None);
    }

    /// An empty-string name is treated as absent.
    #[test]
    fn parse_empty_name_is_none() {
        let body = serde_json::json!({
            "match": "exact_match",
            "compilation": { "name": "   " }
        });
        assert_eq!(parse_verified_name(&body), None);
    }

    /// Malformed bodies (missing `match` entirely, or not even an object).
    #[test]
    fn parse_malformed_is_none() {
        let body = serde_json::json!({ "foo": "bar" });
        assert_eq!(parse_verified_name(&body), None);

        let body2 = serde_json::json!("not an object");
        assert_eq!(parse_verified_name(&body2), None);

        let body3 = serde_json::json!(null);
        assert_eq!(parse_verified_name(&body3), None);
    }
}
