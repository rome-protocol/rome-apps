-- P3 (rome-arc AUDIT-TRAIL-IMPL-PLAN.md §1.4 `capture_manifest` / §2 C1
-- `asset_event`, this task's own numbering vs. the plan's §3 P2 — same
-- scope). `capture_manifest` is a FROZEN Tier-1 input (§1.2 point 4,
-- self-recording resolution); `asset_event` is REBUILDABLE, Tier-2-style —
-- a pure function of `audit.chain_event` + `capture_manifest`, never a
-- hand-written fact (`resolve::asset_event::rebuild_asset_event`
-- TRUNCATE+recomputes it, same doctrine as `tier2::rebuild::rebuild_tier2`).

-- `manifest_hash` = keccak256(JCS(manifest minus generated_at)) —
-- `resolve::manifest::CaptureManifest::manifest_hash` (IMPL-PLAN §6 open Q8:
-- hash function is a P6 decision; this crate's own choice is documented
-- there, not re-litigated in DDL).
CREATE TABLE audit.capture_manifest (
    manifest_hash       BYTEA  PRIMARY KEY,
    chain_id            BIGINT NOT NULL,
    asset_id            TEXT   NOT NULL,
    registry_commit_sha TEXT   NOT NULL,
    resolved_sources    JSONB  NOT NULL,
    source_intervals    JSONB  NOT NULL,
    generated_at        BIGINT NOT NULL
);
CREATE INDEX capture_manifest_asset ON audit.capture_manifest (chain_id, asset_id, generated_at DESC);

-- Many-to-many scope junction (capture §1.2 C1): a shared-source event
-- (router/factory/storefront) maps into MANY assets' rows. `manifest_hash`
-- records exactly which resolution produced this membership row — a later
-- re-resolution that discovers a NEW source never mutates an existing row,
-- it just adds new ones under its own (possibly different) manifest_hash.
CREATE TABLE audit.asset_event (
    chain_id      BIGINT NOT NULL,
    asset_id      TEXT   NOT NULL,
    event_id      BIGINT NOT NULL REFERENCES audit.chain_event (event_id),
    manifest_hash BYTEA  NOT NULL REFERENCES audit.capture_manifest (manifest_hash),
    PRIMARY KEY (chain_id, asset_id, event_id)
);
CREATE INDEX asset_event_event ON audit.asset_event (event_id);
