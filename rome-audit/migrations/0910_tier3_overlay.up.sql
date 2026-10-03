-- P5a: Tier-3 overlay INGEST — the audit-side identity/evidence feed Bloom
-- pushes into. Per rome-arc AUDIT-TRAIL-IMPL-PLAN.md §2 Tier 3 + §5.1/§0.2:
-- these tables are EMPTY on creation (forward-only — no backfill of any
-- existing Bloom identity/binding/KYC content) and are populated ONLY by
-- pushed `/overlay/*` requests from the first post-deploy request onward.
--
-- Normalization choice (a) per the P5a task: `audit.identity` holds NO
-- inline key columns (unlike the IMPL-PLAN §2 sketch's single-key-inline
-- shape) — every verified key, including the first, lives in
-- `audit.identity_key`. This is a P5-build decision the IMPL-PLAN's own
-- Tier-3 DDL comment (migrations/... `identity_key` doc) flags as open.
--
-- `identity`/`identity_key`/`evidence_record` are append-only, same doctrine
-- as `audit.chain_event` (migration 0001): corrections are new rows, and a
-- `BEFORE UPDATE OR DELETE` trigger enforces it structurally.
-- `address_label` is curated, audit-side, display-only (product §6) and is
-- deliberately mutable — it is NOT part of the Bloom-pushed feed.

CREATE TABLE audit.identity (
    identity_id TEXT   PRIMARY KEY,   -- opaque Bloom-minted id; NEVER a vendor id
    created_at  BIGINT NOT NULL       -- unix seconds, server-stamped at first-seen
);

CREATE TABLE audit.identity_key (
    identity_id           TEXT   NOT NULL REFERENCES audit.identity,
    root_key_type         TEXT   NOT NULL CHECK (root_key_type IN ('SOLANA', 'EVM')),
    root_key              TEXT   NOT NULL,
    derivation_fn_version TEXT   NOT NULL,
    -- Pure-derivation output, stored verbatim as fed by Bloom (recorder —
    -- no server-side re-derivation here). REQUIRED (20-byte) for SOLANA,
    -- MUST be NULL for EVM — the SOLANA-vs-EVM branch of that rule is
    -- enforced by the `/overlay/identity` handler (a 422 needs to run
    -- before any row is written; see routes.rs), which the CHECK below
    -- cannot express (it has no way to see `root_key_type`). MED-3: the
    -- byte-WIDTH half of the rule (20 bytes when present) IS expressible
    -- here and is added as belt-and-suspenders against any future
    -- non-handler write path — the handler 422 still runs first regardless.
    synthetic_evm_address BYTEA CHECK (synthetic_evm_address IS NULL OR octet_length(synthetic_evm_address) = 20),
    added_at              BIGINT NOT NULL,
    PRIMARY KEY (root_key, derivation_fn_version)
);
CREATE INDEX ik_identity ON audit.identity_key (identity_id);

CREATE TABLE audit.evidence_record (
    evidence_id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    -- Dedup/idempotency key ONLY — never rendered, never hashed (§14.2).
    source_ref            TEXT   NOT NULL UNIQUE,
    identity_id            TEXT   NOT NULL REFERENCES audit.identity,
    kind                   TEXT   NOT NULL CHECK (kind IN ('KYC_STATUS', 'SANCTIONS_SCREEN')),
    vendor                 TEXT   NOT NULL,
    -- MED-3: fixed 32-byte width enforced at the DB level (defense-in-depth;
    -- the handler's is_hex32_lowercase 422 already runs before any write).
    subject_ref            BYTEA  NOT NULL CHECK (octet_length(subject_ref) = 32),
    subject_ref_version    TEXT   NOT NULL,
    status                 TEXT   NOT NULL,
    -- GREEN-only policy window, unix seconds; NULL for RED (handler-enforced).
    valid_through          BIGINT,
    vendor_timestamp       BIGINT NOT NULL,
    -- Server-stamped on receipt (unix seconds) — never client-supplied.
    received_at            BIGINT NOT NULL,
    -- SHA-256(JCS(9 canonical fields)) — server-computed; the feeder sends
    -- no hash (canonical.rs).
    evidence_hash          BYTEA  NOT NULL
);
CREATE INDEX er_identity ON audit.evidence_record (identity_id, received_at);

-- Display-only, curated audit-side (product §6) — zero compliance
-- semantics, NOT part of the Bloom-pushed feed, and deliberately MUTABLE
-- (no append-only trigger).
CREATE TABLE audit.address_label (
    chain_id BIGINT NOT NULL,
    address  BYTEA  NOT NULL,
    label    TEXT   NOT NULL,
    note     TEXT,
    PRIMARY KEY (chain_id, address)
);

-- ---- Append-only triggers (mirrors migration 0001's
-- `reject_chain_event_mutation` pattern exactly) ----

CREATE OR REPLACE FUNCTION audit.reject_identity_mutation() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'audit.identity is append-only: % is not permitted', TG_OP;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER identity_append_only
    BEFORE UPDATE OR DELETE ON audit.identity
    FOR EACH ROW EXECUTE FUNCTION audit.reject_identity_mutation();

CREATE OR REPLACE FUNCTION audit.reject_identity_key_mutation() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'audit.identity_key is append-only: % is not permitted', TG_OP;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER identity_key_append_only
    BEFORE UPDATE OR DELETE ON audit.identity_key
    FOR EACH ROW EXECUTE FUNCTION audit.reject_identity_key_mutation();

CREATE OR REPLACE FUNCTION audit.reject_evidence_record_mutation() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'audit.evidence_record is append-only: % is not permitted', TG_OP;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER evidence_record_append_only
    BEFORE UPDATE OR DELETE ON audit.evidence_record
    FOR EACH ROW EXECUTE FUNCTION audit.reject_evidence_record_mutation();
