-- P1: the finalized-ingest Record. Scope per rome-arc AUDIT-TRAIL-IMPL-PLAN.md
-- §2/§3 P1: ONLY these two tables (Tier-2/3/4 land in later phases).

CREATE SCHEMA IF NOT EXISTS audit;

-- Append-only Record; materialized ONLY up to the audit worker's own
-- verified-finality watermark (§5.2). `event_id` is a DB surrogate ONLY —
-- it must NEVER enter any hashed report content (capture spec C1): an
-- independent re-index from genesis produces the same facts with different
-- `event_id`s, since it's `GENERATED ALWAYS AS IDENTITY` (assigned by
-- insertion order on THIS instance).
CREATE TABLE audit.chain_event (
    event_id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    chain_id        BIGINT NOT NULL,          -- load-bearing: shared DB across chains
    -- NO asset_id column (IMPL-PLAN §1.2/§11 C1): an event can belong to
    -- many assets' graphs; scoping is resolved at projection time (later phases).
    source_contract BYTEA  NOT NULL,
    source_kind     TEXT   NOT NULL,
    event_name      TEXT   NOT NULL,
    projection_tag  TEXT   NOT NULL CHECK (projection_tag IN ('primary', 'supporting')),
    topic0          BYTEA  NOT NULL,
    block_number    BIGINT NOT NULL,
    block_hash      BYTEA  NOT NULL,
    block_timestamp BIGINT NOT NULL,
    tx_hash         BYTEA  NOT NULL,
    tx_index        INT    NOT NULL,
    log_index       INT    NOT NULL,
    tx_signer       BYTEA  NOT NULL,
    args            JSONB  NOT NULL,
    -- block_hash in the key: a reorged re-land is a NEW row, never a
    -- collision with the row it replaces (capture spec §3/H4).
    UNIQUE (chain_id, tx_hash, log_index, block_hash)
);

CREATE INDEX ce_total_order ON audit.chain_event (chain_id, block_number, tx_index, log_index);
CREATE INDEX ce_source      ON audit.chain_event (chain_id, source_contract, block_number, log_index);
CREATE INDEX ce_name        ON audit.chain_event (chain_id, event_name, block_number);
CREATE INDEX ce_tx          ON audit.chain_event (chain_id, tx_hash);

-- Append-only is enforced structurally, not just by convention: this
-- trigger raises on any UPDATE/DELETE attempt against the table, so even a
-- bug in the ingest pipeline can't silently mutate a landed row.
CREATE OR REPLACE FUNCTION audit.reject_chain_event_mutation() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'audit.chain_event is append-only: % is not permitted', TG_OP;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER chain_event_append_only
    BEFORE UPDATE OR DELETE ON audit.chain_event
    FOR EACH ROW EXECUTE FUNCTION audit.reject_chain_event_mutation();

-- The audit worker's own durable verified-finality watermark (§5.2). One
-- row per chain; the ingest pipeline advances `verified_through_slot` only
-- after all three §5.2 conditions hold for a candidate slot.
CREATE TABLE audit.ingest_watermark (
    chain_id              BIGINT PRIMARY KEY,
    verified_through_slot BIGINT NOT NULL,
    updated_at            BIGINT NOT NULL
);
