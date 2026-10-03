-- H2 (Fable P1 review): a terminal (deterministic, will-never-resolve) log
-- processing error must never silently drop the log, and must never wedge
-- the watermark forever. `audit.quarantine` is the "saw it, couldn't
-- process it" record — the offending log's raw shape + why, so nothing is
-- lost even when it can't be decoded into `audit.chain_event`.

CREATE TABLE audit.quarantine (
    quarantine_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    chain_id        BIGINT NOT NULL,
    slot_number     BIGINT NOT NULL,
    tx_hash         BYTEA  NOT NULL,
    log_ordinal     INT    NOT NULL,
    topic0          BYTEA,
    source_contract BYTEA,
    raw_topics      JSONB  NOT NULL,
    raw_data        BYTEA,
    reason          TEXT   NOT NULL,
    first_seen_at   BIGINT NOT NULL,
    -- Re-processing the same slot (a later same-slot transient error holds
    -- the tick's watermark advance; a crash between this INSERT and the
    -- watermark persist) must re-quarantine the SAME log idempotently, not
    -- duplicate it — the ingest pipeline's INSERT relies on this via
    -- `ON CONFLICT ... DO NOTHING`, mirroring `chain_event`'s idempotency.
    UNIQUE (chain_id, slot_number, tx_hash, log_ordinal)
);

CREATE INDEX quarantine_chain_slot ON audit.quarantine (chain_id, slot_number);

-- A quarantined log is a permanent audit fact — "saw it, couldn't process
-- it" — just like `audit.chain_event`; the same append-only trigger
-- pattern applies (distinct function/trigger names, same doctrine).
CREATE OR REPLACE FUNCTION audit.reject_quarantine_mutation() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'audit.quarantine is append-only: % is not permitted', TG_OP;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER quarantine_append_only
    BEFORE UPDATE OR DELETE ON audit.quarantine
    FOR EACH ROW EXECUTE FUNCTION audit.reject_quarantine_mutation();
