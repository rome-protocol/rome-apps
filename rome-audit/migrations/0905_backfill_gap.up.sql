-- P3c a.6: the honesty mechanism for a newly-discovered source whose
-- history starts BEHIND the ingest watermark — `resolve::pass::run_resolve_pass`
-- inserts a row here instead of silently backfilling (backfill execution
-- itself is P4, deliberately deferred: see `resolve/pass.rs`'s module doc).

CREATE TABLE audit.backfill_gap (
    chain_id                BIGINT NOT NULL,
    source_contract         BYTEA  NOT NULL,
    source_kind             TEXT   NOT NULL,
    from_block               BIGINT NOT NULL,
    watermark_at_detection   BIGINT NOT NULL,
    manifest_hash            BYTEA  NOT NULL REFERENCES audit.capture_manifest (manifest_hash),
    detected_at              BIGINT NOT NULL,
    -- Not "resolved_at": "resolve" in this crate means graph resolution
    -- (resolve::resolve(token)) — this column is the LATER, separate act of
    -- actually backfilling the missed events (P4's executor). Nothing
    -- writes it yet.
    remediated_at            BIGINT,
    PRIMARY KEY (chain_id, source_contract, from_block)
);
