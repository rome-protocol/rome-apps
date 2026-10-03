-- P4a §3: the actually-INGESTABLE (address, filter_fingerprint) map — a
-- more precise "previously known" universe than `capture_manifest`'s
-- `resolved_sources` (which records EVERY graph source, including
-- descriptor-less ones never added to the live ingest map — see
-- `resolve::pass::detect_backfill_gaps`'s doc for the silent-gap bug this
-- re-key closes). Upserted every resolve pass with the POST-retain
-- EFFECTIVE ingest map (`resolve::pass::run_resolve_pass`'s `sources`), so
-- gap detection can ask "was THIS EXACT (address, filter) pair ever
-- actually ingestable before" rather than "was this address ever seen in
-- any manifest" — the former also catches filter-widening (a 2nd asset
-- sharing a chain-wide token broadens the union'd filter, exposing history
-- the narrower filter never captured).
CREATE TABLE audit.ingest_scope (
    chain_id           BIGINT NOT NULL,
    address            BYTEA  NOT NULL,
    source_kind        TEXT   NOT NULL,
    filter_fingerprint TEXT   NOT NULL,
    first_seen_at      BIGINT NOT NULL,
    PRIMARY KEY (chain_id, address, filter_fingerprint)
);
