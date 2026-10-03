-- 0100_enrich_tables.up.sql
-- Derived / enrichment tables for rome-via-enrich workers.
-- All tables live in the rome_via schema. Normally the schema is created
-- by rome-via-sync's 0001_create_schema migration, but we create it here
-- too so rome-via-enrich can run standalone (e.g., in CI or when sync is
-- slower to start).
CREATE SCHEMA IF NOT EXISTS rome_via;

-- ─────────────────────────────────────────────────────────────────────────────
-- Method signatures — selector → human-readable name
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS rome_via.method_signatures (
    selector  VARCHAR(10) PRIMARY KEY,    -- "0x" + 8 hex chars, e.g. "0xa9059cbb"
    signature TEXT        NOT NULL,       -- e.g. "transfer(address,uint256)"
    source    TEXT        NOT NULL,       -- "seed" | "4byte" | "manual"
    added_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ─────────────────────────────────────────────────────────────────────────────
-- Token metadata — ERC-20 name / symbol / decimals / supply
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS rome_via.token_metadata (
    chain_id     BIGINT       NOT NULL,
    address      VARCHAR(42)  NOT NULL,
    name         TEXT,
    symbol       TEXT,
    decimals     SMALLINT,
    total_supply NUMERIC,
    kind         TEXT         NOT NULL DEFAULT 'ERC-20',  -- 'ERC-20' | 'SPL' | 'Token-2022'
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (chain_id, address)
);

-- ─────────────────────────────────────────────────────────────────────────────
-- Token transfers — one row per ERC-20 Transfer log event
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS rome_via.token_transfers (
    chain_id      BIGINT      NOT NULL,
    tx_hash       VARCHAR(66) NOT NULL,
    log_index     INTEGER     NOT NULL,
    token_address VARCHAR(42) NOT NULL,
    from_addr     VARCHAR(42) NOT NULL,
    to_addr       VARCHAR(42) NOT NULL,
    amount        NUMERIC     NOT NULL,
    block_number  BIGINT      NOT NULL,
    timestamp     TIMESTAMPTZ,
    PRIMARY KEY (chain_id, tx_hash, log_index)
);

CREATE INDEX IF NOT EXISTS ix_token_transfers_token
    ON rome_via.token_transfers (chain_id, token_address);
CREATE INDEX IF NOT EXISTS ix_token_transfers_from
    ON rome_via.token_transfers (chain_id, from_addr);
CREATE INDEX IF NOT EXISTS ix_token_transfers_to
    ON rome_via.token_transfers (chain_id, to_addr);

-- ─────────────────────────────────────────────────────────────────────────────
-- Token holders — current balance per (token, address)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS rome_via.token_holders (
    chain_id        BIGINT      NOT NULL,
    token_address   VARCHAR(42) NOT NULL,
    holder_address  VARCHAR(42) NOT NULL,
    balance         NUMERIC     NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (chain_id, token_address, holder_address)
);

CREATE INDEX IF NOT EXISTS ix_token_holders_token
    ON rome_via.token_holders (chain_id, token_address)
    WHERE balance > 0;

-- ─────────────────────────────────────────────────────────────────────────────
-- Token holder counts — pre-aggregated count WHERE balance > 0
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS rome_via.token_holder_counts (
    chain_id      BIGINT      NOT NULL,
    token_address VARCHAR(42) NOT NULL,
    holder_count  BIGINT      NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (chain_id, token_address)
);

-- ─────────────────────────────────────────────────────────────────────────────
-- Address stats — tx counts, first/last seen, contract flag
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS rome_via.address_stats (
    chain_id    BIGINT      NOT NULL,
    address     VARCHAR(42) NOT NULL,
    tx_count    BIGINT      NOT NULL DEFAULT 0,
    first_seen  TIMESTAMPTZ,
    last_seen   TIMESTAMPTZ,
    is_contract BOOLEAN     NOT NULL DEFAULT FALSE,
    code_hash   VARCHAR(66),
    PRIMARY KEY (chain_id, address)
);

-- ─────────────────────────────────────────────────────────────────────────────
-- Search index — pg_trgm GIN index for fuzzy text search
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS rome_via.search_index (
    chain_id      BIGINT NOT NULL,
    entity_type   TEXT   NOT NULL,   -- 'tx' | 'block' | 'address' | 'token'
    entity_id     TEXT   NOT NULL,   -- hash / block number / address
    display_label TEXT   NOT NULL,   -- text the user searches against
    weight        INTEGER NOT NULL DEFAULT 50,
    PRIMARY KEY (chain_id, entity_type, entity_id)
);

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX IF NOT EXISTS ix_search_index_trgm
    ON rome_via.search_index USING GIN (display_label gin_trgm_ops);

-- ─────────────────────────────────────────────────────────────────────────────
-- Enrich cursors — per-worker progress tracking
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS rome_via.enrich_cursors (
    chain_id          BIGINT      NOT NULL,
    worker            TEXT        NOT NULL,
    last_processed    BIGINT      NOT NULL DEFAULT 0,
    last_processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (chain_id, worker)
);
