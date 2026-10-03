-- P4b-ii (rome-arc AUDIT-TRAIL-IMPL-PLAN.md §5 governance/attribution
-- Tier-2 correlations): four more Tier-2 derived tables, same doctrine as
-- migrations 0003/0008 — pure functions of `audit.chain_event`,
-- REBUILDABLE (`tier2::rebuild::rebuild_tier2`), chain-scoped DELETE, never
-- a global TRUNCATE (P4a M3).

-- ---- role_interval — ← RoleGranted/RoleRevoked, SOURCE-scoped (not
-- asset-scoped): built ONCE per DISTINCT source_contract shared by any
-- number of assets. PK is the tripwire against a double-build.
CREATE TABLE audit.role_interval (
    chain_id        BIGINT NOT NULL,
    source_contract BYTEA  NOT NULL,
    role            BYTEA  NOT NULL,
    account         BYTEA  NOT NULL,
    from_block      BIGINT NOT NULL,
    to_block        BIGINT,
    opened_by_event BIGINT NOT NULL REFERENCES audit.chain_event (event_id),
    closed_by_event BIGINT REFERENCES audit.chain_event (event_id),
    PRIMARY KEY (chain_id, source_contract, role, account, from_block)
);
CREATE INDEX role_interval_open ON audit.role_interval (chain_id, source_contract, role, account)
    WHERE to_block IS NULL;

-- ---- code_change — governance/attribution correlation over the per-asset
-- code/module surface. `before`/`after` are always 20-byte address-shaped
-- values when present; `subject` varies width (20-byte address for
-- UPGRADE/STOREFRONT_UPGRADE, 32-byte typeId for
-- MODULE_SET/MODULE_LINKED/ROUTER_GLOBAL) — stored as BYTEA, never split
-- into two nullable fixed-width columns.
CREATE TABLE audit.code_change (
    chain_id            BIGINT NOT NULL,
    asset_id            TEXT   NOT NULL,
    kind                TEXT   NOT NULL CHECK (kind IN
        ('UPGRADE', 'MODULE_SET', 'MODULE_LINKED', 'ROUTER_GLOBAL', 'STOREFRONT_UPGRADE')),
    subject             BYTEA  NOT NULL,
    block_number        BIGINT NOT NULL,
    tx_index            INT    NOT NULL,
    log_index           INT    NOT NULL,
    before              BYTEA,
    after               BYTEA,
    signer_attribution  TEXT   NOT NULL CHECK (signer_attribution IN
        ('ISSUER_KEY', 'ROME_MULTISIG', 'USER', 'UNKNOWN')),
    event_id            BIGINT NOT NULL REFERENCES audit.chain_event (event_id),
    PRIMARY KEY (chain_id, asset_id, event_id)
);
CREATE INDEX code_change_by_subject ON audit.code_change (chain_id, asset_id, kind, subject, block_number, tx_index, log_index);

-- ---- sale — a storefront purchase paired with its (nullable) token and
-- payment settlement legs. NULL leg IS the "missing" flag — no redundant
-- boolean column.
CREATE TABLE audit.sale (
    chain_id              BIGINT  NOT NULL,
    asset_id              TEXT    NOT NULL,
    block_number          BIGINT  NOT NULL,
    tx_index              INT     NOT NULL,
    purchase_log_index    INT     NOT NULL,
    buyer                 BYTEA   NOT NULL,
    amount                NUMERIC(78,0) NOT NULL,
    price_paid            NUMERIC(78,0) NOT NULL,
    purchase_event        BIGINT  NOT NULL REFERENCES audit.chain_event (event_id),
    token_transfer_event  BIGINT REFERENCES audit.chain_event (event_id),
    payment_transfer_event BIGINT REFERENCES audit.chain_event (event_id),
    PRIMARY KEY (chain_id, asset_id, block_number, tx_index, purchase_log_index)
);

-- ---- holder_balance — per-holder end-of-block running balance. Negative
-- values are STORED with integrity_alarm=TRUE, never clamped (the
-- rome-via-enrich #506 lesson).
CREATE TABLE audit.holder_balance (
    chain_id         BIGINT  NOT NULL,
    asset_id         TEXT    NOT NULL,
    address          BYTEA   NOT NULL,
    block_number     BIGINT  NOT NULL,
    balance          NUMERIC(78,0) NOT NULL,
    integrity_alarm  BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (chain_id, asset_id, address, block_number)
);
