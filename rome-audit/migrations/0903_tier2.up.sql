-- P2 (rome-arc AUDIT-TRAIL-IMPL-PLAN.md §2 Tier-2, this task's own numbering
-- vs. the plan's §3 P3 — same scope): interval/timeline derivations, a pure
-- function of `audit.chain_event`. Every table here is REBUILDABLE — CI
-- drops and rebuilds it (product §13.1); there is no append-only trigger and
-- no UPDATE/DELETE restriction (the opposite of chain_event's doctrine:
-- these tables are TRUNCATE+rebuilt by `tier2::rebuild::rebuild_tier2`, never
-- hand-edited).
--
-- `chain_id` is carried on every table (H1 sweep, IMPL-PLAN §2) even where
-- `asset_id` already encodes it (`chainId:tokenAddress`) — defense in depth
-- for anyone querying/joining without parsing the composite key.

-- ---- allowlist_interval — ← WhitelistStatusChanged (canonical; Added/
-- Removed are redundant and never separately counted). address(0) never
-- appears (gated mint/burn revert, spec §4.1) — not enforced here (the chain
-- already guarantees it), just documented.
CREATE TABLE audit.allowlist_interval (
    chain_id        BIGINT NOT NULL,
    asset_id        TEXT   NOT NULL,
    address         BYTEA  NOT NULL,
    from_block      BIGINT NOT NULL,
    to_block        BIGINT,
    opened_by_event BIGINT NOT NULL REFERENCES audit.chain_event (event_id),
    closed_by_event BIGINT REFERENCES audit.chain_event (event_id),
    PRIMARY KEY (chain_id, asset_id, address, from_block)
);
CREATE INDEX allowlist_interval_open ON audit.allowlist_interval (chain_id, asset_id, address)
    WHERE to_block IS NULL;

-- ---- gate_interval — ← TransfersRestrictionToggled, COALESCED: the setter
-- has no changed-guard, so two toggles landing on the same `gated` value
-- collapse into one interval, never a zero-width open/close pair.
CREATE TABLE audit.gate_interval (
    chain_id        BIGINT  NOT NULL,
    asset_id        TEXT    NOT NULL,
    gated           BOOLEAN NOT NULL,
    from_block      BIGINT  NOT NULL,
    to_block        BIGINT,
    opened_by_event BIGINT NOT NULL REFERENCES audit.chain_event (event_id),
    closed_by_event BIGINT REFERENCES audit.chain_event (event_id),
    PRIMARY KEY (chain_id, asset_id, from_block)
);

-- ---- sanction_denyset_interval — ← Sanctioned/Unsanctioned, keyed by
-- MODULE (chain-global — NOT per-asset; the same module can in principle
-- exist at the same address on two different chains, hence chain_id in the key).
CREATE TABLE audit.sanction_denyset_interval (
    chain_id        BIGINT NOT NULL,
    module_address  BYTEA  NOT NULL,
    address         BYTEA  NOT NULL,
    from_block      BIGINT NOT NULL,
    to_block        BIGINT,
    opened_by_event BIGINT NOT NULL REFERENCES audit.chain_event (event_id),
    closed_by_event BIGINT REFERENCES audit.chain_event (event_id),
    PRIMARY KEY (chain_id, module_address, address, from_block)
);
CREATE INDEX sanction_denyset_interval_open ON audit.sanction_denyset_interval (chain_id, module_address, address)
    WHERE to_block IS NULL;

-- ---- router_sanctions_epoch — ← ModuleTypeRegistered (open) /
-- GlobalImplementationUpdated + ModuleTypeRemoved (close), filtered to
-- typeId = GLOBAL_SANCTIONS_TYPE. Router-scoped, not per-asset (spec §4.2:
-- "enablement is per-ROUTER").
CREATE TABLE audit.router_sanctions_epoch (
    chain_id        BIGINT NOT NULL,
    router_address  BYTEA  NOT NULL,
    module_address  BYTEA  NOT NULL,
    from_block      BIGINT NOT NULL,
    to_block        BIGINT,
    opened_by_event BIGINT NOT NULL REFERENCES audit.chain_event (event_id),
    closed_by_event BIGINT REFERENCES audit.chain_event (event_id),
    PRIMARY KEY (chain_id, router_address, from_block)
);

-- ---- exposure_window — §7 detector: any UNGATED gate_interval on a
-- previously-gated asset. `pattern` is a PROJECTION classification derived
-- from the Transfer shape inside the window (never a new on-chain fact):
-- ISSUANCE = mint only (no burn, no third-party transfer); CLAWBACK = both a
-- mint and a burn, no third-party transfer; OTHER = any third-party transfer
-- (both parties non-zero) — including a transfer to a never-allowlisted
-- address, which `flags` records explicitly.
CREATE TABLE audit.exposure_window (
    chain_id        BIGINT NOT NULL,
    asset_id        TEXT   NOT NULL,
    open_block      BIGINT NOT NULL,
    close_block     BIGINT,
    pattern         TEXT   NOT NULL CHECK (pattern IN ('CLAWBACK', 'ISSUANCE', 'OTHER')),
    flags           JSONB  NOT NULL DEFAULT '{}',
    opened_by_event BIGINT NOT NULL REFERENCES audit.chain_event (event_id),
    closed_by_event BIGINT REFERENCES audit.chain_event (event_id),
    PRIMARY KEY (chain_id, asset_id, open_block)
);
