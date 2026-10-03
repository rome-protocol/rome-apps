-- Create the rome_via schema for all mirrored tables.
-- All tables are in this schema; cross-chain queries join on chain_id.
CREATE SCHEMA IF NOT EXISTS rome_via;
