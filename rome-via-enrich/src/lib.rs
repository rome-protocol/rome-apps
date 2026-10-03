// rome-via-enrich: Enrichment worker for derived Rome Via data.
// Phase 3 implementation — workers + migrations + config.

pub mod cli;
pub mod config;
pub mod maintenance;
pub mod server;
pub mod supervisor;
pub mod workers;

#[cfg(test)]
mod migrations_test;
