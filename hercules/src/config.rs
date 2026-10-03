use crate::api::admin::{start_rpc_server, HerculesAdmin};
use rome_sdk::rome_evm_client::indexer::config::{
    RollupIndexerConfig, SolanaBlockLoaderConfig, StorageConfig,
};
use rome_sdk::rome_evm_client::indexer::{ProgramResult, StandaloneIndexer};
use solana_sdk::clock::Slot;
#[allow(unused_imports)]
use solana_commitment_config::CommitmentLevel;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[derive(Clone, serde::Serialize, serde::Deserialize, Debug)]
pub enum HerculesMode {
    Indexer,
    Recovery,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct HerculesConfig {
    pub start_slot: u64,
    pub end_slot: Option<u64>,
    pub storage: StorageConfig,
    pub block_loader: Option<SolanaBlockLoaderConfig>,
    pub admin_rpc: SocketAddr,
    pub rollup_indexer: Option<RollupIndexerConfig>,
    pub mode: Option<HerculesMode>,
    pub indexing_interval_ms: Option<u64>,
}

const DEFAULT_INDEXING_INT_MS: u64 = 400;

impl HerculesConfig {
    pub async fn init(
        self,
        cancellation_token: CancellationToken,
    ) -> anyhow::Result<(
        jsonrpsee::server::ServerHandle,
        Vec<JoinHandle<ProgramResult<()>>>,
    )> {
        let (solana_block_storage, ethereum_block_storage) =
            self.storage.init(self.start_slot).await?;
        let (indexer_started_tx, indexer_started_rx) = tokio::sync::oneshot::channel();

        let solana_block_loader = self
            .block_loader
            .map(|config| config.init(self.start_slot, solana_block_storage.clone()));

        let rollup_indexer = self.rollup_indexer.map(|config| {
            config.init(
                solana_block_storage.clone(),
                ethereum_block_storage.clone(),
                solana_block_loader.as_ref().map(|b| b.program_id),
            )
        });

        let indexer = StandaloneIndexer {
            solana_block_loader,
            rollup_indexer,
        };

        let (progress_tx, progress_rx) = tokio::sync::watch::channel(0 as Slot);

        let server_handle = start_rpc_server(
            Arc::new(HerculesAdmin::new(
                solana_block_storage,
                ethereum_block_storage,
                indexer_started_rx,
                progress_rx,
            )),
            self.admin_rpc,
        )
        .await?;

        let indexer_handles = match self.mode.clone().unwrap_or(HerculesMode::Indexer) {
            HerculesMode::Indexer => indexer.start_indexing(
                Some(self.start_slot),
                Some(indexer_started_tx),
                self.indexing_interval_ms.unwrap_or(DEFAULT_INDEXING_INT_MS),
                Some(progress_tx),
                cancellation_token,
            ),
            HerculesMode::Recovery => {
                indexer.start_recovery(self.start_slot, self.end_slot, cancellation_token)
            }
        };

        Ok((server_handle, indexer_handles))
    }
}
