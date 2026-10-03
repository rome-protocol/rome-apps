use {
    clap::{Parser, Subcommand},
    ethers::types::{Address, U256},
    rome_sdk::{
        rome_evm_client::RomeEVMClient as Client,
        rome_solana::{
            indexers::clock::SolanaClockIndexer,
            tower::SolanaTower,
            types::{AsyncAtomicRpcClient},
        },
    },
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_commitment_config::{
        CommitmentConfig, CommitmentLevel::Confirmed,
    },
    solana_sdk::pubkey::Pubkey, 
    std::{
        sync::Arc, path::PathBuf,
    },
};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
#[command(about = "cli application for the rome-evm program", long_about = None) ]
pub struct Cli {
    /// rome-evm program_id
    #[arg(short, long)]
    pub program_id: Pubkey,
    /// chain_id of rollup (optional, not required for get-rollups, reg-rollup)
    #[arg(short, long)]
    pub chain_id: Option<u64>,
    /// URL for Solana's JSON RPC: http://localhost:8899
    #[arg(short, long)]
    pub url: String,
    /// filepath to a keypair
    #[arg(short, long)]
    pub keypair: Option<String>,
    /// rome-evm instruction
    #[command(subcommand)]
    pub cmd: Cmd,
}

#[derive(Subcommand, Debug)]
pub enum Cmd {
    /// registry a rollup in rome-evm contract
    RegRollup {
        /// Is the rollup configured for a single-state
        #[arg(action = clap::ArgAction::Set)]
        is_single_state: bool,
        /// SPL Mint pubkey (if the SPL token will be used as the native token of rollup)
        mint: Option<Pubkey>,
    },
    /// Deposit funds to the rome-evm balance account.
    ///
    /// Branches by rollup gas-token type (`Client::get_rollup_info(program_id, rpc, chain_id).mint`):
    ///   - **SOL-gas rollup** (no `mint` configured): native SOL is transferred from the
    ///     Solana user's wallet to the rome-evm sol_wallet PDA. Amount in Wei must be a
    ///     multiple of 10^9 (SOL precision is 9, rome-evm token precision is 18).
    ///     Rate: 1 SOL = 1 rome-evm token.
    ///   - **SPL-gas rollup** (e.g. USDC-gas Marcus): the configured SPL token is transferred
    ///     from the user's ATA to the rome-evm gas-pool ATA via SPL CPI. Amount in Wei must
    ///     be a multiple of 10^(18 - mint_decimals) (e.g. for USDC, decimals=6 → multiple of
    ///     10^12). Rate: 1 unit of mint (in mint's smallest denomination) = 1 rome-evm token
    ///     scaled to 18 decimals.
    ///
    /// Special type 0x7E of rlp is used. Rome-evm mints the funds on the user account.
    /// The amount in Wei is used as rlp.mint.
    ///
    /// This solana transaction must be signed by solana user's wallet private key.
    Deposit {
        /// the user's address to mint a balance
        address: Address,
        /// balance in Wei to mint (multiple of 10^(18 - underlying_decimals); e.g.
        /// 10^9 for SOL-gas, 10^12 for USDC-gas)
        balance: u128,
    },
    /// get balance
    GetBalance { address: Address },
    /// get contract code
    GetCode { address: Address },
    /// get storage slot
    GetStorageAt { address: Address, slot: U256 },
    /// get transaction count
    GetTransactionCount { address: Address },
    /// get list of registered rollups
    GetRollups,
    /// get build info of the rome-evm program
    GetProgramBuildInfo,
    /// create list of treasure accounts
    CreateTreasure,
    /// transferring of the treasure account balances to the upgrade_authority spl_associated_account
    JoinTreasure,
    /// balances of treasure accounts
    GetTreasureBalance,
    /// allocate resources of proxy
    AllocateResources { config: PathBuf},
    /// deallocate resources of proxy
    DeallocateResources { config: PathBuf},
    /// show allocated resources
    Resources { config: PathBuf},
    /// Offline ALT manager: provision / health-check / retire persistent lookup tables
    Alt {
        #[command(subcommand)]
        action: AltCmd,
    },
}

#[derive(Subcommand, Debug)]
pub enum AltCmd {
    /// Verify each registered table's on-chain contents against its recorded hash.
    Health {
        /// path to alts.json
        manifest: String,
    },
    /// Deactivate a table (closeable + rent-reclaimable after the cooldown).
    Retire {
        /// lookup-table address
        table: Pubkey,
    },
}

pub async fn sdk_client(cli: &Cli) -> anyhow::Result<Client> {
    let async_client = async_rpc_client(&cli.url);
    let solana_clock_indexer = SolanaClockIndexer::new(async_client.clone())
        .await
        .expect("create solana clock indexer error");
    let clock = solana_clock_indexer.get_current_clock();
    let tower = SolanaTower::new(async_client, clock);

    let cli = Client::new_checked_version(
        cli.chain_id.expect("chain_id expected"),
        cli.program_id,
        tower,
        None,
        vec![],
        1.0,
        None,
        None,
    )
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    Ok(cli)
}

pub fn async_rpc_client(url: &str) -> AsyncAtomicRpcClient {
    Arc::new(RpcClient::new_with_commitment(
        url.to_string(),
        CommitmentConfig {
            commitment: Confirmed,
        },
    ))
}
