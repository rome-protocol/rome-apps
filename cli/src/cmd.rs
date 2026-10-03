use {
    crate::{
        program_option::{async_rpc_client, sdk_client, Cli, Cmd, },
        ix::{deposit, treasure_balance,},
        aux::keypair,
        resources::{allocate_resources, deallocate_resources, show_resources,},
    },
    rome_sdk::{
        rome_evm_client::RomeEVMClient as Client,
    },
    std::sync::Arc,
};

pub async fn execute(cli: Cli) -> anyhow::Result<()> {
    match &cli.cmd {
        Cmd::Deposit {
            address,
            balance,
        } => {
            let skd_cli = sdk_client(&cli).await?;
            let keypair = keypair(&cli, false)?;
            deposit(&skd_cli, *address, *balance, keypair).await?
        }
        Cmd::RegRollup {
            is_single_state,
            mint,
        } => {
            let keypair = keypair(&cli, true)?;
            let chain_id = cli.chain_id.expect("chain_id expected");

            Client::reg_rollup(
                &cli.program_id,
                chain_id,
                &keypair,
                *mint,
                *is_single_state,
                async_rpc_client(&cli.url),
            )
            .await?;
            println!(
                "chain_id {} has been registered, mint: {:?}, single-state: {}",
                chain_id, mint, is_single_state
            );
        }
        Cmd::GetBalance { address } => {
            let sdk_cli = sdk_client(&cli).await?;
            let balance = sdk_cli.get_balance(*address).await?;
            println!("balance: {}", balance);
        }
        Cmd::GetCode { address } => {
            let sdk_cli = sdk_client(&cli).await?;
            let code = sdk_cli.get_code(*address).await?;
            println!("code: {:#}", hex::encode(code.as_ref()));
        }
        Cmd::GetStorageAt { address, slot } => {
            let sdk_cli = sdk_client(&cli).await?;
            let value = sdk_cli.eth_get_storage_at(*address, *slot).await?;
            println!("value: {}", value);
        }
        Cmd::GetTransactionCount { address } => {
            let sdk_cli = sdk_client(&cli).await?;
            let nonce = sdk_cli.transaction_count(*address).await?;
            println!("nonce: {}", nonce);
        }
        Cmd::GetRollups => {
            let rollups = Client::get_rollups(&cli.program_id, async_rpc_client(&cli.url))?;
            rollups.iter().for_each(|a| println!("{:?}", a));
        }
        Cmd::GetProgramBuildInfo => {
            let info = Client::program_build_info(&cli.program_id, async_rpc_client(&cli.url)).await?;
            println!("rome-evm build info: {:?}", info)
        }
        Cmd::CreateTreasure => {
            let keypair = keypair(&cli, false)?;
            let chain_id = cli.chain_id.expect("chain_id expected");

            Client::create_treasure(
                &cli.program_id,
                chain_id,
                Arc::new(keypair),
                async_rpc_client(&cli.url),
            )
                .await?;
            println!("treasure accounts have been created");
        }
        Cmd::JoinTreasure => {
            let keypair = keypair(&cli, false)?;
            let chain_id = cli.chain_id.expect("chain_id expected");

            Client::join_treasure(
                &cli.program_id,
                chain_id,
                Arc::new(keypair),
                async_rpc_client(&cli.url),
            )
                .await?;
            println!("balances of all treasure accounts have been transferred to upgrade_authority spl_associated_account");
        }
        Cmd::GetTreasureBalance => {
            treasure_balance(&cli).await?;
        }
        Cmd::AllocateResources { config } => {
            allocate_resources(&cli, config).await?;
        }
        Cmd::DeallocateResources { config } => {
            deallocate_resources(&cli, config).await?;
        }
        Cmd::Resources { config} => {
            show_resources(&cli, config).await?;
        }
        Cmd::Alt { action } => {
            crate::alt::dispatch(&cli, action).await?;
        }
    }

    Ok(())
}
