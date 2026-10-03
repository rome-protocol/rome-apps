use {
    crate::{
        program_option::{async_rpc_client, Cli,}, aux::*,
    },
    ethers::types::Address,
    rome_sdk::{
        rome_evm_client::{
            RomeEVMClient as Client,
            emulator,
            rome_evm::{
                pda::Pda, TREASURE_NUMBER, TREASURE_LAMPORTS,
            },
            emulator::stubs::Stubs,
        },
        rome_solana::{
            solana_rpc_client::SolanaRpcClient
        },
    },
    solana_sdk::{
        sysvar::Sysvar, rent::Rent,
        signature::{Keypair, Signer}, program_stubs::set_syscall_stubs,
        pubkey::Pubkey,
    },
    spl_associated_token_account_interface::address::get_associated_token_address_with_program_id as ata,
    std::{
        sync::Arc,
    },
};


pub async fn deposit(
    sdk_cli: &Client,
    address: Address,
    amount: u128,
    keypair: Keypair,
) -> anyhow::Result<()> {
    let user = &keypair.pubkey();
    let wallet = sdk_cli.sol_wallet();
    let rpc = sdk_cli.rpc_client();

    let rollup = Client::get_rollup_info(
        sdk_cli.program_id(),
        rpc.clone(),
        sdk_cli.chain_id(),
    )?;

    let f = async |opt: Option<(Pubkey, Pubkey)>, key: &Pubkey| {
        if let Some((mint, spl_program)) = opt.as_ref() {
            spl_balance(rpc.clone(), key, mint, spl_program).await
        } else {
            lamports(rpc.clone(), key).await
        }
    };

    let opt = if let Some(mint) = rollup.mint {
        Some((mint, get_spl_program(rpc.clone(), &mint).await?))
    } else {
        None
    };

    let user_b1 = f(opt, user).await?;
    let wallet_b1 = f(opt, &wallet).await?;

    sdk_cli.deposit(address, amount.into(), &keypair).await?;

    let user_b2 = f(opt, user).await?;
    let wallet_b2 = f(opt, &wallet).await?;
    let wei = sdk_cli.get_balance(address).await?;

    println!(
        "Funds have been deposited: chain_id {}, address {},  mint: {:?}, tokens: {}",
        sdk_cli.chain_id(),
        address,
        rollup.mint,
        amount,
    );

    if let Some((mint, spl_program)) = opt.as_ref() {
        println!("mint:            {}, spl_program: {}", mint, spl_program);
        println!("user wallet:     {}, spl_associated_account: {}, ", user, ata(user, mint, spl_program));
        println!("rome-evm wallet: {}, spl_associated_account: {}, ", wallet, ata(&wallet, mint, spl_program));
    } else {
        println!("user wallet:     {}", user);
        println!("rome-evm wallet: {}", wallet);
    }
    println!("user balance (Wei):  {}", wei);
    println!("user wallet:         {} -> {}", user_b1, user_b2);
    println!("rome-evm wallet:     {} -> {}", wallet_b1, wallet_b2);

    Ok(())
}

pub async fn treasure_balance(cli: &Cli) -> anyhow::Result<()>{
    let chain_id = cli.chain_id.expect("chain_id expected");
    let pda = Pda::new_(&cli.program_id, chain_id);
    let client = async_rpc_client(&cli.url);
    let mut total  = 0_u64;

    let stubs = Stubs::from_chain(Arc::clone(&client.get_account_storage()))?;
    set_syscall_stubs(stubs);
    let rent = Rent::get()?.minimum_balance(0);

    let upg_auth = emulator::upgrade_auth_key(&cli.program_id, client.get_account_storage())?;
    let upg_auth_lamports = client.get_balance(&upg_auth).await?;

    for i in 0..TREASURE_NUMBER {
        let (key, _) = pda.treasure_wallet(i);
        let lamports = client.get_balance(&key).await?.saturating_sub(rent);
        total += lamports;
        println!("{} {} {}", i, key, lamports)
    }

    println!("total lamports:  {}", total);
    println!("number of treasure payments:  {}\n", total/TREASURE_LAMPORTS);
    println!("upgrade_authority: {} {}", upg_auth, upg_auth_lamports);

    Ok(())
}
