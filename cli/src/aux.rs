use {
    crate::program_option::Cli,
    rome_sdk::{
        rome_evm_client::{
            Payer, ResourceItem,
        },
        rome_utils::config::ReadableConfig,
        rome_solana::solana_rpc_client::SolanaRpcClient,
    },
    solana_sdk::{
        pubkey::Pubkey, signature::{read_keypair_file, Keypair,}, account::Account,
    },
    std::{path::Path, sync::Arc,},
    solana_cli_config::{
        CONFIG_FILE, Config,
    },
    proxy::ProxyConfig,
    spl_token_2022_interface::{
        extension::StateWithExtensionsOwned, state::Account as Account2022,
    },
    spl_associated_token_account_interface::address::get_associated_token_address_with_program_id as ata,
};

pub const SPL_TOKEN_ID: Pubkey = Pubkey::from_str_const("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");

pub async fn get_acc(rpc: Arc<dyn SolanaRpcClient>, key: &Pubkey) -> anyhow::Result<Option<Account>> {
    let opt = rpc
        .get_account_with_commitment(key, rpc.commitment())
        .await?
        .value;
    
    Ok(opt)
}
pub async fn spl_balance(rpc: Arc<dyn SolanaRpcClient>, owner: &Pubkey, mint: &Pubkey, spl_program: &Pubkey) -> anyhow::Result<u64> {
    let ata = ata(owner, mint, spl_program);

    let acc = get_acc(rpc, &ata)
        .await?
        .unwrap_or_else(|| panic!(
            "spl_associated_account not found: {}, owner: {}, mint: {}, spl_progam: {}",
            ata, owner, mint, spl_program
        ));

    let amount = StateWithExtensionsOwned::<Account2022>::unpack(acc.data)?.base.amount;

    Ok(amount)
}

pub async fn lamports(rpc: Arc<dyn SolanaRpcClient>, key: &Pubkey) -> anyhow::Result<u64> {
    let x = get_acc(rpc, key).await?.map(|a| a.lamports ).unwrap_or_default();

    Ok(x)
}

pub fn keypair(cli: &Cli, use_config: bool) -> anyhow::Result<Keypair> {
    let path = if let Some(keypair) = cli.keypair.as_ref() {
        keypair.clone()
    } else {
        if !use_config {
            return Err(anyhow::anyhow!("instruction requires to specify --keypair parameter"))
        }

        if let Some(ref file) = *CONFIG_FILE {
            let cfg: Config = solana_cli_config::load_config_file(file)
                .expect("unable to open default config file");

            println!("keypair is not specified, the default keypair will be used: {}", cfg.keypair_path);
            cfg.keypair_path
        } else {
            return Err(anyhow::anyhow!("load config error: home_dir() is not available"))
        }
    };

    let keypair = read_keypair_file(Path::new(&path))
        .unwrap_or_else(|_| panic!("read keypair error {}", path));

    Ok(keypair)
}

pub async fn parse_config(config: &Path) -> anyhow::Result<Vec<ResourceItem>> {
    let payer_cfgs = ProxyConfig::read(config)
        .await
        .expect("error to read proxy config")
        .payers;

    let items = Payer::from_config_list(&payer_cfgs)
        .await?
        .into_iter()
        .map(ResourceItem::from_payer)
        .collect::<Vec<_>>()
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    
    Ok(items)    
}

pub async fn get_spl_program(rpc: Arc<dyn SolanaRpcClient>, mint: &Pubkey) -> anyhow::Result<Pubkey> {
    let acc = get_acc(rpc.clone(), mint)
        .await?
        .ok_or(anyhow::anyhow!("spl mint account not found {}", mint))?;

    assert!(acc.owner == spl_token_2022_interface::ID || acc.owner == SPL_TOKEN_ID);

    Ok(acc.owner)
}
