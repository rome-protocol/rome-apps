use {
    crate::{
        alt_compat::{alt_slots_key, parse_alt_slots},
        program_option::{async_rpc_client, Cli,}, aux::*,
    },
    rome_sdk::{
        rome_evm_client::{
            RomeEVMClient as Client,
            rome_evm::pda::Pda,
            ResourceItem,
        },
        rome_solana::{
            types::AsyncAtomicRpcClient,
        },
    },
    solana_sdk::{
        signature::Signer,
        account_info::IntoAccountInfo, pubkey::Pubkey,
    },
    std::{
        sync::Arc, fmt::Formatter,
    },
    solana_address_lookup_table_interface::{
        instruction::*, state::AddressLookupTable,
    },
};

pub async fn allocate_resources(cli: &Cli, config: &std::path::Path) -> anyhow::Result<()> {
    let items = parse_config(config).await?;
    let chain = cli.chain_id.expect("chain_id expected");

    for item in items {
        Client::alloc_resource(
            &cli.program_id,
            chain,
            Arc::clone(&item.payer_keypair),
            async_rpc_client(&cli.url),
            item.holder,
        )
            .await?;
    }

    println!("resources have been allocated");
    Ok(())
}

enum PdaType {
    TxHolder,
    StateHolder,
    AltSlots_,
    Alt
}
use PdaType::*;

impl std::fmt::Debug for PdaType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TxHolder => write!(f, "TxHolder"),
            StateHolder => write!(f, "StateHolder"),
            AltSlots_ => write!(f, "AltSlots"),
            Alt => write!(f, "Alt"),
        }
    }
}
pub struct PdaTyped {
    key: Pubkey,
    typ: PdaType,
}

impl PdaTyped {
    pub async fn lamports(&self, rpc: &AsyncAtomicRpcClient, remark: &str) -> anyhow::Result<u64> {
        match self.typ {
            TxHolder | StateHolder | AltSlots_ => {
                if let Some(acc) = get_acc(rpc.clone(), &self.key).await? {
                    println!("      {:<47} {:>10} {:>8} {:>12?} {}", self.key.to_string(), acc.lamports, acc.data.len(), self.typ, remark);
                    return Ok(acc.lamports)
                }
            },
            Alt => {
                if let Some(mut alt) = get_acc(rpc.clone(), &self.key).await? {
                    let info = (&self.key, &mut alt).into_account_info();
                    let data = info.try_borrow_data()?;
                    let state =  AddressLookupTable::deserialize(&data)?;
                    let active = if state.meta.deactivation_slot == u64::MAX {
                        "(active)"
                    } else {
                        "(deactivated)"
                    };
                    let len = state.addresses.len();

                    println!("      {:<47} {:>10} {:>8} {:>12?} {} {} keys {}",
                        self.key.to_string(), info.lamports(), info.data_len(), self.typ, active, len, remark);
                    return Ok(info.lamports())
                }
            },
        }

        Ok(0)
    }
}

pub async fn find_pda_keys(
    rpc: AsyncAtomicRpcClient,
    payer: Pubkey,
    holder: u64,
    program_id: Pubkey,
    chain: u64,
) -> anyhow::Result<Vec<PdaTyped>> {
    let mut keys = vec![];
    let pda = Pda::new_(&program_id, chain);
    let (tx_holder, _) = pda.tx_holder_key(&payer, holder);
    let (state_holder, _) = pda.state_holder_key(&payer, holder);
    let alt_slots = alt_slots_key(&program_id, chain, &payer, holder);

    keys.push(PdaTyped{
        key: tx_holder,
        typ: TxHolder,
    });
    keys.push(PdaTyped{
        key: state_holder,
        typ: StateHolder,
    });
    keys.push( PdaTyped{
        key: alt_slots,
        typ: AltSlots_,
    });

    // Read-only: the on-chain `AltSlots` struct itself was deleted in the v1
    // migration (`AltDealloc` is now `unsupported`), so any slots this finds
    // are stranded rent from a pre-upgrade holder — see the `deallocate`
    // caller's "stranded" label. Never written here.
    if let Some(acc) = get_acc(rpc.clone(), &alt_slots).await? {
        let mut alt = parse_alt_slots(&acc.data)?
            .into_iter()
            .map(|slot|
                PdaTyped{
                    key: derive_lookup_table_address(&payer, slot).0,
                    typ: Alt,
                }
            )
            .collect::<Vec<_>>();
        keys.append(&mut alt);
    }

    Ok(keys)
}

async fn count_allocated(vec: &Vec<PdaTyped>, rpc: &AsyncAtomicRpcClient, remark: &str) -> anyhow::Result<(u64, u64)> {
    let mut cnt = 0_u64;
    let mut total = 0_u64;

    for pda in vec {
        let lamports = pda.lamports(rpc, remark).await?;
        cnt += (lamports > 0) as u64;
        total += lamports;
    }

    if cnt > 0 {
        println!("      lamports: {}", total);
    }
    Ok((cnt, total))
}

pub async fn payers_lamports(rpc: &AsyncAtomicRpcClient, vec: &Vec<ResourceItem>) -> anyhow::Result<u64> {
    let mut lamports = 0_u64;
    for i in vec {
        lamports += rpc.get_balance(&i.payer_keypair.pubkey()).await?;
    }

    Ok(lamports)
}

pub async fn deallocate_resources(cli: &Cli, config: &std::path::Path) -> anyhow::Result<()> {
    let items = parse_config(config).await?;
    let rpc = async_rpc_client(&cli.url);

    let mut active_cnt = 0;
    let mut active_lamports = 0;
    let mut stranded_alt_cnt = 0;
    let lamports1  = payers_lamports(&rpc, &items).await?;

    for item in items.iter() {
        let payer = item.payer_keypair.pubkey();
        println!("\npayer: {}", payer);
        println!("  resource: {}", item.holder);
        let keys = find_pda_keys(
            rpc.clone(),
            payer,
            item.holder,
            cli.program_id,
            cli.chain_id.expect("chain_id expected"),
        )
            .await?;

        let (cnt, _) = count_allocated(&keys, &rpc, "").await?;
        if cnt > 0 {
            println!();
            Client::dealloc_resource(
                &cli.program_id,
                cli.chain_id.unwrap(),
                Arc::clone(&item.payer_keypair),
                async_rpc_client(&cli.url),
                item.holder,
            )
                .await?;
            println!();

            let (cnt, lamports) = count_allocated(&keys, &rpc, " -- unable to deallocate").await?;
            active_cnt += cnt;
            active_lamports += lamports;
            // The merged program's `dealloc_resource` never touched AltSlots
            // (only tx/state holders) — `AltDealloc` is `unsupported`. Any
            // AltSlots/Alt row still active after the call above is stranded
            // rent, not a "retry in 512 slots" case; count it separately so
            // the summary doesn't mislead the operator into retrying.
            for pda in keys.iter().filter(|p| matches!(p.typ, AltSlots_ | Alt)) {
                if get_acc(rpc.clone(), &pda.key).await?.is_some_and(|a| a.lamports > 0) {
                    stranded_alt_cnt += 1;
                }
            }
        }
    }
    let lamports2  = payers_lamports(&rpc, &items).await?;

    println!("\nrefunded lamports: {}", lamports2.saturating_sub(lamports1));
    if active_cnt != 0 {
        println!("Cannot deallocate {} accounts with {} lamports", active_cnt, active_lamports );
        let retryable = active_cnt - stranded_alt_cnt;
        if retryable > 0 {
            println!("{retryable} account(s): try to deallocate again in 512 slots");
        }
        if stranded_alt_cnt > 0 {
            println!(
                "{stranded_alt_cnt} AltSlots account(s): stranded — program no longer supports \
                 AltDealloc (reclaim on legacy-program chains before upgrade)"
            );
        }
    } else {
        println!("resources have been deallocated");
    }

    Ok(())
}
pub async fn show_resources(cli: &Cli, config: &std::path::Path) -> anyhow::Result<()> {
    let items = parse_config(config).await?;
    let rpc = async_rpc_client(&cli.url);
    let mut total = 0;
    let mut total_lamports = 0;

    for item in items.iter() {
        let payer = item.payer_keypair.pubkey();
        println!("\npayer: {}", payer);
        println!("  resource: {}", item.holder);
        let keys = find_pda_keys(
            rpc.clone(),
            payer,
            item.holder,
            cli.program_id,
            cli.chain_id.expect("chain_id expected"),
        )
            .await?;

        let (cnt, lamports) = count_allocated(&keys, &rpc, "").await?;
        total += cnt;
        total_lamports += lamports;
    }

    println!("\ntotal:");
    println!("  accounts: {}", total);
    println!("  lamports: {}", total_lamports);

    Ok(())
}
