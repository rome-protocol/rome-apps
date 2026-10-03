//! `alt` subcommand — operator-facing driver for the persistent-ALT
//! wind-down: `health` (on-chain contents vs the recorded hash) and `retire`
//! (deactivate). Provisioning/grow/derive verbs were removed in the v1
//! migration (the sender feature they fed, and `rome_solana::alt_manager` /
//! `alt_provision`, are both gone) — see `crate::alt_compat` for the
//! vendored read-side primitives this module still depends on.

use {
    crate::{
        alt_compat::{build_deactivate_ix, check_contents, AltsManifest, TableHealth},
        aux::keypair,
        program_option::{async_rpc_client, AltCmd, Cli},
    },
    rome_sdk::rome_solana::types::AsyncAtomicRpcClient,
    solana_address_lookup_table_interface::state::AddressLookupTable,
    solana_sdk::{
        instruction::Instruction, pubkey::Pubkey, signature::Keypair, signer::Signer,
        transaction::Transaction,
    },
    std::path::Path,
};

pub async fn dispatch(cli: &Cli, action: &AltCmd) -> anyhow::Result<()> {
    match action {
        AltCmd::Health { manifest } => health(cli, Path::new(manifest)).await,
        AltCmd::Retire { table } => retire(cli, table).await,
    }
}

/// Verify every registered table's live on-chain contents against its hash.
async fn health(cli: &Cli, manifest: &Path) -> anyhow::Result<()> {
    let rpc = async_rpc_client(&cli.url);
    let m = AltsManifest::load(manifest)?;
    let mut bad = 0usize;
    for rec in &m.tables {
        match fetch_table_addresses(&rpc, &rec.pubkey).await {
            Ok(on_chain) => match check_contents(&on_chain, &rec.contents_hash) {
                TableHealth::Ok => {
                    println!("OK    {} ({:?}, {} addrs)", rec.pubkey, rec.tier, on_chain.len())
                }
                TableHealth::Drift { .. } => {
                    println!("DRIFT {} ({:?})", rec.pubkey, rec.tier);
                    bad += 1;
                }
            },
            Err(e) => {
                println!("MISS  {} ({:?}): {e}", rec.pubkey, rec.tier);
                bad += 1;
            }
        }
    }
    if bad > 0 {
        anyhow::bail!("{bad} table(s) drifted or missing");
    }
    println!("all {} table(s) healthy", m.tables.len());
    Ok(())
}

/// Deactivate a table (closeable + rent-reclaimable after the cooldown).
async fn retire(cli: &Cli, table: &Pubkey) -> anyhow::Result<()> {
    let payer = keypair(cli, false)?;
    let authority = payer.pubkey();
    let rpc = async_rpc_client(&cli.url);
    send(
        &rpc,
        &payer,
        vec![build_deactivate_ix(table, &authority)],
        "deactivate",
    )
    .await?;
    println!("deactivated {table}; closeable + rent-reclaimable after the ~512-slot cooldown");
    Ok(())
}

async fn fetch_table_addresses(
    rpc: &AsyncAtomicRpcClient,
    table: &Pubkey,
) -> anyhow::Result<Vec<Pubkey>> {
    let acct = rpc.get_account(table).await?;
    let alt = AddressLookupTable::deserialize(&acct.data)
        .map_err(|e| anyhow::anyhow!("deserialize ALT {table}: {e}"))?;
    Ok(alt.addresses.to_vec())
}

async fn send(
    rpc: &AsyncAtomicRpcClient,
    payer: &Keypair,
    ixs: Vec<Instruction>,
    label: &str,
) -> anyhow::Result<()> {
    let bh = rpc.get_latest_blockhash().await?;
    let tx = Transaction::new_signed_with_payer(&ixs, Some(&payer.pubkey()), &[payer], bh);
    let sig = rpc.send_and_confirm_transaction(&tx).await?;
    println!("  {label}: {sig}");
    Ok(())
}
