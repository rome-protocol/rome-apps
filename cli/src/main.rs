mod alt;
mod alt_compat;
mod cmd;
mod program_option;
    mod aux;
mod ix;
mod resources;

use {clap::Parser, program_option::Cli};


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    cmd::execute(cli).await
}
