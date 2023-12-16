use clap::Parser;
use dfsql::cli::Cli;

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    cli.run()?;
    Ok(())
}
