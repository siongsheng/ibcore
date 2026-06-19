use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "ibkr-diag", about = "Diagnostics CLI for IB Gateway health checks")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Print version information for ibkr-diag, ibcore, and ibapi
    Version,
}

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Commands::Version => {
            println!("ibkr-diag {}", env!("CARGO_PKG_VERSION"));
            println!("ibcore    {}", ibcore::version());
            println!("ibapi     {}", ibcore::IBAPI_VERSION);
        }
    }
}
