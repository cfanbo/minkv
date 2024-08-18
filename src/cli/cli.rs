use super::super::server;
use clap::arg;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(arg_required_else_help = true)]
#[command(version, version=option_env!("VERGEN_GIT_DESCRIBE").unwrap_or("unknown"), about, long_about = None)]
struct Cli {
    /// Sets a custom config file
    #[arg(short, long, value_name = "FILE")]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// startup KV database server
    Serve {
        /// Sets a custom config file
        #[arg(short, long, value_name = "FILE")]
        config: Option<PathBuf>,
    },
}

pub fn parse() -> anyhow::Result<()> {
    env_logger::init();

    let cli = Cli::parse();

    match &cli.command {
        Some(Commands::Serve { config }) => server::start_server(config),
        None => Ok(()),
    }
}
