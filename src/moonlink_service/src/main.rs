use clap::Parser;
use moonlink_service::{start_with_config, Result, ServiceConfig};

#[derive(Parser)]
#[command(name = "moonlink-service")]
#[command(about = "Moonlink data ingestion service")]
struct Cli {
    /// Base path for Moonlink data storage
    base_path: String,

    /// Port for REST API server (optional, defaults to 3030)
    #[arg(long, short = 'p')]
    rest_api_port: Option<u16>,

    /// Disable REST API server
    #[arg(long)]
    no_rest_api: bool,
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let cli = Cli::parse();

    let config = ServiceConfig {
        base_path: cli.base_path,
        rest_api_port: if cli.no_rest_api {
            None
        } else {
            Some(cli.rest_api_port.unwrap_or(3030))
        },
    };

    start_with_config(config).await
}
