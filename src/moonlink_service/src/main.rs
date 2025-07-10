use clap::Parser;
use moonlink_service::{start, Result};

#[derive(Parser)]
struct Cli {
    base_path: String,
    metadata_store_uris: Vec<String>,
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let cli = Cli::parse();
    start(cli.base_path, cli.metadata_store_uris).await
}
