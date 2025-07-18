use clap::Parser;
use moonlink_service::{start, Result};

#[derive(Parser)]
struct Cli {
    base_path: String,
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let cli = Cli::parse();
    start(cli.base_path).await
}
