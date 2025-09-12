use clap::Parser;
use datafusion::prelude::SessionContext;
use datafusion_cli::exec::exec_from_repl;
use datafusion_cli::print_format::PrintFormat;
use datafusion_cli::print_options::{MaxRows, PrintOptions};
use moonlink_datafusion::MooncakeCatalogProvider;
use std::error::Error;
use std::sync::Arc;

#[derive(Parser)]
struct Cli {
    uri: String,
}

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    let ctx = SessionContext::new();
    let catalog = MooncakeCatalogProvider::try_new(cli.uri).await?;
    ctx.register_catalog("mooncake", Arc::new(catalog));

    // EXAMPLE:
    // let df = ctx
    //     .sql("SELECT * FROM mooncake.'<database_id>'.'<table_id>'")
    //     .await?;
    // df.show().await?;

    let mut print_options = PrintOptions {
        format: PrintFormat::Automatic,
        quiet: false,
        maxrows: MaxRows::Limited(40),
        color: true,
    };
    exec_from_repl(&ctx, &mut print_options).await?;
    Ok(())
}
