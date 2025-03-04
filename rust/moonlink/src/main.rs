mod sink;

use pg_replicate::pipeline::{
    batching::{data_pipeline::BatchDataPipeline, BatchConfig},
    sources::postgres::{PostgresSource, TableNamesFrom},
    PipelineAction,
};
use sink::Sink;
use std::{error::Error, time::Duration};

async fn main_impl() -> Result<(), Box<dyn Error>> {
    let source = PostgresSource::new(
        "localhost",
        5432,
        "postgres",
        "postgres",
        Some("password".to_string()),
        Some("moonlink_slot".to_string()),
        TableNamesFrom::Publication("moonlink_pub".to_string()),
    )
    .await?;
    let sink = Sink;
    let batch_config = BatchConfig::new(1000, Duration::from_secs(1));
    let mut pipeline = BatchDataPipeline::new(source, sink, PipelineAction::Both, batch_config);
    pipeline.start().await?;
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    if let Err(e) = main_impl().await {
        eprintln!("{e}");
    }
    Ok(())
}
