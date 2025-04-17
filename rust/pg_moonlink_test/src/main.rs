use moonlink_connectors::pg_replicate::pipeline::{
    batching::{data_pipeline::BatchDataPipeline, BatchConfig},
    sources::postgres::{PostgresSource, TableNamesFrom},
    PipelineAction,
};
use moonlink_connectors::Sink;
use std::{error::Error, path::PathBuf, time::Duration};

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
    let sink = Sink::new(None, PathBuf::from("/home/vscode/moonlink_test/"));
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
