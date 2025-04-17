use moonlink::TableEvent;
use moonlink_connectors::pg_replicate::pipeline::{
    batching::{data_pipeline::BatchDataPipeline, BatchConfig},
    sources::postgres::{PostgresSource, TableNamesFrom},
    PipelineAction,
};
use moonlink_connectors::Sink;
use std::path::PathBuf;
use std::{error::Error, time::Duration};
use tokio::sync::mpsc;
use tokio::sync::oneshot;
#[derive(Debug)]
pub struct MooncakeTableMetadata {
    pub data_files: Vec<String>,
    pub position_deletes: Vec<(u32, u32)>,
}

pub async fn replicate_pg_table_and_create_parquet_async(
    _table_name: &str,
    _output_dir: &str,
) -> Result<MooncakeTableMetadata, Box<dyn Error>> {
    let source = PostgresSource::new(
        "localhost",
        28817,
        "pg_mooncake",
        "vscode",
        Some("password".to_string()),
        Some("moonlink_slot".to_string()),
        TableNamesFrom::Publication("moonlink_pub".to_string()),
    )
    .await?;
    let (reader_notifier, mut reader_notifier_receiver) = mpsc::channel(1);

    let sink = Sink::new(
        Some(reader_notifier),
        PathBuf::from("/home/vscode/mooncake_test/"),
    );
    let batch_config = BatchConfig::new(1000, Duration::from_secs(1));
    let mut pipeline = BatchDataPipeline::new(source, sink, PipelineAction::CdcOnly, batch_config);
    let fut = tokio::spawn(async move { pipeline.start().await });

    let res = reader_notifier_receiver.recv().await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let (response_channel, response_receiver) = oneshot::channel();
    res.unwrap()
        .send(TableEvent::PrepareRead {
            response_channel: (response_channel),
        })
        .await
        .unwrap();

    let (data_files, position_deletes) = response_receiver.await.unwrap();

    fut.abort();
    Ok(MooncakeTableMetadata {
        data_files: data_files
            .iter()
            .map(|p| p.to_string_lossy().to_string())
            .collect(),
        position_deletes: position_deletes
            .iter()
            .map(|(start, end)| (*start as u32, *end as u32))
            .collect(),
    })
}

pub fn replicate_pg_table_and_create_parquet(
    table_name: &str,
    output_dir: &str,
) -> MooncakeTableMetadata {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let metadata = rt.block_on(replicate_pg_table_and_create_parquet_async(
        table_name, output_dir,
    ));
    if let Err(e) = metadata {
        panic!("Error: {:?}", e)
    } else {
        metadata.unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[ignore] // Only works with a local postgres running on port 28817
    #[test]
    fn test_replicate_pg_table_and_create_parquet() {
        let metadata = replicate_pg_table_and_create_parquet("test", "test");
        println!("{:?}", metadata);
    }
}
