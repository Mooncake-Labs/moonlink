mod sink;
mod util;
use crate::pg_replicate::pipeline::{
    batching::data_pipeline::BatchDataPipeline,
    batching::BatchConfig,
    sinks::InfallibleSinkError,
    sources::postgres::{PostgresSource, PostgresSourceError, TableNamesFrom},
    PipelineAction, PipelineError,
};
use moonlink::ReadStateManager;

use sink::*;
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_postgres::{connect, Client, NoTls};

pub struct MoonlinkPostgresSource {
    uri: String,
    postgres_client: Client,
    handle: Option<
        JoinHandle<
            std::result::Result<(), PipelineError<PostgresSourceError, InfallibleSinkError>>,
        >,
    >,
}

impl MoonlinkPostgresSource {
    pub async fn new(uri: String) -> Result<Self, PostgresSourceError> {
        let (postgres_client, connection) = connect(&uri, NoTls).await.unwrap();
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                panic!("connection error: {}", e);
            }
        });
        postgres_client
            .simple_query(
                "DROP PUBLICATION IF EXISTS moonlink_pub; CREATE PUBLICATION moonlink_pub;",
            )
            .await
            .unwrap();
        Ok(Self {
            uri,
            postgres_client,
            handle: None,
        })
    }

    pub async fn add_table(
        &mut self,
        table: &str,
    ) -> Result<ReadStateManager, PostgresSourceError> {
        self.postgres_client
            .simple_query(&format!(
                "ALTER PUBLICATION moonlink_pub ADD TABLE {};",
                table
            ))
            .await
            .unwrap();
        self.postgres_client
            .simple_query(&format!("ALTER TABLE {} REPLICA IDENTITY FULL;", table))
            .await
            .unwrap();
        let source = PostgresSource::new(
            &self.uri,
            Some("moonlink_slot".to_string()),
            TableNamesFrom::Publication("moonlink_pub".to_string()),
        )
        .await?;
        let (reader_notifier, mut reader_notifier_receiver) = mpsc::channel(1);

        let sink = Sink::new(reader_notifier, PathBuf::from("./mooncake_test/"));
        let batch_config = BatchConfig::new(1000, Duration::from_secs(1));
        let mut pipeline =
            BatchDataPipeline::new(source, sink, PipelineAction::CdcOnly, batch_config);
        self.handle = Some(tokio::spawn(async move { pipeline.start().await }));

        let res = reader_notifier_receiver.recv().await;
        Ok(res.unwrap())
    }

    pub fn check_table_belongs_to_source(&self, uri: &str) -> bool {
        self.uri == uri
    }
}
