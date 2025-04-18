mod sink;
mod util;

use moonlink::Result;
use pg_replicate::pipeline::batching::data_pipeline::BatchDataPipeline;
use pg_replicate::pipeline::batching::BatchConfig;
use pg_replicate::pipeline::sinks::InfallibleSinkError;
use pg_replicate::pipeline::sources::postgres::PostgresSourceError;
use pg_replicate::pipeline::sources::postgres::{PostgresSource, TableNamesFrom};
use pg_replicate::pipeline::PipelineAction;
use pg_replicate::pipeline::PipelineError;
use sink::*;
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_postgres::{config::ReplicationMode, Client, Config, NoTls};

#[derive(Debug, PartialEq, Eq)]
pub struct PostgresSourceMetadata {
    host: String,
    port: u16,
    database: String,
    username: String,
    password: String,
}

impl PostgresSourceMetadata {
    pub fn new(
        host: String,
        port: u16,
        database: String,
        username: String,
        password: String,
    ) -> Self {
        Self {
            host,
            port,
            database,
            username,
            password,
        }
    }
}

pub struct MoonlinkPostgresSource {
    metadata: PostgresSourceMetadata,
    postgres_client: Client,
    handle: Option<
        JoinHandle<
            std::result::Result<(), PipelineError<PostgresSourceError, InfallibleSinkError>>,
        >,
    >,
}

impl MoonlinkPostgresSource {
    pub async fn new(metadata: PostgresSourceMetadata) -> Result<Self> {
        let mut config = Config::new();
        config
            .host(&metadata.host)
            .port(metadata.port)
            .dbname(&metadata.database)
            .user(&metadata.username)
            .password(&metadata.password)
            .replication_mode(ReplicationMode::Logical);

        let (postgres_client, connection) = config.connect(NoTls).await.unwrap();
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
            metadata,
            postgres_client,
            handle: None,
        })
    }

    pub async fn add_table(
        &mut self,
        schema: &str,
        table: &str,
    ) -> Result<mpsc::Sender<moonlink::TableEvent>> {
        self.postgres_client
            .simple_query(&format!(
                "ALTER PUBLICATION moonlink_pub ADD TABLE {}.{};",
                schema, table
            ))
            .await
            .unwrap();
        let source = PostgresSource::new(
            &self.metadata.host,
            self.metadata.port,
            &self.metadata.database,
            &self.metadata.username,
            Some(self.metadata.password.clone()),
            Some("moonlink_slot".to_string()),
            TableNamesFrom::Publication("moonlink_pub".to_string()),
        )
        .await?;
        let (reader_notifier, mut reader_notifier_receiver) = mpsc::channel(1);

        let sink = Sink::new(
            reader_notifier,
            PathBuf::from("/home/vscode/mooncake_test/"),
        );
        let batch_config = BatchConfig::new(1000, Duration::from_secs(1));
        let mut pipeline =
            BatchDataPipeline::new(source, sink, PipelineAction::CdcOnly, batch_config);
        self.handle = Some(tokio::spawn(async move { pipeline.start().await }));

        let res = reader_notifier_receiver.recv().await;
        Ok(res.unwrap())
    }

    pub fn check_table_belongs_to_source(&self, metadata: &PostgresSourceMetadata) -> bool {
        self.metadata == *metadata
    }
}
