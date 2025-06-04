use crate::pg_replicate::conversions::cdc_event::CdcEventConversionError;
use crate::pg_replicate::moonlink_sink::Sink;
use crate::pg_replicate::postgres_source::CdcStream;
use crate::pg_replicate::postgres_source::{CdcStreamError, PostgresSource, TableNamesFrom};
use crate::pg_replicate::table_init::build_table_components;
use crate::Result;
use moonlink::{IcebergTableEventManager, ReadStateManager};
use std::sync::Arc;
use tokio::pin;
use tokio::time::Duration;
use tokio_postgres::types::PgLsn;

use crate::pg_replicate::replication_state::ReplicationState;
use crate::pg_replicate::table::{TableId, TableSchema};
use futures::StreamExt;
use moonlink::TableEvent;
use std::collections::HashMap;
use std::path::Path;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_postgres::{connect, Client, Config, NoTls};

pub enum Command {
    AddTable {
        table_id: TableId,
        schema: TableSchema,
        event_sender: mpsc::Sender<TableEvent>,
        commit_lsn_tx: watch::Sender<u64>,
    },
    DropTable {
        table_id: TableId,
    },
}

pub struct ReplicationConnection {
    uri: String,
    table_base_path: String,
    table_temp_files_directory: String,
    postgres_client: Client,
    handle: Option<JoinHandle<Result<()>>>,
    table_readers: HashMap<TableId, ReadStateManager>,
    iceberg_table_event_managers: HashMap<TableId, IcebergTableEventManager>,
    cmd_tx: mpsc::Sender<Command>,
    cmd_rx: Option<mpsc::Receiver<Command>>,
    replication_state: Arc<ReplicationState>,
    source: PostgresSource,
}

impl ReplicationConnection {
    pub async fn new(
        uri: String,
        table_base_path: String,
        table_temp_files_directory: String,
    ) -> Result<Self> {
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

        let db_name = uri
            .parse::<Config>()
            .ok()
            .and_then(|c| c.get_dbname().map(|s| s.to_string()))
            .unwrap_or_else(|| "".to_string());
        let slot_name = if db_name.is_empty() {
            "moonlink_slot".to_string()
        } else {
            format!("moonlink_slot_{}", db_name)
        };

        let postgres_source = PostgresSource::new(
            &uri,
            Some(slot_name),
            TableNamesFrom::Publication("moonlink_pub".to_string()),
        )
        .await
        .unwrap();

        let (cmd_tx, cmd_rx) = mpsc::channel(8);

        Ok(Self {
            uri,
            table_base_path,
            table_temp_files_directory,
            postgres_client,
            handle: None,
            table_readers: HashMap::new(),
            iceberg_table_event_managers: HashMap::new(),
            cmd_tx,
            cmd_rx: Some(cmd_rx),
            replication_state: ReplicationState::new(),
            source: postgres_source,
        })
    }

    async fn alter_table_replica_identity(&self, table_name: &str) -> Result<()> {
        self.postgres_client
            .simple_query(&format!(
                "ALTER TABLE {} REPLICA IDENTITY FULL;",
                table_name
            ))
            .await
            .unwrap();
        Ok(())
    }

    async fn add_table_to_publication(&self, table_name: &str) -> Result<()> {
        self.postgres_client
            .simple_query(&format!(
                "ALTER PUBLICATION moonlink_pub ADD TABLE {};",
                table_name
            ))
            .await
            .unwrap();
        self.postgres_client
            .simple_query(&format!(
                "ALTER TABLE {} REPLICA IDENTITY FULL;",
                table_name
            ))
            .await
            .unwrap();
        Ok(())
    }

    async fn remove_table_from_publication(&self, table_name: &str) -> Result<()> {
        self.postgres_client
            .simple_query(&format!(
                "ALTER PUBLICATION moonlink_pub DROP TABLE {};",
                table_name
            ))
            .await
            .unwrap();
        Ok(())
    }

    pub fn get_table_reader(&self, table_id: TableId) -> &ReadStateManager {
        self.table_readers.get(&table_id).unwrap()
    }

    pub fn get_iceberg_table_event_manager(
        &mut self,
        table_id: TableId,
    ) -> &mut IcebergTableEventManager {
        self.iceberg_table_event_managers
            .get_mut(&table_id)
            .unwrap()
    }

    async fn spawn_replication_task(
        &mut self,
        sink: Sink,
        cmd_rx: mpsc::Receiver<Command>,
    ) -> JoinHandle<Result<()>> {
        self.source
            .commit_transaction()
            .await
            .expect("failed to commit transaction");

        // TODO: start from the last lsn in replication state for recovery.
        // TODO: track tables copied in replication state and recover these before starting the event loop.
        let mut last_lsn: u64 = 0;
        last_lsn += 1;
        let stream = self
            .source
            .get_cdc_stream(last_lsn.into())
            .await
            .expect("failed to get cdc stream");

        tokio::spawn(async move { run_event_loop(stream, sink, cmd_rx).await })
    }

    async fn add_table_to_replication(&mut self, schema: &TableSchema) -> Result<()> {
        let table_id = schema.table_id;
        let resources = build_table_components(
            schema,
            Path::new(&self.table_base_path),
            self.table_temp_files_directory.clone(),
            &self.replication_state,
        )
        .await?;

        self.table_readers
            .insert(table_id, resources.read_state_manager);
        self.iceberg_table_event_managers
            .insert(table_id, resources.iceberg_table_event_manager);
        self.cmd_tx
            .send(Command::AddTable {
                table_id,
                schema: schema.clone(),
                event_sender: resources.event_sender,
                commit_lsn_tx: resources.commit_lsn_tx,
            })
            .await
            .unwrap();

        Ok(())
    }

    async fn remove_table_from_replication(&mut self, table_id: u32) -> Result<()> {
        self.drop_iceberg_table(table_id).await?;

        self.table_readers.remove_entry(&table_id).unwrap();
        self.iceberg_table_event_managers
            .remove_entry(&table_id)
            .unwrap();
        self.cmd_tx
            .send(Command::DropTable { table_id })
            .await
            .unwrap();

        Ok(())
    }

    /// Clean up iceberg table in a blocking manner.
    async fn drop_iceberg_table(&mut self, table_id: u32) -> Result<()> {
        let iceberg_state_manager = self
            .iceberg_table_event_managers
            .get_mut(&table_id)
            .unwrap();
        iceberg_state_manager.drop_table().await?;
        Ok(())
    }

    pub async fn start_replication(&mut self) -> Result<()> {
        let table_schemas = self.source.get_table_schemas().await;

        let (tx, rx) = mpsc::channel(8);
        self.cmd_tx = tx;
        self.cmd_rx = Some(rx);

        let sink = Sink::new(self.replication_state.clone());
        let receiver = self.cmd_rx.take().unwrap();
        self.handle = Some(self.spawn_replication_task(sink, receiver).await);

        for schema in table_schemas.values() {
            self.add_table_to_replication(schema).await?;
        }

        Ok(())
    }

    pub async fn add_table(&mut self, table_name: &str) -> Result<TableId> {
        // TODO: We should not naively alter the replica identity of a table. We should only do this if we are sure that the table does not already have a FULL replica identity. [https://github.com/Mooncake-Labs/moonlink/issues/104]
        self.alter_table_replica_identity(table_name).await?;
        let table_schema = self.source.fetch_table_schema(table_name, None).await?;

        self.add_table_to_replication(&table_schema).await?;

        self.add_table_to_publication(table_name).await?;

        Ok(table_schema.table_id)
    }

    /// Remove the given table from connection.
    pub async fn drop_table(&mut self, table_id: u32) -> Result<()> {
        let table_name = self.source.get_table_name_from_id(table_id);
        // Remove table from publication as the first step, to prevent further events.
        self.remove_table_from_publication(&table_name).await?;
        self.source.remove_table_schema(table_id);
        self.remove_table_from_replication(table_id).await?;
        Ok(())
    }

    pub fn check_table_belongs_to_source(&self, uri: &str) -> bool {
        self.uri == uri
    }
}

async fn run_event_loop(
    stream: CdcStream,
    mut sink: Sink,
    mut cmd_rx: mpsc::Receiver<Command>,
) -> Result<()> {
    pin!(stream);

    let mut status_interval = tokio::time::interval(Duration::from_secs(10));
    let mut last_lsn = PgLsn::from(0);

    loop {
        tokio::select! {
             _ = status_interval.tick() => {
                let _ = stream.as_mut().send_status_update(last_lsn).await;
            },
            Some(cmd) = cmd_rx.recv() => match cmd {
                Command::AddTable { table_id, schema, event_sender, commit_lsn_tx } => {
                    sink.add_table(table_id, event_sender, commit_lsn_tx);
                    stream.as_mut().add_table_schema(schema);
                }
                Command::DropTable { table_id } => {
                    sink.drop_table(table_id);
                }
            },
            event = StreamExt::next(&mut stream) => {
                let Some(event) = event else { break; };
                if let Err(CdcStreamError::CdcEventConversion(CdcEventConversionError::MissingSchema(_))) = &event {
                    continue;
                }
                let event = event?;
                last_lsn = sink.process_cdc_event(event).await.unwrap();
            }
        }
    }

    Ok(())
}
