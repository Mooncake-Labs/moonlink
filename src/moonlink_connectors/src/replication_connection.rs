use crate::pg_replicate::clients::postgres::ReplicationClient;
use crate::pg_replicate::conversions::cdc_event::CdcEventConversionError;
use crate::pg_replicate::initial_copy::copy_table_stream_impl;
use crate::pg_replicate::moonlink_sink::{SchemaChangeRequest, Sink};
use crate::pg_replicate::postgres_source::{
    CdcStreamConfig, CdcStreamError, PostgresSource, PostgresSourceError,
};
use crate::pg_replicate::table_init::build_table_components;
use crate::Result;
use moonlink::{
    MoonlinkTableConfig, ObjectStorageCache, ReadStateManager, TableEventManager, TableStatusReader,
};
use std::io::{Error, ErrorKind};
use std::sync::Arc;
use tokio::pin;
use tokio::time::Duration;
use tokio_postgres::error::SqlState;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{tls::NoTlsStream, Connection, Socket};

use crate::pg_replicate::replication_state::ReplicationState;
use crate::pg_replicate::table::{SrcTableId, TableSchema};
use futures::StreamExt;
use moonlink::TableEvent;
use std::collections::HashMap;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_postgres::{connect, Client, Config, NoTls};
use tracing::Instrument;
use tracing::{debug, error, info_span, warn};

pub enum Command {
    AddTable {
        src_table_id: SrcTableId,
        schema: TableSchema,
        event_sender: mpsc::Sender<TableEvent>,
        commit_lsn_tx: watch::Sender<u64>,
        flush_lsn_rx: watch::Receiver<u64>,
        wal_flush_lsn_rx: watch::Receiver<u64>,
    },
    DropTable {
        src_table_id: SrcTableId,
    },
    Shutdown,
}

struct TableState {
    schema: TableSchema,
    reader: ReadStateManager,
    event_manager: TableEventManager,
    status_reader: TableStatusReader,
}
/// Manages replication for table(s) within a database.
pub struct ReplicationConnection {
    uri: String,
    database_id: u32,
    table_base_path: String,
    postgres_client: Client,
    handle: Option<JoinHandle<Result<()>>>,
    table_states: HashMap<SrcTableId, TableState>,
    cmd_tx: mpsc::Sender<Command>,
    cmd_rx: Option<mpsc::Receiver<Command>>,
    replication_state: Arc<ReplicationState>,
    source: Arc<PostgresSource>,
    replication_started: bool,
    slot_name: String,
    /// Object storage cache.
    object_storage_cache: ObjectStorageCache,
    /// Background retry handles for drop operations.
    retry_handles: Vec<JoinHandle<Result<()>>>,
}

impl ReplicationConnection {
    pub async fn new(
        uri: String,
        database_id: u32,
        table_base_path: String,
        object_storage_cache: ObjectStorageCache,
    ) -> Result<Self> {
        debug!(%uri, "initializing replication connection");

        let (postgres_client, connection) = connect(&uri, NoTls)
            .await
            .map_err(PostgresSourceError::from)?;
        tokio::spawn(
            async move {
                if let Err(e) = connection.await {
                    warn!("connection error: {}", e);
                }
            }
            .instrument(info_span!("postgres_connection_monitor")),
        );
        postgres_client
            .simple_query("SET lock_timeout = '100ms';")
            .await?;
        postgres_client
            .simple_query(
                "DROP PUBLICATION IF EXISTS moonlink_pub; CREATE PUBLICATION moonlink_pub WITH (publish_via_partition_root = true);",
            )
            .await
            .map_err(PostgresSourceError::from)?;

        let db_name = uri
            .parse::<Config>()
            .ok()
            .and_then(|c| c.get_dbname().map(|s| s.to_string()))
            .unwrap_or_else(|| "".to_string());
        let slot_name = if db_name.is_empty() {
            "moonlink_slot".to_string()
        } else {
            format!("moonlink_slot_{db_name}")
        };

        let postgres_source = PostgresSource::new(
            &uri,
            Some(slot_name.clone()),
            Some("moonlink_pub".to_string()),
            true,
        )
        .await?;

        let (cmd_tx, cmd_rx) = mpsc::channel(8);

        debug!("replication connection ready");

        Ok(Self {
            uri,
            database_id,
            table_base_path,
            postgres_client,
            handle: None,
            table_states: HashMap::new(),
            cmd_tx,
            cmd_rx: Some(cmd_rx),
            replication_state: ReplicationState::new(),
            source: Arc::new(postgres_source),
            replication_started: false,
            slot_name,
            object_storage_cache,
            retry_handles: Vec::new(),
        })
    }

    pub fn replication_started(&self) -> bool {
        self.replication_started
    }

    /// Include full row in cdc stream (not just primary keys).
    async fn alter_table_replica_identity(&self, table_name: &str) -> Result<()> {
        self.postgres_client
            .simple_query(&format!("ALTER TABLE {table_name} REPLICA IDENTITY FULL;"))
            .await?;
        Ok(())
    }

    fn retry_drop(uri: &str, drop_query: &str) -> JoinHandle<Result<()>> {
        debug!("spawning retry drop");
        let uri = uri.to_string();
        let drop_query = drop_query.to_string();
        tokio::spawn(async move {
            let (drop_client, _) = connect(&uri, NoTls)
                .await
                .map_err(PostgresSourceError::from)?;
            drop_client
                .simple_query(&drop_query)
                .await
                .map_err(PostgresSourceError::from)?;
            Ok(())
        })
    }

    /// Clean up completed retry handles.
    fn cleanup_completed_retries(&mut self) {
        debug!("cleaning up completed retry handles");
        self.retry_handles.retain(|handle| !handle.is_finished());
    }

    async fn attempt_drop_else_retry(&mut self, drop_query: &str) -> Result<()> {
        // Clean up any completed retry handles first
        self.cleanup_completed_retries();

        self.postgres_client
            .simple_query(drop_query)
            .await
            .or_else(|e| match e.code() {
                Some(&SqlState::LOCK_NOT_AVAILABLE) => {
                    warn!("lock not available, retrying");
                    // Store the handle so we can track its completion
                    let handle = Self::retry_drop(&self.uri, drop_query);
                    self.retry_handles.push(handle);
                    Ok(vec![])
                }
                Some(&SqlState::UNDEFINED_TABLE) => {
                    warn!("table already dropped, skipping");
                    Ok(vec![])
                }
                _ => Err(PostgresSourceError::from(e)),
            })?;
        Ok(())
    }

    async fn remove_table_from_publication(&mut self, table_name: &str) -> Result<()> {
        self.attempt_drop_else_retry(&format!(
            "ALTER PUBLICATION moonlink_pub DROP TABLE {table_name};"
        ))
        .await?;
        Ok(())
    }

    pub async fn drop_publication(&mut self) -> Result<()> {
        self.attempt_drop_else_retry("DROP PUBLICATION IF EXISTS moonlink_pub;")
            .await?;
        Ok(())
    }

    pub async fn drop_replication_slot(&self) -> Result<()> {
        // First, terminate any active connections using this slot
        let terminate_query = format!(
            "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = '{}';",
            self.slot_name
        );
        let _ = self.postgres_client.simple_query(&terminate_query).await;

        // Then drop the replication slot
        let drop_query = format!("SELECT pg_drop_replication_slot('{}');", self.slot_name);
        self.postgres_client
            .simple_query(&drop_query)
            .await
            .map_err(PostgresSourceError::from)?;

        Ok(())
    }

    pub fn get_table_reader(&self, src_table_id: SrcTableId) -> &ReadStateManager {
        &self.table_states.get(&src_table_id).unwrap().reader
    }

    pub fn get_table_status_reader(&self, src_table_id: SrcTableId) -> &TableStatusReader {
        &self.table_states.get(&src_table_id).unwrap().status_reader
    }

    pub fn get_table_status_readers(&self) -> Vec<&TableStatusReader> {
        self.table_states
            .values()
            .map(|cur_table_state| &cur_table_state.status_reader)
            .collect::<Vec<_>>()
    }

    pub fn table_count(&self) -> usize {
        self.table_states.len()
    }

    pub fn get_table_event_manager(&mut self, src_table_id: SrcTableId) -> &mut TableEventManager {
        &mut self
            .table_states
            .get_mut(&src_table_id)
            .unwrap()
            .event_manager
    }

    async fn spawn_replication_task(
        &mut self,
        sink: Sink,
        cmd_rx: mpsc::Receiver<Command>,
    ) -> JoinHandle<Result<()>> {
        let uri = self.uri.clone();
        let cfg = self.source.get_cdc_stream_config().unwrap();
        let source = self.source.clone();

        tokio::spawn(async move {
            let (client, connection) = ReplicationClient::connect_no_tls(&uri, true)
                .await
                .map_err(PostgresSourceError::from)?;

            run_event_loop(client, cfg, connection, sink, cmd_rx, source).await
        })
    }

    /// # Arguments
    ///
    /// * override_table_base_path: mooncake table directory, fallback to [`self.table_base_path`] if unassigned.
    async fn add_table_to_replication<T: std::fmt::Display>(
        &mut self,
        schema: &TableSchema,
        mooncake_table_id: &T,
        table_id: u32,
        moonlink_table_config: MoonlinkTableConfig,
        is_recovery: bool,
    ) -> Result<()> {
        let src_table_id = schema.src_table_id;
        debug!(src_table_id, "adding table to replication");
        let table_resources = build_table_components(
            mooncake_table_id.to_string(),
            self.database_id,
            table_id,
            schema,
            &self.table_base_path,
            &self.replication_state,
            self.object_storage_cache.clone(),
            moonlink_table_config,
        )
        .await?;

        let event_sender_clone = table_resources.event_sender.clone();

        self.table_states.insert(
            src_table_id,
            TableState {
                schema: schema.clone(),
                reader: table_resources.read_state_manager,
                event_manager: table_resources.table_event_manager,
                status_reader: table_resources.table_status_reader,
            },
        );
        if let Err(e) = self
            .cmd_tx
            .send(Command::AddTable {
                src_table_id,
                schema: schema.clone(),
                event_sender: table_resources.event_sender,
                commit_lsn_tx: table_resources.commit_lsn_tx,
                flush_lsn_rx: table_resources.flush_lsn_rx,
                wal_flush_lsn_rx: table_resources.wal_flush_lsn_rx,
            })
            .await
        {
            error!(error = ?e, "failed to enqueue AddTable command");
        }

        // Create a dedicated source for the copy
        let mut copy_source = PostgresSource::new(&self.uri, None, None, false).await?;

        // Check if there are existing rows
        let row_count = copy_source.get_row_count(&schema.table_name).await?;

        // Only perform initial copy for new tables, not during recovery.
        // Early return if there are no rows to copy.
        if !is_recovery && row_count > 0 {
            if let Err(e) = event_sender_clone.send(TableEvent::StartInitialCopy).await {
                error!(error = ?e, "failed to send StartInitialCopy event");
            }

            // Alter the publication to add the table.
            // Add table to publication first to begin accumulating any cdc events.
            // We can check where our initial copy started from and discard any rows we have already seen.
            copy_source
                .add_table_to_publication(&schema.table_name)
                .await?;

            let schema_clone = schema.clone();
            tokio::spawn(async move {
                let (stream, start_lsn) = copy_source
                    .get_table_copy_stream(&schema_clone.table_name, &schema_clone.column_schemas)
                    .await
                    .expect("failed to get table copy stream");
                let res = copy_table_stream_impl(schema_clone, stream, &event_sender_clone).await;

                if let Err(e) = res {
                    error!(error = ?e, table_id = src_table_id, "failed to copy table");
                }
                // Commit the transaction
                copy_source
                    .commit_transaction()
                    .await
                    .expect("failed to commit transaction");

                if let Err(e) = event_sender_clone
                    .send(TableEvent::FinishInitialCopy {
                        start_lsn: start_lsn.into(),
                    })
                    .await
                {
                    error!(error = ?e, table_id = src_table_id, "failed to send FinishTableCopy command");
                }
            });
        } else {
            // If there are no rows to copy, we still need to add the table to publication.
            copy_source
                .add_table_to_publication(&schema.table_name)
                .await?;
        }

        debug!(table_id, "table added to replication");

        Ok(())
    }

    async fn remove_table_from_replication(&mut self, src_table_id: SrcTableId) -> Result<()> {
        debug!(src_table_id, "removing table from replication");
        let TableState {
            mut event_manager, ..
        } = self.table_states.remove(&src_table_id).unwrap();
        // Notify the table handler to clean up cache, mooncake and iceberg table state.
        debug!(src_table_id, "drop table from table handler");
        event_manager.drop_table().await?;
        if let Err(e) = self.cmd_tx.send(Command::DropTable { src_table_id }).await {
            error!(error = ?e, "failed to enqueue DropTable command");
        }

        debug!(src_table_id, "table removed from replication");

        Ok(())
    }

    pub async fn start_replication(&mut self) -> Result<()> {
        debug!("starting replication");

        let sink = Sink::new(self.replication_state.clone());
        let receiver = self.cmd_rx.take().unwrap();
        self.handle = Some(self.spawn_replication_task(sink, receiver).await);

        self.replication_started = true;

        debug!("replication started");

        Ok(())
    }

    pub async fn add_table<T: std::fmt::Display>(
        &mut self,
        table_name: &str,
        mooncake_table_id: &T,
        table_id: u32,
        moonlink_table_config: MoonlinkTableConfig,
        is_recovery: bool,
    ) -> Result<SrcTableId> {
        debug!(table_name, "adding table");
        // TODO: We should not naively alter the replica identity of a table. We should only do this if we are sure that the table does not already have a FULL replica identity. [https://github.com/Mooncake-Labs/moonlink/issues/104]
        self.alter_table_replica_identity(table_name).await?;
        let table_schema = self
            .source
            .fetch_table_schema(None, Some(table_name), None)
            .await?;

        self.add_table_to_replication(
            &table_schema,
            mooncake_table_id,
            table_id,
            moonlink_table_config,
            is_recovery,
        )
        .await?;

        debug!(src_table_id = table_schema.src_table_id, "table added");

        Ok(table_schema.src_table_id)
    }

    /// Remove the given table from connection.
    pub async fn drop_table(&mut self, src_table_id: u32) -> Result<()> {
        debug!(src_table_id, "dropping table");
        let table_name = self
            .table_states
            .get(&src_table_id)
            .unwrap()
            .schema
            .table_name
            .to_string();

        // Remove table from publication as the first step, to prevent further events.
        self.remove_table_from_publication(&table_name).await?;
        self.remove_table_from_replication(src_table_id).await?;

        debug!(src_table_id, "table dropped");
        Ok(())
    }

    pub fn check_table_belongs_to_source(&self, uri: &str) -> bool {
        self.uri == uri
    }

    /// Wait for all pending retry operations to complete.
    async fn wait_for_pending_retries(&mut self) {
        if !self.retry_handles.is_empty() {
            debug!(
                "waiting for {} pending retry operations",
                self.retry_handles.len()
            );
            let handles = std::mem::take(&mut self.retry_handles);
            for handle in handles {
                let _ = handle.await;
            }
        }
    }

    pub fn shutdown(mut self) -> JoinHandle<Result<()>> {
        tokio::spawn(async move {
            debug!("shutting down replication connection");
            if self.replication_started {
                if let Err(e) = self.cmd_tx.send(Command::Shutdown).await {
                    warn!(error = ?e, "failed to send shutdown command");
                }
                if let Some(handle) = self.handle.take() {
                    let _ = handle.await;
                }
                self.replication_started = false;
            }

            self.drop_publication().await?;
            self.drop_replication_slot().await?;

            // Wait for any pending retry operations to complete
            self.wait_for_pending_retries().await;

            debug!("replication connection shut down");
            Ok(())
        })
    }
}

#[tracing::instrument(name = "replication_event_loop", skip_all)]
async fn run_event_loop(
    client: ReplicationClient,
    cfg: CdcStreamConfig,
    connection: Connection<Socket, NoTlsStream>,
    mut sink: Sink,
    mut cmd_rx: mpsc::Receiver<Command>,
    postgres_source: Arc<PostgresSource>,
) -> Result<()> {
    pin!(connection);

    // Create stream while driving connection
    let stream = tokio::select! {
        s = PostgresSource::create_cdc_stream(client, cfg) => s?,
        _ = &mut connection => {
            return Err(PostgresSourceError::Io(Error::new(ErrorKind::ConnectionAborted, "connection closed during setup")).into());
        }
    };

    // Now run the main event loop
    pin!(stream);

    debug!("replication event loop started");

    let mut status_interval = tokio::time::interval(Duration::from_secs(10));
    let mut flush_lsn_rxs: HashMap<SrcTableId, watch::Receiver<u64>> = HashMap::new();
    // TODO(Paul): Currently unused. In preparation for acknowledging latest WAL flush LSN to replication sink.
    let mut _wal_flush_lsn_rxs: HashMap<SrcTableId, watch::Receiver<u64>> = HashMap::new();

    loop {
        tokio::select! {
            _ = status_interval.tick() => {
                let mut confirmed_lsn: Option<u64> = None;
                for rx in flush_lsn_rxs.values() {
                    let lsn = *rx.borrow();
                    confirmed_lsn = Some(match confirmed_lsn {
                        Some(v) => v.min(lsn),
                        None => lsn,
                    });
                }
                let lsn_to_send = confirmed_lsn.map(PgLsn::from).unwrap_or(PgLsn::from(0));
                if let Err(e) = stream
                    .as_mut()
                    .send_status_update(lsn_to_send)
                    .await
                {
                    error!(error = ?e, "failed to send status update");
                }
            },
            Some(cmd) = cmd_rx.recv() => match cmd {
                Command::AddTable { src_table_id, schema, event_sender, commit_lsn_tx, flush_lsn_rx, wal_flush_lsn_rx } => {
                    sink.add_table(src_table_id, event_sender, commit_lsn_tx, &schema);
                    flush_lsn_rxs.insert(src_table_id, flush_lsn_rx);
                    _wal_flush_lsn_rxs.insert(src_table_id, wal_flush_lsn_rx);
                    stream.as_mut().add_table_schema(schema);
                }
                Command::DropTable { src_table_id } => {
                    sink.drop_table(src_table_id);
                    flush_lsn_rxs.remove(&src_table_id);
                    _wal_flush_lsn_rxs.remove(&src_table_id);
                    stream.as_mut().remove_table_schema(src_table_id);
                }
                Command::Shutdown => {
                    debug!("received shutdown command");
                    break;
                }
            },
            event = StreamExt::next(&mut stream) => {
                let Some(event_result) = event else {
                    error!("replication stream ended unexpectedly");
                    break;
                };

                match event_result {
                    Err(CdcStreamError::CdcEventConversion(CdcEventConversionError::MissingSchema(_))) => {
                        continue;
                    }
                    Err(CdcStreamError::CdcEventConversion(CdcEventConversionError::MessageNotSupported)) => {
                        // TODO: Add support for Truncate and Origin messages and remove this.
                        warn!("message not supported");
                        continue;
                    }
                    Err(e) => {
                        error!(error = ?e, "cdc stream error");
                        break;
                    }
                    Ok(event) => {
                        let res = sink.process_cdc_event(event).await.unwrap();
                        if let Some(SchemaChangeRequest(src_table_id)) = res {
                            let table_schema = postgres_source.fetch_table_schema(Some(src_table_id), None, None).await?;
                            sink.alter_table(src_table_id, &table_schema).await;
                            stream.as_mut().add_table_schema(table_schema);
                        }
                    }
                }
            }
            _ = &mut connection => {
                error!("replication connection closed");
                break;
            }
        }
    }

    debug!("replication event loop stopped");
    Ok(())
}
