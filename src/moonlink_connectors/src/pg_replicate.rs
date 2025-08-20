#![allow(warnings)]

pub mod clients;
pub mod conversions;
pub mod initial_copy;
pub mod moonlink_sink;
pub mod postgres_source;
pub mod table;
pub mod table_init;
pub mod util;

use crate::pg_replicate::clients::postgres::ReplicationClient;
use crate::pg_replicate::conversions::cdc_event::CdcEventConversionError;
use crate::pg_replicate::initial_copy::copy_table_stream_impl;
use crate::pg_replicate::moonlink_sink::{SchemaChangeRequest, Sink};
use crate::pg_replicate::postgres_source::{
    CdcStreamConfig, CdcStreamError, PostgresSource, PostgresSourceError,
};
use crate::pg_replicate::table::{SrcTableId, TableSchema};
use crate::pg_replicate::table_init::{build_table_components, TableComponents};
use crate::replication_state::ReplicationState;
use crate::Result;
use futures::StreamExt;
use moonlink::{
    MoonlinkTableConfig, ObjectStorageCache, ReadStateFilepathRemap, TableEvent, WalManager,
};
use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::mem::take;
use std::sync::Arc;
use std::time::Duration;
use tokio::pin;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_postgres::error::SqlState;
use tokio_postgres::tls::NoTlsStream;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{connect, Client, Config, NoTls};
use tokio_postgres::{Connection, Socket};
use tracing::{debug, error, info_span, warn, Instrument};

pub enum PostgresReplicationCommand {
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

pub struct PostgresConnection {
    pub uri: String,
    pub postgres_client: Client,
    pub source: Arc<PostgresSource>,
    pub slot_name: String,
    pub cmd_tx: mpsc::Sender<PostgresReplicationCommand>,
    pub cmd_rx: Option<mpsc::Receiver<PostgresReplicationCommand>>,
    pub replication_state: Arc<ReplicationState>,
    pub retry_handles: Vec<JoinHandle<Result<()>>>,
}

impl PostgresConnection {
    pub async fn new(uri: String) -> Result<Self> {
        debug!(%uri, "initializing postgres connection");

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

        Ok(Self {
            uri,
            postgres_client,
            source: Arc::new(postgres_source),
            slot_name,
            cmd_tx,
            cmd_rx: Some(cmd_rx),
            replication_state: ReplicationState::new(),
            retry_handles: Vec::new(),
        })
    }

    /// Include full row in cdc stream (not just primary keys).
    pub async fn alter_table_replica_identity(&self, table_name: &str) -> Result<()> {
        self.postgres_client
            .simple_query(&format!("ALTER TABLE {table_name} REPLICA IDENTITY FULL;"))
            .await?;
        Ok(())
    }

    #[must_use]
    /// Perform initial copy of existing table data
    /// Returns true if initial copy was performed, false otherwise.
    pub async fn perform_initial_copy(
        &self,
        schema: &TableSchema,
        event_sender: mpsc::Sender<TableEvent>,
        is_recovery: bool,
        commit_lsn_tx: watch::Sender<u64>,
    ) -> Result<(bool)> {
        let src_table_id = schema.src_table_id;
        // Create a dedicated source for the copy
        let mut copy_source = PostgresSource::new(&self.uri, None, None, false).await?;

        // Check if there are existing rows
        let row_count = copy_source.get_row_count(&schema.table_name).await?;

        // Only perform initial copy for new tables, not during recovery.
        // Early return if there are no rows to copy.
        if !is_recovery && row_count > 0 {
            if let Err(e) = event_sender.send(TableEvent::StartInitialCopy).await {
                error!(error = ?e, "failed to send StartInitialCopy event");
            }

            // Alter the publication to add the table.
            // Add table to publication first to begin accumulating any cdc events.
            // We can check where our initial copy started from and discard any rows we have already seen.
            copy_source
                .add_table_to_publication(&schema.table_name)
                .await?;

            let (stream, start_lsn) = copy_source
                .get_table_copy_stream(&schema.table_name, &schema.column_schemas)
                .await
                .expect("failed to get table copy stream");
            let res = copy_table_stream_impl(schema.clone(), stream, &event_sender).await;

            if let Err(e) = res {
                error!(error = ?e, table_id = src_table_id, "failed to copy table");
            }

            // Commit the transaction
            copy_source
                .commit_transaction()
                .await
                .expect("failed to commit transaction");

            if let Err(e) = event_sender
                .send(TableEvent::FinishInitialCopy {
                    start_lsn: start_lsn.into(),
                })
                .await
            {
                error!(error = ?e, table_id = src_table_id, "failed to send FinishTableCopy command");
            }

            // Notify read state manager with the commit LSN for the initial copy boundary.
            if let Err(e) = commit_lsn_tx.send(start_lsn.into()) {
                warn!(error = ?e, table_id = src_table_id, "failed to send initial copy commit lsn");
            }

            Ok(true)
        } else {
            // If there are no rows to copy, we still need to add the table to publication.
            copy_source
                .add_table_to_publication(&schema.table_name)
                .await?;
            Ok(false)
        }
    }

    pub fn retry_drop(uri: &str, drop_query: &str) -> JoinHandle<Result<()>> {
        let uri = uri.to_string();
        let drop_query = drop_query.to_string();
        tokio::spawn(async move {
            let mut retry_count = 0;
            loop {
                match connect(&uri, NoTls).await {
                    Ok((client, connection)) => {
                        tokio::spawn(async move {
                            if let Err(e) = connection.await {
                                warn!("connection error: {}", e);
                            }
                        });
                        match client.simple_query(&drop_query).await {
                            Ok(_) => break Ok(()),
                            Err(e) => {
                                if e.code() == Some(&SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE) {
                                    break Ok(());
                                }
                                if retry_count >= 3 {
                                    break Err(e.into());
                                }
                                retry_count += 1;
                                sleep(Duration::from_secs(1)).await;
                            }
                        }
                    }
                    Err(e) => {
                        if retry_count >= 3 {
                            break Err(e.into());
                        }
                        retry_count += 1;
                        sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        })
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

    pub async fn remove_table_from_publication(&mut self, table_name: &str) -> Result<()> {
        let drop_query = format!("ALTER PUBLICATION moonlink_pub DROP TABLE {table_name};");
        self.attempt_drop_else_retry(&drop_query).await?;
        Ok(())
    }

    /// Get a clone of the replication state
    pub fn get_replication_state(&self) -> Arc<ReplicationState> {
        self.replication_state.clone()
    }

    /// Add table to PostgreSQL replication
    pub async fn add_table_to_replication(
        &self,
        src_table_id: SrcTableId,
        schema: TableSchema,
        event_sender: mpsc::Sender<TableEvent>,
        commit_lsn_tx: watch::Sender<u64>,
        flush_lsn_rx: watch::Receiver<u64>,
        wal_flush_lsn_rx: watch::Receiver<u64>,
    ) -> Result<()> {
        let cmd = PostgresReplicationCommand::AddTable {
            src_table_id,
            schema,
            event_sender,
            commit_lsn_tx,
            flush_lsn_rx,
            wal_flush_lsn_rx,
        };
        self.cmd_tx.send(cmd).await?;
        Ok(())
    }

    /// Drop table from PostgreSQL replication
    pub async fn drop_table_from_replication(&self, src_table_id: SrcTableId) -> Result<()> {
        let cmd = PostgresReplicationCommand::DropTable { src_table_id };
        self.cmd_tx.send(cmd).await?;
        Ok(())
    }

    /// Shutdown PostgreSQL replication
    pub async fn shutdown_replication(&self) -> Result<()> {
        let cmd = PostgresReplicationCommand::Shutdown;
        self.cmd_tx.send(cmd).await?;
        Ok(())
    }

    /// Clean up completed retry handles.
    pub fn cleanup_completed_retries(&mut self) {
        debug!("cleaning up completed retry handles");
        self.retry_handles.retain(|handle| !handle.is_finished());
    }

    /// Attempt to drop with retry logic
    pub async fn attempt_drop_else_retry(&mut self, drop_query: &str) -> Result<()> {
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

    /// Drop publication
    pub async fn drop_publication(&mut self) -> Result<()> {
        self.attempt_drop_else_retry("DROP PUBLICATION IF EXISTS moonlink_pub;")
            .await?;
        Ok(())
    }

    /// Add table to PostgreSQL replication
    pub async fn add_table<T: std::fmt::Display>(
        &self,
        table_name: &str,
        mooncake_table_id: &T,
        moonlink_table_config: MoonlinkTableConfig,
        is_recovery: bool,
        table_base_path: &str,
        read_state_filepath_remap: ReadStateFilepathRemap,
        object_storage_cache: ObjectStorageCache,
    ) -> Result<(SrcTableId, crate::pg_replicate::table_init::TableResources)> {
        debug!(table_name, "adding table");
        // TODO: We should not naively alter the replica identity of a table. We should only do this if we are sure that the table does not already have a FULL replica identity. [https://github.com/Mooncake-Labs/moonlink/issues/104]
        self.alter_table_replica_identity(table_name).await?;
        let table_schema = self
            .source
            .fetch_table_schema(None, Some(table_name), None)
            .await?;

        let (arrow_schema, identity) =
            crate::pg_replicate::util::postgres_schema_to_moonlink_schema(&table_schema);
        let table_components = TableComponents {
            read_state_filepath_remap,
            object_storage_cache,
            moonlink_table_config,
        };

        let mut table_resources = build_table_components(
            mooncake_table_id.to_string(),
            arrow_schema,
            identity,
            table_name.to_string(),
            table_schema.src_table_id,
            &table_base_path.to_string(),
            &self.replication_state,
            table_components,
            is_recovery,
        )
        .await?;

        // Send command to add table to replication
        let commit_lsn_tx = table_resources
            .commit_lsn_tx
            .take()
            .expect("commit_lsn_tx is None");
        let commit_lsn_tx_for_copy = commit_lsn_tx.clone();
        self.add_table_to_replication(
            table_schema.src_table_id,
            table_schema.clone(),
            table_resources.event_sender.clone(),
            commit_lsn_tx,
            table_resources
                .flush_lsn_rx
                .take()
                .expect("flush_lsn_rx is None"),
            table_resources
                .wal_flush_lsn_rx
                .take()
                .expect("wal_flush_lsn_rx is None"),
        )
        .await?;

        // Perform initial copy
        let initial_copy_performed = self
            .perform_initial_copy(
                &table_schema,
                table_resources.event_sender.clone(),
                is_recovery,
                commit_lsn_tx_for_copy,
            )
            .await?;

        if is_recovery {
            assert!(
                !initial_copy_performed,
                "initial copy should not be performed during recovery"
            );
            debug!(
                "Performing recovery for table with ID {:?}",
                table_schema.src_table_id
            );
            // Perform recovery
            WalManager::replay_recovery_from_wal(
                table_resources.event_sender.clone(),
                table_resources.wal_persistence_metadata.clone(),
                table_resources.wal_file_accessor.clone(),
                table_resources.last_iceberg_snapshot_lsn,
            )
            .await?;
            debug!(
                "Finished recovery for table with ID {:?}",
                table_schema.src_table_id
            );
        }

        debug!(src_table_id = table_schema.src_table_id, "table added");

        Ok((table_schema.src_table_id, table_resources))
    }

    /// Drop table from PostgreSQL replication
    pub async fn drop_table(&mut self, src_table_id: u32, table_name: &str) -> Result<()> {
        debug!(src_table_id, "dropping table");

        // Remove table from publication as the first step, to prevent further events.
        self.remove_table_from_publication(table_name).await?;

        // Send command to drop table from replication
        self.drop_table_from_replication(src_table_id).await?;

        debug!(src_table_id, "table dropped");
        Ok(())
    }

    /// Wait for all pending retry operations to complete.
    pub async fn wait_for_pending_retries(&mut self) {
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

    /// Shutdown PostgreSQL replication
    /// If postgres drop all is false, then we will not drop the PostgreSQL publication and replication slot,
    /// which allows for recovery from the PostgreSQL replication slot.
    pub async fn shutdown(&mut self, drop_all: bool) -> Result<()> {
        if drop_all {
            self.drop_publication().await?;
            self.drop_replication_slot().await?;
        }
        // Wait for any pending retry operations to complete
        self.wait_for_pending_retries().await;

        debug!("replication connection shut down");
        Ok(())
    }

    /// Spawn replication task
    pub async fn spawn_replication_task(&mut self) -> JoinHandle<Result<()>> {
        let sink = Sink::new(self.replication_state.clone());
        let receiver = self.cmd_rx.take().unwrap();

        let uri = self.uri.clone();
        let cfg = self.source.get_cdc_stream_config().unwrap();
        let source = self.source.clone();

        tokio::spawn(async move {
            let (client, connection) =
                crate::pg_replicate::clients::postgres::ReplicationClient::connect_no_tls(
                    &uri, true,
                )
                .await
                .map_err(PostgresSourceError::from)?;

            run_event_loop(client, cfg, connection, sink, receiver, source).await
        })
    }
}

#[tracing::instrument(name = "replication_event_loop", skip_all)]
pub async fn run_event_loop(
    client: ReplicationClient,
    cfg: CdcStreamConfig,
    connection: Connection<Socket, NoTlsStream>,
    mut sink: Sink,
    mut cmd_rx: mpsc::Receiver<PostgresReplicationCommand>,
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
    let mut wal_flush_lsn_rxs: HashMap<SrcTableId, watch::Receiver<u64>> = HashMap::new();

    loop {
        tokio::select! {
            _ = status_interval.tick() => {
                let mut confirmed_lsn: Option<u64> = None;
                for rx in wal_flush_lsn_rxs.values() {
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
                PostgresReplicationCommand::AddTable { src_table_id, schema, event_sender, commit_lsn_tx, flush_lsn_rx, wal_flush_lsn_rx } => {
                    sink.add_table(src_table_id, event_sender, commit_lsn_tx, &schema);
                    flush_lsn_rxs.insert(src_table_id, flush_lsn_rx);
                    wal_flush_lsn_rxs.insert(src_table_id, wal_flush_lsn_rx);
                    stream.as_mut().add_table_schema(schema);
                }
                PostgresReplicationCommand::DropTable { src_table_id } => {
                    sink.drop_table(src_table_id);
                    flush_lsn_rxs.remove(&src_table_id);
                    wal_flush_lsn_rxs.remove(&src_table_id);
                    stream.as_mut().remove_table_schema(src_table_id);
                }
                PostgresReplicationCommand::Shutdown => {
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
                            stream.as_mut().update_table_schema(table_schema);
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

#[cfg(test)]
mod tests;
