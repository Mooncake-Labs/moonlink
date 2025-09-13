#![allow(warnings)]

pub mod clients;
pub mod conversions;
pub mod initial_copy;
pub mod initial_copy_writer;
pub mod moonlink_sink;
pub mod postgres_source;
pub mod table;
pub mod table_init;
pub mod util;

use crate::pg_replicate::clients::postgres::{build_tls_connector, ReplicationClient};
use crate::pg_replicate::conversions::cdc_event::{CdcEvent, CdcEventConversionError};
use crate::pg_replicate::initial_copy::copy_table_stream;
use crate::pg_replicate::initial_copy::{InitialCopyConfig, InitialCopyReaderConfig};
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
    MooncakeTableId, MoonlinkTableConfig, ObjectStorageCache, ReadStateFilepathRemap, TableEvent,
    VisibilityLsn, WalManager,
};
use native_tls::{Certificate, TlsConnector};
use postgres_native_tls::{MakeTlsConnector, TlsStream};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{Error, ErrorKind};
use std::mem::take;
use std::sync::Arc;
use std::time::Duration;
use tokio::pin;
use tokio::sync::oneshot;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_postgres::error::SqlState;
use tokio_postgres::types::PgLsn;
use tokio_postgres::SimpleQueryMessage;
use tokio_postgres::{connect, Client, Config};
use tokio_postgres::{Connection, Socket};
use tracing::{debug, error, info_span, warn, Instrument};

fn is_transport_like(sqlstate: Option<&SqlState>) -> bool {
    match sqlstate {
        None => true,
        Some(code) => {
            let c = code.code();
            code == &SqlState::ADMIN_SHUTDOWN
                || code == &SqlState::CRASH_SHUTDOWN
                || code == &SqlState::CANNOT_CONNECT_NOW
                || c.starts_with("08")
        }
    }
}

pub enum PostgresReplicationCommand {
    AddTable {
        src_table_id: SrcTableId,
        schema: TableSchema,
        event_sender: mpsc::Sender<TableEvent>,
        visibility_tx: watch::Sender<VisibilityLsn>,
        flush_lsn_rx: watch::Receiver<u64>,
        wal_flush_lsn_rx: watch::Receiver<u64>,
        ready_tx: oneshot::Sender<()>,
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

        let tls = build_tls_connector().map_err(PostgresSourceError::from)?;

        let (postgres_client, connection) = connect(&uri, tls)
            .await
            .map_err(PostgresSourceError::from)?;

        debug!(%uri, "connected to postgres");
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

    /// Reconnect the control-plane client and reapply session settings
    async fn reconnect_control_client(&mut self) -> Result<()> {
        let tls = build_tls_connector().map_err(PostgresSourceError::from)?;
        let (client, connection) = connect(&self.uri, tls)
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
        // Reapply desired session settings
        client
            .simple_query("SET lock_timeout = '100ms';")
            .await
            .map_err(PostgresSourceError::from)?;
        self.postgres_client = client;
        Ok(())
    }

    /// Centralized control-plane query executor. Retries with backoff on connection errors.
    pub async fn run_control_query(&mut self, sql: &str) -> Result<Vec<SimpleQueryMessage>> {
        // Simple linear backoff for transport errors only.
        // Total attempts = 1 initial + MAX_RETRIES.
        const MAX_RETRIES: usize = 3;
        const BASE_DELAY_MS: u64 = 300;

        for attempt in 0..=MAX_RETRIES {
            if attempt > 0 {
                // Backoff before retrying and reconnecting the control-plane client.
                let delay_ms = BASE_DELAY_MS * (attempt as u64);
                sleep(Duration::from_millis(delay_ms)).await;
                if let Err(err) = self.reconnect_control_client().await {
                    // If reconnect fails on the final attempt, bubble up.
                    if attempt == MAX_RETRIES {
                        return Err(err);
                    }
                    // Otherwise, continue to next retry iteration.
                    continue;
                }
            }

            match self.postgres_client.simple_query(sql).await {
                Ok(messages) => return Ok(messages),
                Err(e) => {
                    let retryable = is_transport_like(e.code());
                    if retryable {
                        if attempt == MAX_RETRIES {
                            return Err(PostgresSourceError::from(e).into());
                        }
                        // Try again after reconnect/backoff.
                        continue;
                    } else {
                        // SQLSTATE present and not retryable: fail fast.
                        return Err(PostgresSourceError::from(e).into());
                    }
                }
            }
        }

        // Should not reach here.
        Err(PostgresSourceError::Io(Error::new(ErrorKind::Other, "unexpected retry exit")).into())
    }

    /// Include full row in cdc stream (not just primary keys).
    pub async fn alter_table_replica_identity(&mut self, table_name: &str) -> Result<()> {
        let sql = format!("ALTER TABLE {table_name} REPLICA IDENTITY FULL;");
        let _ = self.run_control_query(&sql).await?;
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
        visibility_tx: watch::Sender<VisibilityLsn>,
        table_base_path: &str,
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

            let ic_config = InitialCopyConfig {
                reader: InitialCopyReaderConfig {
                    uri: self.uri.clone(),
                    shard_count: 4,
                },
                writer: Default::default(),
            };
            let progress =
                copy_table_stream(schema.clone(), &event_sender, table_base_path, ic_config)
                    .await
                    .expect(&format!(
                        "failed to copy table for src_table_id: {}",
                        src_table_id
                    ));

            if let Err(e) = event_sender
                .send(TableEvent::FinishInitialCopy {
                    start_lsn: progress.boundary_lsn.into(),
                })
                .await
            {
                error!(error = ?e, table_id = src_table_id, "failed to send FinishTableCopy command");
            }

            // Notify read state manager with the commit LSN for the initial copy boundary.
            if let Err(e) = visibility_tx.send(VisibilityLsn {
                commit_lsn: progress.boundary_lsn.into(),
                replication_lsn: progress.boundary_lsn.into(),
            }) {
                warn!(error = ?e, table_id = src_table_id, "failed to send initial copy commit lsn");
            }
            self.replication_state.mark(progress.boundary_lsn.into());

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
                let tls = build_tls_connector().map_err(PostgresSourceError::from)?;
                match connect(&uri, tls).await {
                    Ok((client, connection)) => {
                        tokio::spawn(async move {
                            if let Err(e) = connection.await {
                                warn!("connection error: {}", e);
                            }
                        });
                        match client.simple_query(&drop_query).await {
                            Ok(_) => break Ok(()),
                            Err(e) => {
                                match e.code() {
                                    Some(&SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE) => {
                                        break Ok(());
                                    }
                                    Some(&SqlState::UNDEFINED_OBJECT) => {
                                        break Ok(());
                                    }
                                    _ => {}
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

    pub async fn drop_replication_slot(&mut self) -> Result<()> {
        // First, terminate any active connections using this slot
        let terminate_query = format!(
            "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = '{}';",
            self.slot_name
        );
        let _ = self.run_control_query(&terminate_query).await;

        // Then drop the replication slot
        let drop_query = format!("SELECT pg_drop_replication_slot('{}');", self.slot_name);
        self.run_control_query(&drop_query).await?;

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
        visibility_tx: watch::Sender<VisibilityLsn>,
        flush_lsn_rx: watch::Receiver<u64>,
        wal_flush_lsn_rx: watch::Receiver<u64>,
    ) -> Result<oneshot::Receiver<()>> {
        let (ready_tx, ready_rx) = oneshot::channel();
        let cmd = PostgresReplicationCommand::AddTable {
            src_table_id,
            schema,
            event_sender,
            visibility_tx,
            flush_lsn_rx,
            wal_flush_lsn_rx,
            ready_tx,
        };
        self.cmd_tx.send(cmd).await?;
        Ok(ready_rx)
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
                    warn!("lock not available, retrying in background");
                    // Store the handle so we can track its completion
                    let handle = Self::retry_drop(&self.uri, drop_query);
                    self.retry_handles.push(handle);
                    Ok(vec![])
                }
                Some(&SqlState::UNDEFINED_TABLE) => {
                    warn!("table already dropped, skipping");
                    Ok(vec![])
                }
                Some(&SqlState::UNDEFINED_OBJECT) => {
                    // Object not present in publication (e.g., relation not part of publication)
                    warn!("object not present (idempotent), skipping");
                    Ok(vec![])
                }
                opt if is_transport_like(opt) => {
                    // Treat connection-exception and shutdowns as transport-like: retry in background
                    warn!("transport-like sqlstate on drop; retrying in background");
                    let handle = Self::retry_drop(&self.uri, drop_query);
                    self.retry_handles.push(handle);
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
    pub async fn add_table(
        &mut self,
        table_name: &str,
        mooncake_table_id: &MooncakeTableId,
        moonlink_table_config: &mut MoonlinkTableConfig,
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
        moonlink_table_config.mooncake_table_config.row_identity = identity;
        let table_components = TableComponents {
            read_state_filepath_remap,
            object_storage_cache,
            moonlink_table_config: moonlink_table_config.clone(),
        };

        let mut table_resources = build_table_components(
            mooncake_table_id.to_string(),
            arrow_schema,
            table_name.to_string(),
            table_schema.src_table_id,
            &table_base_path.to_string(),
            &self.replication_state,
            table_components,
            is_recovery,
        )
        .await?;

        // Send command to add table to replication
        let visibility_tx = table_resources
            .visibility_tx
            .take()
            .expect("visibility_lsn_tx is None");
        let visibility_tx_for_copy = visibility_tx.clone();
        let ready_rx = self
            .add_table_to_replication(
                table_schema.src_table_id,
                table_schema.clone(),
                table_resources.event_sender.clone(),
                visibility_tx,
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

        // Wait until the event loop has registered sink and schema
        if let Err(e) = ready_rx.await {
            error!(error = ?e, "failed to add table to replication");
        }

        // Perform initial copy
        let initial_copy_performed = self
            .perform_initial_copy(
                &table_schema,
                table_resources.event_sender.clone(),
                is_recovery,
                visibility_tx_for_copy,
                table_base_path,
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
            run_event_loop(cfg, sink, receiver, source)
                .await
                .map_err(|err| {
                    error!("Postgres replication eventloop failed: {:?}", err);
                    err
                })
        })
    }
}

/// Compute the lsn to send to postgres as the `confirmed_flush_lsn` [https://www.postgresql.org/docs/current/view-pg-replication-slots.html]
/// In the event that the WAL has not been written yet, we return the iceberg flush lsn.
fn compute_confirmed_wal_flush_lsn(
    wal_flush_lsn_rxs: &HashMap<SrcTableId, watch::Receiver<u64>>,
    flush_lsn_rxs: &HashMap<SrcTableId, watch::Receiver<u64>>,
) -> Option<PgLsn> {
    let mut confirmed_lsn: Option<u64> = None;
    for (table_id, wal_rx) in wal_flush_lsn_rxs.iter() {
        let wal_lsn = *wal_rx.borrow();
        let iceberg_lsn = flush_lsn_rxs
            .get(table_id)
            .map(|rx| *rx.borrow())
            .unwrap_or(0);
        let effective_lsn = if wal_lsn > 0 { wal_lsn } else { iceberg_lsn };
        confirmed_lsn = Some(match confirmed_lsn {
            Some(v) => v.min(effective_lsn),
            None => effective_lsn,
        });
    }
    confirmed_lsn.map(PgLsn::from)
}

#[tracing::instrument(name = "replication_event_loop", skip_all)]
pub async fn run_event_loop(
    cfg: CdcStreamConfig,
    mut sink: Sink,
    mut cmd_rx: mpsc::Receiver<PostgresReplicationCommand>,
    postgres_source: Arc<PostgresSource>,
) -> Result<()> {
    // Persist across reconnects
    let mut saved_schemas: Vec<TableSchema> = Vec::new();
    let mut flush_lsn_rxs: HashMap<SrcTableId, watch::Receiver<u64>> = HashMap::new();
    let mut wal_flush_lsn_rxs: HashMap<SrcTableId, watch::Receiver<u64>> = HashMap::new();
    let mut last_seen_end_lsn: Option<PgLsn> = None;

    // Linear backoff parameters
    let backoff_step = Duration::from_secs(1);
    let backoff_cap = Duration::from_secs(60);
    let mut current_backoff = Duration::from_secs(0);

    'outer: loop {
        // Prepare connection and stream
        let confirmed_flush_lsn =
            compute_confirmed_wal_flush_lsn(&wal_flush_lsn_rxs, &flush_lsn_rxs)
                .unwrap_or(PgLsn::from(0));

        let (client, mut connection) = postgres_source.connect_replication().await?;

        // We should explicitly set the confirmed flush lsn here, because we may not have completed `send_status_update` before reconnecting.
        let mut cfg = cfg.clone();
        // Always start from persisted confirmed_flush_lsn; never advertise in-memory watermark as this will drop unpersisted records on PG.
        cfg.confirmed_flush_lsn = confirmed_flush_lsn;
        let mut connection_pin = Box::pin(connection);

        // Create stream while driving connection
        let stream = tokio::select! {
            s = PostgresSource::create_cdc_stream(client, cfg) => s?,
            _ = &mut connection_pin => {
                return Err(PostgresSourceError::Io(Error::new(ErrorKind::ConnectionAborted, "connection closed during setup")).into());
            }
        };

        // Reset keepalive floor for this connection
        sink.reset_keepalive_floor();

        // Reset backoff after a successful stream creation
        current_backoff = Duration::from_secs(0);

        // Now run the main event loop for this connection
        let mut stream = Box::pin(stream);

        stream.as_mut().set_skip_before_end_lsn(None);

        // Rehydrate any previously known schemas into the stream
        if !saved_schemas.is_empty() {
            for schema in &saved_schemas {
                stream.as_mut().add_table_schema(schema.clone());
            }
            debug!(
                count = saved_schemas.len(),
                "rehydrated table schemas into stream"
            );
        }

        const MAX_EVENTS_PER_WAKE: usize = 512;
        let mut batch: Vec<std::result::Result<CdcEvent, CdcStreamError>> =
            Vec::with_capacity(MAX_EVENTS_PER_WAKE);

        debug!("replication event loop started");

        /// We use the same status interval as Postgres wal_receiver default.
        /// https://github.com/postgres/postgres/blob/c13070a27b63d9ce4850d88a63bf889a6fde26f0/src/backend/utils/misc/guc_tables.c#L2306
        const DEFAULT_STATUS_INTERVAL: Duration = Duration::from_secs(10);

        let mut status_interval = tokio::time::interval(DEFAULT_STATUS_INTERVAL);

        loop {
            tokio::select! {
                _ = status_interval.tick() => {
                    let lsn_to_send = compute_confirmed_wal_flush_lsn(&wal_flush_lsn_rxs, &flush_lsn_rxs).unwrap_or(PgLsn::from(0));
                    if let Err(e) = stream
                        .as_mut()
                        .send_status_update(lsn_to_send)
                        .await
                    {
                        error!(error = ?e, "failed to send status update");
                    }
                },
                Some(cmd) = cmd_rx.recv() => match cmd {
                    PostgresReplicationCommand::AddTable { src_table_id, schema, event_sender, visibility_tx, flush_lsn_rx, wal_flush_lsn_rx, ready_tx } => {
                        sink.add_table(src_table_id, event_sender, visibility_tx, &schema);
                        flush_lsn_rxs.insert(src_table_id, flush_lsn_rx);
                        wal_flush_lsn_rxs.insert(src_table_id, wal_flush_lsn_rx);
                        stream.as_mut().add_table_schema(schema);
                        if let Err(e) = ready_tx.send(()) {
                            error!(error = ?e, "failed to send ready signal");
                        }
                    }
                    PostgresReplicationCommand::DropTable { src_table_id } => {
                        sink.drop_table(src_table_id);
                        flush_lsn_rxs.remove(&src_table_id);
                        wal_flush_lsn_rxs.remove(&src_table_id);
                        stream.as_mut().remove_table_schema(src_table_id);
                    }
                    PostgresReplicationCommand::Shutdown => {
                        debug!("received shutdown command");
                        // Break outer to skip the reconnection attempt
                        break 'outer;
                    }
                },
                (n, last_end_lsn) = stream.as_mut().next_batch_msgs(&mut batch, MAX_EVENTS_PER_WAKE) => {
                    if n == 0 {
                        // If we produced no events but did observe frames (skipped), just continue.
                        if last_end_lsn.is_some() {
                            continue;
                        }

                        error!("replication stream ended unexpectedly");
                        // Snapshot schemas before reconnecting
                        let snapshot = stream.as_mut().schemas_snapshot();
                        debug!(count = snapshot.len(), "snapshotting schemas before reconnect (stream ended)");
                        saved_schemas = snapshot;
                        break; // break inner loop to reconnect
                    }

                    for event in batch.drain(..n) {
                        match event {
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
                                // Snapshot schemas before reconnecting (once)
                                let snapshot = stream.as_mut().schemas_snapshot();
                                debug!(count = snapshot.len(), "snapshotting schemas before reconnect (cdc error)");
                                saved_schemas = snapshot;
                                break;
                            }
                            Ok(event) => {
                                if let Some(SchemaChangeRequest(src_table_id)) = sink.process_cdc_event(event).await.unwrap() {
                                    let table_schema = postgres_source.fetch_table_schema(Some(src_table_id), None, None).await?;
                                    sink.alter_table(src_table_id, &table_schema).await;
                                    stream.as_mut().update_table_schema(table_schema);
                                }
                            }
                        }
                    }
                    // Advance watermark to last end LSN after finishing the batch without errors
                    if let Some(end) = last_end_lsn {
                        last_seen_end_lsn = Some(end);
                    }
                },
                _ = &mut connection_pin => {
                    error!("replication connection closed");
                    // Snapshot schemas before reconnecting
                    let snapshot = stream.as_mut().schemas_snapshot();
                    debug!(count = snapshot.len(), "snapshotting schemas before reconnect (connection closed)");
                    saved_schemas = snapshot;
                    break; // break inner loop to reconnect
                }
            }
        }

        // If we reached here without shutdown, apply linear backoff before reconnect
        if current_backoff < backoff_cap {
            current_backoff = (current_backoff + backoff_step).min(backoff_cap);
        }
        if current_backoff > Duration::from_secs(0) {
            debug!(
                backoff_ms = current_backoff.as_millis(),
                "backing off before reconnect"
            );
            tokio::time::sleep(current_backoff).await;
        }

        // Loop continues to reconnect
    }

    debug!("replication event loop stopped");
    Ok(())
}

#[cfg(test)]
mod tests;
