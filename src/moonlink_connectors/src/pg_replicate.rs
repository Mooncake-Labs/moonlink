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
use crate::pg_replicate::moonlink_sink::Sink;
use crate::pg_replicate::postgres_source::{
    CdcStreamConfig, CdcStreamError, PostgresSource, PostgresSourceError,
};
use crate::pg_replicate::table::{SrcTableId, TableName, TableSchema};
use crate::pg_replicate::table_init::{build_table_components, TableComponents};
use crate::Result;
use futures::StreamExt;
use moonlink::{
    CommitState, MooncakeTableId, MoonlinkTableConfig, ObjectStorageCache, ReadStateFilepathRemap,
    ReplicationState, TableEvent, TableStatus,
};
use native_tls::{Certificate, TlsConnector};
use pg_escape::{quote_identifier, quote_literal};
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
        commit_state: Arc<CommitState>,
        flush_lsn_rx: watch::Receiver<u64>,
        table_status_rx: watch::Receiver<TableStatus>,
        /// Restart the stream once the table is registered, so postgres replays from the slot.
        replay_from_slot: bool,
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
    /// Event senders of the tables registered with the replication loop, so they can be put in
    /// the errored state if the loop ever stops unexpectedly.
    registered_tables: RegisteredTables,
}

type RegisteredTables = Arc<std::sync::Mutex<HashMap<SrcTableId, mpsc::Sender<TableEvent>>>>;

/// Something that tracks an LSN watermark (`CommitState` and `ReplicationState` are both `LsnState`).
trait LsnMark {
    fn mark_lsn(&self, lsn: u64);
}

impl LsnMark for CommitState {
    fn mark_lsn(&self, lsn: u64) {
        self.mark(lsn);
    }
}

/// Publish the initial copy boundary. The table's commit LSN must be marked before the replication
/// LSN, the same order the sink uses for commits: readers rely on never observing a replication
/// LSN that is ahead of the commit LSN it covers.
fn mark_copy_boundary(commit_state: &impl LsnMark, replication_state: &impl LsnMark, lsn: u64) {
    commit_state.mark_lsn(lsn);
    replication_state.mark_lsn(lsn);
}

/// Identifies the database a connection string points at, independent of how it is spelled
/// (user, host alias, options). One replication slot and publication exist per database, so every
/// connection string for the same database must share one replication connection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceIdentity {
    pub system_id: String,
    pub database: String,
}

impl SourceIdentity {
    pub async fn identify(uri: &str) -> Result<Self> {
        let (client, connection) = ReplicationClient::connect(uri, true)
            .await
            .map_err(PostgresSourceError::from)?;
        let connection_task = tokio::spawn(async move {
            if let Err(e) = connection.await {
                debug!("identify connection closed: {}", e);
            }
        });
        let identified = client
            .identify_system()
            .await
            .map_err(PostgresSourceError::from);
        drop(client);
        connection_task.abort();
        let (system_id, database) = identified?;
        Ok(Self {
            system_id,
            database,
        })
    }

    /// Key used to share one replication connection per source database.
    pub fn key(&self) -> String {
        format!("{}/{}", self.system_id, self.database)
    }
}

/// Postgres only accepts `[a-z0-9_]` in slot names, up to 63 bytes.
fn is_valid_slot_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 63
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

/// FNV-1a; slot names must stay stable across releases, which std hashers do not promise.
fn stable_hash32(input: &str) -> u32 {
    input.bytes().fold(0x811c9dc5u32, |hash, b| {
        (hash ^ b as u32).wrapping_mul(0x01000193)
    })
}

/// Slot name for a database: `moonlink_slot_<db>` when that is a valid slot name, otherwise a
/// sanitized prefix of it plus a hash of the database name.
pub fn slot_name_for_database(database: &str) -> String {
    let legacy = format!("moonlink_slot_{database}");
    if is_valid_slot_name(&legacy) {
        return legacy;
    }
    let sanitized: String = legacy
        .chars()
        .map(|c| {
            let c = c.to_ascii_lowercase();
            if c.is_ascii_lowercase() || c.is_ascii_digit() {
                c
            } else {
                '_'
            }
        })
        .take(63 - 9)
        .collect();
    format!("{sanitized}_{:08x}", stable_hash32(database))
}

/// Keep the slot name earlier releases derived from the connection string when such a slot
/// already exists for this database, so upgrades keep replaying from it.
async fn choose_slot_name(client: &Client, uri: &str, database: &str) -> Result<String> {
    let canonical = slot_name_for_database(database);
    let legacy = match uri
        .parse::<Config>()
        .ok()
        .and_then(|c| c.get_dbname().map(str::to_string))
    {
        Some(db) => format!("moonlink_slot_{db}"),
        None => "moonlink_slot".to_string(),
    };
    if legacy == canonical || !is_valid_slot_name(&legacy) {
        return Ok(canonical);
    }
    let query = format!(
        "SELECT 1 FROM pg_replication_slots WHERE slot_name = {} AND database = {};",
        quote_literal(&legacy),
        quote_literal(database)
    );
    let legacy_exists = client
        .simple_query(&query)
        .await
        .map_err(PostgresSourceError::from)?
        .iter()
        .any(|m| matches!(m, SimpleQueryMessage::Row(_)));
    Ok(if legacy_exists { legacy } else { canonical })
}

/// Create `moonlink_pub` unless it already exists. It must never be dropped and re-created while
/// tables are mirrored: pgoutput filters changes by publication membership as of each change's LSN,
/// so anything committed between a re-create and the table being added back would never be sent.
async fn ensure_publication_exists(client: &Client) -> Result<()> {
    let exists = client
        .simple_query("SELECT 1 FROM pg_publication WHERE pubname = 'moonlink_pub';")
        .await
        .map_err(PostgresSourceError::from)?
        .iter()
        .any(|m| matches!(m, SimpleQueryMessage::Row(_)));
    if exists {
        return Ok(());
    }
    match client
        .simple_query("CREATE PUBLICATION moonlink_pub WITH (publish_via_partition_root = true);")
        .await
    {
        Ok(_) => Ok(()),
        // Lost a race with another connection to the same database.
        Err(e) if e.code() == Some(&SqlState::DUPLICATE_OBJECT) => Ok(()),
        Err(e) => Err(PostgresSourceError::from(e).into()),
    }
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
        ensure_publication_exists(&postgres_client).await?;

        let identity = SourceIdentity::identify(&uri).await?;
        let slot_name = choose_slot_name(&postgres_client, &uri, &identity.database).await?;

        // Preemptively terminate any stale backend holding this slot
        let terminate_query = format!(
            "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = {};",
            quote_literal(&slot_name)
        );
        if let Err(e) = postgres_client.simple_query(&terminate_query).await {
            warn!(slot_name = %slot_name, error = %e, "failed to terminate existing backend for slot");
        }

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
            registered_tables: Arc::default(),
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
        let ident = match TableName::parse_schema_name(table_name) {
            Ok((schema, name)) => {
                format!("{}.{}", quote_identifier(&schema), quote_identifier(&name))
            }
            Err(_) => quote_identifier(table_name).into_owned(),
        };
        let sql = format!("ALTER TABLE {} REPLICA IDENTITY FULL;", ident);
        self.run_control_query(&sql).await?;
        Ok(())
    }

    /// Add the table to the publication and, for a new mirror, copy its current contents.
    ///
    /// The table joins the publication before the copy snapshot is taken, so every change after the
    /// snapshot's boundary LSN is streamed; the table handler drops streamed changes up to the
    /// boundary, which the copy already contains. This runs for empty tables too: changes streamed
    /// between joining the publication and the snapshot must still be dropped.
    /// Returns whether an initial copy was performed.
    pub async fn perform_initial_copy(
        &self,
        schema: &TableSchema,
        event_sender: mpsc::Sender<TableEvent>,
        is_recovery: bool,
        commit_lsn_tx: Arc<CommitState>,
        table_base_path: &str,
    ) -> Result<bool> {
        let src_table_id = schema.src_table_id;
        let mut publication_source = PostgresSource::new(&self.uri, None, None, false).await?;
        if is_recovery {
            publication_source
                .add_table_to_publication(&schema.table_name)
                .await?;
            return Ok(false);
        }

        if let Err(e) = event_sender.send(TableEvent::StartInitialCopy).await {
            error!(error = ?e, "failed to send StartInitialCopy event");
        }
        publication_source
            .add_table_to_publication(&schema.table_name)
            .await?;

        let ic_config = InitialCopyConfig {
            reader: InitialCopyReaderConfig {
                uri: self.uri.clone(),
                shard_count: 4,
            },
            writer: Default::default(),
        };
        let progress = copy_table_stream(schema.clone(), &event_sender, table_base_path, ic_config)
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
        mark_copy_boundary(
            commit_lsn_tx.as_ref(),
            self.replication_state.as_ref(),
            progress.boundary_lsn.into(),
        );

        Ok(true)
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
            "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = {};",
            quote_literal(&self.slot_name)
        );
        self.run_control_query(&terminate_query).await?;

        // Then drop the replication slot. The terminated walsender may still hold it for a moment.
        let drop_query = format!(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = {};",
            quote_literal(&self.slot_name)
        );
        const MAX_ATTEMPTS: u32 = 20;
        for attempt in 1..=MAX_ATTEMPTS {
            match self.postgres_client.simple_query(&drop_query).await {
                Ok(_) => return Ok(()),
                Err(e) if e.code() == Some(&SqlState::OBJECT_IN_USE) && attempt < MAX_ATTEMPTS => {
                    sleep(Duration::from_millis(250)).await;
                }
                Err(_) => {
                    self.run_control_query(&drop_query).await?;
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    pub async fn remove_table_from_publication(&mut self, table_name: &str) -> Result<()> {
        let ident = match TableName::parse_schema_name(table_name) {
            Ok((schema, name)) => {
                format!("{}.{}", quote_identifier(&schema), quote_identifier(&name))
            }
            Err(_) => quote_identifier(table_name).into_owned(),
        };
        let drop_query = format!("ALTER PUBLICATION moonlink_pub DROP TABLE {};", ident);
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
        commit_state: Arc<CommitState>,
        flush_lsn_rx: watch::Receiver<u64>,
        table_status_rx: watch::Receiver<TableStatus>,
        replay_from_slot: bool,
    ) -> Result<oneshot::Receiver<()>> {
        let (ready_tx, ready_rx) = oneshot::channel();
        self.registered_tables
            .lock()
            .unwrap()
            .insert(src_table_id, event_sender.clone());
        let cmd = PostgresReplicationCommand::AddTable {
            src_table_id,
            schema,
            event_sender,
            commit_state,
            flush_lsn_rx,
            table_status_rx,
            replay_from_slot,
            ready_tx,
        };
        self.cmd_tx.send(cmd).await?;
        Ok(ready_rx)
    }

    /// Drop table from PostgreSQL replication
    pub async fn drop_table_from_replication(&self, src_table_id: SrcTableId) -> Result<()> {
        self.registered_tables.lock().unwrap().remove(&src_table_id);
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
        )
        .await?;

        // Send command to add table to replication
        let commit_lsn_tx = table_resources
            .commit_state
            .take()
            .expect("commit_lsn_tx is None");
        let commit_lsn_tx_for_copy = commit_lsn_tx.clone();
        let ready_rx = self
            .add_table_to_replication(
                table_schema.src_table_id,
                table_schema.clone(),
                table_resources.event_sender.clone(),
                commit_lsn_tx,
                table_resources
                    .flush_lsn_rx
                    .take()
                    .expect("flush_lsn_rx is None"),
                table_resources.table_event_manager.subscribe_table_status(),
                // A recovered table's changes after its iceberg snapshot may already have been
                // streamed (and dropped) before it was registered.
                /*replay_from_slot=*/
                is_recovery,
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
                commit_lsn_tx_for_copy,
                table_base_path,
            )
            .await?;

        assert!(
            !(is_recovery && initial_copy_performed),
            "initial copy should not be performed during recovery"
        );

        debug!(src_table_id = table_schema.src_table_id, "table added");

        Ok((table_schema.src_table_id, table_resources))
    }

    /// Drop table from PostgreSQL replication
    pub async fn drop_table(&mut self, src_table_id: u32, table_name: &str) -> Result<()> {
        debug!(src_table_id, "dropping table");

        // Remove table from publication as the first step, to prevent further events.
        self.remove_table_from_publication(table_name).await?;

        // Send command to drop table from replication. If the loop is gone there is nothing to
        // unregister, and the drop must still succeed.
        if let Err(e) = self.drop_table_from_replication(src_table_id).await {
            warn!(error = ?e, src_table_id, "replication loop already stopped while dropping table");
        }

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

        let cfg = self.source.get_cdc_stream_config().unwrap();
        let source = self.source.clone();

        let event_loop = tokio::spawn(run_event_loop(cfg, sink, receiver, source));
        supervise_replication(event_loop, self.registered_tables.clone())
    }
}

/// Compute the lsn to send to postgres as the `confirmed_flush_lsn` [https://www.postgresql.org/docs/current/view-pg-replication-slots.html]
/// Postgres may discard WAL up to this LSN, so it must not exceed what every table has persisted into iceberg;
/// after a restart, everything past it is replayed from the replication slot.
/// Errored tables never apply changes again (they can only be dropped and re-created), so they must
/// not hold the slot back; otherwise the source would retain WAL forever.
fn compute_confirmed_flush_lsn(
    flush_lsn_rxs: &HashMap<SrcTableId, watch::Receiver<u64>>,
    table_status_rxs: &HashMap<SrcTableId, watch::Receiver<TableStatus>>,
) -> Option<PgLsn> {
    flush_lsn_rxs
        .iter()
        .filter(|(table_id, _)| {
            table_status_rxs
                .get(table_id)
                .is_none_or(|status| *status.borrow() == TableStatus::Healthy)
        })
        .map(|(_, rx)| *rx.borrow())
        .min()
        .map(PgLsn::from)
}

/// Wait for the replication loop. It only returns on shutdown; if it fails or panics instead, no
/// table of this database would ever receive changes again, so put every registered table in the
/// errored state rather than letting their mirrors silently go stale.
fn supervise_replication(
    event_loop: JoinHandle<Result<()>>,
    registered_tables: RegisteredTables,
) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        let (reason, result) = match event_loop.await {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(e)) => (format!("replication from the source stopped: {e}"), Err(e)),
            Err(join_error) => {
                let message = if join_error.is_panic() {
                    let payload = join_error.into_panic();
                    payload
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
                        .unwrap_or_else(|| "unknown panic".to_string())
                } else {
                    "cancelled".to_string()
                };
                let reason = format!("replication from the source stopped unexpectedly: {message}");
                let err = PostgresSourceError::Io(Error::new(ErrorKind::Other, reason.clone()));
                (reason, Err(err.into()))
            }
        };
        error!(%reason, "replication event loop stopped");
        let senders: Vec<_> = registered_tables
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect();
        for sender in senders {
            let _ = sender
                .send(TableEvent::EnterErrored {
                    reason: reason.clone(),
                })
                .await;
        }
        result
    })
}

/// Linear reconnect backoff, 1 s steps up to one minute.
fn next_backoff(current: Duration) -> Duration {
    (current + Duration::from_secs(1)).min(Duration::from_secs(60))
}

/// Apply a table command while no replication stream is open; the schemas are handed to the next
/// stream. Returns false when the loop must stop.
fn apply_command_without_stream(
    cmd: Option<PostgresReplicationCommand>,
    sink: &mut Sink,
    flush_lsn_rxs: &mut HashMap<SrcTableId, watch::Receiver<u64>>,
    table_status_rxs: &mut HashMap<SrcTableId, watch::Receiver<TableStatus>>,
    saved_schemas: &mut Vec<TableSchema>,
) -> bool {
    match cmd {
        Some(PostgresReplicationCommand::AddTable {
            src_table_id,
            schema,
            event_sender,
            commit_state,
            flush_lsn_rx,
            table_status_rx,
            // The next stream starts from the slot anyway.
            replay_from_slot: _,
            ready_tx,
        }) => {
            sink.add_table(src_table_id, event_sender, commit_state, &schema);
            flush_lsn_rxs.insert(src_table_id, flush_lsn_rx);
            table_status_rxs.insert(src_table_id, table_status_rx);
            saved_schemas.retain(|s| s.src_table_id != src_table_id);
            saved_schemas.push(schema);
            if let Err(e) = ready_tx.send(()) {
                error!(error = ?e, "failed to send ready signal");
            }
            true
        }
        Some(PostgresReplicationCommand::DropTable { src_table_id }) => {
            sink.drop_table(src_table_id);
            flush_lsn_rxs.remove(&src_table_id);
            table_status_rxs.remove(&src_table_id);
            saved_schemas.retain(|s| s.src_table_id != src_table_id);
            true
        }
        Some(PostgresReplicationCommand::Shutdown) | None => false,
    }
}

/// Sleep for `delay` while still applying table commands, so tables can be added, dropped or the
/// connection shut down while the source is unreachable. Returns false when the loop must stop.
async fn wait_applying_commands(
    delay: Duration,
    cmd_rx: &mut mpsc::Receiver<PostgresReplicationCommand>,
    sink: &mut Sink,
    flush_lsn_rxs: &mut HashMap<SrcTableId, watch::Receiver<u64>>,
    table_status_rxs: &mut HashMap<SrcTableId, watch::Receiver<TableStatus>>,
    saved_schemas: &mut Vec<TableSchema>,
) -> bool {
    let sleep = tokio::time::sleep(delay);
    tokio::pin!(sleep);
    loop {
        tokio::select! {
            _ = &mut sleep => return true,
            cmd = cmd_rx.recv() => {
                if !apply_command_without_stream(cmd, sink, flush_lsn_rxs, table_status_rxs, saved_schemas) {
                    return false;
                }
            }
        }
    }
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
    let mut table_status_rxs: HashMap<SrcTableId, watch::Receiver<TableStatus>> = HashMap::new();
    let mut last_seen_end_lsn: Option<PgLsn> = None;

    let mut current_backoff = Duration::from_secs(0);

    'outer: loop {
        // With no table registered every change would be dropped, including what a recovering
        // table needs replayed from the slot, so wait for the first table before streaming.
        while flush_lsn_rxs.is_empty() {
            let cmd = cmd_rx.recv().await;
            if !apply_command_without_stream(
                cmd,
                &mut sink,
                &mut flush_lsn_rxs,
                &mut table_status_rxs,
                &mut saved_schemas,
            ) {
                break 'outer;
            }
        }

        // Prepare connection and stream
        let confirmed_flush_lsn = compute_confirmed_flush_lsn(&flush_lsn_rxs, &table_status_rxs)
            .unwrap_or(PgLsn::from(0));

        // Transient source errors (restarts, network, slot briefly still active) must never end
        // replication: retry with backoff while still applying table commands.
        let (client, mut connection) = match postgres_source.connect_replication().await {
            Ok(connected) => connected,
            Err(e) => {
                current_backoff = next_backoff(current_backoff);
                error!(error = ?e, backoff_ms = current_backoff.as_millis(), "failed to connect for replication, retrying");
                if !wait_applying_commands(
                    current_backoff,
                    &mut cmd_rx,
                    &mut sink,
                    &mut flush_lsn_rxs,
                    &mut table_status_rxs,
                    &mut saved_schemas,
                )
                .await
                {
                    break 'outer;
                }
                continue 'outer;
            }
        };

        // We should explicitly set the confirmed flush lsn here, because we may not have completed `send_status_update` before reconnecting.
        let mut cfg = cfg.clone();
        // Always start from persisted confirmed_flush_lsn; never advertise in-memory watermark as this will drop unpersisted records on PG.
        cfg.confirmed_flush_lsn = confirmed_flush_lsn;
        let mut connection_pin = Box::pin(connection);

        // Create stream while driving connection
        let created = tokio::select! {
            s = PostgresSource::create_cdc_stream(client, cfg) => s.map_err(|e| format!("{e:?}")),
            _ = &mut connection_pin => Err("connection closed during setup".to_string()),
        };
        let stream = match created {
            Ok(stream) => stream,
            Err(e) => {
                current_backoff = next_backoff(current_backoff);
                error!(error = %e, backoff_ms = current_backoff.as_millis(), "failed to start replication stream, retrying");
                if !wait_applying_commands(
                    current_backoff,
                    &mut cmd_rx,
                    &mut sink,
                    &mut flush_lsn_rxs,
                    &mut table_status_rxs,
                    &mut saved_schemas,
                )
                .await
                {
                    break 'outer;
                }
                continue 'outer;
            }
        };

        // Reset keepalive floor for this connection
        sink.reset_keepalive_floor();
        sink.abort_in_progress_streams().await;

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
                    let lsn_to_send = compute_confirmed_flush_lsn(&flush_lsn_rxs, &table_status_rxs).unwrap_or(PgLsn::from(0));
                    if let Err(e) = stream
                        .as_mut()
                        .send_status_update(lsn_to_send)
                        .await
                    {
                        error!(error = ?e, "failed to send status update");
                    }
                },
                Some(cmd) = cmd_rx.recv() => match cmd {
                    PostgresReplicationCommand::AddTable { src_table_id, schema, event_sender, commit_state, flush_lsn_rx, table_status_rx, replay_from_slot, ready_tx } => {
                        sink.add_table(src_table_id, event_sender, commit_state, &schema);
                        flush_lsn_rxs.insert(src_table_id, flush_lsn_rx);
                        table_status_rxs.insert(src_table_id, table_status_rx);
                        stream.as_mut().add_table_schema(schema);
                        if let Err(e) = ready_tx.send(()) {
                            error!(error = ?e, "failed to send ready signal");
                        }
                        if replay_from_slot {
                            debug!(src_table_id, "reconnecting to replay recovered table from the slot");
                            saved_schemas = stream.as_mut().schemas_snapshot();
                            break;
                        }
                    }
                    PostgresReplicationCommand::DropTable { src_table_id } => {
                        sink.drop_table(src_table_id);
                        flush_lsn_rxs.remove(&src_table_id);
                        table_status_rxs.remove(&src_table_id);
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

                    let mut reconnect = false;
                    for event in batch.drain(..n) {
                        match event {
                            Err(CdcStreamError::CdcEventConversion(CdcEventConversionError::MissingSchema(_))) => {
                                continue;
                            }
                            // A change of one table that cannot be applied faithfully fails that
                            // table only; never skip it and carry on as if it had been applied.
                            Err(CdcStreamError::CdcEventConversion(e)) if e.src_table_id().is_some() => {
                                let src_table_id = e.src_table_id().unwrap();
                                sink.fail_table(src_table_id, format!("a change from the source table could not be applied: {e}")).await;
                                continue;
                            }
                            Err(CdcStreamError::CdcEventConversion(CdcEventConversionError::MessageNotSupported)) => {
                                // TODO: Add support for Truncate and Origin messages and remove this.
                                warn!("message not supported");
                                continue;
                            }
                            Err(e) => {
                                // Not attributable to a table (transport or protocol error): drop the
                                // rest of the batch and reconnect; postgres re-sends it from the slot.
                                error!(error = ?e, "cdc stream error, reconnecting");
                                reconnect = true;
                                break;
                            }
                            Ok(event) => {
                                let Ok(()) = sink.process_cdc_event(event).await;
                            }
                        }
                    }
                    if reconnect {
                        saved_schemas = stream.as_mut().schemas_snapshot();
                        break;
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
        current_backoff = next_backoff(current_backoff);
        debug!(
            backoff_ms = current_backoff.as_millis(),
            "backing off before reconnect"
        );
        if !wait_applying_commands(
            current_backoff,
            &mut cmd_rx,
            &mut sink,
            &mut flush_lsn_rxs,
            &mut table_status_rxs,
            &mut saved_schemas,
        )
        .await
        {
            break 'outer;
        }

        // Loop continues to reconnect
    }

    debug!("replication event loop stopped");
    Ok(())
}

#[cfg(test)]
mod copy_boundary_tests {
    use super::{mark_copy_boundary, LsnMark};
    use std::sync::Mutex;

    struct Recorder<'a> {
        name: &'static str,
        log: &'a Mutex<Vec<(&'static str, u64)>>,
    }

    impl LsnMark for Recorder<'_> {
        fn mark_lsn(&self, lsn: u64) {
            self.log.lock().unwrap().push((self.name, lsn));
        }
    }

    #[test]
    fn copy_boundary_marks_commit_before_replication() {
        let log = Mutex::new(Vec::new());
        let commit = Recorder {
            name: "commit",
            log: &log,
        };
        let replication = Recorder {
            name: "replication",
            log: &log,
        };
        mark_copy_boundary(&commit, &replication, 42);
        assert_eq!(
            log.into_inner().unwrap(),
            vec![("commit", 42), ("replication", 42)]
        );
    }
}

#[cfg(test)]
mod supervisor_tests {
    use super::{supervise_replication, RegisteredTables};
    use moonlink::TableEvent;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn panicking_replication_loop_errors_every_registered_table() {
        let registered = RegisteredTables::default();
        let (tx_a, mut rx_a) = mpsc::channel(4);
        let (tx_b, mut rx_b) = mpsc::channel(4);
        registered.lock().unwrap().insert(1, tx_a);
        registered.lock().unwrap().insert(2, tx_b);
        let event_loop = tokio::spawn(async { panic!("boom") });
        assert!(supervise_replication(event_loop, registered)
            .await
            .unwrap()
            .is_err());
        for rx in [&mut rx_a, &mut rx_b] {
            match rx.recv().await {
                Some(TableEvent::EnterErrored { reason }) => assert!(reason.contains("boom")),
                other => panic!("expected EnterErrored, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn clean_shutdown_leaves_tables_alone() {
        let registered = RegisteredTables::default();
        let (tx, mut rx) = mpsc::channel(4);
        registered.lock().unwrap().insert(1, tx);
        let event_loop = tokio::spawn(async { Ok(()) });
        assert!(supervise_replication(event_loop, registered)
            .await
            .unwrap()
            .is_ok());
        assert!(rx.try_recv().is_err());
    }
}

#[cfg(test)]
mod slot_name_tests {
    use super::slot_name_for_database;

    #[test]
    fn valid_database_names_keep_the_legacy_slot_name() {
        assert_eq!(slot_name_for_database("postgres"), "moonlink_slot_postgres");
        assert_eq!(slot_name_for_database("app_db2"), "moonlink_slot_app_db2");
    }

    #[test]
    fn invalid_database_names_get_a_stable_valid_slot_name() {
        let names = ["my-app", "MyApp", "caf\u{e9}", &"x".repeat(80)];
        let slots: Vec<String> = names.iter().map(|n| slot_name_for_database(n)).collect();
        for slot in &slots {
            assert!(slot.len() <= 63, "{slot}");
            assert!(
                slot.bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_'),
                "{slot}"
            );
        }
        // Distinct databases that sanitize to the same prefix still get distinct slots.
        assert_ne!(
            slot_name_for_database("my-app"),
            slot_name_for_database("my_app")
        );
        assert_ne!(slots[0], slots[1]);
        // Stable across calls and releases.
        assert_eq!(
            slot_name_for_database("my-app"),
            "moonlink_slot_my_app_ef484009"
        );
    }
}

#[cfg(test)]
mod tests;
