#![allow(warnings)]

pub mod clients;
pub mod conversions;
pub mod initial_copy;
pub mod initial_copy_writer;
pub mod moonlink_sink;
pub mod postgres_source;
pub mod source_fingerprint;
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
use crate::pg_replicate::source_fingerprint::{Drift, SourceFingerprint};
use crate::pg_replicate::table::{LookupKey, SrcTableId, TableName, TableSchema};
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
use std::sync::atomic::{AtomicBool, Ordering};
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
        replication_state: Arc<ReplicationState>,
        flush_lsn_rx: watch::Receiver<u64>,
        table_status_rx: TableStatusWatch,
        /// The table was recovered and needs its changes after its snapshot from the slot. Before
        /// streaming starts that needs nothing more; a table registered later (a recovery that is
        /// retried) restarts the stream so postgres replays from the slot.
        replay_from_slot: bool,
        ready_tx: oneshot::Sender<()>,
    },
    DropTable {
        src_table_id: SrcTableId,
    },
    /// The table's initial copy finished at `boundary_lsn` (see [`Sink::finish_initial_copy`]).
    FinishInitialCopy {
        src_table_id: SrcTableId,
        boundary_lsn: u64,
        rows_copied: bool,
    },
    /// The table has its initial data (recovered, or initial copy done): from now on it may report
    /// replication progress as its flush LSN when it has nothing left to persist.
    TableReady {
        src_table_id: SrcTableId,
    },
    /// Start streaming from the slot. Sent once every table that needs changes from the slot is
    /// registered: until then the slot must not advance past changes a table still needs, and
    /// changes for an unregistered table would be dropped.
    StartStreaming,
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
    /// Source fingerprints of the replicated tables, polled for changes the stream does not show.
    watched_tables: WatchedTables,
    fingerprint_watch: Option<JoinHandle<()>>,
    /// Set when the replication loop stopped unexpectedly (see [`supervise_replication`]).
    replication_stopped: Arc<AtomicBool>,
    /// While set, the slot is not advanced: a mirror of this database that could not be restored
    /// yet will replay from it once it can.
    slot_hold: Arc<AtomicBool>,
}

type RegisteredTables = Arc<std::sync::Mutex<HashMap<SrcTableId, mpsc::Sender<TableEvent>>>>;

#[derive(Clone)]
struct WatchedTable {
    fingerprint: SourceFingerprint,
    mooncake_table_id: MooncakeTableId,
    table_base_path: String,
}

type WatchedTables = Arc<std::sync::Mutex<HashMap<SrcTableId, WatchedTable>>>;

/// How often replicated tables are checked for source changes the stream does not show.
const FINGERPRINT_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Identifies the database a connection string points at, independent of how it is spelled
/// (user, host alias, options). One replication slot and publication exist per database, so every
/// connection string for the same database must share one replication connection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceIdentity {
    pub system_id: String,
    /// The server's port: physical copies of a cluster (pg_basebackup clones, standbys) share its
    /// system identifier, and on one host they differ by port. Host names are not used, since one
    /// server is reachable under several (localhost, 127.0.0.1, a socket).
    pub port: String,
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
        let port = connect_control_client(uri)
            .await?
            .simple_query("SELECT current_setting('port');")
            .await
            .map_err(PostgresSourceError::from)?
            .iter()
            .find_map(|m| match m {
                SimpleQueryMessage::Row(row) => row.get(0).map(str::to_string),
                _ => None,
            })
            .unwrap_or_default();
        Ok(Self {
            system_id,
            port,
            database,
        })
    }

    /// Key used to share one replication connection per source database.
    pub fn key(&self) -> String {
        format!("{}/{}/{}", self.system_id, self.port, self.database)
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

/// A column type mirrors cannot store refuses the table, saying which column and type.
fn refuse_unsupported_column(table_name: &str, error: PostgresSourceError) -> crate::Error {
    match error {
        PostgresSourceError::ReplicationClient(
            crate::pg_replicate::clients::postgres::ReplicationClientError::UnsupportedType(
                column,
                _,
                _,
            ),
        ) => crate::Error::unsupported_source_table(
            table_name,
            &format!("column {column} has a type mirrors cannot store"),
        ),
        error => error.into(),
    }
}

/// `table_name` ("schema.table" or "table") as a quoted literal to cast to regclass.
fn regclass_literal(table_name: &str) -> String {
    let ident = match TableName::parse_schema_name(table_name) {
        Ok((schema, name)) => format!("{}.{}", quote_identifier(&schema), quote_identifier(&name)),
        Err(_) => quote_identifier(table_name).into_owned(),
    };
    quote_literal(&ident).to_string()
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
            watched_tables: Arc::default(),
            fingerprint_watch: None,
            replication_stopped: Arc::default(),
            slot_hold: Arc::default(),
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
        // REPLICA IDENTITY is per relation and not inherited: every partition needs it, or its
        // updates and deletes arrive without the old row (and, without a primary key, postgres
        // rejects them on the source). Partitions attached later are covered at the next start;
        // until then their changes put the mirror in the errored state.
        // pg_partition_tree() returns nothing for a plain table, so include the table itself.
        let query = format!(
            "SELECT c.oid::regclass::text FROM pg_class c WHERE c.relreplident <> 'f' \
             AND (c.oid = {table}::regclass \
                  OR c.oid IN (SELECT relid FROM pg_partition_tree({table}::regclass)));",
            table = regclass_literal(table_name)
        );
        let relations: Vec<String> = self
            .run_control_query(&query)
            .await?
            .into_iter()
            .filter_map(|message| match message {
                SimpleQueryMessage::Row(row) => row.get(0).map(str::to_string),
                _ => None,
            })
            .collect();
        for relation in relations {
            let sql = format!("ALTER TABLE {relation} REPLICA IDENTITY FULL;");
            self.run_control_query(&sql).await?;
        }
        Ok(())
    }

    /// Refuse regular tables with inheritance children: the copy would include the children's rows
    /// (like SELECT from the parent does), but their changes are never published with the parent's,
    /// so the mirror would silently diverge. (Partitions are published through their root.)
    /// Checked at creation and at every recovery; a child added while running is caught by the
    /// source comparison (see source_fingerprint).
    async fn check_not_inheritance_parent(&mut self, table_name: &str) -> Result<()> {
        let query = format!(
            "SELECT 1 FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhparent \
             JOIN pg_class child ON child.oid = i.inhrelid \
             WHERE i.inhparent = {}::regclass AND c.relkind = 'r' \
             AND child.relpersistence <> 't' LIMIT 1;",
            regclass_literal(table_name)
        );
        let has_children = self
            .run_control_query(&query)
            .await?
            .iter()
            .any(|m| matches!(m, SimpleQueryMessage::Row(_)));
        if has_children {
            return Err(crate::Error::unsupported_source_table(
                table_name,
                "it has inheritance children, whose changes are not replicated with it",
            ));
        }
        Ok(())
    }

    /// Refuse tables with generated columns: logical replication does not publish them, so the
    /// mirror would lack a column its definition has. Only checked at creation; existing mirrors
    /// keep working for their other columns.
    async fn check_no_generated_columns(&mut self, table_name: &str) -> Result<()> {
        let query = format!(
            "SELECT a.attname FROM pg_attribute a WHERE a.attrelid = {}::regclass \
             AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated <> '' ORDER BY a.attnum;",
            regclass_literal(table_name)
        );
        let generated: Vec<String> = self
            .run_control_query(&query)
            .await?
            .iter()
            .filter_map(|m| match m {
                SimpleQueryMessage::Row(row) => row.get(0).map(|name| format!("\"{name}\"")),
                _ => None,
            })
            .collect();
        if !generated.is_empty() {
            return Err(crate::Error::unsupported_source_table(
                table_name,
                &format!(
                    "generated columns ({}) are not replicated",
                    generated.join(", ")
                ),
            ));
        }
        Ok(())
    }

    /// At recovery: refuse a mirror whose source changed since it was recorded (while moonlink was
    /// stopped, or unnoticed before). Returns the recorded fingerprint, None for mirrors written by
    /// an older version.
    async fn check_source_unchanged(
        &mut self,
        table_name: &str,
        mooncake_table_id: &MooncakeTableId,
        table_base_path: &str,
    ) -> Result<Option<SourceFingerprint>> {
        let Some(recorded) = source_fingerprint::read(table_base_path, mooncake_table_id).await
        else {
            return Ok(None);
        };
        match source_fingerprint::drift(&self.postgres_client, &recorded)
            .await
            .map_err(PostgresSourceError::from)?
        {
            Drift::Unchanged | Drift::NewEmptyPartitions { .. } => Ok(Some(recorded)),
            Drift::Changed(reason) => Err(crate::Error::source_table_changed(table_name, &reason)),
        }
    }

    /// Refuse partitioned tables without a primary key: their partitions need REPLICA IDENTITY
    /// FULL, which a partition created later does not inherit, and until moonlink sets it postgres
    /// rejects UPDATE and DELETE on that partition for the user.
    async fn check_partitioned_source_has_key(
        &mut self,
        table_name: &str,
        table_schema: &TableSchema,
    ) -> Result<()> {
        if !matches!(table_schema.lookup_key, LookupKey::FullRow) {
            return Ok(());
        }
        let query = format!(
            "SELECT 1 FROM pg_class WHERE oid = {} AND relkind = 'p';",
            table_schema.src_table_id
        );
        let partitioned = self
            .run_control_query(&query)
            .await?
            .iter()
            .any(|m| matches!(m, SimpleQueryMessage::Row(_)));
        if partitioned {
            return Err(crate::Error::unsupported_source_table(
                table_name,
                "it is partitioned and has no primary key (postgres would reject updates and \
                 deletes on partitions created later until their replica identity is set); add a \
                 primary key",
            ));
        }
        Ok(())
    }

    /// Why the table cannot be mirrored next to the partition tree it is in, if it cannot: postgres
    /// publishes a partition's changes only as those of the topmost partitioned table in the
    /// publication (publish_via_partition_root), so of a partitioned table and its partition only
    /// one mirror would receive anything.
    async fn partition_overlap(
        &self,
        src_table_id: SrcTableId,
    ) -> std::result::Result<Option<String>, tokio_postgres::Error> {
        const BOTH: &str = "a partitioned table and its partitions cannot both be mirrored \
                            (postgres publishes a partition's changes only as the partitioned \
                            table's)";
        let client = &self.postgres_client;
        let ancestors = source_fingerprint::published_ancestors(client, src_table_id).await?;
        let partitions = source_fingerprint::partitions(client, src_table_id).await?;
        let mirrored: std::collections::HashSet<SrcTableId> = self
            .registered_tables
            .lock()
            .unwrap()
            .keys()
            .copied()
            .collect();
        if let Some((oid, ancestor)) = ancestors.first() {
            return Ok(Some(if mirrored.contains(oid) {
                format!(
                    "it is a partition of {ancestor}, which is already mirrored, and {BOTH}; drop \
                     that mirror first"
                )
            } else {
                format!(
                    "it is a partition of {ancestor}, which is in publication moonlink_pub without \
                     a running mirror, so postgres publishes its changes only as {ancestor}'s; \
                     drop {ancestor}'s errored mirror, or if it has none, run ALTER PUBLICATION \
                     moonlink_pub DROP TABLE ONLY {ancestor}"
                )
            }));
        }
        // A published partition without a running mirror does not matter: nothing reads it.
        Ok(partitions
            .iter()
            .find(|(oid, _)| mirrored.contains(oid))
            .map(|(_, partition)| {
                format!(
                    "its partition {partition} is already mirrored, and {BOTH}; drop that mirror \
                     first"
                )
            }))
    }

    /// Add the table to the publication and, for a new mirror, copy its current contents.
    ///
    /// The table joins the publication before the copy snapshot is taken, so every change after the
    /// snapshot's boundary LSN is streamed; the table handler drops streamed changes up to the
    /// boundary, which the copy already contains. This runs for empty tables too: changes streamed
    /// between joining the publication and the snapshot must still be dropped.
    /// Returns the LSN the copied rows are committed at, if the copy loaded any.
    pub async fn perform_initial_copy(
        &self,
        schema: &TableSchema,
        event_sender: mpsc::Sender<TableEvent>,
        is_recovery: bool,
        table_base_path: &str,
    ) -> Result<Option<u64>> {
        let src_table_id = schema.src_table_id;
        let mut publication_source = PostgresSource::new(&self.uri, None, None, false).await?;
        if is_recovery {
            publication_source
                .add_table_to_publication(&schema.table_name)
                .await?;
            return Ok(None);
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
        let progress =
            copy_table_stream(schema.clone(), &event_sender, table_base_path, ic_config).await?;

        if let Err(e) = event_sender
            .send(TableEvent::FinishInitialCopy {
                start_lsn: progress.boundary_lsn.into(),
            })
            .await
        {
            error!(error = ?e, table_id = src_table_id, "failed to send FinishTableCopy command");
        }

        // The sink publishes the table's commit LSN from here on, in order with the stream: the
        // boundary is usually ahead of what the stream has applied, and the replication LSN is
        // shared by every mirror of the database.
        self.cmd_tx
            .send(PostgresReplicationCommand::FinishInitialCopy {
                src_table_id,
                boundary_lsn: progress.boundary_lsn.into(),
                rows_copied: progress.rows_copied > 0,
            })
            .await?;

        Ok((progress.rows_copied > 0).then_some(progress.boundary_lsn.into()))
    }

    pub fn retry_drop(uri: &str, drop_query: &str) -> JoinHandle<Result<()>> {
        let uri = uri.to_string();
        let drop_query = drop_query.to_string();
        tokio::spawn(async move {
            let result = Self::retry_drop_impl(&uri, &drop_query).await;
            if let Err(e) = &result {
                warn!(error = ?e, drop_query, "gave up retrying a cleanup statement");
            }
            result
        })
    }

    async fn retry_drop_impl(uri: &str, drop_query: &str) -> Result<()> {
        let mut retry_count = 0;
        loop {
            let tls = build_tls_connector().map_err(PostgresSourceError::from)?;
            match connect(uri, tls).await {
                Ok((client, connection)) => {
                    tokio::spawn(async move {
                        if let Err(e) = connection.await {
                            warn!("connection error: {}", e);
                        }
                    });
                    match client.simple_query(drop_query).await {
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

    /// Take the source table with oid `src_table_id` out of the publication, if it is still in it.
    /// By oid, not by name: a removal retried after the mirror is gone must not take out a table
    /// re-created under the same name (and mirrored again), and finds nothing to do once the table
    /// or its schema is gone.
    pub async fn remove_table_from_publication(&mut self, src_table_id: SrcTableId) -> Result<()> {
        self.attempt_drop_else_retry(&remove_from_publication_query(src_table_id))
            .await?;
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
        replication_state: Arc<ReplicationState>,
        flush_lsn_rx: watch::Receiver<u64>,
        table_status_rx: TableStatusWatch,
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
            replication_state,
            flush_lsn_rx,
            table_status_rx,
            replay_from_slot,
            ready_tx,
        };
        self.cmd_tx.send(cmd).await?;
        Ok(ready_rx)
    }

    /// Let the replication loop start streaming; see [`PostgresReplicationCommand::StartStreaming`].
    pub async fn start_streaming(&self) -> Result<()> {
        self.cmd_tx
            .send(PostgresReplicationCommand::StartStreaming)
            .await?;
        Ok(())
    }

    /// Drop table from PostgreSQL replication
    pub async fn drop_table_from_replication(&self, src_table_id: SrcTableId) -> Result<()> {
        self.registered_tables.lock().unwrap().remove(&src_table_id);
        self.watched_tables.lock().unwrap().remove(&src_table_id);
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
        // A drop of an earlier mirror of this table may still be retrying in the background: if
        // it removed the table from the publication after this mirror added it, this mirror would
        // receive nothing.
        self.wait_for_pending_retries().await;
        self.check_not_inheritance_parent(table_name).await?;
        // A mirror whose source changed since it was recorded is not resumed.
        let recorded_fingerprint = if is_recovery {
            self.check_source_unchanged(table_name, mooncake_table_id, table_base_path)
                .await?
        } else {
            self.check_no_generated_columns(table_name).await?;
            None
        };
        // TODO: We should not naively alter the replica identity of a table. We should only do this if we are sure that the table does not already have a FULL replica identity. [https://github.com/Mooncake-Labs/moonlink/issues/104]
        self.alter_table_replica_identity(table_name).await?;
        let table_schema = self
            .source
            .fetch_table_schema(None, Some(table_name), None)
            .await
            .map_err(|e| refuse_unsupported_column(table_name, e))?;
        if is_recovery {
            match &recorded_fingerprint {
                Some(recorded) if recorded.relid != table_schema.src_table_id => {
                    return Err(crate::Error::source_table_changed(
                        table_name,
                        "it was dropped and re-created (or replaced by another table of that name)",
                    ));
                }
                Some(_) => {}
                None => {
                    let columns =
                        source_fingerprint::columns_with_changed_value_format(&table_schema);
                    if !columns.is_empty() {
                        return Err(crate::Error::source_table_changed(
                            table_name,
                            &format!(
                                "its mirror was written by an older moonlink version that stored \
                                 columns {} differently, and without a primary key its rows are \
                                 matched by value, so later updates and deletes would miss them",
                                columns.join(", ")
                            ),
                        ));
                    }
                }
            }
        } else {
            self.check_partitioned_source_has_key(table_name, &table_schema)
                .await?;
        }
        let fingerprint =
            source_fingerprint::fetch(&self.postgres_client, table_schema.src_table_id)
                .await
                .map_err(PostgresSourceError::from)?
                .ok_or_else(|| {
                    crate::Error::source_table_changed(
                        table_name,
                        "it was dropped while being added",
                    )
                })?;
        if !is_recovery {
            let unreplicated = source_fingerprint::unreplicated_partition(
                &self.postgres_client,
                table_schema.src_table_id,
            )
            .await
            .map_err(PostgresSourceError::from)?;
            if let Some(partition) = unreplicated {
                return Err(crate::Error::unsupported_source_table(
                    table_name,
                    &format!("{partition}; only regular (logged) partitions are supported"),
                ));
            }
            if let Some(overlap) = self
                .partition_overlap(table_schema.src_table_id)
                .await
                .map_err(PostgresSourceError::from)?
            {
                return Err(crate::Error::unsupported_source_table(table_name, &overlap));
            }
        }

        // One mirror per source table: changes are routed by source table id.
        if self
            .registered_tables
            .lock()
            .unwrap()
            .contains_key(&table_schema.src_table_id)
        {
            return Err(crate::Error::source_already_mirrored(table_name));
        }

        let (arrow_schema, identity) =
            crate::pg_replicate::util::postgres_schema_to_moonlink_schema(&table_schema)
                .map_err(|e| refuse_unsupported_column(table_name, e.into()))?;
        moonlink_table_config.mooncake_table_config.row_identity = identity;
        let table_components = TableComponents {
            read_state_filepath_remap,
            object_storage_cache,
            moonlink_table_config: moonlink_table_config.clone(),
        };

        // A new mirror starts from the source alone: an iceberg table left at its path (by a
        // mirror of the same name whose drop could not finish) must not be loaded.
        if !is_recovery {
            crate::errored_tables::drop_iceberg_table_files(
                &moonlink_table_config.iceberg_table_config,
            )
            .await?;
        }
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
        // A new mirror is in copy mode before the stream can deliver anything for it: the slot
        // may replay the table's history (a re-created mirror), which the copy already holds and
        // the handler must buffer and judge against the copy's boundary.
        if !is_recovery {
            if table_resources
                .event_sender
                .send(TableEvent::StartInitialCopy)
                .await
                .is_err()
            {
                return Err(crate::Error::table_not_found(table_name.to_string()));
            }
        }
        // An errored table stops holding the slot only once its errored state is persisted: if the
        // process died before, it would come back healthy and need the changes the slot keeps.
        let (slot_status_tx, slot_status_rx) = watch::channel(TableStatus::Healthy);
        table_resources.slot_status_tx = Some(slot_status_tx);
        let ready_rx = self
            .add_table_to_replication(
                table_schema.src_table_id,
                table_schema.clone(),
                table_resources.event_sender.clone(),
                commit_lsn_tx,
                table_resources.replication_state.clone(),
                table_resources
                    .flush_lsn_rx
                    .take()
                    .expect("flush_lsn_rx is None"),
                TableStatusWatch {
                    table: table_resources.table_event_manager.subscribe_table_status(),
                    slot: slot_status_rx,
                },
                /*replay_from_slot=*/ is_recovery,
            )
            .await?;

        // Wait until the event loop has registered sink and schema
        if let Err(e) = ready_rx.await {
            error!(error = ?e, "failed to add table to replication");
        }
        // Recovery starts streaming once every recovered table is registered.
        if !is_recovery {
            self.start_streaming().await?;
        }

        // Perform initial copy
        table_resources.initial_copy_lsn = match self
            .perform_initial_copy(
                &table_schema,
                table_resources.event_sender.clone(),
                is_recovery,
                table_base_path,
            )
            .await
        {
            Ok(copy_lsn) => copy_lsn,
            Err(e) => {
                error!(error = ?e, table_name, "initial copy failed, removing the table again");
                self.undo_add_table(
                    table_schema.src_table_id,
                    table_name,
                    &mut table_resources,
                    table_base_path,
                )
                .await;
                return Err(e);
            }
        };

        assert!(
            !(is_recovery && table_resources.initial_copy_lsn.is_some()),
            "initial copy should not be performed during recovery"
        );
        self.cmd_tx
            .send(PostgresReplicationCommand::TableReady {
                src_table_id: table_schema.src_table_id,
            })
            .await?;

        if let Err(e) =
            source_fingerprint::write(table_base_path, mooncake_table_id, &fingerprint).await
        {
            warn!(error = %e, table_name, "failed to persist source fingerprint");
        }
        self.watched_tables.lock().unwrap().insert(
            table_schema.src_table_id,
            WatchedTable {
                fingerprint,
                mooncake_table_id: mooncake_table_id.clone(),
                table_base_path: table_base_path.to_string(),
            },
        );

        debug!(src_table_id = table_schema.src_table_id, "table added");

        Ok((table_schema.src_table_id, table_resources))
    }

    /// Undo a table registration that did not complete: unregister it from the replication loop,
    /// drop its handler and files, and take it out of the publication. Best effort; each step logs.
    async fn undo_add_table(
        &mut self,
        src_table_id: SrcTableId,
        table_name: &str,
        table_resources: &mut crate::pg_replicate::table_init::TableResources,
        table_base_path: &str,
    ) {
        if let Err(e) = self.drop_table_from_replication(src_table_id).await {
            warn!(error = ?e, table_name, "failed to unregister table from replication");
        }
        if let Err(e) = table_resources.table_event_manager.drop_table().await {
            warn!(error = ?e, table_name, "failed to drop table handler");
        }
        if let Err(e) = self.remove_table_from_publication(src_table_id).await {
            warn!(error = ?e, table_name, "failed to remove table from publication");
        }
        let copy_dir = crate::pg_replicate::initial_copy::initial_copy_output_dir(
            table_base_path,
            src_table_id,
        );
        if let Err(e) = tokio::fs::remove_dir_all(&copy_dir).await {
            if e.kind() != ErrorKind::NotFound {
                warn!(error = ?e, table_name, "failed to remove initial copy files");
            }
        }
    }

    /// Drop table from PostgreSQL replication
    pub async fn drop_table(&mut self, src_table_id: u32, table_name: &str) -> Result<()> {
        debug!(src_table_id, "dropping table");

        // Not compared with its source any more: leaving the publication would error it.
        self.watched_tables.lock().unwrap().remove(&src_table_id);
        // Remove table from publication as the first step, to prevent further events.
        self.remove_table_from_publication(src_table_id).await?;

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
        if let Some(watch) = self.fingerprint_watch.take() {
            watch.abort();
        }
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

        let checker = FingerprintChecker::new(self.uri.clone(), &self.watched_tables);
        let event_loop = tokio::spawn(run_event_loop(
            cfg,
            sink,
            receiver,
            source,
            checker,
            self.slot_hold.clone(),
        ));
        self.fingerprint_watch = Some(spawn_fingerprint_watch(
            self.uri.clone(),
            &self.watched_tables,
            self.registered_tables.clone(),
        ));
        supervise_replication(
            event_loop,
            self.registered_tables.clone(),
            self.replication_stopped.clone(),
        )
    }

    /// The oid of a source table, if it exists.
    pub async fn source_table_oid(&mut self, table_name: &str) -> Result<Option<SrcTableId>> {
        let query = format!(
            "SELECT c.oid FROM pg_class c WHERE c.oid = to_regclass({});",
            regclass_literal(table_name)
        );
        Ok(self
            .run_control_query(&query)
            .await?
            .iter()
            .find_map(|m| match m {
                SimpleQueryMessage::Row(row) => row.get(0).and_then(|oid| oid.parse().ok()),
                _ => None,
            }))
    }

    /// Hold the slot where it is (see [`PostgresConnection::slot_hold`]) or release it.
    pub fn set_slot_hold(&self, hold: bool) {
        self.slot_hold.store(hold, Ordering::SeqCst);
    }

    /// Set once the replication loop stopped unexpectedly. The errored state that causes for every
    /// table is not a problem of the tables: the slot still covers them, so it is not persisted
    /// and a restart resumes them.
    pub fn replication_stopped(&self) -> Arc<AtomicBool> {
        self.replication_stopped.clone()
    }
}

/// Refuse to mirror `table_name` for a connection string whose role cannot read it. Tables are
/// added through the database's one shared connection, i.e. with the role of the connection string
/// that opened it; without this check another role could mirror (and read through the mirror) a
/// table it has no access to.
pub async fn check_role_can_read_table(uri: &str, table_name: &str) -> Result<()> {
    let client = connect_control_client(uri).await?;
    let query = format!(
        "SELECT has_table_privilege({}::regclass, 'SELECT');",
        regclass_literal(table_name)
    );
    let allowed = client
        .simple_query(&query)
        .await
        .map_err(PostgresSourceError::from)?
        .iter()
        .any(|m| matches!(m, SimpleQueryMessage::Row(row) if row.get(0) == Some("t")));
    if !allowed {
        return Err(crate::Error::unsupported_source_table(
            table_name,
            "the role of this connection string has no SELECT privilege on it",
        ));
    }
    Ok(())
}

/// Advance an unused slot of the source to its current WAL position. For a database whose
/// remaining mirrors are all errored: they never apply changes again, and without a replication
/// loop nothing else confirms the slot. A slot in use (a mirror was just created) is left alone.
pub async fn advance_unused_slot(uri: &str) -> Result<()> {
    let identity = SourceIdentity::identify(uri).await?;
    let client = connect_control_client(uri).await?;
    let slot_name = choose_slot_name(&client, uri, &identity.database).await?;
    client
        .simple_query(&format!(
            "SELECT pg_replication_slot_advance(slot_name, pg_current_wal_lsn()) \
             FROM pg_replication_slots WHERE slot_name = {} AND NOT active;",
            quote_literal(&slot_name)
        ))
        .await
        .map_err(PostgresSourceError::from)?;
    Ok(())
}

/// A control connection for background checks, with the same session settings as the main one.
async fn connect_control_client(uri: &str) -> Result<Client> {
    let tls = build_tls_connector().map_err(PostgresSourceError::from)?;
    let (client, connection) = connect(uri, tls).await.map_err(PostgresSourceError::from)?;
    tokio::spawn(
        async move {
            if let Err(e) = connection.await {
                warn!("connection error: {}", e);
            }
        }
        .instrument(info_span!("postgres_connection_monitor")),
    );
    client.simple_query("SET lock_timeout = '100ms';").await?;
    Ok(client)
}

/// Checks watched tables' source fingerprints (see [`source_fingerprint`]) over its own control
/// connection. Failing to reach the source is not a change: it is logged and retried later.
struct FingerprintChecker {
    uri: String,
    watched: std::sync::Weak<std::sync::Mutex<HashMap<SrcTableId, WatchedTable>>>,
    client: Option<Client>,
}

impl FingerprintChecker {
    fn new(uri: String, watched: &WatchedTables) -> Self {
        Self {
            uri,
            watched: Arc::downgrade(watched),
            client: None,
        }
    }

    /// Ids of the watched tables, or None once the connection is gone.
    fn watched_ids(&self) -> Option<Vec<SrcTableId>> {
        let watched = self.watched.upgrade()?;
        let ids = watched.lock().unwrap().keys().copied().collect();
        Some(ids)
    }

    /// Why the table's mirror can no longer follow its source, if it cannot. Adopts new partitions
    /// that hold nothing the mirror lacks, setting their replica identity.
    async fn check(&mut self, src_table_id: SrcTableId) -> Option<String> {
        let table = self
            .watched
            .upgrade()?
            .lock()
            .unwrap()
            .get(&src_table_id)
            .cloned()?;
        if self.client.is_none() {
            match connect_control_client(&self.uri).await {
                Ok(client) => self.client = Some(client),
                Err(e) => {
                    warn!(error = ?e, "cannot check source table for changes; retrying later");
                    return None;
                }
            }
        }
        let client = self.client.as_ref().unwrap();
        let drift = match source_fingerprint::drift(client, &table.fingerprint).await {
            Ok(drift) => drift,
            Err(e) => {
                warn!(error = ?e, "cannot check source table for changes; retrying later");
                self.client = None;
                return None;
            }
        };
        match drift {
            Drift::Unchanged => None,
            Drift::NewEmptyPartitions {
                current,
                new_partitions,
            } => {
                if let Err(e) = set_replica_identity_full(client, &new_partitions).await {
                    warn!(error = ?e, ?new_partitions, "cannot set replica identity of new partitions yet");
                    return None;
                }
                if let Err(e) = source_fingerprint::write(
                    &table.table_base_path,
                    &table.mooncake_table_id,
                    &current,
                )
                .await
                {
                    warn!(error = %e, "failed to persist source fingerprint");
                }
                if let Some(watched) = self.watched.upgrade() {
                    if let Some(entry) = watched.lock().unwrap().get_mut(&src_table_id) {
                        entry.fingerprint = current;
                    }
                }
                None
            }
            Drift::Changed(reason) => {
                if let Some(watched) = self.watched.upgrade() {
                    watched.lock().unwrap().remove(&src_table_id);
                }
                Some(format!("the source table changed: {reason}"))
            }
        }
    }
}

/// Poll the watched tables until the connection is gone, for changes no Relation message shows
/// (or not until the table's next change). A table that can no longer be followed is errored.
fn spawn_fingerprint_watch(
    uri: String,
    watched: &WatchedTables,
    registered_tables: RegisteredTables,
) -> JoinHandle<()> {
    let mut checker = FingerprintChecker::new(uri, watched);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(FINGERPRINT_POLL_INTERVAL);
        loop {
            interval.tick().await;
            let Some(ids) = checker.watched_ids() else {
                return;
            };
            for src_table_id in ids {
                if let Some(reason) = checker.check(src_table_id).await {
                    let sender = registered_tables
                        .lock()
                        .unwrap()
                        .get(&src_table_id)
                        .cloned();
                    if let Some(sender) = sender {
                        error!(src_table_id, %reason, "mirror can no longer follow its source table");
                        let _ = sender.send(TableEvent::EnterErrored { reason }).await;
                    }
                }
            }
        }
    })
}

async fn set_replica_identity_full(
    client: &Client,
    relations: &[String],
) -> std::result::Result<(), tokio_postgres::Error> {
    for relation in relations {
        client
            .simple_query(&format!("ALTER TABLE {relation} REPLICA IDENTITY FULL;"))
            .await?;
    }
    Ok(())
}

/// Status ticks between warnings about mirrors that receive no changes (about a minute).
const ERRORED_WARNING_EVERY_TICKS: u64 = 6;

/// Errored mirrors stay errored until dropped and re-created, and a held slot keeps WAL on the
/// source; say so periodically rather than only once, when it happened.
fn warn_about_stalled_mirrors(
    sink: &Sink,
    table_status_rxs: &HashMap<SrcTableId, TableStatusWatch>,
    slot_held: bool,
) {
    let errored = errored_table_names(sink, table_status_rxs);
    if !errored.is_empty() {
        warn!(mirrors = ?errored, "mirrors in the errored state receive no changes; drop and re-create them");
    }
    if slot_held {
        warn!("replication slot is held until every mirror of this database is restored; the source retains WAL meanwhile");
    }
}

fn errored_table_names(
    sink: &Sink,
    table_status_rxs: &HashMap<SrcTableId, TableStatusWatch>,
) -> Vec<String> {
    let mut names: Vec<String> = table_status_rxs
        .iter()
        .filter(|(_, status)| *status.table.borrow() != TableStatus::Healthy)
        .map(|(id, status)| {
            let name = sink.table_name(*id).unwrap_or_else(|| id.to_string());
            if *status.slot.borrow() == TableStatus::Healthy {
                format!("{name} (errored state not written yet; the slot stays held for it)")
            } else {
                name
            }
        })
        .collect();
    names.sort();
    names
}

/// A table's status, and its status as the replication slot sees it: errored only once the errored
/// state is persisted (see `spawn_errored_marker_writer`).
pub struct TableStatusWatch {
    pub table: watch::Receiver<TableStatus>,
    pub slot: watch::Receiver<TableStatus>,
}

/// Removes the table with oid `src_table_id` from moonlink_pub if the publication still has it,
/// under the name the oid has now, and fails (rolling back) if that name took out another table.
/// ONLY: without it postgres also drops the table's inheritance children, which other mirrors may
/// still need.
fn remove_from_publication_query(src_table_id: SrcTableId) -> String {
    format!(
        "DO $moonlink$ \
         DECLARE pub oid; rel regclass; \
         BEGIN \
             SELECT p.oid, r.prrelid::regclass INTO pub, rel FROM pg_publication p \
                 JOIN pg_publication_rel r ON r.prpubid = p.oid \
                 WHERE p.pubname = 'moonlink_pub' AND r.prrelid = {src_table_id}; \
             IF rel IS NULL THEN RETURN; END IF; \
             EXECUTE format('ALTER PUBLICATION moonlink_pub DROP TABLE ONLY %s', rel); \
             IF EXISTS (SELECT 1 FROM pg_publication_rel WHERE prpubid = pub AND prrelid = {src_table_id}) THEN \
                 RAISE EXCEPTION 'removing relation {src_table_id} from moonlink_pub took out another table'; \
             END IF; \
         END $moonlink$;"
    )
}

/// Compute the lsn to send to postgres as the `confirmed_flush_lsn` [https://www.postgresql.org/docs/current/view-pg-replication-slots.html]
/// Postgres may discard WAL up to this LSN, so it must not exceed what every table has persisted into iceberg;
/// after a restart, everything past it is replayed from the replication slot.
/// Errored tables never apply changes again (they can only be dropped and re-created), so they must
/// not hold the slot back; otherwise the source would retain WAL forever.
fn compute_confirmed_flush_lsn(
    flush_lsn_rxs: &HashMap<SrcTableId, watch::Receiver<u64>>,
    table_status_rxs: &HashMap<SrcTableId, TableStatusWatch>,
) -> Option<PgLsn> {
    flush_lsn_rxs
        .iter()
        .filter(|(table_id, _)| {
            table_status_rxs
                .get(table_id)
                .is_none_or(|status| *status.slot.borrow() == TableStatus::Healthy)
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
    replication_stopped: Arc<AtomicBool>,
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
        replication_stopped.store(true, Ordering::SeqCst);
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
async fn apply_command_without_stream(
    cmd: Option<PostgresReplicationCommand>,
    streaming_requested: &mut bool,
    sink: &mut Sink,
    flush_lsn_rxs: &mut HashMap<SrcTableId, watch::Receiver<u64>>,
    table_status_rxs: &mut HashMap<SrcTableId, TableStatusWatch>,
    saved_schemas: &mut Vec<TableSchema>,
) -> bool {
    match cmd {
        Some(PostgresReplicationCommand::AddTable {
            src_table_id,
            schema,
            event_sender,
            commit_state,
            replication_state,
            flush_lsn_rx,
            table_status_rx,
            replay_from_slot,
            ready_tx,
        }) => {
            // The next stream starts from the slot anyway.
            sink.add_table(
                src_table_id,
                event_sender,
                commit_state,
                &schema,
                /*initial_copy=*/ !replay_from_slot,
            );
            sink.track_table_replication(src_table_id, replication_state);
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
        Some(PostgresReplicationCommand::FinishInitialCopy {
            src_table_id,
            boundary_lsn,
            rows_copied,
        }) => {
            sink.finish_initial_copy(src_table_id, boundary_lsn, rows_copied)
                .await;
            true
        }
        Some(PostgresReplicationCommand::TableReady { src_table_id }) => {
            sink.mark_ready_for_progress(src_table_id);
            true
        }
        Some(PostgresReplicationCommand::StartStreaming) => {
            *streaming_requested = true;
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
    streaming_requested: &mut bool,
    sink: &mut Sink,
    flush_lsn_rxs: &mut HashMap<SrcTableId, watch::Receiver<u64>>,
    table_status_rxs: &mut HashMap<SrcTableId, TableStatusWatch>,
    saved_schemas: &mut Vec<TableSchema>,
) -> bool {
    let sleep = tokio::time::sleep(delay);
    tokio::pin!(sleep);
    loop {
        tokio::select! {
            _ = &mut sleep => return true,
            cmd = cmd_rx.recv() => {
                if !apply_command_without_stream(cmd, streaming_requested, sink, flush_lsn_rxs, table_status_rxs, saved_schemas).await {
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
    mut fingerprint_checker: FingerprintChecker,
    slot_hold: Arc<AtomicBool>,
) -> Result<()> {
    // Persist across reconnects
    let mut saved_schemas: Vec<TableSchema> = Vec::new();
    let mut flush_lsn_rxs: HashMap<SrcTableId, watch::Receiver<u64>> = HashMap::new();
    let mut table_status_rxs: HashMap<SrcTableId, TableStatusWatch> = HashMap::new();
    let mut last_seen_end_lsn: Option<PgLsn> = None;
    let mut streaming_requested = false;

    let mut current_backoff = Duration::from_secs(0);

    'outer: loop {
        // Stream only once asked to (after recovery has registered every recovered table) and while
        // some table is registered: with none, every change would be dropped.
        while !streaming_requested || flush_lsn_rxs.is_empty() {
            let cmd = cmd_rx.recv().await;
            if !apply_command_without_stream(
                cmd,
                &mut streaming_requested,
                &mut sink,
                &mut flush_lsn_rxs,
                &mut table_status_rxs,
                &mut saved_schemas,
            )
            .await
            {
                break 'outer;
            }
        }

        // Prepare connection and stream
        // 0 starts from the slot's confirmed position (and confirms nothing).
        let confirmed_flush_lsn = if slot_hold.load(Ordering::SeqCst) {
            PgLsn::from(0)
        } else {
            compute_confirmed_flush_lsn(&flush_lsn_rxs, &table_status_rxs).unwrap_or(PgLsn::from(0))
        };

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
                    &mut streaming_requested,
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
                    &mut streaming_requested,
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
        let mut status_ticks: u64 = 0;

        loop {
            tokio::select! {
                _ = status_interval.tick() => {
                    status_ticks += 1;
                    if status_ticks % ERRORED_WARNING_EVERY_TICKS == 1 {
                        warn_about_stalled_mirrors(&sink, &table_status_rxs, slot_hold.load(Ordering::SeqCst));
                    }
                    // Tables report it back through their flush LSN, in time for a later update.
                    sink.send_replication_progress();
                    // With no healthy table (every one errored) nothing needs the WAL received so far.
                    let lsn_to_send = if slot_hold.load(Ordering::SeqCst) {
                        PgLsn::from(0)
                    } else {
                        compute_confirmed_flush_lsn(&flush_lsn_rxs, &table_status_rxs)
                            .unwrap_or_else(|| PgLsn::from(sink.replication_lsn()))
                    };
                    if let Err(e) = stream
                        .as_mut()
                        .send_status_update(lsn_to_send)
                        .await
                    {
                        error!(error = ?e, "failed to send status update");
                    }
                },
                Some(cmd) = cmd_rx.recv() => match cmd {
                    PostgresReplicationCommand::AddTable { src_table_id, schema, event_sender, commit_state, replication_state, flush_lsn_rx, table_status_rx, replay_from_slot, ready_tx } => {
                        sink.add_table(src_table_id, event_sender, commit_state, &schema, /*initial_copy=*/ !replay_from_slot);
                        sink.track_table_replication(src_table_id, replication_state);
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
                    PostgresReplicationCommand::TableReady { src_table_id } => {
                        sink.mark_ready_for_progress(src_table_id);
                    }
                    PostgresReplicationCommand::StartStreaming => {}
                    PostgresReplicationCommand::DropTable { src_table_id } => {
                        sink.drop_table(src_table_id);
                        flush_lsn_rxs.remove(&src_table_id);
                        table_status_rxs.remove(&src_table_id);
                        stream.as_mut().remove_table_schema(src_table_id);
                    }
                    PostgresReplicationCommand::FinishInitialCopy { src_table_id, boundary_lsn, rows_copied } => {
                        sink.finish_initial_copy(src_table_id, boundary_lsn, rows_copied)
                .await;
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
                                sink.fail_table_for_change(src_table_id, format!("a change from the source table could not be applied: {e}")).await;
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
                            // Before the table's next change: a definition or partition change
                            // that the Relation message itself does not show.
                            Ok(CdcEvent::RelationSeen { src_table_id }) => {
                                if let Some(reason) = fingerprint_checker.check(src_table_id).await {
                                    sink.fail_table(src_table_id, reason).await;
                                }
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
            &mut streaming_requested,
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
        assert!(
            supervise_replication(event_loop, registered, Default::default())
                .await
                .unwrap()
                .is_err()
        );
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
        assert!(
            supervise_replication(event_loop, registered, Default::default())
                .await
                .unwrap()
                .is_ok()
        );
        assert!(rx.try_recv().is_err());
    }
}

#[cfg(test)]
mod stalled_mirror_tests {
    use super::{errored_table_names, TableStatusWatch};
    use crate::pg_replicate::moonlink_sink::Sink;
    use crate::pg_replicate::table::{LookupKey, TableName, TableSchema};
    use moonlink::{CommitState, ReplicationState, TableStatus};
    use std::collections::HashMap;
    use tokio::sync::{mpsc, watch};

    #[test]
    fn errored_mirrors_are_listed_by_source_table() {
        let mut sink = Sink::new(ReplicationState::new());
        let mut statuses = HashMap::new();
        for (id, status) in [
            (1, TableStatus::Healthy),
            (2, TableStatus::Errored("truncated".to_string())),
            (3, TableStatus::Errored("truncated".to_string())),
        ] {
            let (tx, _rx) = mpsc::channel(1);
            let schema = TableSchema {
                table_name: TableName {
                    schema: "public".into(),
                    name: format!("t{id}"),
                },
                src_table_id: id,
                column_schemas: vec![],
                lookup_key: LookupKey::FullRow,
            };
            sink.add_table(id, tx, CommitState::new(), &schema, false);
            let persisted = if id == 3 {
                TableStatus::Healthy
            } else {
                status.clone()
            };
            statuses.insert(
                id,
                TableStatusWatch {
                    table: watch::channel(status).1,
                    slot: watch::channel(persisted).1,
                },
            );
        }
        assert_eq!(
            errored_table_names(&sink, &statuses),
            vec![
                "public.t2",
                "public.t3 (errored state not written yet; the slot stays held for it)"
            ]
        );
    }
}

#[cfg(test)]
mod source_identity_tests {
    use super::SourceIdentity;

    #[test]
    fn clones_of_a_cluster_on_other_ports_are_different_sources() {
        let identity = |port: &str| SourceIdentity {
            system_id: "7300000000000000001".to_string(),
            port: port.to_string(),
            database: "app".to_string(),
        };
        assert_ne!(identity("5432").key(), identity("5433").key());
        assert_eq!(identity("5432").key(), identity("5432").key());
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
