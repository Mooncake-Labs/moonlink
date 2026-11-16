use crate::pg_replicate::{table::SrcTableId, PostgresConnection};
use crate::{Error, Result};
use moonlink::{
    MooncakeTableId, MoonlinkTableConfig, ObjectStorageCache, ReadStateFilepathRemap,
    ReadStateManager, TableEventManager, TableStatusReader,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::errored_tables;
use crate::pg_replicate::source_fingerprint;
use moonlink::TableStatus;
use std::collections::HashMap;
use std::hash::Hash;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, error, warn};

struct TableState {
    src_table_name: String,
    reader: Arc<ReadStateManager>,
    event_manager: TableEventManager,
    status_reader: TableStatusReader,
    /// Persists the errored state when the table enters it.
    errored_marker_writer: JoinHandle<()>,
    /// To delete the table's files when its handler cannot (it died).
    iceberg_table_config: moonlink::IcebergTableConfig,
}

/// Id which uniquely identifies a table, including source information (src uri, src table id) and destination information (mooncake table id).
#[derive(Clone, Eq, PartialEq, Hash)]
struct UniqueTableId {
    mooncake_table_id: MooncakeTableId,
    src_table_id: u32,
}

/// First and longest wait between attempts to persist an errored state.
const ERRORED_MARKER_RETRY_INITIAL: std::time::Duration = std::time::Duration::from_millis(500);
const ERRORED_MARKER_RETRY_MAX: std::time::Duration = std::time::Duration::from_secs(30);

/// Record the errored state on disk as soon as the table enters it, so it survives restarts.
/// Not when the replication loop itself stopped: nothing is wrong with the table, the slot still
/// covers it, and a restart resumes it.
fn spawn_errored_marker_writer(
    base_path: String,
    mooncake_table_id: MooncakeTableId,
    mut table_status: watch::Receiver<TableStatus>,
    replication_stopped: Arc<AtomicBool>,
    slot_status: Option<watch::Sender<TableStatus>>,
    mut connection_shutdown: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let errored = match &*table_status.borrow_and_update() {
                TableStatus::Errored(message) => Some(message.clone()),
                TableStatus::Healthy => None,
            };
            if let Some(message) = errored {
                if replication_stopped.load(Ordering::SeqCst) {
                    warn!(table = %mooncake_table_id, "errored because replication stopped; not persisted, a restart resumes it");
                    return;
                }
                // Retried until it succeeds, the mirror is dropped (which aborts this task) or its
                // connection shuts down.
                let mut backoff = ERRORED_MARKER_RETRY_INITIAL;
                loop {
                    match errored_tables::write_marker(&base_path, &mooncake_table_id, &message)
                        .await
                    {
                        // Only now may the slot move past what the table did not apply.
                        Ok(()) => {
                            if let Some(slot_status) = &slot_status {
                                let _ = slot_status.send(TableStatus::Errored(message));
                            }
                            return;
                        }
                        Err(e) => {
                            error!(error = %e, table = %mooncake_table_id, retry_in = ?backoff, "failed to persist errored state; the replication slot stays held for this mirror until it is persisted");
                        }
                    }
                    // What the table did not apply stays in the slot (it was never released), so
                    // a restart resumes from there.
                    tokio::select! {
                        _ = tokio::time::sleep(backoff) => {}
                        _ = connection_shutdown.wait_for(|shutdown| *shutdown) => return,
                    }
                    backoff = (backoff * 2).min(ERRORED_MARKER_RETRY_MAX);
                }
            }
            if table_status.changed().await.is_err() {
                return;
            }
        }
    })
}

/// Manages replication for table(s) within a database from PostgreSQL CDC.
pub struct ReplicationConnection {
    table_base_path: String,
    /// Postgres logical replication source.
    source: PostgresConnection,
    // Common fields
    handle: Option<JoinHandle<Result<()>>>,
    /// Maps from unique table id to its table states.
    table_states: HashMap<UniqueTableId, TableState>,
    /// Whether replication has started.
    replication_started: bool,
    /// Object storage cache.
    object_storage_cache: ObjectStorageCache,
    /// Set on shutdown, so errored-state writers stop retrying (see [`spawn_errored_marker_writer`]).
    shutdown_tx: watch::Sender<bool>,
}

impl ReplicationConnection {
    pub async fn new(
        uri: String,
        table_base_path: String,
        object_storage_cache: ObjectStorageCache,
    ) -> Result<Self> {
        let source = PostgresConnection::new(uri).await?;

        Ok(Self {
            source,
            table_base_path,
            object_storage_cache,
            table_states: HashMap::new(),
            replication_started: false,
            handle: None,
            shutdown_tx: watch::channel(false).0,
        })
    }

    /// Connection string the replication connection was opened with.
    pub fn set_slot_hold(&self, hold: bool) {
        self.source.set_slot_hold(hold);
    }

    pub fn uri(&self) -> &str {
        &self.source.uri
    }

    /// Check if replication has started.
    pub fn replication_started(&self) -> bool {
        self.replication_started
    }

    pub fn get_table_reader(
        &self,
        mooncake_table_id: &MooncakeTableId,
        src_table_id: SrcTableId,
    ) -> Arc<ReadStateManager> {
        let unique_table_id = UniqueTableId {
            mooncake_table_id: mooncake_table_id.clone(),
            src_table_id,
        };
        self.table_states
            .get(&unique_table_id)
            .unwrap()
            .reader
            .clone()
    }

    pub fn get_table_status_reader(
        &self,
        mooncake_table_id: &MooncakeTableId,
        src_table_id: SrcTableId,
    ) -> &TableStatusReader {
        let unique_table_id = UniqueTableId {
            mooncake_table_id: mooncake_table_id.clone(),
            src_table_id,
        };
        &self
            .table_states
            .get(&unique_table_id)
            .unwrap()
            .status_reader
    }

    pub fn get_table_status_readers(&self) -> HashMap<MooncakeTableId, &TableStatusReader> {
        self.table_states
            .iter()
            .map(|(unique_table_id, cur_table_state)| {
                (
                    unique_table_id.mooncake_table_id.clone(),
                    &cur_table_state.status_reader,
                )
            })
            .collect::<HashMap<_, _>>()
    }

    pub fn table_count(&self) -> usize {
        self.table_states.len()
    }

    pub fn get_table_event_manager(
        &mut self,
        mooncake_table_id: &MooncakeTableId,
        src_table_id: SrcTableId,
    ) -> &mut TableEventManager {
        let unique_table_id = UniqueTableId {
            mooncake_table_id: mooncake_table_id.clone(),
            src_table_id,
        };
        &mut self
            .table_states
            .get_mut(&unique_table_id)
            .unwrap()
            .event_manager
    }

    pub async fn start_replication(&mut self) -> Result<()> {
        assert!(!self.replication_started);
        assert!(self.handle.is_none());

        debug!("starting replication");

        let handle = self.source.spawn_replication_task().await;
        self.replication_started = true;

        self.handle = Some(handle);
        debug!("replication started");
        Ok(())
    }

    /// Let the replication task stream from the slot; tables added before this are all in place.
    pub async fn start_streaming(&self) -> Result<()> {
        self.source.start_streaming().await
    }

    /// Add a table for replication
    pub async fn add_table_replication(
        &mut self,
        src_table_name: &str,
        mooncake_table_id: &MooncakeTableId,
        moonlink_table_config: &mut MoonlinkTableConfig,
        read_state_filepath_remap: ReadStateFilepathRemap,
        is_recovery: bool,
    ) -> Result<SrcTableId> {
        debug!(src_table_name, "adding PostgreSQL table for replication");

        let (src_table_id, mut table_resources) = self
            .source
            .add_table(
                src_table_name,
                mooncake_table_id,
                moonlink_table_config,
                is_recovery,
                &self.table_base_path,
                read_state_filepath_remap,
                self.object_storage_cache.clone(),
            )
            .await?;

        let errored_marker_writer = spawn_errored_marker_writer(
            self.table_base_path.clone(),
            mooncake_table_id.clone(),
            table_resources.table_event_manager.subscribe_table_status(),
            self.source.replication_stopped(),
            table_resources.slot_status_tx.take(),
            self.shutdown_tx.subscribe(),
        );
        let table_state = TableState {
            src_table_name: src_table_name.to_string(),
            reader: Arc::new(table_resources.read_state_manager),
            event_manager: table_resources.table_event_manager,
            status_reader: table_resources.table_status_reader,
            errored_marker_writer,
            iceberg_table_config: moonlink_table_config.iceberg_table_config.clone(),
        };

        // TODO(hjiang): Add assertion or error propagation.
        let unique_table_id = UniqueTableId {
            mooncake_table_id: mooncake_table_id.clone(),
            src_table_id,
        };
        self.table_states.insert(unique_table_id, table_state);
        debug!(src_table_id, "PostgreSQL table added for replication");
        Ok(src_table_id)
    }

    /// The oid of a source table, if it exists.
    pub async fn source_table_oid(&mut self, src_table_name: &str) -> Result<Option<SrcTableId>> {
        self.source.source_table_oid(src_table_name).await
    }

    /// Whether a mirror served by this connection replicates the source table `src_table_name`.
    pub async fn mirrors_source_table(&mut self, src_table_name: &str) -> Result<bool> {
        let Some(oid) = self.source.source_table_oid(src_table_name).await? else {
            return Ok(false);
        };
        Ok(self.table_states.keys().any(|id| id.src_table_id == oid))
    }

    /// Take a source table out of the publication (for a mirror this connection does not serve).
    pub async fn remove_table_from_publication(&mut self, src_table_name: &str) -> Result<()> {
        self.source
            .remove_table_from_publication(src_table_name)
            .await
    }

    /// Remove the given table from connection.
    pub async fn drop_table(
        &mut self,
        mooncake_table_id: &MooncakeTableId,
        src_table_id: u32,
    ) -> Result<()> {
        debug!(src_table_id, "dropping table");

        // Get table state and remove it from the map
        let unique_table_id = UniqueTableId {
            mooncake_table_id: mooncake_table_id.clone(),
            src_table_id,
        };
        let table_state = self
            .table_states
            .remove(&unique_table_id)
            .ok_or(Error::table_not_found(mooncake_table_id.to_string()))?;

        let table_name = &table_state.src_table_name;

        // it is important to drop table from table handler first,
        // otherwise table handler will panic when trying to send back notification to sinks
        debug!(src_table_id, "drop table from table handler");
        table_state.errored_marker_writer.abort();
        let mut event_manager = table_state.event_manager;
        // The table is already unregistered; finish the drop even if one step fails, otherwise
        // the mirror could never be dropped (or re-created) again.
        if let Err(e) = event_manager.drop_table().await {
            warn!(error = ?e, src_table_id, "table handler could not complete the drop");
            // Delete what the handler would have: a mirror re-created with this name must not
            // load this one's iceberg snapshot.
            let _ =
                errored_tables::drop_iceberg_table_files(&table_state.iceberg_table_config).await;
            let local_files =
                std::path::Path::new(&self.table_base_path).join(mooncake_table_id.to_string());
            if let Err(e) = tokio::fs::remove_dir_all(&local_files).await {
                if e.kind() != std::io::ErrorKind::NotFound {
                    warn!(error = ?e, ?local_files, "failed to delete the dropped mirror's local files");
                }
            }
        }

        // Drop from the appropriate source
        if let Err(e) = self.source.drop_table(src_table_id, table_name).await {
            warn!(error = ?e, src_table_id, "failed to remove table from replication");
        }

        errored_tables::remove_marker(&self.table_base_path, mooncake_table_id).await;
        source_fingerprint::remove(&self.table_base_path, mooncake_table_id).await;
        debug!(src_table_id, "table dropped");
        Ok(())
    }

    /// Shuts down the replication event loop and finalizes the source connection.
    ///
    /// If postgres drop all is false, then we will not drop the PostgreSQL publication and replication slot,
    /// which allows for recovery from the PostgreSQL replication slot.
    pub fn shutdown(mut self, postgres_drop_all: bool) -> JoinHandle<Result<()>> {
        tokio::spawn(async move {
            // Stop the replication event loop
            if self.replication_started {
                // The loop may have stopped already; the slot and publication still need dropping.
                if let Err(e) = self.source.shutdown_replication().await {
                    warn!(error = ?e, "replication loop already stopped");
                }
                if let Some(handle) = self.handle.take() {
                    if let Err(e) = handle.await {
                        warn!(error = ?e, "task join error during shutdown");
                    }
                }
                self.replication_started = false;
            }
            // A write in progress may still finish; a writer still retrying stops.
            self.shutdown_tx.send_replace(true);

            // Finalize the source connection
            self.source.shutdown(postgres_drop_all).await?;

            debug!("replication connection shutdown complete");
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The persisted marker after the table errors, and the status the replication slot sees.
    async fn marker_after_error(
        base: &str,
        replication_stopped: bool,
    ) -> (Option<String>, TableStatus) {
        let id = MooncakeTableId {
            database: "db".to_string(),
            table: "t".to_string(),
        };
        let (tx, rx) = watch::channel(TableStatus::Healthy);
        let (slot_status_tx, slot_status_rx) = watch::channel(TableStatus::Healthy);
        let writer = spawn_errored_marker_writer(
            base.to_string(),
            id.clone(),
            rx,
            Arc::new(AtomicBool::new(replication_stopped)),
            Some(slot_status_tx),
            watch::channel(false).1,
        );
        tx.send(TableStatus::Errored("broken".to_string())).unwrap();
        writer.await.unwrap();
        let slot_status = slot_status_rx.borrow().clone();
        (errored_tables::read_marker(base, &id).await, slot_status)
    }

    #[tokio::test]
    async fn errored_state_is_persisted_unless_replication_stopped() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().to_str().unwrap();
        let (marker, slot_status) = marker_after_error(base, false).await;
        assert_eq!(marker.as_deref(), Some("broken"));
        assert_eq!(slot_status, TableStatus::Errored("broken".to_string()));
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().to_str().unwrap();
        assert_eq!(
            marker_after_error(base, true).await,
            (None, TableStatus::Healthy)
        );
    }

    /// Until the errored state is persisted, the slot keeps seeing the table as healthy (holding
    /// back what it did not apply); the write is retried, and the slot released once it succeeds.
    #[tokio::test]
    async fn slot_is_held_until_the_errored_state_can_be_persisted() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("base");
        // A file where the marker's directory has to go: writes fail until it is removed.
        std::fs::write(&base, "").unwrap();
        let id = MooncakeTableId {
            database: "db".to_string(),
            table: "t".to_string(),
        };
        let (tx, rx) = watch::channel(TableStatus::Healthy);
        let (slot_status_tx, mut slot_status_rx) = watch::channel(TableStatus::Healthy);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let writer = spawn_errored_marker_writer(
            base.to_str().unwrap().to_string(),
            id.clone(),
            rx,
            Arc::new(AtomicBool::new(false)),
            Some(slot_status_tx),
            shutdown_rx,
        );
        tx.send(TableStatus::Errored("broken".to_string())).unwrap();
        tokio::time::sleep(ERRORED_MARKER_RETRY_INITIAL * 3).await;
        assert!(!writer.is_finished(), "gave up after a failed write");
        assert_eq!(*slot_status_rx.borrow(), TableStatus::Healthy);

        std::fs::remove_file(&base).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(10), slot_status_rx.changed())
            .await
            .expect("not retried")
            .unwrap();
        assert_eq!(
            *slot_status_rx.borrow(),
            TableStatus::Errored("broken".to_string())
        );
        writer.await.unwrap();
        assert_eq!(
            errored_tables::read_marker(base.to_str().unwrap(), &id)
                .await
                .as_deref(),
            Some("broken")
        );
    }

    /// A writer still retrying when its connection shuts down stops, leaving the slot held.
    #[tokio::test]
    async fn errored_state_writer_stops_retrying_on_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("base");
        std::fs::write(&base, "").unwrap();
        let id = MooncakeTableId {
            database: "db".to_string(),
            table: "t".to_string(),
        };
        let (tx, rx) = watch::channel(TableStatus::Healthy);
        let (slot_status_tx, slot_status_rx) = watch::channel(TableStatus::Healthy);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let writer = spawn_errored_marker_writer(
            base.to_str().unwrap().to_string(),
            id.clone(),
            rx,
            Arc::new(AtomicBool::new(false)),
            Some(slot_status_tx),
            shutdown_rx,
        );
        tx.send(TableStatus::Errored("broken".to_string())).unwrap();
        tokio::time::sleep(ERRORED_MARKER_RETRY_INITIAL * 3).await;
        assert!(!writer.is_finished(), "gave up after a failed write");
        shutdown_tx.send_replace(true);
        tokio::time::timeout(std::time::Duration::from_secs(5), writer)
            .await
            .expect("kept retrying after the connection shut down")
            .unwrap();
        assert_eq!(*slot_status_rx.borrow(), TableStatus::Healthy);
    }
}
