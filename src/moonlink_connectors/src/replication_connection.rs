use crate::pg_replicate::{table::SrcTableId, PostgresConnection};
use crate::{Error, Result};
use moonlink::{
    MooncakeTableId, MoonlinkTableConfig, ObjectStorageCache, ReadStateFilepathRemap,
    ReadStateManager, TableEventManager, TableStatusReader,
};

use crate::errored_tables;
use moonlink::TableStatus;
use std::collections::HashMap;
use std::hash::Hash;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, error, warn};

struct TableState {
    src_table_name: String,
    reader: ReadStateManager,
    event_manager: TableEventManager,
    status_reader: TableStatusReader,
    /// Persists the errored state when the table enters it.
    errored_marker_writer: JoinHandle<()>,
}

/// Id which uniquely identifies a table, including source information (src uri, src table id) and destination information (mooncake table id).
#[derive(Clone, Eq, PartialEq, Hash)]
struct UniqueTableId {
    mooncake_table_id: MooncakeTableId,
    src_table_id: u32,
}

/// Record the errored state on disk as soon as the table enters it, so it survives restarts.
fn spawn_errored_marker_writer(
    base_path: String,
    mooncake_table_id: MooncakeTableId,
    mut table_status: watch::Receiver<TableStatus>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let errored = match &*table_status.borrow_and_update() {
                TableStatus::Errored(message) => Some(message.clone()),
                TableStatus::Healthy => None,
            };
            if let Some(message) = errored {
                if let Err(e) =
                    errored_tables::write_marker(&base_path, &mooncake_table_id, &message).await
                {
                    error!(error = %e, table = %mooncake_table_id, "failed to persist errored state");
                }
                return;
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
        })
    }

    /// Connection string the replication connection was opened with.
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
    ) -> &ReadStateManager {
        let unique_table_id = UniqueTableId {
            mooncake_table_id: mooncake_table_id.clone(),
            src_table_id,
        };
        &self.table_states.get(&unique_table_id).unwrap().reader
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

        let (src_table_id, table_resources) = self
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
        );
        let table_state = TableState {
            src_table_name: src_table_name.to_string(),
            reader: table_resources.read_state_manager,
            event_manager: table_resources.table_event_manager,
            status_reader: table_resources.table_status_reader,
            errored_marker_writer,
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
        }

        // Drop from the appropriate source
        if let Err(e) = self.source.drop_table(src_table_id, table_name).await {
            warn!(error = ?e, src_table_id, "failed to remove table from replication");
        }

        errored_tables::remove_marker(&self.table_base_path, mooncake_table_id).await;
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

            // Finalize the source connection
            self.source.shutdown(postgres_drop_all).await?;

            debug!("replication connection shutdown complete");
            Ok(())
        })
    }
}
