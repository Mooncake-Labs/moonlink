use crate::pg_replicate::table_init::build_table_components;
use crate::pg_replicate::PostgresConnection;
use crate::pg_replicate::{table::SrcTableId, table_init::TableComponents};
use crate::rest_ingest::event_request::EventRequest;
use crate::rest_ingest::RestApiConnection;
use crate::{Error, Result};
use moonlink::{
    MooncakeTableId, MoonlinkTableConfig, ObjectStorageCache, ReadStateFilepathRemap,
    ReadStateManager, TableEventManager, TableStatusReader,
};

use arrow_schema::Schema as ArrowSchema;
use std::collections::HashMap;
use std::hash::Hash;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

#[allow(clippy::large_enum_variant)]
pub enum SourceType {
    Postgres(PostgresConnection),
    RestApi(RestApiConnection),
}

impl SourceType {
    async fn spawn_task(&mut self) -> JoinHandle<Result<()>> {
        match self {
            SourceType::Postgres(conn) => conn.spawn_replication_task().await,
            SourceType::RestApi(conn) => conn.spawn_rest_task().await,
        }
    }

    async fn shutdown_task(&mut self) -> Result<()> {
        match self {
            SourceType::Postgres(conn) => conn.shutdown_replication().await,
            SourceType::RestApi(conn) => conn.shutdown_replication().await,
        }
    }

    async fn drop_table(&mut self, src_table_id: u32, table_name: &str) -> Result<()> {
        match self {
            SourceType::Postgres(conn) => conn.drop_table(src_table_id, table_name).await,
            SourceType::RestApi(conn) => conn.drop_table(src_table_id, table_name).await,
        }
    }

    /// If postgres drop all is false, then we will not drop the PostgreSQL publication and replication slot,
    /// which allows for recovery from the PostgreSQL replication slot.
    async fn finalize(&mut self, postgres_drop_all: bool) -> Result<()> {
        match self {
            SourceType::Postgres(conn) => conn.shutdown(postgres_drop_all).await,
            SourceType::RestApi(_) => Ok(()),
        }
    }
}

struct TableState {
    src_table_name: String,
    reader: ReadStateManager,
    event_manager: TableEventManager,
    status_reader: TableStatusReader,
}

/// Id which uniquely identifies a table, including source information (src uri, src table id) and destination information (mooncake table id).
#[derive(Clone, Eq, PartialEq, Hash)]
struct UniqueTableId {
    mooncake_table_id: MooncakeTableId,
    src_table_id: u32,
}

/// Manages replication for table(s) within a database from various sources (PostgreSQL CDC, REST API, etc.).
pub struct ReplicationConnection {
    table_base_path: String,
    // Source-specific connections
    source: SourceType,
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
        let source = if uri.starts_with("rest://") {
            SourceType::RestApi(RestApiConnection::new().await?)
        } else {
            SourceType::Postgres(PostgresConnection::new(uri).await?)
        };

        Ok(Self {
            source,
            table_base_path,
            object_storage_cache,
            table_states: HashMap::new(),
            replication_started: false,
            handle: None,
        })
    }

    /// Check if replication has started.
    pub fn replication_started(&self) -> bool {
        self.replication_started
    }

    /// Get the REST request sender for submitting REST API requests
    pub fn get_rest_request_sender(&self) -> mpsc::Sender<EventRequest> {
        match &self.source {
            SourceType::Postgres(_) => {
                panic!("rest request sender not available for postgres source")
            }
            SourceType::RestApi(conn) => conn.get_rest_request_sender(),
        }
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

        let handle = self.source.spawn_task().await;
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
        match &mut self.source {
            SourceType::Postgres(conn) => {
                debug!(src_table_name, "adding PostgreSQL table for replication");

                let (src_table_id, table_resources) = conn
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

                let table_state = TableState {
                    src_table_name: src_table_name.to_string(),
                    reader: table_resources.read_state_manager,
                    event_manager: table_resources.table_event_manager,
                    status_reader: table_resources.table_status_reader,
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
            SourceType::RestApi(_) => {
                panic!("Cannot add replication table to REST API connection")
            }
        }
    }

    /// Add a table for REST API ingestion with Arrow schema
    ///
    /// # Arguments
    ///
    /// * persist_lsn: only assigned at recovery, used to indicate and update replication LSN.
    #[allow(clippy::too_many_arguments)]
    pub async fn add_table_api(
        &mut self,
        src_table_name: &str,
        mooncake_table_id: &MooncakeTableId,
        arrow_schema: ArrowSchema,
        moonlink_table_config: MoonlinkTableConfig,
        read_state_filepath_remap: ReadStateFilepathRemap,
        persist_lsn: Option<u64>,
    ) -> Result<SrcTableId> {
        match &mut self.source {
            SourceType::RestApi(conn) => {
                debug!(src_table_name, "adding REST API table");

                let src_table_id = conn.next_src_table_id();
                let table_components = TableComponents {
                    read_state_filepath_remap,
                    object_storage_cache: self.object_storage_cache.clone(),
                    moonlink_table_config,
                };

                // Create MooncakeTable resources using the table init function.
                let replication_state = conn.get_replication_state();
                let mut table_resources = build_table_components(
                    mooncake_table_id.to_string(),
                    arrow_schema.clone(),
                    src_table_name.to_string(),
                    src_table_id,
                    &self.table_base_path,
                    &replication_state,
                    table_components,
                    /*is_recovery=*/ persist_lsn.is_some(),
                )
                .await?;

                // Add table to RestSource and connect to RestSink
                conn.add_table(
                    src_table_name.to_string(),
                    src_table_id,
                    std::sync::Arc::new(arrow_schema),
                    table_resources.event_sender.clone(),
                    table_resources
                        .commit_lsn_tx
                        .take()
                        .expect("commit_lsn_tx not set"),
                    table_resources
                        .flush_lsn_rx
                        .take()
                        .expect("flush_lsn_rx not set"),
                    table_resources
                        .wal_flush_lsn_rx
                        .take()
                        .expect("wal_flush_lsn_rx not set"),
                    persist_lsn,
                )
                .await?;

                // Store table state
                let table_state = TableState {
                    src_table_name: src_table_name.to_string(),
                    reader: table_resources.read_state_manager,
                    event_manager: table_resources.table_event_manager,
                    status_reader: table_resources.table_status_reader,
                };

                let unique_table_id = UniqueTableId {
                    mooncake_table_id: mooncake_table_id.clone(),
                    src_table_id,
                };
                // TODO(hjiang): Add assertion or error propagation.
                self.table_states.insert(unique_table_id, table_state);
                debug!(
                    src_table_id,
                    src_table_name, "REST API table added successfully"
                );
                Ok(src_table_id)
            }
            SourceType::Postgres(_) => {
                panic!("Cannot add API table to PostgreSQL connection")
            }
        }
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
        let mut event_manager = table_state.event_manager;
        event_manager.drop_table().await?;

        // Drop from the appropriate source
        self.source.drop_table(src_table_id, table_name).await?;

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
                self.source.shutdown_task().await?;
                if let Some(handle) = self.handle.take() {
                    if let Err(e) = handle.await {
                        warn!(error = ?e, "task join error during shutdown");
                    }
                }
                self.replication_started = false;
            }

            // Finalize the source connection
            self.source.finalize(postgres_drop_all).await?;

            debug!("replication connection shutdown complete");
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use moonlink::{ObjectStorageCache, ObjectStorageCacheConfig};

    #[tokio::test]
    async fn test_rest_api_connection_creation() {
        let temp_dir = tempfile::tempdir().unwrap();
        let cache_config = ObjectStorageCacheConfig::new(
            1 << 30, // 1GB
            temp_dir.path().to_str().unwrap().to_string(),
            false, // optimize_local_filesystem
        );
        let object_storage_cache = ObjectStorageCache::new(cache_config);

        let mut connection = ReplicationConnection::new(
            crate::replication_manager::REST_API_URI.to_string(),
            temp_dir.path().join("tables").to_string_lossy().to_string(),
            object_storage_cache,
        )
        .await
        .unwrap();

        assert!(matches!(connection.source, SourceType::RestApi(_)));
        let _ = connection.get_rest_request_sender();
        connection.start_replication().await.unwrap();
        assert!(connection.replication_started());
    }
}
