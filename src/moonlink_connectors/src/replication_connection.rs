use crate::pg_replicate::table::SrcTableId;
use crate::pg_replicate::PostgresConnection;
use crate::Result;
use moonlink::{
    MoonlinkTableConfig, ObjectStorageCache, ReadStateManager, TableEventManager, TableStatusReader,
};

use std::collections::HashMap;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

struct TableState {
    src_table_name: String,
    reader: ReadStateManager,
    event_manager: TableEventManager,
    status_reader: TableStatusReader,
}
/// Manages replication for table(s) within a database.
pub struct ReplicationConnection {
    postgres_connection: PostgresConnection,
    table_base_path: String,
    handle: Option<JoinHandle<Result<()>>>,
    table_states: HashMap<SrcTableId, TableState>,
    /// Object storage cache.
    object_storage_cache: ObjectStorageCache,
}

impl ReplicationConnection {
    pub async fn new(
        uri: String,
        table_base_path: String,
        object_storage_cache: ObjectStorageCache,
    ) -> Result<Self> {
        debug!(%uri, "initializing replication connection");

        let postgres_connection = PostgresConnection::new(uri).await?;

        Ok(Self {
            postgres_connection,
            table_base_path,
            handle: None,
            table_states: HashMap::new(),
            object_storage_cache,
        })
    }

    pub fn replication_started(&self) -> bool {
        self.postgres_connection.replication_started
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

    pub async fn start_replication(&mut self) -> Result<()> {
        debug!("starting replication");

        self.handle = Some(self.postgres_connection.spawn_replication_task().await);

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
        let (src_table_id, table_resources) = self
            .postgres_connection
            .add_table(
                table_name,
                mooncake_table_id,
                table_id,
                moonlink_table_config,
                is_recovery,
                &self.table_base_path,
                self.object_storage_cache.clone(),
            )
            .await?;

        let table_state = TableState {
            src_table_name: table_name.to_string(),
            reader: table_resources.read_state_manager,
            event_manager: table_resources.table_event_manager,
            status_reader: table_resources.table_status_reader,
        };

        self.table_states.insert(src_table_id, table_state);

        Ok(src_table_id)
    }

    /// Remove the given table from connection.
    pub async fn drop_table(&mut self, src_table_id: u32) -> Result<()> {
        debug!(src_table_id, "dropping table");

        // Get table state and remove it from the map
        let table_state = self
            .table_states
            .remove(&src_table_id)
            .expect("table not found");

        let table_name = &table_state.src_table_name;

        // it is important to drop table from table handler first,
        // otherwise table handler will panic when trying to send back notification to sinks
        debug!(src_table_id, "drop table from table handler");
        let mut event_manager = table_state.event_manager;
        event_manager.drop_table().await?;

        // Drop from PostgreSQL replication
        self.postgres_connection
            .drop_table(src_table_id, table_name)
            .await?;

        Ok(())
    }

    pub fn shutdown(mut self) -> JoinHandle<Result<()>> {
        tokio::spawn(async move {
            self.postgres_connection.shutdown().await?;

            if let Some(handle) = self.handle {
                if let Err(e) = handle.await {
                    warn!(error = ?e, "task join error during shutdown");
                }
            }

            debug!("replication connection shutdown complete");
            Ok(())
        })
    }
}
