use crate::pg_replicate::table::SrcTableId;
use crate::ReplicationConnection;
use crate::{Error, Result};
use moonlink::TableStatusReader;
use moonlink::{MoonlinkTableConfig, ObjectStorageCache, ReadStateManager, TableEventManager};
use std::collections::HashMap;
use std::hash::Hash;
use tokio::task::JoinHandle;
use tracing::debug;

/// Manage replication sources keyed by their connection URI.
///
/// This struct abstracts the lifecycle of `MoonlinkPostgresSource` and
/// provides a single entry point to add new tables to a running
/// replication. A new replication will automatically be started when a
/// table is added for a URI that is not currently being replicated.
pub struct ReplicationManager<T: Clone + Eq + Hash + std::fmt::Display> {
    /// Maps from uri to replication connection.
    connections: HashMap<String, ReplicationConnection>,
    /// Maps from mooncake table id to (uri, source table id).
    table_info: HashMap<T, (String, SrcTableId)>,
    /// Base directory for mooncake tables.
    table_base_path: String,
    /// Object storage cache.
    object_storage_cache: ObjectStorageCache,
    /// Background shutdown handles.
    shutdown_handles: Vec<JoinHandle<Result<()>>>,
}

impl<T: Clone + Eq + Hash + std::fmt::Display> ReplicationManager<T> {
    pub fn new(table_base_path: String, object_storage_cache: ObjectStorageCache) -> Self {
        Self {
            connections: HashMap::new(),
            table_info: HashMap::new(),
            table_base_path,
            object_storage_cache,
            shutdown_handles: Vec::new(),
        }
    }

    /// Add a table to be replicated from the given `uri`.
    ///
    /// If replication for this `uri` is not yet running a new replication
    /// source will be created.
    ///
    /// # Arguments
    ///
    /// * secret_entry: secret necessary to access object storage, use local filesystem if not assigned.
    #[allow(clippy::too_many_arguments)]
    pub async fn add_table(
        &mut self,
        src_uri: &str,
        mooncake_table_id: T,
        table_id: u32,
        table_name: &str,
        moonlink_table_config: MoonlinkTableConfig,
        is_recovery: bool,
    ) -> Result<()> {
        debug!(%src_uri, table_name, "adding table through manager");
        if !self.connections.contains_key(src_uri) {
            debug!(%src_uri, "creating replication connection");
            // Lazily create the directory that will hold all tables.
            // This will not overwrite any existing directory.
            tokio::fs::create_dir_all(&self.table_base_path).await?;
            let base_path = tokio::fs::canonicalize(&self.table_base_path).await?;
            let replication_connection = ReplicationConnection::new(
                src_uri.to_string(),
                base_path.to_str().unwrap().to_string(),
                self.object_storage_cache.clone(),
            )
            .await?;
            self.connections
                .insert(src_uri.to_string(), replication_connection);
        }
        let replication_connection = self.connections.get_mut(src_uri).unwrap();

        let src_table_id = replication_connection
            .add_table(
                table_name,
                &mooncake_table_id,
                table_id,
                moonlink_table_config,
                is_recovery,
            )
            .await?;
        self.table_info
            .insert(mooncake_table_id, (src_uri.to_string(), src_table_id));

        debug!(src_table_id, "table added through manager");

        Ok(())
    }

    pub async fn start_replication(&mut self, src_uri: &str) -> Result<()> {
        assert!(self.connections.contains_key(src_uri));

        let connection = self.connections.get_mut(src_uri).unwrap();
        if !connection.replication_started() {
            connection.start_replication().await?;
        }
        Ok(())
    }

    /// Drop table specified by the given table id.
    /// If the table is not tracked, logs a message and returns successfully.
    /// Return whether the table is tracked by moonlink.
    pub async fn drop_table(&mut self, mooncake_table_id: T) -> Result<bool> {
        let (table_uri, src_table_id) = match self.table_info.get(&mooncake_table_id) {
            Some(info) => info.clone(),
            None => {
                debug!("attempted to drop table that is not tracked by moonlink - table may already be dropped");
                return Ok(false);
            }
        };
        debug!(src_table_id, %table_uri, "dropping table through manager");
        let repl_conn = self.connections.get_mut(&table_uri).unwrap();
        repl_conn.drop_table(src_table_id).await?;
        if repl_conn.table_count() == 0 {
            self.shutdown_connection(&table_uri);
        }

        debug!(src_table_id, "table dropped through manager");
        Ok(true)
    }

    pub fn get_table_reader(&self, mooncake_table_id: &T) -> Result<&ReadStateManager> {
        let (src_table_id, connection) = self.get_replication_connection(mooncake_table_id)?;
        Ok(connection.get_table_reader(src_table_id))
    }

    pub fn get_table_state_reader(&self, mooncake_table_id: &T) -> Result<&TableStatusReader> {
        let (src_table_id, connection) = self.get_replication_connection(mooncake_table_id)?;
        Ok(connection.get_table_status_reader(src_table_id))
    }

    /// Return mapping from mooncake table id to its table status readers.
    pub fn get_table_status_readers(&self) -> HashMap<T, Vec<&TableStatusReader>> {
        let mut table_state_readers = HashMap::with_capacity(self.connections.len());
        for (mooncake_table_id, (uri, _)) in self.table_info.iter() {
            let cur_repl_conn = self
                .connections
                .get(uri)
                .unwrap_or_else(|| panic!("replication connection with uri {uri} should exist."));
            table_state_readers.insert(
                mooncake_table_id.clone(),
                cur_repl_conn.get_table_status_readers(),
            );
        }
        table_state_readers
    }

    pub fn get_table_event_manager(
        &mut self,
        mooncake_table_id: &T,
    ) -> Result<&mut TableEventManager> {
        let (uri, src_table_id) = self
            .table_info
            .get(mooncake_table_id)
            .ok_or_else(|| Error::TableNotFound(mooncake_table_id.to_string()))?;
        let connection = self
            .connections
            .get_mut(uri)
            // Directly panic: table connection uri existence here is an invariant.
            .unwrap_or_else(|| panic!("connection {uri} not found"));
        Ok(connection.get_table_event_manager(*src_table_id))
    }

    /// Gracefully shutdown a replication connection by its URI.
    pub fn shutdown_connection(&mut self, uri: &str) {
        // Clean up completed shutdown handles first
        self.cleanup_completed_shutdowns();

        if let Some(conn) = self.connections.remove(uri) {
            let shutdown_handle = conn.shutdown();
            self.shutdown_handles.push(shutdown_handle);
            self.table_info.retain(|_, (u, _)| u != uri);
        }
    }

    /// Get replication connection by mooncake table id.
    fn get_replication_connection(
        &self,
        mooncake_table_id: &T,
    ) -> Result<(SrcTableId, &ReplicationConnection)> {
        let (uri, src_table_id) = self
            .table_info
            .get(mooncake_table_id)
            .ok_or_else(|| Error::TableNotFound(mooncake_table_id.to_string()))?;
        let connection = self
            .connections
            .get(uri)
            // Directly panic: table connection uri existence here is an invariant.
            .unwrap_or_else(|| panic!("connection {uri} not found"));
        Ok((*src_table_id, connection))
    }

    /// Clean up completed shutdown handles.
    fn cleanup_completed_shutdowns(&mut self) {
        self.shutdown_handles.retain(|handle| !handle.is_finished());
    }
}
