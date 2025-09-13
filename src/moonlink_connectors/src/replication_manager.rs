use crate::pg_replicate::table::SrcTableId;
use crate::rest_ingest::event_request::EventRequest;
use crate::ReplicationConnection;
use crate::{Error, Result};
use moonlink::{
    MooncakeTableId, MoonlinkTableConfig, ObjectStorageCache, ReadStateManager, TableEventManager,
};
use moonlink::{ReadStateFilepathRemap, TableStatusReader};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use tokio::task::JoinHandle;
use tracing::debug;

pub const REST_API_URI: &str = "rest://api";

/// Manage replication sources keyed by their connection URI.
///
/// This struct abstracts the lifecycle of `MoonlinkPostgresSource` and
/// provides a single entry point to add new tables to a running
/// replication. A new replication will automatically be started when a
/// table is added for a URI that is not currently being replicated.
pub struct ReplicationManager {
    /// Maps from uri to replication connection.
    connections: HashMap<String, ReplicationConnection>,
    /// Maps from mooncake table id to (uri, source table id).
    table_info: HashMap<MooncakeTableId, (String, SrcTableId)>,
    /// Base directory for mooncake tables.
    table_base_path: String,
    /// Object storage cache.
    object_storage_cache: ObjectStorageCache,
    /// Background shutdown handles.
    shutdown_handles: Vec<JoinHandle<Result<()>>>,
}

impl ReplicationManager {
    pub fn new(table_base_path: String, object_storage_cache: ObjectStorageCache) -> Self {
        Self {
            connections: HashMap::new(),
            table_info: HashMap::new(),
            table_base_path,
            object_storage_cache,
            shutdown_handles: Vec::new(),
        }
    }

    pub async fn get_or_create_connection(
        &mut self,
        src_uri: &str,
    ) -> Result<&mut ReplicationConnection> {
        let replication_connection = match self.connections.entry(src_uri.to_string()) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                debug!(%src_uri, "creating replication connection");

                tokio::fs::create_dir_all(&self.table_base_path).await?;
                let base_path = tokio::fs::canonicalize(&self.table_base_path).await?;
                let replication_connection = ReplicationConnection::new(
                    src_uri.to_string(),
                    base_path.to_str().unwrap().to_string(),
                    self.object_storage_cache.clone(),
                )
                .await?;
                entry.insert(replication_connection)
            }
        };

        Ok(replication_connection)
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
        mooncake_table_id: MooncakeTableId,
        table_name: &str,
        moonlink_table_config: &mut MoonlinkTableConfig,
        read_state_filepath_remap: ReadStateFilepathRemap,
        is_recovery: bool,
    ) -> Result<()> {
        debug!(%src_uri, table_name, "adding table through manager");

        // Error handling: don't allow duplicate mooncake table id be registered.
        if self.table_info.contains_key(&mooncake_table_id) {
            return Err(Error::repl_duplicate_table(mooncake_table_id.to_string()));
        }

        let replication_connection = self.get_or_create_connection(src_uri).await?;
        if !replication_connection.replication_started() {
            replication_connection.start_replication().await?;
        }

        let src_table_id = replication_connection
            .add_table_replication(
                table_name,
                &mooncake_table_id,
                moonlink_table_config,
                read_state_filepath_remap,
                is_recovery,
            )
            .await?;

        assert!(self
            .table_info
            .insert(
                mooncake_table_id.clone(),
                (src_uri.to_string(), src_table_id)
            )
            .is_none());

        debug!(src_table_id, "table added through manager");

        Ok(())
    }

    /// Add a table for REST API ingestion from the given REST API URI.
    ///
    /// The REST API connection must already exist - this will fail if it doesn't.
    ///
    /// # Arguments
    ///
    /// * src_uri: should be a REST API URL
    /// * arrow_schema: Arrow schema for the table
    /// * flush_lsn: only assigned when recovery, which indicates the iceberg persistence LSN; otherwise it's a fresh table.
    #[allow(clippy::too_many_arguments)]
    pub async fn add_rest_table(
        &mut self,
        src_uri: &str,
        mooncake_table_id: MooncakeTableId,
        src_table_name: &str,
        arrow_schema: arrow_schema::Schema,
        moonlink_table_config: MoonlinkTableConfig,
        read_state_filepath_remap: ReadStateFilepathRemap,
        flush_lsn: Option<u64>,
    ) -> Result<()> {
        debug!(%src_uri, src_table_name, "adding REST API table through manager");

        // Fail if REST API connection doesn't exist
        if !self.connections.contains_key(src_uri) {
            return Err(crate::Error::rest_api(
                format!("REST API connection '{src_uri}' not found. Initialize REST API first."),
                None,
            ));
        }

        let replication_connection = self.connections.get_mut(src_uri).unwrap();

        let src_table_id = replication_connection
            .add_table_api(
                src_table_name,
                &mooncake_table_id,
                arrow_schema,
                moonlink_table_config,
                read_state_filepath_remap,
                flush_lsn,
            )
            .await?;
        match self.table_info.entry(mooncake_table_id) {
            Entry::Vacant(entry) => {
                entry.insert((src_uri.to_string(), src_table_id));
            }
            Entry::Occupied(_) => {
                return Err(Error::rest_duplicate_table(src_table_id));
            }
        }

        debug!(src_table_id, "REST API table added through manager");

        Ok(())
    }

    /// Initialize event API connection for data ingestion.
    /// Returns the event request sender channel for the API to use.
    pub async fn initialize_event_api_for_once(
        &mut self,
        base_path: &str,
    ) -> Result<tokio::sync::mpsc::Sender<EventRequest>> {
        if self.connections.contains_key(REST_API_URI) {
            return Ok(self
                .connections
                .get(REST_API_URI)
                .as_ref()
                .unwrap()
                .get_rest_request_sender());
        }

        // Create the directory that will hold all tables
        tokio::fs::create_dir_all(base_path).await?;
        let base_path = tokio::fs::canonicalize(base_path).await?;

        // Create event API connection
        let replication_connection = crate::ReplicationConnection::new(
            REST_API_URI.to_string(),
            base_path.to_str().unwrap().to_string(),
            self.object_storage_cache.clone(),
        )
        .await?;

        // Get the sender before inserting the connection
        let rest_sender = replication_connection.get_rest_request_sender();
        // Insert the connection
        self.connections
            .insert(REST_API_URI.to_string(), replication_connection);

        // Start the REST API replication
        self.start_replication(REST_API_URI).await?;

        Ok(rest_sender)
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
    pub async fn drop_table(&mut self, mooncake_table_id: &MooncakeTableId) -> Result<bool> {
        let (table_uri, src_table_id) = match self.table_info.remove(mooncake_table_id) {
            Some(info) => info.clone(),
            None => {
                debug!("attempted to drop table that is not tracked by moonlink - table may already be dropped");
                return Ok(false);
            }
        };
        debug!(src_table_id, %table_uri, "dropping table through manager");
        let repl_conn = self.connections.get_mut(&table_uri).unwrap();
        repl_conn
            .drop_table(mooncake_table_id, src_table_id)
            .await?;
        if repl_conn.table_count() == 0 && table_uri != REST_API_URI {
            self.shutdown_connection(&table_uri, /*postgres_drop_all=*/ true);
        }

        debug!(src_table_id, "table dropped through manager");
        Ok(true)
    }

    pub fn get_table_reader(
        &self,
        mooncake_table_id: &MooncakeTableId,
    ) -> Result<&ReadStateManager> {
        let (src_table_id, connection) = self.get_replication_connection(mooncake_table_id)?;
        Ok(connection.get_table_reader(mooncake_table_id, src_table_id))
    }

    pub fn get_table_state_reader(
        &self,
        mooncake_table_id: &MooncakeTableId,
    ) -> Result<&TableStatusReader> {
        let (src_table_id, connection) = self.get_replication_connection(mooncake_table_id)?;
        Ok(connection.get_table_status_reader(mooncake_table_id, src_table_id))
    }

    /// Return mapping from mooncake table id to its table status readers.
    pub fn get_table_status_readers(&self) -> HashMap<MooncakeTableId, &TableStatusReader> {
        let mut table_state_readers = HashMap::with_capacity(self.connections.len());
        for (_, (src_uri, _)) in self.table_info.iter() {
            let cur_repl_conn = self.connections.get(src_uri).unwrap_or_else(|| {
                panic!("replication connection with uri {src_uri} should exist.")
            });

            let table_status_readers = cur_repl_conn.get_table_status_readers();
            for (cur_mooncake_table_id, cur_table_status_reader) in table_status_readers.into_iter()
            {
                // Multiple mooncake tables could reference to one single replication connection, so duplicate key expected.
                table_state_readers.insert(cur_mooncake_table_id, cur_table_status_reader);
            }
        }
        table_state_readers
    }

    pub fn get_table_event_manager(
        &mut self,
        mooncake_table_id: &MooncakeTableId,
    ) -> Result<&mut TableEventManager> {
        let (uri, src_table_id) = self
            .table_info
            .get(mooncake_table_id)
            .ok_or_else(|| Error::table_not_found(mooncake_table_id.to_string()))?;
        let connection = self
            .connections
            .get_mut(uri)
            // Directly panic: table connection uri existence here is an invariant.
            .unwrap_or_else(|| panic!("connection {uri} not found"));
        Ok(connection.get_table_event_manager(mooncake_table_id, *src_table_id))
    }

    /// Gracefully shutdown a replication connection by its URI.
    /// If postgres drop all is false, then we will not drop the PostgreSQL publication and replication slot,
    /// which allows for recovery from the PostgreSQL replication slot.
    pub fn shutdown_connection(&mut self, uri: &str, postgres_drop_all: bool) {
        // Clean up completed shutdown handles first
        self.cleanup_completed_shutdowns();

        if let Some(conn) = self.connections.remove(uri) {
            let shutdown_handle = conn.shutdown(postgres_drop_all);
            self.shutdown_handles.push(shutdown_handle);
            self.table_info.retain(|_, (u, _)| u != uri);
        }
    }

    /// Get replication connection by mooncake table id.
    fn get_replication_connection(
        &self,
        mooncake_table_id: &MooncakeTableId,
    ) -> Result<(SrcTableId, &ReplicationConnection)> {
        let (uri, src_table_id) = self
            .table_info
            .get(mooncake_table_id)
            .ok_or_else(|| Error::table_not_found(mooncake_table_id.to_string()))?;
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
