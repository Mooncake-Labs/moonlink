use crate::errored_tables::{self, ErroredTable};
use crate::pg_replicate::source_fingerprint;
use crate::pg_replicate::table::SrcTableId;
use crate::pg_replicate::{PostgresConnection, SourceIdentity};
use crate::ReplicationConnection;
use crate::{Error, Result};
use moonlink::{
    MooncakeTableId, MoonlinkTableConfig, ObjectStorageCache, ReadStateManager, TableEventManager,
};
use moonlink::{ReadStateFilepathRemap, TableStatusReader};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

/// Manage replication sources, one per source database.
///
/// Connections are keyed by [`SourceIdentity::key`], not by the connection string: each database
/// has a single replication slot and publication, so two spellings of the same database (another
/// user, host alias, options) must share one connection instead of taking the slot over from it.
///
/// This struct abstracts the lifecycle of `MoonlinkPostgresSource` and
/// provides a single entry point to add new tables to a running
/// replication. A new replication will automatically be started when a
/// table is added for a URI that is not currently being replicated.
pub struct ReplicationManager {
    /// Maps from source key to replication connection.
    connections: HashMap<String, ReplicationConnection>,
    /// Maps every connection string seen so far to its source key.
    uri_keys: HashMap<String, String>,
    /// Maps from mooncake table id to (source key, source table id).
    table_info: HashMap<MooncakeTableId, (String, SrcTableId)>,
    /// Recovered mirrors that are not served (errored before, or failed to recover).
    errored_tables: HashMap<MooncakeTableId, ErroredTable>,
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
            uri_keys: HashMap::new(),
            table_info: HashMap::new(),
            errored_tables: HashMap::new(),
            table_base_path,
            object_storage_cache,
            shutdown_handles: Vec::new(),
        }
    }

    /// Resolve a connection string to its source key, contacting the source the first time.
    async fn source_key(&mut self, src_uri: &str) -> Result<String> {
        if let Some(key) = self.uri_keys.get(src_uri) {
            return Ok(key.clone());
        }
        let key = SourceIdentity::identify(src_uri).await?.key();
        self.uri_keys.insert(src_uri.to_string(), key.clone());
        Ok(key)
    }

    pub async fn get_or_create_connection(
        &mut self,
        src_uri: &str,
    ) -> Result<&mut ReplicationConnection> {
        let key = self.source_key(src_uri).await?;
        let replication_connection = match self.connections.entry(key) {
            Entry::Occupied(entry) => {
                if entry.get().uri() != src_uri {
                    info!(
                        "reusing the replication connection already open for this database; \
                         its connection string is used for the new table too"
                    );
                }
                entry.into_mut()
            }
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
        if self.table_info.contains_key(&mooncake_table_id)
            || self.errored_tables.contains_key(&mooncake_table_id)
        {
            return Err(Error::repl_duplicate_table(mooncake_table_id.to_string()));
        }

        let key = self.source_key(src_uri).await?;
        let replication_connection = self.get_or_create_connection(src_uri).await?;
        if replication_connection.uri() != src_uri {
            crate::pg_replicate::check_role_can_read_table(src_uri, table_name).await?;
        }
        if !replication_connection.replication_started() {
            replication_connection.start_replication().await?;
        }

        let added = replication_connection
            .add_table_replication(
                table_name,
                &mooncake_table_id,
                moonlink_table_config,
                read_state_filepath_remap,
                is_recovery,
            )
            .await;
        let src_table_id = match added {
            Ok(src_table_id) => src_table_id,
            Err(e) => {
                // A failed first mirror must not leave a replication slot behind that holds the
                // source's WAL forever.
                if replication_connection.table_count() == 0 && !is_recovery {
                    self.shutdown_connection_by_key(&key, true);
                }
                return Err(e);
            }
        };

        assert!(self
            .table_info
            .insert(mooncake_table_id.clone(), (key, src_table_id))
            .is_none());

        debug!(src_table_id, "table added through manager");

        Ok(())
    }

    /// Message of a mirror persisted as errored by an earlier run, if any.
    pub async fn persisted_errored_message(
        &self,
        mooncake_table_id: &MooncakeTableId,
    ) -> Option<String> {
        errored_tables::read_marker(&self.table_base_path, mooncake_table_id).await
    }

    /// Register a recovered mirror that is not served: every request on it returns `message`, and
    /// dropping it deletes its data. With `persist`, it stays errored across restarts.
    pub async fn add_errored_table(
        &mut self,
        mooncake_table_id: MooncakeTableId,
        errored: ErroredTable,
        persist: bool,
    ) {
        if persist {
            if let Err(e) = errored_tables::write_marker(
                &self.table_base_path,
                &mooncake_table_id,
                &errored.message,
            )
            .await
            {
                warn!(error = %e, table = %mooncake_table_id, "failed to persist errored-table marker");
            }
        }
        self.errored_tables.insert(mooncake_table_id, errored);
    }

    /// After dropping a placeholder: take its table out of the publication, and if no live
    /// mirror of its database remains, drop the database's slot and publication as dropping the
    /// last live mirror would (placeholders never resume, so nothing needs them). Best effort.
    async fn release_errored_source(&mut self, errored: &ErroredTable) {
        let live_connection = match self.source_key(&errored.src_uri).await {
            Ok(key) => self.connections.get_mut(&key),
            Err(e) => {
                warn!(error = ?e, "source of the dropped mirror is unreachable; slot left in place");
                return;
            }
        };
        if let Some(connection) = live_connection {
            if let Err(e) = connection
                .remove_table_from_publication(&errored.src_table_name)
                .await
            {
                warn!(error = ?e, "failed to remove the dropped mirror's table from the publication");
            }
            return;
        }
        let cleanup = async {
            let mut source = PostgresConnection::new(errored.src_uri.clone()).await?;
            source
                .remove_table_from_publication(&errored.src_table_name)
                .await?;
            source.shutdown(/*drop_all=*/ true).await
        };
        if let Err(e) = cleanup.await {
            warn!(error = ?e, "failed to drop the slot and publication of the dropped mirror's source");
        }
    }

    fn check_not_errored(&self, mooncake_table_id: &MooncakeTableId) -> Result<()> {
        match self.errored_tables.get(mooncake_table_id) {
            Some(errored) => Err(Error::table_errored(&errored.message)),
            None => Ok(()),
        }
    }

    pub async fn start_replication(&mut self, src_uri: &str) -> Result<()> {
        let key = self.source_key(src_uri).await?;
        let Some(connection) = self.connections.get_mut(&key) else {
            return Err(Error::table_not_found(src_uri.to_string()));
        };
        if !connection.replication_started() {
            connection.start_replication().await?;
        }
        Ok(())
    }

    /// Drop table specified by the given table id.
    /// If the table is not tracked, logs a message and returns successfully.
    /// Return whether the table is tracked by moonlink.
    pub async fn drop_table(&mut self, mooncake_table_id: &MooncakeTableId) -> Result<bool> {
        if let Some(errored) = self.errored_tables.remove(mooncake_table_id) {
            // Best effort: the mirror is gone from moonlink either way.
            let _ = errored_tables::drop_iceberg_table_files(&errored.iceberg_table_config).await;
            errored_tables::remove_marker(&self.table_base_path, mooncake_table_id).await;
            source_fingerprint::remove(&self.table_base_path, mooncake_table_id).await;
            self.release_errored_source(&errored).await;
            return Ok(true);
        }
        let (table_uri, src_table_id) = match self.table_info.get(mooncake_table_id) {
            Some(info) => info.clone(),
            None => {
                debug!("attempted to drop table that is not tracked by moonlink - table may already be dropped");
                return Ok(false);
            }
        };
        debug!(src_table_id, %table_uri, "dropping table through manager");
        let repl_conn = self.connections.get_mut(&table_uri).unwrap();
        match repl_conn.drop_table(mooncake_table_id, src_table_id).await {
            Ok(()) => {
                // Clear manager mapping after successful drop
                self.table_info.remove(mooncake_table_id);
                if repl_conn.table_count() == 0 {
                    self.shutdown_connection_by_key(&table_uri, true);
                }

                debug!(src_table_id, "table dropped through manager");
                Ok(true)
            }
            Err(e) => Err(e),
        }
    }

    pub fn get_table_reader(
        &self,
        mooncake_table_id: &MooncakeTableId,
    ) -> Result<&ReadStateManager> {
        self.check_not_errored(mooncake_table_id)?;
        let (src_table_id, connection) = self.get_replication_connection(mooncake_table_id)?;
        Ok(connection.get_table_reader(mooncake_table_id, src_table_id))
    }

    pub fn get_table_state_reader(
        &self,
        mooncake_table_id: &MooncakeTableId,
    ) -> Result<&TableStatusReader> {
        self.check_not_errored(mooncake_table_id)?;
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
        self.check_not_errored(mooncake_table_id)?;
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
        if let Some(key) = self.uri_keys.get(uri).cloned() {
            self.shutdown_connection_by_key(&key, postgres_drop_all);
        }
    }

    fn shutdown_connection_by_key(&mut self, key: &str, postgres_drop_all: bool) {
        // Clean up completed shutdown handles first
        self.cleanup_completed_shutdowns();

        if let Some(conn) = self.connections.remove(key) {
            let shutdown_handle = conn.shutdown(postgres_drop_all);
            self.shutdown_handles.push(shutdown_handle);
            self.table_info.retain(|_, (k, _)| k != key);
            self.uri_keys.retain(|_, k| k != key);
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
