mod error;
pub mod file_utils;
mod logging;
pub mod mooncake_table_id;
mod recovery_utils;
pub mod table_config;
pub mod table_status;

use arrow_schema::Schema;
pub use error::{Error, Result};
use mooncake_table_id::MooncakeTableId;
pub use moonlink::ReadState;
use moonlink::{ReadStateFilepathRemap, TableEventManager};
use moonlink_connectors::ReplicationManager;
pub use moonlink_connectors::{
    rest_ingest::rest_source::{EventOperation, EventRequest},
    REST_API_URI,
};
use moonlink_metadata_store::base_metadata_store::MetadataStoreTrait;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::recovery_utils::BackendAttributes;
use crate::table_config::TableConfig;
use crate::table_status::TableStatus;

pub struct MoonlinkBackend {
    // Base directory for all tables.
    base_path: String,
    // Functor used to remap local filepath within [`ReadState`] to data server URI if specified and if possible, so table access is routed to data server.
    read_state_filepath_remap: ReadStateFilepathRemap,
    // Directory used to store union read temporary files.
    temp_files_dir: String,
    // Metadata storage accessor.
    metadata_store_accessor: Box<dyn MetadataStoreTrait>,

    replication_manager: RwLock<ReplicationManager<MooncakeTableId>>,

    event_api_sender: Option<tokio::sync::mpsc::Sender<EventRequest>>,
}

impl MoonlinkBackend {
    pub async fn new(
        base_path: String,
        data_server_uri: Option<String>,
        metadata_store_accessor: Box<dyn MetadataStoreTrait>,
    ) -> Result<Self> {
        logging::init_logging();

        // Create local filepath remap logic, so IO requests could be routed to data server.
        let base_path_arc = Arc::new(base_path.clone());
        let data_server_uri_arc = Arc::new(data_server_uri.clone());

        let read_state_filepath_remap: Arc<dyn Fn(String) -> String + Send + Sync> = {
            let base_path_arc = Arc::clone(&base_path_arc);
            let data_server_uri_arc = Arc::clone(&data_server_uri_arc);
            Arc::new(move |local_filepath: String| {
                if let Some(ref data_server_uri) = *data_server_uri_arc {
                    if let Some(stripped) = local_filepath.strip_prefix(&*base_path_arc) {
                        return format!(
                            "{}/{}",
                            data_server_uri.trim_end_matches('/'),
                            stripped.trim_start_matches('/')
                        );
                    }
                }
                local_filepath
            })
        };

        // Canonicalize moonlink backend directory, so all paths stored are of absolute path.
        tokio::fs::create_dir_all(&base_path).await?;
        let base_path = tokio::fs::canonicalize(base_path).await?;
        let base_path_str = base_path.to_str().unwrap();

        // Re-create directory for temporary files directory and read cache files directory under base directory.
        let temp_files_dir = file_utils::get_temp_file_directory_under_base(base_path_str);
        let read_cache_files_dir = file_utils::get_cache_directory_under_base(base_path_str);
        file_utils::recreate_directory(temp_files_dir.to_str().unwrap()).unwrap();
        file_utils::recreate_directory(read_cache_files_dir.to_str().unwrap()).unwrap();

        let mut replication_manager = ReplicationManager::new(
            base_path_str.to_string(),
            file_utils::create_default_object_storage_cache(read_cache_files_dir),
        );

        let backend_attributes = BackendAttributes {
            temp_files_dir: temp_files_dir.to_str().unwrap().to_string(),
        };
        recovery_utils::recover_all_tables(
            backend_attributes,
            &*metadata_store_accessor,
            read_state_filepath_remap.clone(),
            &mut replication_manager,
        )
        .await?;

        Ok(Self {
            base_path: base_path_str.to_string(),
            read_state_filepath_remap,
            temp_files_dir: temp_files_dir.to_str().unwrap().to_string(),
            replication_manager: RwLock::new(replication_manager),
            metadata_store_accessor,
            event_api_sender: None,
        })
    }

    /// Create an iceberg snapshot with the given LSN, return when the a snapshot is successfully created.
    /// If the requested database or table doesn't exist, return [`TableNotFound`] error.
    pub async fn create_snapshot(&self, schema: String, table: String, lsn: u64) -> Result<()> {
        let rx = {
            let mut manager = self.replication_manager.write().await;
            let mooncake_table_id = MooncakeTableId { schema, table };
            let writer = manager.get_table_event_manager(&mooncake_table_id)?;
            writer.initiate_snapshot(lsn).await
        };
        TableEventManager::synchronize_force_snapshot_request(rx, lsn).await?;
        Ok(())
    }

    /// Create a table in the database.
    ///
    /// # Arguments
    ///
    /// * database_id: database id of the table, which must exist.
    /// * table_id: table id assigned to this table.
    /// * src_table_name: Table name at the data source
    /// * src_uri: URI to the data source
    /// * table_config: json serialized table configuration.
    pub async fn create_table(
        &self,
        schema: String,
        table: String,
        src_table_name: String,
        src_uri: String,
        table_config: String,
        input_schema: Option<Schema>,
    ) -> Result<()> {
        let mooncake_table_id = MooncakeTableId {
            schema: schema.clone(),
            table: table.clone(),
        };

        // Add mooncake table to replication, and create corresponding mooncake table.
        let table_config = TableConfig::from_json_or_default(&table_config, &self.base_path)?;
        let moonlink_table_config = table_config
            .take_as_moonlink_config(self.temp_files_dir.clone(), mooncake_table_id.to_string());
        {
            let mut manager = self.replication_manager.write().await;
            if src_uri == REST_API_URI {
                manager
                    .add_rest_table(
                        &src_uri,
                        mooncake_table_id,
                        &src_table_name,
                        input_schema.expect("arrow_schema is required for REST API"),
                        moonlink_table_config.clone(),
                        self.read_state_filepath_remap.clone(),
                        /*is_recovery=*/ false,
                    )
                    .await?;
            } else {
                manager
                    .add_table(
                        &src_uri,
                        mooncake_table_id,
                        &src_table_name,
                        moonlink_table_config.clone(),
                        self.read_state_filepath_remap.clone(),
                        /*is_recovery=*/ false,
                    )
                    .await?;
                manager.start_replication(&src_uri).await?;
            }
        };

        // Create metadata store entry.
        self.metadata_store_accessor
            .store_table_metadata(
                &schema,
                &table,
                &src_table_name,
                &src_uri,
                moonlink_table_config,
            )
            .await?;

        Ok(())
    }

    pub async fn drop_table(&self, schema: String, table: String) {
        let mooncake_table_id = MooncakeTableId { schema, table };

        let table_exists = {
            let mut manager = self.replication_manager.write().await;
            manager.drop_table(&mooncake_table_id).await.unwrap()
        };
        if !table_exists {
            return;
        }

        self.metadata_store_accessor
            .delete_table_metadata(&mooncake_table_id.schema, &mooncake_table_id.table)
            .await
            .unwrap()
    }

    /// Get the base directory for all mooncake tables.
    pub fn get_base_path(&self) -> String {
        self.base_path.clone()
    }

    /// Get the current mooncake table schema.
    /// If the requested database or table doesn't exist, return [`TableNotFound`] error.
    pub async fn get_table_schema(&self, schema: String, table: String) -> Result<Arc<Schema>> {
        let table_schema = {
            let manager = self.replication_manager.read().await;
            let mooncake_table_id = MooncakeTableId { schema, table };
            let table_state_reader = manager.get_table_state_reader(&mooncake_table_id)?;
            table_state_reader.get_current_table_schema().await?
        };
        Ok(table_schema)
    }

    /// List all tables at moonlink backend, and return their states.
    pub async fn list_tables(&self) -> Result<Vec<TableStatus>> {
        let mut table_statuses = vec![];
        let manager = self.replication_manager.read().await;
        let table_state_readers = manager.get_table_status_readers();
        for (mooncake_table_id, cur_table_state_readers) in table_state_readers.into_iter() {
            for cur_reader in cur_table_state_readers.iter() {
                let table_snapshot_status = cur_reader.get_current_table_state().await?;
                let table_status = TableStatus {
                    schema: mooncake_table_id.schema.clone(),
                    table: mooncake_table_id.table.clone(),
                    commit_lsn: table_snapshot_status.commit_lsn,
                    flush_lsn: table_snapshot_status.flush_lsn,
                    iceberg_warehouse_location: table_snapshot_status.iceberg_warehouse_location,
                };
                table_statuses.push(table_status);
            }
        }
        Ok(table_statuses)
    }

    /// Perform a table maintenance operation based on requested mode, block wait until maintenance results have been persisted.
    /// Notice, it's only exposed for debugging, testing and admin usage.
    ///
    /// There're currently three modes supported:
    /// - "data": perform a data compaction, only data files smaller than a threshold, or with too many deleted rows will be compacted.
    /// - "index": perform an index merge operation, only index files smaller than a threshold, or with too many deleted rows will be merged.    
    /// - "full": perform a full compaction, which merges all data files and all index files, whatever file size they are of.
    pub async fn optimize_table(&self, schema: String, table: String, mode: &str) -> Result<()> {
        let mut rx = {
            let mut manager = self.replication_manager.write().await;
            let mooncake_table_id = MooncakeTableId { schema, table };
            let writer = manager.get_table_event_manager(&mooncake_table_id)?;

            match mode {
                "data" => writer.initiate_data_compaction().await,
                "index" => writer.initiate_index_merge().await,
                "full" => writer.initiate_full_compaction().await,
                _ => {
                    return Err(Error::InvalidArgumentError(format!(
                        "Unrecognizable table optimization mode `{mode}`, expected one of `data`, `index`, or `full`"
                    )))
                }
            }
        };

        rx.recv().await.unwrap().unwrap();
        Ok(())
    }

    /// If the requested database or table doesn't exist, return [`TableNotFound`] error.
    pub async fn scan_table(
        &self,
        schema: String,
        table: String,
        lsn: Option<u64>,
    ) -> Result<Arc<ReadState>> {
        let read_state = {
            let manager = self.replication_manager.read().await;
            let mooncake_table_id = MooncakeTableId { schema, table };
            let table_reader = manager.get_table_reader(&mooncake_table_id)?;
            table_reader.try_read(lsn).await?
        };

        Ok(read_state.clone())
    }

    /// Wait for the WAL flush LSN to reach the requested LSN. Note that WAL flush LSN will update
    /// up till the latest commit that has been persisted in to the WAL.
    pub async fn wait_for_wal_flush(&self, schema: String, table: String, lsn: u64) -> Result<()> {
        let mut manager = self.replication_manager.write().await;
        let mooncake_table_id = MooncakeTableId { schema, table };
        let writer = manager.get_table_event_manager(&mooncake_table_id)?;

        // Wait for WAL flush LSN to reach the requested LSN
        let mut rx = writer.subscribe_wal_flush_lsn();
        while *rx.borrow() < lsn {
            rx.changed().await.unwrap();
        }
        Ok(())
    }

    /// Gracefully shutdown a replication connection identified by its URI.
    /// If postgres drop all is false, then we will not drop the PostgreSQL publication and replication slot,
    /// which allows for recovery from the PostgreSQL replication slot.
    pub async fn shutdown_connection(&self, uri: &str, postgres_drop_all: bool) {
        let mut manager = self.replication_manager.write().await;
        manager.shutdown_connection(uri, postgres_drop_all);
    }

    /// Initialize event API connection for data ingestion.
    /// This should be called during service startup to ensure event API is ready.
    pub async fn initialize_event_api(&mut self) -> Result<()> {
        let event_api_sender = {
            let mut manager = self.replication_manager.write().await;
            manager.initialize_event_api(&self.base_path).await?
        };

        self.event_api_sender = Some(event_api_sender);
        Ok(())
    }

    pub async fn send_event_request(&self, request: EventRequest) -> Result<()> {
        self.event_api_sender
            .as_ref()
            .expect("event api sender not initialized")
            .send(request)
            .await
            .map_err(|e| Error::MoonlinkConnectorError { source: e.into() })
    }
}
