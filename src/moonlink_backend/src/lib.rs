mod error;
pub mod file_utils;
mod logging;
mod parquet_utils;
mod recovery_utils;
pub mod table_config;
pub mod table_status;

use arrow_schema::Schema;
pub use error::{Error, Result};
use futures::{stream, StreamExt, TryStreamExt};
use moonlink::MooncakeTableId;
pub use moonlink::ReadState;
use moonlink::{ReadStateFilepathRemap, TableEventManager};
pub use moonlink_connectors::rest_ingest::event_request::{
    EventRequest, FileEventOperation, FileEventRequest, RowEventOperation, RowEventRequest,
};
pub use moonlink_connectors::rest_ingest::rest_event::RestEvent;
pub use moonlink_connectors::rest_ingest::rest_source::RestSource;
use moonlink_connectors::ReplicationManager;
pub use moonlink_connectors::REST_API_URI;
use moonlink_metadata_store::base_metadata_store::MetadataStoreTrait;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::recovery_utils::BackendAttributes;
use crate::table_config::TableConfig;
use crate::table_status::TableStatus;

/// Type alias for filepath remap function, which remaps http URI to local filepath if possible.
type HttpFilepathRemap = std::sync::Arc<dyn Fn(String) -> String + Send + Sync>;

/// Max concurrency to fetch parquet metadata in parallel.
const DEFAULT_PARQUET_METADATA_FETCH_PARALLELISM: usize = 128;

pub struct MoonlinkBackend {
    // Base directory for all tables.
    base_path: String,
    // Functor used to remap local filepath within [`ReadState`] to data server URI if specified and if possible, so table access is routed to data server.
    read_state_filepath_remap: ReadStateFilepathRemap,
    // Functor used to remap URI for data files back to local filepath if specified and if applicable.
    http_filepath_remap: HttpFilepathRemap,
    // Directory used to store union read temporary files.
    temp_files_dir: String,
    // Metadata storage accessor.
    metadata_store_accessor: Box<dyn MetadataStoreTrait>,

    replication_manager: RwLock<ReplicationManager>,

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

        let http_filepath_remap: Arc<dyn Fn(String) -> String + Send + Sync> = {
            let base_path_arc = Arc::clone(&base_path_arc);
            let data_server_uri_arc = Arc::clone(&data_server_uri_arc);
            Arc::new(move |http_filepath: String| {
                if let Some(ref data_server_uri) = *data_server_uri_arc {
                    // Normalize to avoid double/missing slashes.
                    let uri_prefix = data_server_uri.trim_end_matches('/');
                    if let Some(stripped) = http_filepath.strip_prefix(uri_prefix) {
                        // Strip the URI prefix and any leading slash in the remainder.
                        let stripped = stripped.trim_start_matches('/');
                        let base = base_path_arc.trim_end_matches('/');
                        return format!("{base}/{stripped}");
                    }
                }
                http_filepath
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
            base_path: base_path.to_str().unwrap().to_string(),
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
            http_filepath_remap,
            temp_files_dir: temp_files_dir.to_str().unwrap().to_string(),
            replication_manager: RwLock::new(replication_manager),
            metadata_store_accessor,
            event_api_sender: None,
        })
    }

    /// Create an iceberg snapshot with the given LSN, return when the a snapshot is successfully created.
    /// If the requested database or table doesn't exist, return [`TableNotFound`] error.
    pub async fn create_snapshot(&self, database: String, table: String, lsn: u64) -> Result<()> {
        let rx = {
            let mut manager = self.replication_manager.write().await;
            let mooncake_table_id = MooncakeTableId { database, table };
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
        database: String,
        table: String,
        src_table_name: String,
        src_uri: String,
        table_config: String,
        input_schema: Option<Schema>,
    ) -> Result<()> {
        let mooncake_table_id = MooncakeTableId {
            database: database.clone(),
            table: table.clone(),
        };

        // Add mooncake table to replication, and create corresponding mooncake table.
        let table_config = TableConfig::from_json_or_default(&table_config, &self.base_path)?;
        let mut moonlink_table_config =
            table_config.take_as_moonlink_config(self.temp_files_dir.clone(), &mooncake_table_id);
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
                        /*flush_lsn=*/ None,
                    )
                    .await?;
            } else {
                manager
                    .add_table(
                        &src_uri,
                        mooncake_table_id,
                        &src_table_name,
                        &mut moonlink_table_config,
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
                &database,
                &table,
                &src_table_name,
                &src_uri,
                moonlink_table_config,
            )
            .await?;

        Ok(())
    }

    pub async fn drop_table(&self, database: String, table: String) -> Result<()> {
        let mooncake_table_id = MooncakeTableId { database, table };

        let table_exists = {
            let mut manager = self.replication_manager.write().await;
            manager.drop_table(&mooncake_table_id).await?
        };
        if !table_exists {
            return Ok(());
        }

        self.metadata_store_accessor
            .delete_table_metadata(&mooncake_table_id.database, &mooncake_table_id.table)
            .await?;
        Ok(())
    }

    /// Get the base directory for all mooncake tables.
    pub fn get_base_path(&self) -> String {
        self.base_path.clone()
    }

    /// Get the serialized parquet metadata for the requested parquet files.
    /// Serialized results are returned in the same order of given data files.
    ///
    /// TODO(hjiang): Currently it only supports local parquet files.
    pub async fn get_parquet_metadatas(&self, data_files: Vec<String>) -> Result<Vec<Vec<u8>>> {
        let http_filepath_remap = self.http_filepath_remap.clone();
        let serialized_metadatas: Vec<Vec<u8>> = stream::iter(data_files.into_iter())
            .map(move |cur_data_file| {
                let http_filepath_remap_clone = http_filepath_remap.clone();
                async move {
                    let local_data_filepath = (http_filepath_remap_clone)(cur_data_file);
                    parquet_utils::get_parquet_serialized_metadata(&local_data_filepath).await
                }
            })
            .buffered(DEFAULT_PARQUET_METADATA_FETCH_PARALLELISM)
            .try_collect()
            .await?;
        Ok(serialized_metadatas)
    }

    /// Get the current mooncake table schema.
    /// If the requested database or table doesn't exist, return [`TableNotFound`] error.
    pub async fn get_table_schema(&self, database: String, table: String) -> Result<Arc<Schema>> {
        let table_schema = {
            let manager = self.replication_manager.read().await;
            let mooncake_table_id = MooncakeTableId { database, table };
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
        for (mooncake_table_id, cur_reader) in table_state_readers.into_iter() {
            let table_snapshot_status = cur_reader.get_current_table_state().await?;
            let table_status = TableStatus {
                database: mooncake_table_id.database.clone(),
                table: mooncake_table_id.table.clone(),
                commit_lsn: table_snapshot_status.commit_lsn,
                flush_lsn: table_snapshot_status.flush_lsn,
                cardinality: table_snapshot_status.cardinality,
                iceberg_warehouse_location: table_snapshot_status.iceberg_warehouse_location,
            };
            table_statuses.push(table_status);
        }
        Ok(table_statuses)
    }

    /// Load the provided files directly into mooncake table and iceberg table in batch mode.
    pub async fn load_files(
        &self,
        _database: String,
        _table: String,
        _files: Vec<String>,
    ) -> Result<()> {
        Ok(())
    }

    /// Perform a table maintenance operation based on requested mode, block wait until maintenance results have been persisted.
    /// Notice, it's only exposed for debugging, testing and admin usage.
    ///
    /// There're currently three modes supported:
    /// - "data": perform a data compaction, only data files smaller than a threshold, or with too many deleted rows will be compacted.
    /// - "index": perform an index merge operation, only index files smaller than a threshold, or with too many deleted rows will be merged.    
    /// - "full": perform a full compaction, which merges all data files and all index files, whatever file size they are of.
    pub async fn optimize_table(&self, database: String, table: String, mode: &str) -> Result<()> {
        let mut rx = {
            let mut manager = self.replication_manager.write().await;
            let mooncake_table_id = MooncakeTableId { database, table };
            let writer = manager.get_table_event_manager(&mooncake_table_id)?;

            match mode {
                "data" => writer.initiate_data_compaction().await,
                "index" => writer.initiate_index_merge().await,
                "full" => writer.initiate_full_compaction().await,
                _ => {
                    return Err(Error::invalid_argument(format!(
                    "Unrecognizable table optimization mode `{mode}`, expected one of `data`, `index`, or `full`"
                    )));
                }
            }
        };

        rx.recv().await.unwrap().unwrap();
        Ok(())
    }

    /// If the requested database or table doesn't exist, return [`TableNotFound`] error.
    pub async fn scan_table(
        &self,
        database: String,
        table: String,
        lsn: Option<u64>,
    ) -> Result<Arc<ReadState>> {
        let read_state = {
            let manager = self.replication_manager.read().await;
            let mooncake_table_id = MooncakeTableId { database, table };
            let table_reader = manager.get_table_reader(&mooncake_table_id)?;
            table_reader.try_read(lsn).await?
        };

        Ok(read_state.clone())
    }

    /// Wait for the WAL flush LSN to reach the requested LSN. Note that WAL flush LSN will update
    /// up till the latest commit that has been persisted in to the WAL.
    #[cfg(feature = "test-utils")]
    pub async fn wait_for_wal_flush(
        &self,
        database: String,
        table: String,
        lsn: u64,
    ) -> Result<()> {
        let mut manager = self.replication_manager.write().await;
        let mooncake_table_id = MooncakeTableId { database, table };
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
            manager
                .initialize_event_api_for_once(&self.base_path)
                .await?
        };

        self.event_api_sender = Some(event_api_sender);
        Ok(())
    }

    pub async fn send_event_request(&self, request: EventRequest) -> Result<()> {
        self.event_api_sender
            .as_ref()
            .expect("event api sender not initialized")
            .send(request)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::parquet_utils::deserialize_parquet_metadata;
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use moonlink_metadata_store::SqliteMetadataStore;
    use parquet::arrow::arrow_writer::ArrowWriter;
    use std::fs::File as StdFile;
    use std::sync::Arc;
    use tempfile::tempdir;

    /// Test util function to dump local parquet file.
    fn write_parquet_file(schema: Arc<Schema>, batch: RecordBatch, parquet_filepath: &str) {
        let file = StdFile::create(parquet_filepath).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, /*prop=*/ None).unwrap();
        writer.write(&batch).unwrap();
        let _ = writer.close().unwrap();
    }

    #[tokio::test]
    async fn test_parquet_metadata_fetch() {
        // Create backend.
        let tmp_dir = tempdir().unwrap();
        let base_path = tmp_dir.path().to_str().unwrap().to_string();
        let sqlite_metadata_store = SqliteMetadataStore::new_with_directory(&base_path)
            .await
            .unwrap();
        let backend = MoonlinkBackend::new(
            /*base_path=*/ base_path,
            /*data_server_uri=*/ None,
            Box::new(sqlite_metadata_store),
        )
        .await
        .unwrap();

        // Write two parquet files.
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, true)]));
        let data_1 = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            Some(2),
            Some(5),
            None,
        ]));
        let batch_1 = RecordBatch::try_new(schema.clone(), vec![data_1]).unwrap();
        let data_2 = Arc::new(Int32Array::from(vec![Some(0), None]));
        let batch_2 = RecordBatch::try_new(schema.clone(), vec![data_2]).unwrap();

        let parquet_filepath_1 = format!("{}/test_1.parquet", tmp_dir.path().to_str().unwrap());
        let parquet_filepath_2 = format!("{}/test_2.parquet", tmp_dir.path().to_str().unwrap());
        write_parquet_file(schema.clone(), batch_1, &parquet_filepath_1);
        write_parquet_file(schema.clone(), batch_2, &parquet_filepath_2);

        // Get parquet metadata.
        let parquet_metadatas = backend
            .get_parquet_metadatas(vec![parquet_filepath_1.clone(), parquet_filepath_2.clone()])
            .await
            .unwrap();
        // Validate metadatas are returned in the correct order.
        assert_eq!(parquet_metadatas.len(), 2);
        let metadata_1 = deserialize_parquet_metadata(&parquet_metadatas[0][..]);
        assert_eq!(metadata_1.num_rows, 5);
        let metadata_2 = deserialize_parquet_metadata(&parquet_metadatas[1][..]);
        assert_eq!(metadata_2.num_rows, 2);
    }
}
