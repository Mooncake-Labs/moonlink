mod config_utils;
mod error;
pub mod file_utils;
mod parquet_utils;
mod recovery_utils;
pub mod table_config;
pub mod table_status;

use arrow_schema::Schema;
pub use error::{Error, Result};
use futures::{stream, StreamExt, TryStreamExt};
pub use moonlink::ReadState;
use moonlink::{MooncakeTableId, MoonlinkTableConfig};
use moonlink::{ReadStateFilepathRemap, TableEventManager};
use moonlink_connectors::ReplicationManager;
use moonlink_metadata_store::base_metadata_store::MetadataStoreTrait;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

use crate::recovery_utils::BackendAttributes;
use crate::table_status::TableStatus;

/// Type alias for filepath remap function, which remaps http URI to local filepath if possible.
type HttpFilepathRemap = std::sync::Arc<dyn Fn(String) -> String + Send + Sync>;

/// Upper bound for a scan or snapshot request waiting until replication reaches the requested
/// LSN. The pg_mooncake backend issuing the request blocks uninterruptibly (no statement_timeout /
/// cancel) for this long.
const DEFAULT_LSN_WAIT_TIMEOUT: Duration = Duration::from_secs(30);

/// Environment variable overriding [`DEFAULT_LSN_WAIT_TIMEOUT`], in whole seconds.
pub const LSN_WAIT_TIMEOUT_ENV: &str = "MOONLINK_LSN_WAIT_TIMEOUT_SECS";

/// Larger LSN wait timeouts are clamped to this; it also keeps the value far from wrapping.
const MAX_LSN_WAIT_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);

fn clamp_lsn_wait_timeout(timeout: Duration) -> Duration {
    if timeout > MAX_LSN_WAIT_TIMEOUT {
        tracing::warn!(
            ?timeout,
            "LSN wait timeout clamped to {}s",
            MAX_LSN_WAIT_TIMEOUT.as_secs()
        );
        return MAX_LSN_WAIT_TIMEOUT;
    }
    timeout
}

/// The LSN wait timeout for a value of [`LSN_WAIT_TIMEOUT_ENV`]; unset or invalid gives the default.
fn parse_lsn_wait_timeout(value: Option<&str>) -> Duration {
    let Some(value) = value else {
        return DEFAULT_LSN_WAIT_TIMEOUT;
    };
    match value.trim().parse::<u64>() {
        Ok(secs) if secs > 0 => clamp_lsn_wait_timeout(Duration::from_secs(secs)),
        _ => {
            tracing::warn!(
                value,
                "invalid {LSN_WAIT_TIMEOUT_ENV} (expected a positive number of seconds), using {}s",
                DEFAULT_LSN_WAIT_TIMEOUT.as_secs()
            );
            DEFAULT_LSN_WAIT_TIMEOUT
        }
    }
}

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

    /// See [`DEFAULT_LSN_WAIT_TIMEOUT`].
    lsn_wait_timeout_ms: AtomicU64,
}

impl MoonlinkBackend {
    pub async fn new(
        base_path: String,
        data_server_uri: Option<String>,
        metadata_store_accessor: Box<dyn MetadataStoreTrait>,
    ) -> Result<Self> {
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
        tokio::fs::create_dir_all(&base_path).await.map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!("Failed to create directory {base_path:?}"),
            )
        })?;
        let base_path = tokio::fs::canonicalize(base_path).await?;
        let base_path_str = base_path.to_str().unwrap();

        // Re-create directory for temporary files directory and read cache files directory under base directory.
        let temp_files_dir = file_utils::get_temp_file_directory_under_base(base_path_str);
        let read_cache_files_dir = file_utils::get_cache_directory_under_base(base_path_str);
        file_utils::recreate_directory(temp_files_dir.to_str().unwrap()).unwrap();
        file_utils::recreate_directory(read_cache_files_dir.to_str().unwrap()).unwrap();

        let object_storage_cache =
            file_utils::create_default_object_storage_cache(read_cache_files_dir)?;
        let mut replication_manager =
            ReplicationManager::new(base_path_str.to_string(), object_storage_cache);

        let backend_attributes = BackendAttributes {
            temp_files_dir: temp_files_dir.to_str().unwrap().to_string(),
            base_path: base_path_str.to_string(),
        };
        let pending_recoveries = recovery_utils::recover_all_tables(
            backend_attributes,
            &*metadata_store_accessor,
            read_state_filepath_remap.clone(),
            &mut replication_manager,
        )
        .await?;
        let replication_manager = Arc::new(RwLock::new(replication_manager));
        recovery_utils::watch_placeholders(Arc::downgrade(&replication_manager));
        if !pending_recoveries.is_empty() {
            recovery_utils::retry_pending_recoveries(
                Arc::downgrade(&replication_manager),
                pending_recoveries,
            );
        }

        Ok(Self {
            base_path: base_path_str.to_string(),
            read_state_filepath_remap,
            http_filepath_remap,
            temp_files_dir: temp_files_dir.to_str().unwrap().to_string(),
            replication_manager,
            metadata_store_accessor,
            lsn_wait_timeout_ms: AtomicU64::new(
                parse_lsn_wait_timeout(std::env::var(LSN_WAIT_TIMEOUT_ENV).ok().as_deref())
                    .as_millis() as u64,
            ),
        })
    }

    /// Override how long [`Self::scan_table`] and [`Self::create_snapshot`] wait for replication
    /// to reach the requested LSN.
    pub fn set_lsn_wait_timeout(&self, timeout: Duration) {
        self.lsn_wait_timeout_ms.store(
            clamp_lsn_wait_timeout(timeout).as_millis() as u64,
            Ordering::Relaxed,
        );
    }

    fn lsn_wait_timeout(&self) -> Duration {
        Duration::from_millis(self.lsn_wait_timeout_ms.load(Ordering::Relaxed))
    }

    /// Create an iceberg snapshot with the given LSN, return when the a snapshot is successfully created.
    /// If the requested database or table doesn't exist, return [`TableNotFound`] error.
    /// Only the wait for replication to reach `lsn` is bounded ([`LSN_WAIT_TIMEOUT_ENV`]).
    pub async fn create_snapshot(&self, database: String, table: String, lsn: u64) -> Result<()> {
        validate_not_empty(&database, "database")?;
        validate_not_empty(&table, "table")?;

        let mooncake_table_id = MooncakeTableId { database, table };
        let table_reader = {
            let manager = self.replication_manager.read().await;
            manager.get_table_reader(&mooncake_table_id)?
        };
        // Resolve the LSN first, without any manager lock: `lsn` is the end of the caller's last
        // transaction, which need not be a commit of this table.
        let timeout = self.lsn_wait_timeout();
        let target_lsn =
            match tokio::time::timeout(timeout, table_reader.wait_for_lsn_coverage(lsn)).await {
                Ok(target_lsn) => target_lsn?,
                Err(_) => {
                    let (_, commit_lsn, replication_lsn) = table_reader.lsn_status();
                    return Err(Error::lsn_wait_timeout(format!(
                        "mirror {mooncake_table_id}: replication did not reach LSN {lsn} within \
                         {timeout:?} (replicated up to {replication_lsn}, committed {commit_lsn}); \
                         replication may be stalled or catching up, or src_uri points at a \
                         different server whose LSNs are not comparable. Retry, or check \
                         mooncake.list_tables()."
                    )));
                }
            };
        let rx = {
            let mut manager = self.replication_manager.write().await;
            let writer = manager.get_table_event_manager(&mooncake_table_id)?;
            writer.initiate_snapshot(target_lsn).await
        };
        // Not bounded: persisting a large backlog to Iceberg legitimately takes long. The wait
        // ends when the snapshot is persisted, or with an error once the mirror is errored or
        // dropped; a stuck-but-healthy table handler keeps the caller waiting.
        TableEventManager::synchronize_force_snapshot_request(rx, target_lsn).await?;
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
        validate_not_empty(&database, "database")?;
        validate_not_empty(&table, "table")?;
        if input_schema.is_some() {
            return Err(Error::invalid_argument(
                "input schema is only supported for event ingestion, which has been removed"
                    .to_string(),
            ));
        }

        let mooncake_table_id = MooncakeTableId {
            database: database.clone(),
            table: table.clone(),
        };

        // Add mooncake table to replication, and create corresponding mooncake table.
        let moonlink_table_config: Result<MoonlinkTableConfig> = {
            let mut manager = self.replication_manager.write().await;
            let mut cur_moonlink_table_config = config_utils::parse_replication_table_config(
                &table_config,
                &mooncake_table_id,
                &self.base_path,
                &self.temp_files_dir,
            )?;
            // Moonlink table config will get updated later at replication manager.
            manager
                .add_table(
                    &src_uri,
                    mooncake_table_id,
                    &src_table_name,
                    &mut cur_moonlink_table_config,
                    self.read_state_filepath_remap.clone(),
                    /*is_recovery=*/ false,
                )
                .await?;
            Ok(cur_moonlink_table_config)
        };

        // Create metadata store entry.
        self.metadata_store_accessor
            .store_table_metadata(
                &database,
                &table,
                &src_table_name,
                &src_uri,
                moonlink_table_config?,
            )
            .await?;

        Ok(())
    }

    pub async fn drop_table(&self, database: String, table: String) -> Result<()> {
        validate_not_empty(&database, "database")?;
        validate_not_empty(&table, "table")?;

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
        validate_not_empty(&database, "database")?;
        validate_not_empty(&table, "table")?;

        let table_schema = {
            let manager = self.replication_manager.read().await;
            let mooncake_table_id = MooncakeTableId { database, table };
            let table_state_reader = manager.get_table_state_reader(&mooncake_table_id)?;
            table_state_reader.get_current_table_schema().await?
        };
        Ok(table_schema)
    }

    /// The errored and the pending mirrors the periodic warning names after a restart.
    #[cfg(feature = "test-utils")]
    pub async fn placeholder_mirror_names(&self) -> (Vec<String>, Vec<String>) {
        recovery_utils::placeholder_names(&self.replication_manager.read().await.placeholders())
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
        validate_not_empty(&database, "database")?;
        validate_not_empty(&table, "table")?;

        let rx = {
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
            }?
        };

        TableEventManager::synchronize_maintenance_request(rx).await?;
        Ok(())
    }

    /// Put a mirror into the errored state, as a runtime failure would.
    #[cfg(feature = "test-utils")]
    pub async fn enter_errored_for_testing(
        &self,
        database: String,
        table: String,
        reason: String,
    ) -> Result<()> {
        let mut manager = self.replication_manager.write().await;
        let mooncake_table_id = MooncakeTableId { database, table };
        manager
            .get_table_event_manager(&mooncake_table_id)?
            .enter_errored(reason)
            .await?;
        Ok(())
    }

    /// If the requested database or table doesn't exist, return [`TableNotFound`] error.
    pub async fn scan_table(
        &self,
        database: String,
        table: String,
        lsn: Option<u64>,
    ) -> Result<Arc<ReadState>> {
        validate_not_empty(&database, "database")?;
        validate_not_empty(&table, "table")?;

        let mooncake_table_id = MooncakeTableId { database, table };
        // Do not hold the manager lock while waiting for the LSN: a queued writer (drop/create
        // table) would otherwise block every scan of every table behind this wait.
        let table_reader = {
            let manager = self.replication_manager.read().await;
            manager.get_table_reader(&mooncake_table_id)?
        };
        let timeout = self.lsn_wait_timeout();
        match tokio::time::timeout(timeout, table_reader.try_read(lsn)).await {
            Ok(read_state) => Ok(read_state?),
            Err(_) => {
                let (snapshot_lsn, commit_lsn, replication_lsn) = table_reader.lsn_status();
                let snapshot_lsn = if snapshot_lsn == u64::MAX {
                    "none".to_string()
                } else {
                    snapshot_lsn.to_string()
                };
                Err(Error::lsn_wait_timeout(format!(
                    "mirror {mooncake_table_id} did not reach LSN {} within {timeout:?} \
                     (replicated up to {replication_lsn}, committed {commit_lsn}, snapshot {snapshot_lsn}); \
                     replication may be stalled or catching up, or src_uri points at a different server \
                     whose LSNs are not comparable. Retry, or check mooncake.list_tables().",
                    lsn.unwrap_or_default()
                )))
            }
        }
    }

    /// Gracefully shutdown a replication connection identified by its URI.
    /// If postgres drop all is false, then we will not drop the PostgreSQL publication and replication slot,
    /// which allows for recovery from the PostgreSQL replication slot.
    pub async fn shutdown_connection(&self, uri: &str, postgres_drop_all: bool) {
        let mut manager = self.replication_manager.write().await;
        manager.shutdown_connection(uri, postgres_drop_all);
    }
}

fn validate_not_empty(field: &str, name: &str) -> Result<()> {
    if field.trim().is_empty() {
        return Err(Error::invalid_argument(format!("{name} cannot be empty")));
    }
    Ok(())
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

    #[tokio::test]
    async fn test_validate_not_empty() {
        // test non-empty input
        let ok_result = validate_not_empty("non_empty_value", "field_name");
        assert!(ok_result.is_ok(), "Expected Ok(()), got {ok_result:?}");

        // test empty input
        let err_result = validate_not_empty("   ", "field_name");
        assert!(err_result.is_err(), "Expected Err, got {err_result:?}");

        // verify error message
        let err = err_result.err().unwrap();
        let msg = format!("{err:?}");
        assert!(
            msg.contains("field_name cannot be empty"),
            "Error message does not contain expected string, got: {msg}"
        );
    }
}

#[cfg(test)]
mod lsn_wait_timeout_tests {
    use super::*;

    #[test]
    fn lsn_wait_timeout_env_value() {
        assert_eq!(parse_lsn_wait_timeout(None), DEFAULT_LSN_WAIT_TIMEOUT);
        assert_eq!(
            parse_lsn_wait_timeout(Some("120")),
            Duration::from_secs(120)
        );
        assert_eq!(parse_lsn_wait_timeout(Some(" 5 ")), Duration::from_secs(5));
        // Clamped instead of wrapping when converted to milliseconds.
        for huge in ["86401", "18446744073709551615"] {
            assert_eq!(parse_lsn_wait_timeout(Some(huge)), MAX_LSN_WAIT_TIMEOUT);
        }
        assert_eq!(clamp_lsn_wait_timeout(Duration::MAX), MAX_LSN_WAIT_TIMEOUT);
        assert_eq!(
            clamp_lsn_wait_timeout(Duration::from_secs(3600)),
            Duration::from_secs(3600)
        );
        for invalid in ["0", "-1", "30s", ""] {
            assert_eq!(
                parse_lsn_wait_timeout(Some(invalid)),
                DEFAULT_LSN_WAIT_TIMEOUT,
                "{invalid:?}"
            );
        }
    }
}
