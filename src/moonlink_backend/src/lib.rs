mod error;
pub mod file_utils;
mod logging;
pub mod mooncake_table_id;
mod recovery_utils;
pub mod table_config;

use arrow_schema::Schema;
pub use error::{Error, Result};
use mooncake_table_id::MooncakeTableId;
pub use moonlink::ReadState;
use moonlink::{TableEventManager, TableStatus};
use moonlink_connectors::ReplicationManager;
use moonlink_metadata_store::base_metadata_store::MetadataStoreTrait;
use std::hash::Hash;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::table_config::TableCreationConfig;

pub struct MoonlinkBackend<
    D: std::convert::From<u32> + Eq + Hash + Clone + std::fmt::Display,
    T: std::convert::From<u32> + Eq + Hash + Clone + std::fmt::Display,
> {
    // Metadata storage accessor.
    metadata_store_accessor: Box<dyn MetadataStoreTrait>,
    // Could be either relative or absolute path.
    replication_manager: RwLock<ReplicationManager<MooncakeTableId<D, T>>>,
}

impl<D, T> MoonlinkBackend<D, T>
where
    D: std::convert::From<u32> + Eq + Hash + Clone + std::fmt::Display,
    T: std::convert::From<u32> + Eq + Hash + Clone + std::fmt::Display,
{
    pub async fn new(
        base_path: String,
        metadata_store_accessor: Box<dyn MetadataStoreTrait>,
    ) -> Result<Self> {
        logging::init_logging();

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
            temp_files_dir.to_str().unwrap().to_string(),
            file_utils::create_default_object_storage_cache(read_cache_files_dir),
        );
        recovery_utils::recover_all_tables(&*metadata_store_accessor, &mut replication_manager)
            .await?;

        Ok(Self {
            replication_manager: RwLock::new(replication_manager),
            metadata_store_accessor,
        })
    }

    /// Create an iceberg snapshot with the given LSN, return when the a snapshot is successfully created.
    pub async fn create_snapshot(&self, database_id: D, table_id: T, lsn: u64) -> Result<()> {
        let rx = {
            let mut manager = self.replication_manager.write().await;
            let mooncake_table_id = MooncakeTableId {
                database_id,
                table_id,
            };
            let writer = manager.get_table_event_manager(&mooncake_table_id);
            writer.initiate_snapshot(lsn).await
        };
        TableEventManager::synchronize_force_snapshot_request(rx, lsn).await?;
        Ok(())
    }

    /// # Arguments
    ///
    /// * src_uri: connection string for source database (row storage database).
    pub async fn create_table(
        &self,
        database_id: D,
        table_id: T,
        src_table_name: String,
        src_uri: String,
        _mooncake_table_config: TableCreationConfig,
    ) -> Result<()> {
        let mooncake_table_id = MooncakeTableId {
            database_id: database_id.clone(),
            table_id,
        };
        let database_id = mooncake_table_id.get_database_id_value();
        let table_id = mooncake_table_id.get_table_id_value();

        // Add mooncake table to replication, and create corresponding mooncake table.
        let moonlink_table_config = {
            let mut manager = self.replication_manager.write().await;
            // TODO(hjiang): Should pass real secrets into the mooncake table.
            let table_config = manager
                .add_table(
                    &src_uri,
                    mooncake_table_id,
                    database_id,
                    table_id,
                    &src_table_name,
                    /*override_iceberg_filesystem_config=*/ None,
                    /*is_recovery=*/ false,
                )
                .await?;
            manager.start_replication(&src_uri).await?;
            table_config
        };

        // Create metadata store entry.
        self.metadata_store_accessor
            .store_table_metadata(
                database_id,
                table_id,
                &src_table_name,
                &src_uri,
                moonlink_table_config,
            )
            .await?;

        Ok(())
    }

    pub async fn drop_table(&self, database_id: D, table_id: T) {
        let mooncake_table_id = MooncakeTableId {
            database_id: database_id.clone(),
            table_id,
        };
        let database_id = mooncake_table_id.get_database_id_value();
        let table_id = mooncake_table_id.get_table_id_value();

        let table_exists = {
            let mut manager = self.replication_manager.write().await;
            manager.drop_table(mooncake_table_id).await.unwrap()
        };
        if !table_exists {
            return;
        }

        self.metadata_store_accessor
            .delete_table_metadata(database_id, table_id)
            .await
            .unwrap()
    }

    /// Get the current mooncake table schema.
    pub async fn get_table_schema(&self, database_id: D, table_id: T) -> Result<Arc<Schema>> {
        let table_schema = {
            let manager = self.replication_manager.read().await;
            let mooncake_table_id = MooncakeTableId {
                database_id,
                table_id,
            };
            let table_state_reader = manager.get_table_state_reader(&mooncake_table_id);
            table_state_reader.get_current_table_schema().await?
        };
        Ok(table_schema)
    }

    /// List all tables at moonlink backend, and return their states.
    pub async fn list_tables(&self) -> Result<Vec<TableStatus>> {
        let mut table_states = vec![];
        let manager = self.replication_manager.read().await;
        let table_state_readers = manager.get_table_status_readers();
        for cur_table_state_reader in table_state_readers.into_iter() {
            table_states.push(cur_table_state_reader.get_current_table_state().await?);
        }
        Ok(table_states)
    }

    /// Perform a table maintaince operation based on requested mode, block wait until maintenance results have been persisted.
    /// Notice, it's only exposed for debugging, testing and admin usage.
    ///
    /// There're currently three modes supported:
    /// - "data": perform a data compaction, only data files smaller than a threshold, or with too many deleted rows will be compacted.
    /// - "index": perform an index merge operation, only index files smaller than a threshold, or with too many deleted rows will be merged.    
    /// - "full": perform a full compaction, which merges all data files and all index files, whatever file size they are of.
    pub async fn optimize_table(&self, database_id: D, table_id: T, mode: &str) -> Result<()> {
        let mut rx = {
            let mut manager = self.replication_manager.write().await;
            let mooncake_table_id = MooncakeTableId {
                database_id,
                table_id,
            };
            let writer = manager.get_table_event_manager(&mooncake_table_id);

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

    pub async fn scan_table(
        &self,
        database_id: D,
        table_id: T,
        lsn: Option<u64>,
    ) -> Result<Arc<ReadState>> {
        let read_state = {
            let manager = self.replication_manager.read().await;
            let mooncake_table_id = MooncakeTableId {
                database_id,
                table_id,
            };
            let table_reader = manager.get_table_reader(&mooncake_table_id);
            table_reader.try_read(lsn).await?
        };

        Ok(read_state.clone())
    }

    /// Gracefully shutdown a replication connection identified by its URI.
    pub async fn shutdown_connection(&self, uri: &str) {
        let mut manager = self.replication_manager.write().await;
        manager.shutdown_connection(uri);
    }
}
