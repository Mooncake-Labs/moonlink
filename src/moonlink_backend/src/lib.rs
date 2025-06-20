mod error;
mod logging;

pub use error::{Error, Result};
pub use moonlink::ReadState;
use moonlink::Result as MoonlinkResult;
use moonlink::{ObjectStorageCache, ObjectStorageCacheConfig};
use moonlink_connectors::ReplicationManager;
use more_asserts as ma;
use std::hash::Hash;
use std::io::ErrorKind;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::RwLock;

// Default local filesystem directory where all tables data will be stored under.
const DEFAULT_MOONLINK_TABLE_BASE_PATH: &str = "./mooncake/";
// Default local filesystem directory where all temporary files (used for union read) will be stored under.
// The whole directory is cleaned up at moonlink backend start, to prevent file leak.
pub const DEFAULT_MOONLINK_TEMP_FILE_PATH: &str = "/tmp/moonlink_temp_file";
// Default object storage cache directory.
// The whole directory is cleaned up at moonlink backend start, to prevent file leak.
pub const DEFAULT_MOONLINK_OBJECT_STORAGE_CACHE_PATH: &str = "/tmp/moonlink_cache_file";
// Min left disk space for on-disk cache of the filesystem which cache directory is mounted on.
const MIN_DISK_SPACE_FOR_CACHE: u64 = 1 << 30; // 1GiB

/// Util function to delete and re-create the given directory.
pub fn recreate_directory(dir: &str) -> Result<()> {
    // Clean up directory to place moonlink temporary files.
    match std::fs::remove_dir_all(dir) {
        Ok(()) => {}
        Err(e) => {
            if e.kind() != ErrorKind::NotFound {
                return Err(error::Error::Io(e));
            }
        }
    }
    std::fs::create_dir_all(dir)?;
    Ok(())
}

pub struct MoonlinkBackend<T: Eq + Hash> {
    // Could be either relative or absolute path.
    replication_manager: RwLock<ReplicationManager<T>>,
}

impl<T: Eq + Hash + Clone> Default for MoonlinkBackend<T> {
    fn default() -> Self {
        Self::new(DEFAULT_MOONLINK_TABLE_BASE_PATH.to_string())
    }
}

/// Util function to get filesystem size for cache directory
fn get_cache_filesystem_size(path: &str) -> u64 {
    let vfs_stat = nix::sys::statvfs::statvfs(path).unwrap();
    let block_size = vfs_stat.block_size();
    let avai_blocks = vfs_stat.files_available();

    (block_size as u64).checked_mul(avai_blocks as u64).unwrap()
}

/// Create default object storage cache.
fn create_default_object_storage_cache() -> ObjectStorageCache {
    let filesystem_size = get_cache_filesystem_size(DEFAULT_MOONLINK_OBJECT_STORAGE_CACHE_PATH);
    ma::assert_ge!(filesystem_size, MIN_DISK_SPACE_FOR_CACHE);

    let cache_config = ObjectStorageCacheConfig {
        max_bytes: filesystem_size - MIN_DISK_SPACE_FOR_CACHE,
        cache_directory: DEFAULT_MOONLINK_OBJECT_STORAGE_CACHE_PATH.to_string(),
    };
    ObjectStorageCache::new(cache_config)
}

impl<T: Eq + Hash + Clone> MoonlinkBackend<T> {
    pub fn new(base_path: String) -> Self {
        logging::init_logging();

        recreate_directory(DEFAULT_MOONLINK_TEMP_FILE_PATH).unwrap();
        recreate_directory(DEFAULT_MOONLINK_OBJECT_STORAGE_CACHE_PATH).unwrap();

        Self {
            replication_manager: RwLock::new(ReplicationManager::new(
                base_path.clone(),
                DEFAULT_MOONLINK_TEMP_FILE_PATH.to_string(),
                create_default_object_storage_cache(),
            )),
        }
    }

    pub async fn create_table(&self, table_id: T, table_name: &str, uri: &str) -> Result<()> {
        let mut manager = self.replication_manager.write().await;
        manager.add_table(uri, table_id, table_name).await?;
        Ok(())
    }

    pub async fn drop_table(&self, external_table_id: T) -> Result<()> {
        let mut manager = self.replication_manager.write().await;
        manager.drop_table(external_table_id).await?;
        Ok(())
    }

    pub async fn scan_table(&self, table_id: &T, lsn: Option<u64>) -> Result<Arc<ReadState>> {
        let snapshot_read_output = {
            let manager = self.replication_manager.read().await;
            let table_reader = manager.get_table_reader(table_id);
            table_reader.try_read(lsn).await?
        };

        let snapshot_read_output = (*snapshot_read_output.clone()).clone();
        Ok(snapshot_read_output.take_as_read_state().await)
    }

    /// Gracefully shutdown a replication connection identified by its URI.
    pub async fn shutdown_connection(&self, uri: &str) -> Result<()> {
        let mut manager = self.replication_manager.write().await;
        manager.shutdown_connection(uri).await?;
        Ok(())
    }

    /// Create an iceberg snapshot with the given LSN, return when the a snapshot is successfully created.
    pub async fn create_iceberg_snapshot(&self, table_id: &T, lsn: u64) -> Result<()> {
        #[allow(unused_assignments)]
        let mut rx: Option<Receiver<MoonlinkResult<()>>> = None;
        {
            let mut manager = self.replication_manager.write().await;
            let writer = manager.get_iceberg_table_event_manager(table_id);
            rx = Some(writer.initiate_snapshot(lsn).await);
        }
        rx.unwrap().recv().await.unwrap()?;
        Ok(())
    }
}
