mod error;
mod logging;

pub use error::{Error, Result};
pub use moonlink::ReadState;
use moonlink::{ObjectStorageCache, ObjectStorageCacheConfig};
use moonlink_connectors::ReplicationManager;
use more_asserts as ma;
use std::hash::Hash;
use std::io::ErrorKind;
use std::sync::Arc;
use tokio::sync::RwLock;

// Default local filesystem directory where all tables data will be stored under.
const DEFAULT_MOONLINK_TABLE_BASE_PATH: &str = "./mooncake/";
// Default local filesystem directory under the above base directory (which defaults to `PGDATA/pg_mooncake`) where all temporary files (used for union read) will be stored under.
// The whole directory is cleaned up at moonlink backend start, to prevent file leak.
pub const DEFAULT_MOONLINK_TEMP_FILE_PATH: &str = "./temp/";
// Default object storage read-through cache directory under the above mooncake directory (which defaults to `PGDATA/pg_mooncake`).
// The whole directory is cleaned up at moonlink backend start, to prevent file leak.
pub const DEFAULT_MOONLINK_OBJECT_STORAGE_CACHE_PATH: &str = "./read_through_cache/";
// Min left disk space for on-disk cache of the filesystem which cache directory is mounted on.
const MIN_DISK_SPACE_FOR_CACHE: u64 = 1 << 30; // 1GiB

/// Get temporary directory under base path.
fn get_temp_file_directory_under_base(base_path: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(base_path).join(DEFAULT_MOONLINK_TEMP_FILE_PATH)
}
/// Get cache directory under base path.
fn get_cache_directory_under_base(base_path: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(base_path).join(DEFAULT_MOONLINK_OBJECT_STORAGE_CACHE_PATH)
}

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

impl<T: Eq + Hash + Clone + std::fmt::Display> Default for MoonlinkBackend<T> {
    fn default() -> Self {
        Self::new(DEFAULT_MOONLINK_TABLE_BASE_PATH.to_string(), Vec::new())
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
/// Precondition: cache directory has been created beforehand.
fn create_default_object_storage_cache(
    cache_directory_pathbuf: std::path::PathBuf,
) -> ObjectStorageCache {
    let cache_directory = cache_directory_pathbuf.to_str().unwrap().to_string();
    let filesystem_size = get_cache_filesystem_size(&cache_directory);
    ma::assert_ge!(filesystem_size, MIN_DISK_SPACE_FOR_CACHE);

    let cache_config = ObjectStorageCacheConfig {
        max_bytes: filesystem_size - MIN_DISK_SPACE_FOR_CACHE,
        cache_directory,
        optimize_local_filesystem: true,
    };
    ObjectStorageCache::new(cache_config)
}

impl<T: Eq + Hash + Clone + std::fmt::Display> MoonlinkBackend<T> {
    pub fn new(base_path: String, uris: Vec<String>) -> Self {
        println!("MoonlinkBackend::new {uris:?}");
        logging::init_logging();

        // Re-create directory for temporary files directory and cache files directory under base directory.
        let temp_files_dir = get_temp_file_directory_under_base(&base_path);
        let cache_files_dir = get_cache_directory_under_base(&base_path);
        recreate_directory(temp_files_dir.to_str().unwrap()).unwrap();
        recreate_directory(cache_files_dir.to_str().unwrap()).unwrap();

        Self {
            replication_manager: RwLock::new(ReplicationManager::new(
                base_path,
                temp_files_dir.to_str().unwrap().to_string(),
                create_default_object_storage_cache(cache_files_dir),
            )),
        }
    }

    pub async fn create_table(&self, table_id: T, table_name: &str, uri: &str) -> Result<()> {
        let mut manager = self.replication_manager.write().await;
        manager.add_table(uri, table_id, table_name).await?;
        Ok(())
    }

    pub async fn drop_table(&self, table_id: T) -> Result<()> {
        let mut manager = self.replication_manager.write().await;
        manager.drop_table(table_id).await?;
        Ok(())
    }

    pub async fn scan_table(&self, table_id: &T, lsn: Option<u64>) -> Result<Arc<ReadState>> {
        let read_state = {
            let manager = self.replication_manager.read().await;
            let table_reader = manager.get_table_reader(table_id);
            table_reader.try_read(lsn).await?
        };

        Ok(read_state.clone())
    }

    /// Gracefully shutdown a replication connection identified by its URI.
    pub async fn shutdown_connection(&self, uri: &str) {
        let mut manager = self.replication_manager.write().await;
        manager.shutdown_connection(uri);
    }

    /// Create an iceberg snapshot with the given LSN, return when the a snapshot is successfully created.
    pub async fn create_iceberg_snapshot(&self, table_id: &T, lsn: u64) -> Result<()> {
        let mut rx = {
            let mut manager = self.replication_manager.write().await;
            let writer = manager.get_iceberg_table_event_manager(table_id);
            writer.initiate_snapshot(lsn).await
        };
        rx.recv().await.unwrap()?;
        Ok(())
    }
}
