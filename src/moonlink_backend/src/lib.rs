pub use moonlink::ReadState;
mod error;

pub use error::Error;
use error::Result;
use moonlink::Result as MoonlinkResult;
use moonlink_connectors::ReplicationManager;
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

impl<T: Eq + Hash + Clone> MoonlinkBackend<T> {
    pub fn new(base_path: String) -> Self {
        recreate_directory(DEFAULT_MOONLINK_TEMP_FILE_PATH).unwrap();
        Self {
            replication_manager: RwLock::new(ReplicationManager::new(
                base_path.clone(),
                DEFAULT_MOONLINK_TEMP_FILE_PATH.to_string(),
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
        let manager = self.replication_manager.read().await;

        let table_reader = manager.get_table_reader(table_id);
        let read_state = table_reader.try_read(lsn).await?;
        Ok(read_state)
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
