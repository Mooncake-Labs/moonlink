//! Mirrors that cannot be served.
//!
//! A mirror that entered the errored state stays errored across restarts: a marker file records
//! it, and recovery registers such a table as a placeholder instead of resuming replication into
//! it (the slot has moved on, so it would silently look healthy with stale data). Tables that fail
//! to recover get a marker too: while they are down the slot advances for the other mirrors of
//! their database, so a later start could not resume them faithfully either. Placeholders reject
//! every request with their message and can always be dropped.

use crate::Result;
use moonlink::{BaseFileSystemAccess, FileSystemAccessor, IcebergTableConfig, MooncakeTableId};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use tracing::warn;

const MARKER_DIRECTORY: &str = "errored_tables";

/// Per-mirror file under `<base>/<directory>`, named by the hex-encoded mirror id so any database
/// or table name is safe.
pub(crate) fn table_file_path(
    base_path: &str,
    directory: &str,
    mooncake_table_id: &MooncakeTableId,
) -> PathBuf {
    let encoded: String = mooncake_table_id
        .to_string()
        .bytes()
        .map(|b| format!("{b:02x}"))
        .collect();
    Path::new(base_path).join(directory).join(encoded)
}

fn marker_path(base_path: &str, mooncake_table_id: &MooncakeTableId) -> PathBuf {
    table_file_path(base_path, MARKER_DIRECTORY, mooncake_table_id)
}

/// Persist that a mirror is errored, with the user-facing message.
pub async fn write_marker(
    base_path: &str,
    mooncake_table_id: &MooncakeTableId,
    message: &str,
) -> std::io::Result<()> {
    let path = marker_path(base_path, mooncake_table_id);
    tokio::fs::create_dir_all(path.parent().unwrap()).await?;
    let tmp = path.with_extension("tmp");
    tokio::fs::write(&tmp, message).await?;
    tokio::fs::rename(&tmp, &path).await
}

/// The persisted message of an errored mirror, if any.
pub async fn read_marker(base_path: &str, mooncake_table_id: &MooncakeTableId) -> Option<String> {
    tokio::fs::read_to_string(marker_path(base_path, mooncake_table_id))
        .await
        .ok()
}

pub async fn remove_marker(base_path: &str, mooncake_table_id: &MooncakeTableId) {
    if let Err(e) = tokio::fs::remove_file(marker_path(base_path, mooncake_table_id)).await {
        if e.kind() != ErrorKind::NotFound {
            warn!(error = %e, table = %mooncake_table_id, "failed to remove errored-table marker");
        }
    }
}

/// Delete a mirror's iceberg table without loading it (it may be unloadable). Uses the file
/// catalog's layout, `<namespace>/<table>` under the catalog root, like its `drop_table`; a
/// re-created mirror with the same name must never pick up the old data.
pub async fn drop_iceberg_table_files(config: &IcebergTableConfig) -> Result<()> {
    let Some(accessor_config) = config
        .metadata_accessor_config
        .get_file_catalog_accessor_config()
    else {
        warn!(table = %config.table_name, "not a file catalog; iceberg table left in place");
        return Ok(());
    };
    let accessor = FileSystemAccessor::new(accessor_config);
    // Same separator iceberg uses to join multi-level namespaces in the file catalog path.
    let directory = format!("{}/{}", config.namespace.join("\u{1f}"), config.table_name);
    match accessor.remove_directory(&directory).await {
        Ok(()) => Ok(()),
        Err(e) => {
            warn!(error = ?e, %directory, "failed to delete iceberg table of errored mirror");
            Err(e.into())
        }
    }
}

/// A recovered mirror that is not served.
pub struct ErroredTable {
    /// User-facing message returned for every request.
    pub message: String,
    pub iceberg_table_config: IcebergTableConfig,
    /// Connection string and table of its source, to clean up the source when it is dropped.
    pub src_uri: String,
    pub src_table_name: String,
}
