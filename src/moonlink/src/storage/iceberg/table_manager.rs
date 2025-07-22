/// This module define interface for table manager.
use std::collections::HashMap;

use crate::storage::iceberg::puffin_utils::PuffinBlobRef;
use crate::storage::index::FileIndex;
use crate::storage::mooncake_table::IcebergSnapshotPayload;
use crate::storage::mooncake_table::Snapshot as MooncakeSnapshot;
use crate::storage::storage_utils::FileId;
use crate::storage::storage_utils::MooncakeDataFileRef;

use async_trait::async_trait;
use iceberg::Result as IcebergResult;

#[cfg(test)]
use mockall::*;

/// File parameters required for snapshot persistence.
pub struct PersistenceFileParams {
    /// Used to generate unique file id.
    pub(crate) table_auto_incr_ids: std::ops::Range<u32>,
}

/// Iceberg persistence results.
#[derive(Clone, Default, Debug)]
pub struct PersistenceResult {
    /// Imported data files, which only contain remote file paths.
    /// NOTICE: It's guaranteed that remote data files contain imported data files and compacted new files; and are placed in this order.
    pub(crate) remote_data_files: Vec<MooncakeDataFileRef>,
    /// Imported file indices, which only contain remote file paths.
    /// NOTICE: It's guaranteed that remote file indices contain imported file indices, merged file indices, and compacted new file indices; and are placed in this order.
    pub(crate) remote_file_indices: Vec<FileIndex>,
    /// Maps from remote data files to their deletion vector puffin blob.
    pub(crate) puffin_blob_ref: HashMap<FileId, PuffinBlobRef>,
}

#[async_trait]
#[cfg_attr(test, automock)]
pub trait TableManager: Send {
    /// Write a new snapshot to iceberg table.
    /// It could be called for multiple times to write and commit multiple snapshots.
    ///
    /// - Apart from data files, it also supports deletion vector (which is introduced in v3) and self-defined hash index,
    ///   both of which are stored in puffin files.
    /// - For deletion vectors, we store one blob in one puffin file.
    /// - For hash index, we store one mooncake file index in one puffin file.
    #[allow(async_fn_in_trait)]
    async fn sync_snapshot(
        &mut self,
        snapshot_payload: IcebergSnapshotPayload,
        file_params: PersistenceFileParams,
    ) -> IcebergResult<PersistenceResult>;

    /// Load the latest snapshot from iceberg table, and return next file id for the current mooncake table.
    /// Notice this function is supposed to call **only once**.
    #[allow(async_fn_in_trait)]
    async fn load_snapshot_from_table(
        &mut self,
    ) -> IcebergResult<(u32 /*next file id*/, MooncakeSnapshot)>;

    /// Drop the current iceberg table.
    #[allow(async_fn_in_trait)]
    async fn drop_table(&mut self) -> IcebergResult<()>;
}
