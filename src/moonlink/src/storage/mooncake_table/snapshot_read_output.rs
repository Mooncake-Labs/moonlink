use crate::storage::cache::object_storage::base_cache::CacheTrait;
use crate::storage::cache::object_storage::object_storage_cache::ObjectStorageCache;
use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::storage_utils::TableUniqueFileId;
use crate::storage::PuffinDeletionBlobAtRead;
use crate::table_notify::EvictedFiles;
use crate::table_notify::TableEvent;
use crate::{NonEvictableHandle, ReadState};

use std::sync::Arc;

use tokio::sync::mpsc::Sender;

/// Mooncake snapshot for read.
///
/// Pass out two types of data files to read.
#[derive(Clone)]
pub enum DataFileForRead {
    /// Temporary data file for in-memory unpersisted data, used for union read.
    TemporaryDataFile(String),
    /// Pass out (file id, remote file path) and rely on read-through cache.
    RemoteFilePath((TableUniqueFileId, String)),
}

impl DataFileForRead {
    /// Get a file path to read.
    #[cfg(test)]
    pub fn get_file_path(&self) -> String {
        match self {
            Self::TemporaryDataFile(file) => file.clone(),
            Self::RemoteFilePath((_, file)) => file.clone(),
        }
    }
}

#[derive(Clone, Default)]
pub struct ReadOutput {
    /// Data files contains two parts:
    /// 1. Committed and persisted data files, which consists of file id and remote path (if any).
    /// 2. Associated files, which include committed but un-persisted records.
    pub data_file_paths: Vec<DataFileForRead>,
    /// Puffin cache handles.
    pub puffin_cache_handles: Vec<NonEvictableHandle>,
    /// Deletion vectors persisted in puffin files.
    pub deletion_vectors: Vec<PuffinDeletionBlobAtRead>,
    /// Committed but un-persisted positional deletion records.
    pub position_deletes: Vec<(u32 /*file_index*/, u32 /*row_index*/)>,
    /// Contains committed but non-persisted record batches, which are persisted as temporary data files on local filesystem.
    pub associated_files: Vec<String>,
    /// Table notifier for query completion; could be none for empty read output.
    pub table_notifier: Option<Sender<TableEvent>>,
    /// Object storage cache, to pin local file cache, could be none for empty read output.
    pub object_storage_cache: Option<ObjectStorageCache>,
    /// Filesystem accessor, to access remote storage, could be none for empty read output.
    pub filesystem_accessor: Option<Arc<dyn BaseFileSystemAccess>>,
}

impl ReadOutput {
    /// Resolve all remote filepaths and convert into [`ReadState`] for query usage.
    ///
    /// TODO(hjiang): Parallelize download and pin.
    pub async fn take_as_read_state(mut self) -> Arc<ReadState> {
        // Resolve remote data files.
        let mut resolved_data_files = Vec::with_capacity(self.data_file_paths.len());
        let mut cache_handles = vec![];
        for cur_data_file in self.data_file_paths.into_iter() {
            match cur_data_file {
                DataFileForRead::TemporaryDataFile(file) => resolved_data_files.push(file),
                DataFileForRead::RemoteFilePath((file_id, remote_filepath)) => {
                    // TODO(hjiang): Better error propagation.
                    let (cache_handle, files_to_delete) = self
                        .object_storage_cache
                        .as_mut()
                        .unwrap()
                        .get_cache_entry(
                            file_id,
                            &remote_filepath,
                            self.filesystem_accessor.as_ref().unwrap().as_ref(),
                        )
                        .await
                        .unwrap();
                    if let Some(cache_handle) = cache_handle {
                        resolved_data_files.push(cache_handle.get_cache_filepath().to_string());
                        cache_handles.push(cache_handle);
                    } else {
                        resolved_data_files.push(remote_filepath);
                    }

                    if !files_to_delete.is_empty() {
                        self.table_notifier
                            .as_mut()
                            .unwrap()
                            .send(TableEvent::EvictedFilesToDelete {
                                evicted_files: EvictedFiles {
                                    files: files_to_delete,
                                },
                            })
                            .await
                            .unwrap();
                    }
                }
            }
        }

        // Construct read state.
        Arc::new(ReadState::new(
            // Data file and positional deletes for query.
            resolved_data_files,
            self.puffin_cache_handles,
            self.deletion_vectors,
            self.position_deletes,
            // Fields used for read state cleanup after query completion.
            self.associated_files,
            cache_handles,
        ))
    }
}
