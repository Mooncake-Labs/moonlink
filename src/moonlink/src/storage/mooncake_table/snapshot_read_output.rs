use crate::storage::cache::object_storage::base_cache::CacheTrait;
use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::storage_utils::TableUniqueFileId;
use crate::table_notify::EvictedFiles;
use crate::table_notify::TableEvent;
use crate::ReadStateFilepathRemap;
use crate::{NonEvictableHandle, ReadState, Result};
use moonlink_table_metadata::{DeletionVector, PositionDelete};

use std::sync::Arc;

use tokio::sync::mpsc::Sender;

/// Mooncake snapshot for read.
///
/// Pass out two types of data files to read.
#[derive(Clone, Debug)]
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
    pub deletion_vectors: Vec<DeletionVector>,
    /// Committed but un-persisted positional deletion records.
    pub position_deletes: Vec<PositionDelete>,
    /// Contains committed but non-persisted record batches, which are persisted as temporary data files on local filesystem.
    pub associated_files: Vec<String>,
    /// Table notifier for query completion; could be none for empty read output.
    pub table_notifier: Option<Sender<TableEvent>>,
    /// Object storage cache, to pin local file cache, could be none for empty read output.
    pub object_storage_cache: Option<Arc<dyn CacheTrait>>,
    /// Filesystem accessor, to access remote storage, could be none for empty read output.
    pub filesystem_accessor: Option<Arc<dyn BaseFileSystemAccess>>,
}

impl ReadOutput {
    /// Helper to notify evicted files if non-empty.
    async fn notify_evicted_files(&mut self, files: Vec<String>) {
        if files.is_empty() {
            return;
        }
        self.table_notifier
            .as_mut()
            .unwrap()
            .send(TableEvent::EvictedFilesToDelete {
                evicted_files: EvictedFiles { files },
            })
            .await
            .unwrap();
    }

    /// Resolve all remote filepaths and convert into [`ReadState`] for query usage.
    ///
    /// TODO(hjiang): Parallelize download and pin.
    pub async fn take_as_read_state(
        mut self,
        read_state_filepath_remap: ReadStateFilepathRemap,
    ) -> Result<Arc<ReadState>> {
        // Resolve remote data files.
        let mut resolved_data_files = Vec::with_capacity(self.data_file_paths.len());
        let mut cache_handles = vec![];
        let data_file_paths = std::mem::take(&mut self.data_file_paths);
        for cur_data_file in data_file_paths.into_iter() {
            match cur_data_file {
                DataFileForRead::TemporaryDataFile(file) => resolved_data_files.push(file),
                DataFileForRead::RemoteFilePath((file_id, remote_filepath)) => {
                    let (object_storage_cache, filesystem_accessor) = (
                        self.object_storage_cache.as_ref().unwrap(),
                        self.filesystem_accessor.as_ref().unwrap(),
                    );
                    let res = object_storage_cache
                        .get_cache_entry(file_id, &remote_filepath, filesystem_accessor.as_ref())
                        .await;

                    match res {
                        Ok((cache_handle, files_to_delete)) => {
                            if let Some(cache_handle) = cache_handle {
                                resolved_data_files
                                    .push(cache_handle.get_cache_filepath().to_string());
                                cache_handles.push(cache_handle);
                            } else {
                                resolved_data_files.push(remote_filepath);
                            }

                            self.notify_evicted_files(files_to_delete.into_vec()).await;
                        }
                        Err(e) => {
                            self.handle_resolution_error(cache_handles).await;
                            return Err(e);
                        }
                    }
                }
            }
        }

        // Construct read state.
        Ok(Arc::new(ReadState::new(
            // Data file and positional deletes for query.
            resolved_data_files,
            self.puffin_cache_handles,
            self.deletion_vectors,
            self.position_deletes,
            // Fields used for read state cleanup after query completion.
            self.associated_files,
            cache_handles,
            read_state_filepath_remap,
        )))
    }

    /// Handle cleanup and notifications when resolving remote filepaths fails.
    async fn handle_resolution_error(&mut self, mut cache_handles: Vec<NonEvictableHandle>) {
        let mut evicted_files_to_delete_on_error: Vec<String> = vec![];
        // Unpin all previously pinned cache handles before propagating error.
        for mut handle in cache_handles.drain(..) {
            let files_to_delete = handle.unreference().await;
            evicted_files_to_delete_on_error.extend(files_to_delete);
        }

        // Also unpin any puffin cache handles included in this read output.
        for mut handle in self.puffin_cache_handles.drain(..) {
            let files_to_delete = handle.unreference().await;
            evicted_files_to_delete_on_error.extend(files_to_delete);
        }

        // Include any temporary associated files created for this read, and notify once.
        evicted_files_to_delete_on_error.extend(std::mem::take(&mut self.associated_files));
        self.notify_evicted_files(evicted_files_to_delete_on_error)
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::cache::object_storage::base_cache::MockCacheTrait;
    use crate::storage::cache::object_storage::object_storage_cache::ObjectStorageCache;
    use crate::storage::filesystem::accessor::base_filesystem_accessor::MockBaseFileSystemAccess;
    use crate::storage::mooncake_table::cache_test_utils::{
        create_infinite_object_storage_cache, import_fake_cache_entry,
    };
    use crate::storage::mooncake_table::test_utils_commons::{get_fake_file_path, FAKE_FILE_ID};
    use crate::table_notify::TableEvent;
    use mockall::Sequence;
    use smallvec::SmallVec;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_take_as_read_state_notifies_files_to_delete_on_success() {
        // Setup a mock cache that returns files_to_delete on success.
        let mut mock_cache = MockCacheTrait::new();
        mock_cache
            .expect_get_cache_entry()
            .once()
            .returning(|_, _, _| {
                Box::pin(async move {
                    let mut files_to_delete = SmallVec::new();
                    files_to_delete.push("old_data_file_cache_file".to_string());
                    Ok((None, files_to_delete))
                })
            });

        // Filesystem accessor mock (unused, but required by signature).
        let filesystem_accessor = MockBaseFileSystemAccess::new();

        // Table notifier channel to capture deletion notifications.
        let (tx, mut rx) = tokio::sync::mpsc::channel::<TableEvent>(8);

        // Prepare a remote path input.
        let temp_dir = tempdir().unwrap();
        let fake_remote_path = get_fake_file_path(&temp_dir);

        // ReadOutput with a single remote file; happy path should notify files_to_delete.
        let read_output = ReadOutput {
            data_file_paths: vec![DataFileForRead::RemoteFilePath((
                FAKE_FILE_ID,
                fake_remote_path,
            ))],
            puffin_cache_handles: Vec::new(),
            deletion_vectors: Vec::new(),
            position_deletes: Vec::new(),
            associated_files: Vec::new(),
            table_notifier: Some(tx),
            object_storage_cache: Some(Arc::new(mock_cache)),
            filesystem_accessor: Some(Arc::new(filesystem_accessor)),
        };

        // Invoke and expect success.
        let res = read_output
            .take_as_read_state(Arc::new(|p: String| p))
            .await;
        assert!(res.is_ok());

        // Receive exactly one notification and validate its content strictly.
        if let Some(TableEvent::EvictedFilesToDelete { evicted_files }) = rx.recv().await {
            assert_eq!(
                evicted_files.files,
                vec!["old_data_file_cache_file".to_string()]
            );
        } else {
            panic!("expected a TableEvent::EvictedFilesToDelete notification");
        }
    }

    #[tokio::test]
    async fn test_take_as_read_state_unpins_on_error() {
        // Prepare a real cache and a pinned handle we will inject via mock.
        let temp_dir = tempdir().unwrap();
        let real_cache: ObjectStorageCache = create_infinite_object_storage_cache(
            &temp_dir, /*optimize_local_filesystem=*/ false,
        );
        let pinned_handle = {
            let mut cache = real_cache.clone();
            import_fake_cache_entry(&temp_dir, &mut cache).await
        };

        // The handle is pinned once now; sanity check.
        assert_eq!(
            real_cache
                .get_non_evictable_entry_ref_count(&FAKE_FILE_ID)
                .await,
            1
        );

        // Build a mock cache that returns the pinned handle first, then an error on second call.
        let mut mock_cache = MockCacheTrait::new();
        let mut seq = Sequence::new();
        let handle_clone = pinned_handle.clone();
        mock_cache
            .expect_get_cache_entry()
            .once()
            .in_sequence(&mut seq)
            .returning(move |_, _, _| {
                let handle_clone = handle_clone.clone();
                let files_to_delete = SmallVec::new();
                Box::pin(async move { Ok((Some(handle_clone), files_to_delete)) })
            });
        mock_cache
            .expect_get_cache_entry()
            .once()
            .in_sequence(&mut seq)
            .returning(|_, _, _| {
                Box::pin(async move {
                    Err(crate::Error::from(std::io::Error::other(
                        "mocked IO failure",
                    )))
                })
            });

        // Before invoking read, request deletion on the data cache entry so that
        // unreference() on error will return its cache filepath to delete.
        let _ = real_cache.try_delete_cache_entry(FAKE_FILE_ID).await;

        // Prepare a separate cache/handle to simulate puffin cache behavior, and
        // also mark it requested-to-delete so unreference returns files.
        let puffin_temp_dir = tempdir().unwrap();
        let mut puffin_cache: ObjectStorageCache = create_infinite_object_storage_cache(
            &puffin_temp_dir,
            /*optimize_local_filesystem=*/ false,
        );
        let puffin_handle = { import_fake_cache_entry(&puffin_temp_dir, &mut puffin_cache).await };
        let _ = puffin_cache.try_delete_cache_entry(FAKE_FILE_ID).await;

        // Filesystem accessor mock (unused, but required by signature).
        let filesystem_accessor = MockBaseFileSystemAccess::new();

        // Table notifier channel to capture deletion notifications.
        let (tx, mut rx) = tokio::sync::mpsc::channel::<TableEvent>(8);

        // Construct ReadOutput with two remote files; second call will error.
        let fake_remote_path = get_fake_file_path(&temp_dir);
        let associated_temp_dir = tempdir().unwrap();
        let read_output = ReadOutput {
            data_file_paths: vec![
                DataFileForRead::RemoteFilePath((FAKE_FILE_ID, fake_remote_path.clone())),
                DataFileForRead::RemoteFilePath((FAKE_FILE_ID, fake_remote_path)),
            ],
            puffin_cache_handles: vec![puffin_handle],
            deletion_vectors: Vec::new(),
            position_deletes: Vec::new(),
            // Add an associated temporary file to trigger error-path notification.
            associated_files: vec![get_fake_file_path(&associated_temp_dir)],
            table_notifier: Some(tx),
            object_storage_cache: Some(Arc::new(mock_cache)),
            filesystem_accessor: Some(Arc::new(filesystem_accessor)),
        };

        // Invoke and expect error; previously pinned handle must be unpinned.
        let res = read_output
            .take_as_read_state(Arc::new(|p: String| p))
            .await;
        assert!(res.is_err());

        // The handle should have been unpinned back to evictable (non-evictable ref count == 0).
        assert_eq!(
            real_cache
                .get_non_evictable_entry_ref_count(&FAKE_FILE_ID)
                .await,
            0
        );

        // Collect all deletion notifications from the channel and validate contents.
        let mut notified_files: Vec<String> = Vec::new();
        while let Some(event) = rx.recv().await {
            if let TableEvent::EvictedFilesToDelete { evicted_files } = event {
                notified_files.extend(evicted_files.files);
            }
        }

        // Expect exactly these three files regardless of ordering.
        let mut notified_files_sorted = notified_files;
        notified_files_sorted.sort();

        let mut expected_files = vec![
            get_fake_file_path(&associated_temp_dir),
            get_fake_file_path(&temp_dir),
            get_fake_file_path(&puffin_temp_dir),
        ];
        expected_files.sort();

        assert_eq!(notified_files_sorted, expected_files);
    }
}
