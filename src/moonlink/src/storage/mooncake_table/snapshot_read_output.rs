use crate::storage::cache::object_storage::base_cache::CacheTrait;
use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::io_utils;
use crate::storage::storage_utils::TableUniqueFileId;
use crate::table_notify::EvictedFiles;
use crate::table_notify::TableEvent;
use crate::ReadStateFilepathRemap;
use crate::{NonEvictableHandle, ReadState, Result};
use futures::{stream, StreamExt};
use moonlink_table_metadata::{DeletionVector, PositionDelete};

use std::sync::Arc;

use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::Sender;

/// Maximum number of parallel remote file operations to prevent excessive resource usage
const MAX_PARALLEL_OPERATIONS: usize = 128;

/// Unpin `handles` and delete `files` of a read abandoned before it became a [`ReadState`] (a
/// panic while it was built). Runs detached, like the release in `ReadState::drop`.
pub(crate) fn release_abandoned_read(handles: Vec<NonEvictableHandle>, files: Vec<String>) {
    if handles.is_empty() && files.is_empty() {
        return;
    }
    tokio::spawn(async move {
        let mut files_to_delete = files;
        for handle in handles {
            files_to_delete.extend(handle.unreference().await);
        }
        if let Err(e) = io_utils::delete_local_files(&files_to_delete).await {
            tracing::error!(
                ?files_to_delete,
                ?e,
                "failed to delete files of an abandoned read"
            );
        }
    });
}

/// Armed while [`ReadOutput::take_as_read_state`] runs. Holds copies of the pins (a copy shares
/// the pin, it does not add one) and owns the data-file pins as they are taken, so that a panic
/// releases them exactly once; the success and error paths disarm it after handing them on.
struct PinsOnPanic {
    puffin_cache_handles: Vec<NonEvictableHandle>,
    associated_files: Vec<String>,
    data_file_cache_handles: Vec<NonEvictableHandle>,
    armed: bool,
}

impl Drop for PinsOnPanic {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut handles = std::mem::take(&mut self.puffin_cache_handles);
        handles.append(&mut self.data_file_cache_handles);
        release_abandoned_read(handles, std::mem::take(&mut self.associated_files));
    }
}

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

/// Represents a remote file with its original index, unique file identifier, and remote file path.
/// This structure is used to maintain the order of files as they appear in the original data file path,
/// which is crucial for operations that depend on the file's position, such as deletions or updates.
struct RemoteFileEntry {
    index: usize,
    file_id: TableUniqueFileId,
    remote_filepath: String,
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
        let sent =
            self.table_notifier
                .as_mut()
                .unwrap()
                .try_send(TableEvent::EvictedFilesToDelete {
                    evicted_files: EvictedFiles { files },
                });
        // The table handler is busy (its queue is full) or gone: delete the files here instead
        // of waiting for it, so a read never depends on the handler making progress.
        if let Err(
            TrySendError::Full(TableEvent::EvictedFilesToDelete { evicted_files })
            | TrySendError::Closed(TableEvent::EvictedFilesToDelete { evicted_files }),
        ) = sent
        {
            if let Err(err) = io_utils::delete_local_files(&evicted_files.files).await {
                tracing::warn!(error = ?err, "failed to delete evicted cache files");
            }
        }
    }

    /// Attempt to download remote files and cache them locally, if possible.
    /// Resolved files will be updated to [`resolved_data_files`] in-place in the given order.
    ///
    /// If any error happens, all involved cache handles will unreferenced, temporary files will be deleted as well.
    async fn resolve_remote_files(
        &mut self,
        object_storage_cache: Arc<dyn CacheTrait>,
        filesystem_accessor: Arc<dyn BaseFileSystemAccess>,
        remote_files_entries: Vec<RemoteFileEntry>,
        resolved_data_files: &mut [String],
        cache_handles: &mut Vec<NonEvictableHandle>,
    ) -> Result<()> {
        let results = stream::iter(remote_files_entries.into_iter())
            .map(|remote_file_entry| {
                let cache = object_storage_cache.clone();
                let fs_accessor = filesystem_accessor.clone();
                async move {
                    let result = cache
                        .get_cache_entry(
                            remote_file_entry.file_id,
                            &remote_file_entry.remote_filepath,
                            fs_accessor.as_ref(),
                        )
                        .await;
                    (
                        remote_file_entry.index,
                        remote_file_entry.remote_filepath,
                        result,
                    )
                }
            })
            .buffer_unordered(MAX_PARALLEL_OPERATIONS)
            .collect::<Vec<_>>()
            .await;

        let mut error_messages = Vec::new();
        for (index, remote_filepath, result) in results {
            match result {
                Ok((cache_handle, files_to_delete)) => {
                    if let Some(cache_handle) = cache_handle {
                        resolved_data_files[index] = cache_handle.get_cache_filepath().to_string();
                        cache_handles.push(cache_handle);
                    } else {
                        resolved_data_files[index] = remote_filepath;
                    }

                    self.notify_evicted_files(files_to_delete.into_vec()).await;
                }
                Err(e) => {
                    error_messages.push(format!("[{index}] {remote_filepath}: {e}"));
                }
            }
        }

        if !error_messages.is_empty() {
            self.handle_resolution_error(std::mem::take(cache_handles))
                .await;
            return Err(crate::Error::from(std::io::Error::other(format!(
                "Failed to resolve {} files: {}",
                error_messages.len(),
                error_messages.join(";\n")
            ))));
        }

        Ok(())
    }

    /// Resolve all remote filepaths and convert into [`ReadState`] for query usage.
    pub async fn take_as_read_state(
        mut self,
        read_state_filepath_remap: ReadStateFilepathRemap,
    ) -> Result<Arc<ReadState>> {
        // Resolve remote data files.
        let mut resolved_data_files = vec![String::new(); self.data_file_paths.len()];
        let mut pins = PinsOnPanic {
            puffin_cache_handles: self.puffin_cache_handles.clone(),
            associated_files: self.associated_files.clone(),
            data_file_cache_handles: Vec::new(),
            armed: true,
        };
        let data_file_paths = std::mem::take(&mut self.data_file_paths);

        // Separate temporary files and remote files while preserving order
        let mut remote_files_entries = Vec::new();
        for (index, cur_data_file) in data_file_paths.into_iter().enumerate() {
            match cur_data_file {
                DataFileForRead::TemporaryDataFile(file) => {
                    resolved_data_files[index] = file;
                }
                DataFileForRead::RemoteFilePath((file_id, remote_filepath)) => {
                    remote_files_entries.push(RemoteFileEntry {
                        index,
                        file_id,
                        remote_filepath,
                    });
                }
            }
        }

        // Process remote files in parallel but maintain order
        if !remote_files_entries.is_empty() {
            let (object_storage_cache, filesystem_accessor) = (
                self.object_storage_cache.as_ref().unwrap(),
                self.filesystem_accessor.as_ref().unwrap(),
            );

            let resolved = self
                .resolve_remote_files(
                    object_storage_cache.clone(),
                    filesystem_accessor.clone(),
                    remote_files_entries,
                    &mut resolved_data_files,
                    &mut pins.data_file_cache_handles,
                )
                .await;
            if resolved.is_err() {
                // resolve_remote_files released everything already.
                pins.armed = false;
            }
            resolved?;
        }
        pins.armed = false;
        let cache_handles = std::mem::take(&mut pins.data_file_cache_handles);

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
        let total_size =
            cache_handles.len() + self.puffin_cache_handles.len() + self.associated_files.len();
        let mut evicted_files_to_delete_on_error: Vec<String> = Vec::with_capacity(total_size);
        // Unpin all previously pinned cache handles before propagating error.
        for handle in cache_handles.drain(..) {
            let files_to_delete = handle.unreference().await;
            evicted_files_to_delete_on_error.extend(files_to_delete);
        }

        // Also unpin any puffin cache handles included in this read output.
        for handle in self.puffin_cache_handles.drain(..) {
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
    use crate::storage::mooncake_table::test_utils_commons::{
        get_fake_file_path, get_unique_table_file_id, FAKE_FILE_ID,
    };
    use crate::storage::storage_utils::FileId;
    use crate::table_notify::TableEvent;
    use crate::union_read::decode_read_state_for_testing;
    use mockall::Sequence;
    use smallvec::SmallVec;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use tempfile::tempdir;

    use crate::storage::filesystem::accessor::filesystem_accessor::FileSystemAccessor;
    use crate::storage::filesystem::accessor_config::AccessorConfig;
    use crate::storage::filesystem::storage_config::StorageConfig;

    /// Evicted files are deleted right away when the table handler's queue is full or the
    /// handler is gone, instead of the read waiting for the handler.
    #[tokio::test]
    async fn evicted_files_are_deleted_when_the_handler_cannot_take_them() {
        let dir = tempdir().unwrap();
        for handler_gone in [false, true] {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let mut keep_rx = Some(rx);
            if handler_gone {
                keep_rx = None;
            } else {
                // Fill the queue; a busy handler does not drain it.
                tx.try_send(TableEvent::EvictedFilesToDelete {
                    evicted_files: EvictedFiles { files: vec![] },
                })
                .unwrap();
            }
            let file = dir.path().join(format!("evicted_{handler_gone}"));
            std::fs::write(&file, b"x").unwrap();
            let mut output = ReadOutput {
                table_notifier: Some(tx),
                ..Default::default()
            };
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                output.notify_evicted_files(vec![file.to_string_lossy().to_string()]),
            )
            .await
            .expect("a read waited for the table handler");
            assert!(!file.exists(), "handler_gone={handler_gone}");
            drop(keep_rx);
        }
    }

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

    // Test that the file order is preserved even if the remote file resolution fails.
    #[tokio::test]
    async fn test_file_order_preserved_with_different_delays() {
        let mut mock_cache = MockCacheTrait::new();
        let call_order = Arc::new(AtomicUsize::new(0));
        let task_notify = Arc::new(tokio::sync::Notify::new());

        mock_cache.expect_get_cache_entry().times(2).returning({
            let call_order = call_order.clone();
            move |_, _, _| {
                let order = call_order.fetch_add(1, Ordering::SeqCst);
                let task_notify = task_notify.clone();
                Box::pin(async move {
                    match order {
                        0 => {
                            task_notify.notified().await;
                        }
                        1 => {
                            task_notify.notify_one();
                        }
                        _ => unreachable!(),
                    };
                    Ok((
                        /*cache_handle=*/ None,
                        /*evicted_files=*/ SmallVec::new(),
                    ))
                })
            }
        });

        let filesystem_accessor = MockBaseFileSystemAccess::new();
        let (tx, _rx) = tokio::sync::mpsc::channel::<TableEvent>(8);

        let read_output = ReadOutput {
            data_file_paths: vec![
                DataFileForRead::TemporaryDataFile("/tmp/filename_1".to_string()),
                DataFileForRead::RemoteFilePath((FAKE_FILE_ID, "/tmp/filename_2".to_string())),
                DataFileForRead::RemoteFilePath((FAKE_FILE_ID, "/tmp/filename_3".to_string())),
                DataFileForRead::TemporaryDataFile("/tmp/filename_4".to_string()),
            ],
            puffin_cache_handles: Vec::new(),
            deletion_vectors: Vec::new(),
            position_deletes: Vec::new(),
            associated_files: Vec::new(),
            table_notifier: Some(tx),
            object_storage_cache: Some(Arc::new(mock_cache)),
            filesystem_accessor: Some(Arc::new(filesystem_accessor)),
        };

        let res = read_output
            .take_as_read_state(Arc::new(|p: String| p))
            .await
            .unwrap();

        let read_state: Arc<ReadState> = res;
        let (data_files, _, _, _) = decode_read_state_for_testing(&read_state);

        assert_eq!(data_files.len(), 4);
        assert_eq!(data_files[0], "/tmp/filename_1".to_string());
        assert_eq!(data_files[1], "/tmp/filename_2".to_string());
        assert_eq!(data_files[2], "/tmp/filename_3".to_string());
        assert_eq!(data_files[3], "/tmp/filename_4".to_string());
    }

    // Test that the cache is correctly pinned when all file operations succeed
    // with a test size of 1000 (greater than our set max parallel read of 128)
    #[tokio::test]
    async fn test_parallel_success() {
        let test_size = 1000;
        // Prepare a real cache and a pinned handle we will inject via mock.
        let temp_dir = tempdir().unwrap();
        let real_cache: ObjectStorageCache = create_infinite_object_storage_cache(
            &temp_dir, /*optimize_local_filesystem=*/ false,
        );

        let mut file_ids = Vec::new();
        let mut test_data_file_paths = Vec::new();
        for i in 0..test_size {
            let file_id = get_unique_table_file_id(FileId(i));
            let filepath = temp_dir.path().join(format!("file_{i}.parquet"));
            tokio::fs::write(&filepath, format!("test data {i}").as_bytes())
                .await
                .unwrap();
            file_ids.push(file_id);
            test_data_file_paths.push(DataFileForRead::RemoteFilePath((
                file_id,
                filepath.to_str().unwrap().to_string(),
            )));
        }

        // Filesystem use for real cache
        let storage_config = StorageConfig::FileSystem {
            root_directory: temp_dir.path().to_string_lossy().to_string(),
            atomic_write_dir: None,
        };
        let accessor_config = AccessorConfig::new_with_storage_config(storage_config);
        let filesystem_accessor = FileSystemAccessor::new(accessor_config);

        // Table notifier channel to capture deletion notifications.
        let (tx, mut _rx) = tokio::sync::mpsc::channel::<TableEvent>(8);

        // Construct ReadOutput with two remote files; second call will error.
        let associated_temp_dir = tempdir().unwrap();
        let read_output = ReadOutput {
            data_file_paths: test_data_file_paths,
            puffin_cache_handles: Vec::new(),
            deletion_vectors: Vec::new(),
            position_deletes: Vec::new(),
            // Add an associated temporary file to trigger error-path notification.
            associated_files: vec![get_fake_file_path(&associated_temp_dir)],
            table_notifier: Some(tx),
            object_storage_cache: Some(Arc::new(real_cache.clone())),
            filesystem_accessor: Some(Arc::new(filesystem_accessor)),
        };

        let _read_state = read_output
            .take_as_read_state(Arc::new(|p: String| p))
            .await
            .unwrap();

        // Check that the handles were properly transferred to ReadState on success
        // While ReadState is alive, the cache handles are owned by ReadState,
        // so the cache's reference count should be 1
        for file_id in &file_ids {
            assert_eq!(
                real_cache.get_non_evictable_entry_ref_count(file_id).await,
                1
            );
        }
    }

    // Test to verify that the cache is correctly unpinned when processing files with alternating success and failure:
    // files with odd IDs fail, while files with even IDs succeed.
    #[tokio::test]
    async fn test_parallel_partial_fail() {
        let test_size = 1000;
        // Prepare a real cache and a pinned handle we will inject via mock.
        let temp_dir = tempdir().unwrap();
        let real_cache: ObjectStorageCache = create_infinite_object_storage_cache(
            &temp_dir, /*optimize_local_filesystem=*/ false,
        );

        let mut file_ids = Vec::new();
        let mut test_data_file_paths = Vec::new();
        for i in 0..test_size {
            let file_id = get_unique_table_file_id(FileId(i));
            let filepath = temp_dir.path().join(format!("file_{i}.parquet"));
            if i % 2 == 0 {
                tokio::fs::write(&filepath, format!("test data_{i}").as_bytes())
                    .await
                    .unwrap();
            }
            file_ids.push(file_id);
            test_data_file_paths.push(DataFileForRead::RemoteFilePath((
                file_id,
                filepath.to_str().unwrap().to_string(),
            )));
        }

        // Filesystem use for real cache
        let storage_config = StorageConfig::FileSystem {
            root_directory: temp_dir.path().to_string_lossy().to_string(),
            atomic_write_dir: None,
        };
        let accessor_config = AccessorConfig::new_with_storage_config(storage_config);
        let filesystem_accessor = FileSystemAccessor::new(accessor_config);

        // Table notifier channel to capture deletion notifications.
        let (tx, mut _rx) = tokio::sync::mpsc::channel::<TableEvent>(8);

        // Construct ReadOutput with two remote files; second call will error.
        let associated_temp_dir = tempdir().unwrap();
        let read_output = ReadOutput {
            data_file_paths: test_data_file_paths,
            puffin_cache_handles: Vec::new(),
            deletion_vectors: Vec::new(),
            position_deletes: Vec::new(),
            // Add an associated temporary file to trigger error-path notification.
            associated_files: vec![get_fake_file_path(&associated_temp_dir)],
            table_notifier: Some(tx),
            object_storage_cache: Some(Arc::new(real_cache.clone())),
            filesystem_accessor: Some(Arc::new(filesystem_accessor)),
        };

        let _read_state_error = read_output
            .take_as_read_state(Arc::new(|p: String| p))
            .await
            .unwrap_err();

        // Ensure the cache is clean after read
        for file_id in file_ids {
            assert_eq!(
                real_cache.get_non_evictable_entry_ref_count(&file_id).await,
                0
            );
        }
    }
}
