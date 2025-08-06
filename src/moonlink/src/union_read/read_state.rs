// A read state is a collection of objects that are shared between moonlink and readers
//
// Meant to be sent using either shared memory or network connection.
//

use super::table_metadata::TableMetadata;
use crate::storage::{io_utils, PuffinDeletionBlobAtRead};
use crate::NonEvictableHandle;

use bincode::config;
use tracing::Instrument;
use tracing::{error, info_span, warn};

const BINCODE_CONFIG: config::Configuration = config::standard();

// TODO(hjiang): A better solution might be wrap clean up in a functor.
#[derive(Debug)]
pub struct ReadState {
    /// Serialized data files and positional deletes for query.
    pub data: Vec<u8>,
    /// Fields related to clean up after query completion.
    pub(crate) associated_files: Vec<String>,
    /// Cache handles for data files.
    cache_handles: Vec<NonEvictableHandle>,
}

impl Drop for ReadState {
    fn drop(&mut self) {
        // Notify query completion for object storage cache unreference.
        // Since we cannot rely on async function at `Drop` function, start a detach task immediately here.
        let cache_handles = std::mem::take(&mut self.cache_handles);
        tokio::spawn(async move {
            let mut evicted_files_to_delete = vec![];
            for mut cur_cache_handle in cache_handles.into_iter() {
                let cur_evicted_files = cur_cache_handle.unreference().await;
                evicted_files_to_delete.extend(cur_evicted_files);
            }
            if let Err(e) = io_utils::delete_local_files(&evicted_files_to_delete).await {
                error!(
                    "Failed to delete unreferenced cache files: {:?}: {:?}",
                    evicted_files_to_delete, e
                );
            }
        });

        // Delete temporarily data files.
        if self.associated_files.is_empty() {
            return;
        }
        let associated_files = std::mem::take(&mut self.associated_files);
        // Perform best-effort deletion by spawning detached task.
        tokio::spawn(
            async move {
                for file in associated_files.into_iter() {
                    if let Err(e) = tokio::fs::remove_file(&file).await {
                        warn!(%file, error = ?e, "failed to delete associated file");
                    }
                }
            }
            .instrument(info_span!("read_state_cleanup")),
        );
    }
}

impl ReadState {
    // TODO(hjiang): Provide a struct for parameters.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        // Data file and positional deletes for query.
        data_files: Vec<String>,
        puffin_cache_handles: Vec<NonEvictableHandle>,
        mut deletion_vectors_at_read: Vec<PuffinDeletionBlobAtRead>,
        mut position_deletes: Vec<(u32 /*file_index*/, u32 /*row_index*/)>,
        // Fields used for read state cleanup after query completion.
        associated_files: Vec<String>,
        mut cache_handles: Vec<NonEvictableHandle>, // Cache handles for data files.
    ) -> Self {
        deletion_vectors_at_read.sort_by(|dv_1, dv_2| {
            dv_1.data_file_index
                .cmp(&dv_2.data_file_index)
                .then_with(|| dv_1.puffin_file_index.cmp(&dv_2.puffin_file_index))
                .then_with(|| dv_1.start_offset.cmp(&dv_2.start_offset))
                .then_with(|| dv_1.blob_size.cmp(&dv_2.blob_size))
        });
        position_deletes.sort();

        let puffin_files = puffin_cache_handles
            .iter()
            .map(|handle| handle.cache_entry.cache_filepath.clone())
            .collect::<Vec<_>>();
        let metadata = TableMetadata {
            data_files,
            puffin_files,
            deletion_vectors: deletion_vectors_at_read,
            position_deletes,
        };
        let data = bincode::encode_to_vec(metadata, BINCODE_CONFIG).unwrap(); // TODO

        cache_handles.extend(puffin_cache_handles);
        Self {
            data,
            associated_files,
            cache_handles,
        }
    }
}

#[cfg(any(test, feature = "test-utils"))]
#[allow(clippy::type_complexity)]
pub fn decode_read_state_for_testing(
    read_state: &ReadState,
) -> (
    Vec<String>, /*data_file_paths*/
    Vec<String>, /*puffin_file_paths*/
    Vec<PuffinDeletionBlobAtRead>,
    Vec<(u32, u32)>,
) {
    let metadata = TableMetadata::decode(&read_state.data);
    (
        metadata.data_files,
        metadata.puffin_files,
        metadata.deletion_vectors,
        metadata.position_deletes,
    )
}
