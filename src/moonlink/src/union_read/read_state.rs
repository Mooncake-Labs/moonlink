// A read state is a collection of objects that are shared between moonlink and readers
//
// Meant to be sent using either shared memory or network connection.
//

use super::table_metadata::TableMetadata;
use crate::storage::PuffinDeletionBlobAtRead;

use bincode::config;

const BINCODE_CONFIG: config::Configuration = config::standard();

#[derive(Debug)]
pub struct ReadState {
    pub data: Vec<u8>,
    associated_files: Vec<String>,
}

impl Drop for ReadState {
    fn drop(&mut self) {
        if self.associated_files.is_empty() {
            return;
        }
        let associated_files = std::mem::take(&mut self.associated_files);
        // Perform best-effort deletion by spawning detached task.
        tokio::spawn(async move {
            for file in associated_files.into_iter() {
                if let Err(e) = tokio::fs::remove_file(&file).await {
                    println!("Failed to delete file {} because {:?}", file, e);
                }
            }
        });
    }
}

impl ReadState {
    pub(super) fn new(
        data_files: Vec<String>,
        puffin_files: Vec<String>,
        mut deletion_vectors_at_read: Vec<PuffinDeletionBlobAtRead>,
        mut position_deletes: Vec<(u32 /*file_index*/, u32 /*row_index*/)>,
        associated_files: Vec<String>,
    ) -> Self {
        deletion_vectors_at_read.sort_by(|dv_1, dv_2| {
            dv_1.data_file_index
                .cmp(&dv_2.data_file_index)
                .then_with(|| dv_1.puffin_file_index.cmp(&dv_2.puffin_file_index))
                .then_with(|| dv_1.start_offset.cmp(&dv_2.start_offset))
                .then_with(|| dv_1.blob_size.cmp(&dv_2.blob_size))
        });
        position_deletes.sort();

        let metadata = TableMetadata {
            data_files,
            puffin_files,
            deletion_vectors: deletion_vectors_at_read,
            position_deletes,
        };
        let data = bincode::encode_to_vec(metadata, BINCODE_CONFIG).unwrap(); // TODO
        Self {
            data,
            associated_files,
        }
    }
}

#[cfg(test)]
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
