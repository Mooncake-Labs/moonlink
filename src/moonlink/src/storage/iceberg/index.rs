use std::collections::HashMap;

use crate::storage::cache::object_storage::base_cache::CacheTrait;
use crate::storage::iceberg::puffin_utils;
use crate::storage::iceberg::utils::to_iceberg_error;
use crate::storage::index::persisted_bucket_hash_map::IndexBlock as MooncakeIndexBlock;
/// This module defines the file index struct used for iceberg, which corresponds to in-memory mooncake table file index structs, and supports the serde between mooncake table format and iceberg format.
use crate::storage::index::FileIndex as MooncakeFileIndex;
use crate::storage::io_utils;
use crate::storage::storage_utils::{create_data_file, FileId, TableId, TableUniqueFileId};
use crate::ObjectStorageCache;

use iceberg::io::FileIO;
use iceberg::puffin::Blob;
use iceberg::spec::DataFile;
use iceberg::{Error as IcebergError, Result as IcebergResult};
use serde::{Deserialize, Serialize};

/// Blob type for index v1.
pub(crate) const MOONCAKE_HASH_INDEX_V1: &str = "mooncake-hash-index-v1";
/// File index puffin blob property.
pub(crate) const MOONCAKE_HASH_INDEX_V1_CARDINALITY: &str = "cardinality";

/// Corresponds to [storage::index::IndexBlock], which records the metadata for each index block.
#[derive(Deserialize, PartialEq, Serialize)]
pub(crate) struct IndexBlock {
    bucket_start_idx: u32,
    bucket_end_idx: u32,
    bucket_start_offset: u64,
    filepath: String,
}

/// Corresponds to [storage::index::FileIndex], used to persist at iceberg table.
#[derive(Default, Deserialize, Serialize)]
pub(crate) struct FileIndex {
    /// Data file paths at iceberg table.
    data_files: Vec<String>,
    /// Corresponds to [storage::index::IndexBlock].
    index_block_files: Vec<IndexBlock>,
    /// Hash related fields.
    num_rows: u32,
    hash_bits: u32,
    hash_upper_bits: u32,
    hash_lower_bits: u32,
    seg_id_bits: u32,
    row_id_bits: u32,
    bucket_bits: u32,
}

impl FileIndex {
    /// Convert from mooncake table [storage::index::FileIndex].
    ///
    /// # Arguments
    ///
    /// * local_index_file_to_remote: hash map from local index filepath to remote filepath, which is to be managed by iceberg.
    /// * local_data_file_to_remote: hash map from local data filepath to remote filepath, which is to be managed by iceberg.
    pub(crate) fn new(
        mooncake_index: &MooncakeFileIndex,
        local_index_file_to_remote: &mut HashMap<String, String>,
        local_data_file_to_remote: &mut HashMap<String, String>,
    ) -> Self {
        Self {
            data_files: mooncake_index
                .files
                .iter()
                .map(|cur_data_file| {
                    // It's possible to have multiple newly imported file indices pointing to remote filepath.
                    // One example is file index merge.
                    local_data_file_to_remote
                        .remove(cur_data_file.file_path())
                        .unwrap_or(cur_data_file.file_path().clone())
                })
                .collect(),
            index_block_files: mooncake_index
                .index_blocks
                .iter()
                .map(|cur_index_block| IndexBlock {
                    bucket_start_idx: cur_index_block.bucket_start_idx,
                    bucket_end_idx: cur_index_block.bucket_end_idx,
                    bucket_start_offset: cur_index_block.bucket_start_offset,
                    filepath: local_index_file_to_remote
                        .remove(cur_index_block.index_file.file_path())
                        .unwrap(),
                })
                .collect(),
            num_rows: mooncake_index.num_rows,
            hash_bits: mooncake_index.hash_bits,
            hash_upper_bits: mooncake_index.hash_upper_bits,
            hash_lower_bits: mooncake_index.hash_lower_bits,
            seg_id_bits: mooncake_index.seg_id_bits,
            row_id_bits: mooncake_index.row_id_bits,
            bucket_bits: mooncake_index.bucket_bits,
        }
    }

    /// Transfer the ownership and convert into [storage::index::FileIndex].
    pub(crate) async fn as_mooncake_file_index(
        &mut self,
        data_file_to_id: &HashMap<String, FileId>,
        mut object_storage_cache: ObjectStorageCache,
        table_id: TableId,
        next_file_id: &mut u64,
    ) -> IcebergResult<MooncakeFileIndex> {
        // All mooncake index blocks.
        let mut mooncake_index_blocks = vec![];

        for cur_index_block in self.index_block_files.iter() {
            let cur_file_id = *next_file_id;
            *next_file_id += 1;
            let table_unique_file_id = TableUniqueFileId {
                table_id,
                file_id: FileId(cur_file_id),
            };
            let (cache_handle, cur_evicted_files) = object_storage_cache
                .get_cache_entry(
                    table_unique_file_id,
                    /*remote_filepath=*/ &cur_index_block.filepath,
                )
                .await
                .map_err(to_iceberg_error)?;
            io_utils::delete_local_files(cur_evicted_files)
                .await
                .map_err(to_iceberg_error)?;

            // File indices should always reside in on-disk cache.
            let cache_handle = cache_handle.unwrap();
            // Transform iceberg index block into mooncake index block.
            let mut cur_index_block = MooncakeIndexBlock::new(
                cur_index_block.bucket_start_idx,
                cur_index_block.bucket_end_idx,
                cur_index_block.bucket_start_offset,
                /*index_file=*/
                create_data_file(cur_file_id, cache_handle.get_cache_filepath().to_string()),
            )
            .await;
            cur_index_block.cache_handle = Some(cache_handle);
            mooncake_index_blocks.push(cur_index_block);
        }
        let file_indice = MooncakeFileIndex {
            files: self
                .data_files
                .iter()
                .map(|path| {
                    let file_id = data_file_to_id.get(path).unwrap();
                    create_data_file(file_id.0, path.to_string())
                })
                .collect(),
            num_rows: self.num_rows,
            hash_bits: self.hash_bits,
            hash_upper_bits: self.hash_upper_bits,
            hash_lower_bits: self.hash_lower_bits,
            seg_id_bits: self.seg_id_bits,
            row_id_bits: self.row_id_bits,
            bucket_bits: self.bucket_bits,
            index_blocks: mooncake_index_blocks,
        };

        Ok(file_indice)
    }
}

/// In-memory structure for one file index blob in the puffin file, which contains multiple `FileIndex` structs.
#[derive(Deserialize, Serialize)]
pub(crate) struct FileIndexBlob {
    /// A blob contains multiple file indexes.
    pub(crate) file_indices: Vec<FileIndex>,
}

impl FileIndexBlob {
    pub fn new(
        file_indices: Vec<&MooncakeFileIndex>,
        mut local_index_file_to_remote: HashMap<String, String>,
        mut local_data_file_to_remote: HashMap<String, String>,
    ) -> Self {
        Self {
            file_indices: file_indices
                .into_iter()
                .map(|file_index| {
                    FileIndex::new(
                        file_index,
                        &mut local_index_file_to_remote,
                        &mut local_data_file_to_remote,
                    )
                })
                .collect(),
        }
    }

    /// Serialize the file index into iceberg puffin blob.
    pub(crate) fn as_blob(&self) -> IcebergResult<Blob> {
        let blob_bytes = serde_json::to_vec(self).map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::DataInvalid,
                format!("Failed to serialize file index into json: {:?}", e),
            )
        })?;
        let mut properties = HashMap::new();
        let total_num_rows: u32 = self
            .file_indices
            .iter()
            .map(|mooncake_file_index| mooncake_file_index.num_rows)
            .sum();
        properties.insert(
            MOONCAKE_HASH_INDEX_V1_CARDINALITY.to_string(),
            total_num_rows.to_string(),
        );

        // Snapshot ID and sequence number are not known at the time the Puffin file is created.
        // `snapshot-id` and `sequence-number` must be set to -1 in blob metadata for Puffin v1.
        Ok(Blob::builder()
            .r#type(MOONCAKE_HASH_INDEX_V1.to_string())
            .fields(vec![])
            .snapshot_id(-1)
            .sequence_number(-1)
            .data(blob_bytes)
            .properties(properties)
            .build())
    }

    /// Load file index from puffin file blob.
    ///
    /// TODO(hjiang): Add unit test for load blob from local filesystem.
    pub async fn load_from_index_blob(
        file_io: FileIO,
        puffin_file: &DataFile,
    ) -> IcebergResult<Self> {
        let blob =
            puffin_utils::load_blob_from_puffin_file(file_io, puffin_file.file_path()).await?;
        FileIndexBlob::from_blob(blob)
    }

    /// Deserialize from iceberg puffin blob.
    pub(crate) fn from_blob(blob: Blob) -> IcebergResult<Self> {
        // Check blob type.
        assert_eq!(
            blob.blob_type(),
            MOONCAKE_HASH_INDEX_V1,
            "Expected hash index v1 blob type is {:?}, actual type is {:?}",
            MOONCAKE_HASH_INDEX_V1,
            blob.blob_type()
        );

        serde_json::from_slice(blob.data()).map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::DataInvalid,
                format!("Failed to deserialize blob from json string: {:?}", e),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::storage::index::persisted_bucket_hash_map::IndexBlock as MooncakeIndexBlock;
    use crate::storage::index::FileIndex as MooncakeFileIndex;
    use crate::storage::storage_utils::create_data_file;

    #[tokio::test]
    async fn test_hash_index_v1_serde() {
        // Test table id.
        let table_id = TableId(0);
        // Test object storage cache.
        let temp_dir = tempfile::tempdir().unwrap();
        let object_storage_cache = ObjectStorageCache::default_for_test(&temp_dir);

        // Fill in meaningless random bytes, mainly to verify the correctness of serde.
        let temp_local_index_file = temp_dir.path().join("local-index.bin");
        let temp_remote_index_file = temp_dir.path().join("remote-index.bin");
        tokio::fs::File::create(&temp_local_index_file)
            .await
            .unwrap();
        tokio::fs::File::create(&temp_remote_index_file)
            .await
            .unwrap();

        // Notice: use the same filepath for index block and data file only for serde testing, no IO involved.
        let local_index_filepath = temp_local_index_file.to_str().unwrap().to_string();
        let remote_index_filepath = temp_remote_index_file.to_str().unwrap().to_string();

        let local_data_filepath = local_index_filepath.clone();
        let remote_data_filepath = remote_index_filepath.clone();
        let local_data_file = create_data_file(/*file_id=*/ 0, local_data_filepath.clone());

        let original_mooncake_file_index = MooncakeFileIndex {
            num_rows: 10,
            hash_bits: 10,
            hash_upper_bits: 4,
            hash_lower_bits: 6,
            seg_id_bits: 6,
            row_id_bits: 3,
            bucket_bits: 5,
            files: vec![local_data_file.clone()],
            index_blocks: vec![
                MooncakeIndexBlock::new(
                    /*bucket_start_idx=*/ 0,
                    /*bucket_end_idx=*/ 3,
                    /*bucket_start_offset=*/ 10,
                    /*index_file=*/
                    create_data_file(/*file_id=*/ 1, local_index_filepath.clone()),
                )
                .await,
            ],
        };

        // Serialization.
        let local_index_file_to_remote = HashMap::<String, String>::from([(
            local_index_filepath.clone(),
            remote_index_filepath.clone(),
        )]);
        let local_data_file_to_remote = HashMap::<String, String>::from([(
            local_data_filepath.clone(),
            remote_data_filepath.clone(),
        )]);
        let file_index_blob = FileIndexBlob::new(
            vec![&original_mooncake_file_index],
            local_index_file_to_remote,
            local_data_file_to_remote,
        );
        let blob = file_index_blob.as_blob().unwrap();

        // Deserialization.
        let mut deserialized_file_index_blob = FileIndexBlob::from_blob(blob).unwrap();
        assert_eq!(deserialized_file_index_blob.file_indices.len(), 1);
        let mut file_index = std::mem::take(&mut deserialized_file_index_blob.file_indices[0]);

        let data_file_to_id =
            HashMap::<String, FileId>::from([(remote_data_filepath.clone(), FileId(0))]);
        let mut next_file_id = 1;
        let mooncake_file_index = file_index
            .as_mooncake_file_index(
                &data_file_to_id,
                object_storage_cache.clone(),
                table_id,
                &mut next_file_id,
            )
            .await
            .unwrap();

        // Check global index are equal before and after serde.
        assert_eq!(
            mooncake_file_index.num_rows,
            original_mooncake_file_index.num_rows
        );
        assert_eq!(
            mooncake_file_index.files,
            original_mooncake_file_index.files
        );
        assert_eq!(
            mooncake_file_index.hash_bits,
            original_mooncake_file_index.hash_bits
        );
        assert_eq!(
            mooncake_file_index.hash_upper_bits,
            original_mooncake_file_index.hash_upper_bits
        );
        assert_eq!(
            mooncake_file_index.hash_lower_bits,
            original_mooncake_file_index.hash_lower_bits
        );
        assert_eq!(
            mooncake_file_index.seg_id_bits,
            original_mooncake_file_index.seg_id_bits
        );
        assert_eq!(
            mooncake_file_index.row_id_bits,
            original_mooncake_file_index.row_id_bits
        );
        assert_eq!(
            mooncake_file_index.bucket_bits,
            original_mooncake_file_index.bucket_bits
        );

        assert_eq!(mooncake_file_index.index_blocks.len(), 1);
        assert_eq!(
            mooncake_file_index.index_blocks[0].bucket_start_idx,
            original_mooncake_file_index.index_blocks[0].bucket_start_idx
        );
        assert_eq!(
            mooncake_file_index.index_blocks[0].bucket_end_idx,
            original_mooncake_file_index.index_blocks[0].bucket_end_idx
        );
        assert_eq!(
            mooncake_file_index.index_blocks[0].bucket_start_offset,
            original_mooncake_file_index.index_blocks[0].bucket_start_offset
        );
    }
}
