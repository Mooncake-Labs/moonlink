use crate::storage::index::persisted_bucket_hash_map::GlobalIndexBuilder;
use crate::{
    create_data_file, storage::mooncake_table::transaction_stream::TransactionStreamCommit,
};
use crate::{AccessorConfig, FileSystemAccessor, StorageConfig};

use super::*;

use futures::{stream, StreamExt};
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::arrow::ProjectionMask;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::File;

const MAX_IN_FLIGHT: usize = 64;

/// Ensure parquet files to ingest lives on local filesystem.
/// Return local parquet files to ingest.
async fn ensure_parquet_files_local_filesystem(
    _filesystem_accessor: Arc<FileSystemAccessor>,
    parquet_file: String,
    _write_through_directory: String,
    storage_config: StorageConfig,
) -> Result<String> {
    match storage_config {
        // Already at local filesystem, skip.
        #[cfg(feature = "storage-fs")]
        StorageConfig::FileSystem { .. } => Ok(parquet_file),
        #[cfg(any(feature = "storage-gcs", feature = "storage-s3"))]
        _ => {
            let filename_without_suffix = std::path::Path::new(&parquet_file)
                .file_stem()
                .unwrap()
                .to_str()
                .unwrap()
                .to_string();

            // Suffix with UUID to avoid filename conflict.
            let unique_filename = format!(
                "{}-{:?}.parquet",
                filename_without_suffix,
                uuid::Uuid::new_v4()
            );
            let local_parquet_file =
                std::path::Path::new(&_write_through_directory).join(unique_filename);
            let local_parquet_filepath = local_parquet_file.to_str().unwrap().to_string();

            _filesystem_accessor
                .copy_from_remote_to_local(&parquet_file, &local_parquet_filepath)
                .await?;
            Ok(local_parquet_filepath)
        }
        #[cfg(all(
            not(feature = "storage-fs"),
            not(feature = "storage-gcs"),
            not(feature = "storage-s3")
        ))]
        _ => {
            panic!("Unknown storage config {:?}", storage_config);
        }
    }
}

impl MooncakeTable {
    /// Batch ingestion the given [`parquet_files`] into mooncake table.
    ///
    /// TODO(hjiang):
    /// 1. Record table events.
    /// 2. It involves IO operations, should be placed at background thread.
    pub(crate) async fn batch_ingest(
        &mut self,
        parquet_files: Vec<String>,
        storage_config: StorageConfig,
        lsn: u64,
    ) {
        let start_id = self.next_file_id;
        self.next_file_id += parquet_files.len() as u32;

        // Create filesystem accessor to download remote parquet files to local filesystem.
        let accessor_config = AccessorConfig::new_with_storage_config(storage_config.clone());
        let filesystem_accessor = Arc::new(FileSystemAccessor::new(accessor_config));
        let write_through_directory = self.metadata.path.to_str().unwrap().to_string();

        let disk_files = stream::iter(parquet_files.into_iter().enumerate().map(
            |(idx, cur_file)| {
                let cur_file_id = (start_id as u64) + idx as u64;
                let filesystem_accessor_clone = filesystem_accessor.clone();
                let write_through_directory_clone = write_through_directory.clone();
                let storage_config_clone = storage_config.clone();

                async move {
                    // If parquet files to ingest doesn't live on local filesystem, copy it to table write-through directory.
                    let local_parquet = ensure_parquet_files_local_filesystem(
                        filesystem_accessor_clone,
                        cur_file,
                        write_through_directory_clone,
                        storage_config_clone,
                    )
                    .await
                    .unwrap();

                    let file = File::open(&local_parquet)
                        .await
                        .unwrap_or_else(|_| panic!("Failed to open {local_parquet}"));
                    let file_size = file
                        .metadata()
                        .await
                        .unwrap_or_else(|_| panic!("Failed to stat {local_parquet}"))
                        .len() as usize;
                    let stream_builder = ParquetRecordBatchStreamBuilder::new(file)
                        .await
                        .unwrap_or_else(|_| {
                            panic!("Failed to read parquet footer for {local_parquet}")
                        });
                    let num_rows = stream_builder.metadata().file_metadata().num_rows() as usize;

                    let mooncake_data_file = create_data_file(cur_file_id, local_parquet.clone());
                    let disk_file_entry = DiskFileEntry {
                        cache_handle: None,
                        num_rows,
                        file_size,
                        committed_deletion_vector: BatchDeletionVector::new(num_rows),
                        puffin_deletion_blob: None,
                    };

                    (mooncake_data_file, disk_file_entry)
                }
            },
        ))
        .buffer_unordered(MAX_IN_FLIGHT) // run up to N at once
        .collect::<hashbrown::HashMap<_, _>>()
        .await;

        // Commit the current crafted streaming transaction.
        let mut commit = TransactionStreamCommit::from_disk_files(disk_files, lsn);

        // Build file index if needed (skip for append-only tables)
        if !matches!(self.metadata.config.row_identity, IdentityProp::None) {
            // Clone owned inputs needed for async build without capturing &self.
            let files = commit.get_flushed_data_files();
            let identity = self.metadata.config.row_identity.clone();
            let table_dir: PathBuf = self.metadata.path.clone();
            let index_file_id = self.next_file_id as u64;

            if let Ok(file_index) =
                Self::build_index_for_files(files, identity, table_dir, index_file_id).await
            {
                commit.add_file_index(file_index);
            } else {
                tracing::error!(
                    "failed to build file index for batch_ingest; proceeding without index"
                );
            }
        }

        self.next_snapshot_task
            .new_streaming_xact
            .push(TransactionStreamOutput::Commit(commit));
        self.next_snapshot_task.new_flush_lsn = Some(lsn);
        self.next_snapshot_task.new_largest_flush_lsn = Some(lsn);
        self.next_snapshot_task.commit_lsn_baseline = lsn;
    }

    /// Build a single GlobalIndex spanning `files` by scanning Parquet with identity projection.
    async fn build_index_for_files(
        files: Vec<MooncakeDataFileRef>,
        identity: IdentityProp,
        table_dir: PathBuf,
        index_file_id: u64,
    ) -> Result<FileIndex> {
        // Accumulate (hash, seg_idx, row_idx)
        let mut entries: Vec<(u64, usize, usize)> = Vec::new();

        for (seg_idx, data_file) in files.iter().enumerate() {
            let file = tokio::fs::File::open(data_file.file_path()).await?;
            let mut stream_builder = ParquetRecordBatchStreamBuilder::new(file).await?;
            let schema_descr = stream_builder.metadata().file_metadata().schema_descr();
            let indices = identity.get_key_indices(schema_descr.num_columns());
            let mask = ProjectionMask::roots(schema_descr, indices);
            stream_builder = stream_builder.with_projection(mask);

            let mut reader = stream_builder.build()?;
            let mut row_idx_within_file: usize = 0;
            while let Some(row_group_reader) = reader.next_row_group().await? {
                let mut batch_stream = row_group_reader;
                while let Some(batch) = batch_stream.next().transpose()? {
                    let rows = MoonlinkRow::from_record_batch(&batch);
                    for row in rows {
                        let hash = identity.get_lookup_key(&row);
                        entries.push((hash, seg_idx, row_idx_within_file));
                        row_idx_within_file += 1;
                    }
                }
            }
        }

        // Build index blocks and attach data files
        let mut builder = GlobalIndexBuilder::new();
        builder.set_directory(table_dir);
        builder.set_files(files);
        let index = builder.build_from_flush(entries, index_file_id).await?;
        Ok(index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::mooncake_table::table_creation_test_utils::create_test_arrow_schema;
    use crate::storage::mooncake_table::test_utils::TestContext;
    use crate::storage::storage_utils::RecordLocation;
    use arrow_array::RecordBatch;
    use parquet::arrow::AsyncArrowWriter;
    use std::sync::Arc;
    use tempfile::tempdir;

    async fn write_parquet_file(path: &std::path::Path, batches: &[RecordBatch]) {
        let file = tokio::fs::File::create(path).await.unwrap();
        let mut writer =
            AsyncArrowWriter::try_new(file, create_test_arrow_schema(), /*props=*/ None).unwrap();
        for batch in batches.iter() {
            writer.write(batch).await.unwrap();
        }
        writer.close().await.unwrap();
    }

    fn batch_with_rows(ids: &[i32]) -> RecordBatch {
        use arrow_array::{Int32Array, RecordBatch, StringArray};
        let schema = create_test_arrow_schema();
        let names: Vec<String> = ids.iter().map(|i| format!("name-{i}")).collect();
        let ages: Vec<i32> = ids.iter().map(|i| 20 + *i).collect();
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names)),
                Arc::new(Int32Array::from(ages)),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_batch_ingest_append_only_skips_index() {
        let context = TestContext::new("batch_ingest_append_only");
        let temp_dir = tempdir().unwrap();
        let data_dir = temp_dir.path();

        let mut table = crate::storage::mooncake_table::test_utils::test_append_only_table(
            &context,
            "t_append_only",
        )
        .await;

        // Prepare one parquet file
        let file1 = data_dir.join("file1.parquet");
        let batch1 = batch_with_rows(&[1, 2, 3]);
        write_parquet_file(&file1, &[batch1]).await;

        let lsn = 100u64;
        let storage_config = crate::StorageConfig::FileSystem {
            root_directory: context.path().to_str().unwrap().to_string(),
            atomic_write_dir: None,
        };
        table
            .batch_ingest(
                vec![file1.to_string_lossy().to_string()],
                storage_config,
                lsn,
            )
            .await;

        // Verify next_snapshot_task updated
        assert_eq!(table.next_snapshot_task.new_streaming_xact.len(), 1);
        assert_eq!(table.next_snapshot_task.new_flush_lsn, Some(lsn));
        assert_eq!(table.next_snapshot_task.commit_lsn_baseline, lsn);

        // Verify commit has files and NO index for append-only
        match &table.next_snapshot_task.new_streaming_xact[0] {
            TransactionStreamOutput::Abort(_) => panic!("unexpected abort"),
            TransactionStreamOutput::Commit(commit) => {
                assert_eq!(commit.get_flushed_data_files().len(), 1);
                assert!(commit.get_file_indices().is_empty());
            }
        }
    }

    #[tokio::test]
    async fn test_batch_ingest_builds_index_for_single_primitive_key() {
        let context = TestContext::new("batch_ingest_single_key");
        let temp_dir = tempdir().unwrap();
        let data_dir = temp_dir.path();
        let mut table = crate::storage::mooncake_table::test_utils::test_table(
            &context,
            "t_single_key",
            IdentityProp::SinglePrimitiveKey(0),
        )
        .await;

        // Prepare two parquet files with known rows
        let file1 = data_dir.join("file1.parquet");
        let file2 = data_dir.join("file2.parquet");
        let b1 = batch_with_rows(&[10, 11, 12]);
        let b2 = batch_with_rows(&[20, 21, 22]);
        write_parquet_file(&file1, &[b1.clone()]).await;
        write_parquet_file(&file2, &[b2.clone()]).await;

        let lsn = 200u64;
        let storage_config = crate::StorageConfig::FileSystem {
            root_directory: context.path().to_str().unwrap().to_string(),
            atomic_write_dir: None,
        };
        table
            .batch_ingest(
                vec![
                    file1.to_string_lossy().to_string(),
                    file2.to_string_lossy().to_string(),
                ],
                storage_config,
                lsn,
            )
            .await;

        // Commit enqueued and index attached
        let (mut flushed_files, mut indices) = match &table.next_snapshot_task.new_streaming_xact[0]
        {
            TransactionStreamOutput::Abort(_) => panic!("unexpected abort"),
            TransactionStreamOutput::Commit(commit) => {
                (commit.get_flushed_data_files(), commit.get_file_indices())
            }
        };
        flushed_files.sort_by_key(|f| f.file_path().clone());
        assert_eq!(flushed_files.len(), 2);
        assert_eq!(indices.len(), 1);

        // Validate index can look up a couple of keys across files
        let index = indices.remove(0);
        let rows_file1 = MoonlinkRow::from_record_batch(&b1);
        let rows_file2 = MoonlinkRow::from_record_batch(&b2);
        let key1 = IdentityProp::SinglePrimitiveKey(0).get_lookup_key(&rows_file1[0]);
        let key2 = IdentityProp::SinglePrimitiveKey(0).get_lookup_key(&rows_file2[2]);
        let lookups = crate::storage::index::persisted_bucket_hash_map::GlobalIndex::prepare_hashes_for_lookup(
            vec![key1, key2].into_iter(),
        );
        let results = index.search_values(&lookups).await;
        let file_ids: Vec<_> = flushed_files.iter().map(|f| f.file_id()).collect();
        assert!(results.contains(&(key1, RecordLocation::DiskFile(file_ids[0], 0))));
        assert!(results.contains(&(key2, RecordLocation::DiskFile(file_ids[1], 2))));
    }

    #[tokio::test]
    async fn test_batch_ingest_builds_index_for_keys_identity() {
        let context = TestContext::new("batch_ingest_keys");
        let temp_dir = tempdir().unwrap();
        let data_dir = temp_dir.path();
        let mut table = crate::storage::mooncake_table::test_utils::test_table(
            &context,
            "t_keys",
            IdentityProp::Keys(vec![0]),
        )
        .await;

        let file1 = data_dir.join("file1.parquet");
        let file2 = data_dir.join("file2.parquet");
        let b1 = batch_with_rows(&[101, 102]);
        let b2 = batch_with_rows(&[201, 202]);
        write_parquet_file(&file1, &[b1.clone()]).await;
        write_parquet_file(&file2, &[b2.clone()]).await;

        let storage_config = crate::StorageConfig::FileSystem {
            root_directory: context.path().to_str().unwrap().to_string(),
            atomic_write_dir: None,
        };
        table
            .batch_ingest(
                vec![
                    file1.to_string_lossy().to_string(),
                    file2.to_string_lossy().to_string(),
                ],
                storage_config,
                500,
            )
            .await;

        let (mut files, mut indices) = match &table.next_snapshot_task.new_streaming_xact[0] {
            TransactionStreamOutput::Commit(commit) => {
                (commit.get_flushed_data_files(), commit.get_file_indices())
            }
            _ => panic!("unexpected"),
        };
        assert_eq!(files.len(), 2);
        assert_eq!(indices.len(), 1);
        let index = indices.remove(0);
        let r1 = MoonlinkRow::from_record_batch(&b1);
        let r2 = MoonlinkRow::from_record_batch(&b2);
        let k1 = IdentityProp::Keys(vec![0]).get_lookup_key(&r1[0]);
        let k2 = IdentityProp::Keys(vec![0]).get_lookup_key(&r2[1]);
        let lookups = crate::storage::index::persisted_bucket_hash_map::GlobalIndex::prepare_hashes_for_lookup(
            vec![k1, k2].into_iter(),
        );
        let results = index.search_values(&lookups).await;
        // Ensure deterministic order for seg_idx mapping in assertions
        files.sort_by_key(|f| f.file_path().clone());
        let file_ids: Vec<_> = files.iter().map(|f| f.file_id()).collect();
        assert!(results.contains(&(k1, RecordLocation::DiskFile(file_ids[0], 0))));
        assert!(results.contains(&(k2, RecordLocation::DiskFile(file_ids[1], 1))));
    }

    #[tokio::test]
    async fn test_batch_ingest_builds_index_for_fullrow_identity() {
        let context = TestContext::new("batch_ingest_fullrow");
        let temp_dir = tempdir().unwrap();
        let data_dir = temp_dir.path();
        let mut table = crate::storage::mooncake_table::test_utils::test_table(
            &context,
            "t_fullrow",
            IdentityProp::FullRow,
        )
        .await;

        let file1 = data_dir.join("file1.parquet");
        let b1 = batch_with_rows(&[301, 302, 303]);
        write_parquet_file(&file1, &[b1.clone()]).await;

        let storage_config = crate::StorageConfig::FileSystem {
            root_directory: context.path().to_str().unwrap().to_string(),
            atomic_write_dir: None,
        };
        table
            .batch_ingest(
                vec![file1.to_string_lossy().to_string()],
                storage_config,
                600,
            )
            .await;

        let (files, mut indices) = match &table.next_snapshot_task.new_streaming_xact[0] {
            TransactionStreamOutput::Commit(commit) => {
                (commit.get_flushed_data_files(), commit.get_file_indices())
            }
            _ => panic!("unexpected"),
        };
        assert_eq!(files.len(), 1);
        assert_eq!(indices.len(), 1);
        let index = indices.remove(0);
        let rows = MoonlinkRow::from_record_batch(&b1);
        let k = IdentityProp::FullRow.get_lookup_key(&rows[2]);
        let lookups = crate::storage::index::persisted_bucket_hash_map::GlobalIndex::prepare_hashes_for_lookup(
            vec![k].into_iter(),
        );
        let results = index.search_values(&lookups).await;
        let file_id = files[0].file_id();
        assert!(results.contains(&(k, RecordLocation::DiskFile(file_id, 2))));
    }

    #[tokio::test]
    async fn test_batch_ingest_index_build_failure_proceeds_without_index() {
        let context = TestContext::new("batch_ingest_index_fail");
        let temp_dir = tempdir().unwrap();
        let data_dir = temp_dir.path();
        let mut table = crate::storage::mooncake_table::test_utils::test_table(
            &context,
            "t_keys",
            IdentityProp::Keys(vec![0]),
        )
        .await;

        // Prepare one parquet file
        let file1 = data_dir.join("file1.parquet");
        let b1 = batch_with_rows(&[1, 2, 3]);
        write_parquet_file(&file1, &[b1]).await;

        // Remove the table directory so index block file creation fails
        let table_dir = context.path();
        tokio::fs::remove_dir_all(&table_dir).await.unwrap();

        let storage_config = crate::StorageConfig::FileSystem {
            root_directory: context.path().to_str().unwrap().to_string(),
            atomic_write_dir: None,
        };
        table
            .batch_ingest(
                vec![file1.to_string_lossy().to_string()],
                storage_config,
                300,
            )
            .await;

        match &table.next_snapshot_task.new_streaming_xact[0] {
            TransactionStreamOutput::Abort(_) => panic!("unexpected abort"),
            TransactionStreamOutput::Commit(commit) => {
                assert_eq!(commit.get_flushed_data_files().len(), 1);
                // Index build should have failed and been skipped
                assert!(commit.get_file_indices().is_empty());
            }
        }
    }

    #[tokio::test]
    async fn test_batch_ingest_empty_input() {
        let context = TestContext::new("batch_ingest_empty");
        let mut table = crate::storage::mooncake_table::test_utils::test_table(
            &context,
            "t_empty",
            IdentityProp::Keys(vec![0]),
        )
        .await;

        let storage_config = crate::StorageConfig::FileSystem {
            root_directory: context.path().to_str().unwrap().to_string(),
            atomic_write_dir: None,
        };
        table.batch_ingest(vec![], storage_config, 400).await;

        match &table.next_snapshot_task.new_streaming_xact[0] {
            TransactionStreamOutput::Abort(_) => panic!("unexpected abort"),
            TransactionStreamOutput::Commit(commit) => {
                assert_eq!(commit.get_flushed_data_files().len(), 0);
                let indices = commit.get_file_indices();
                if indices.is_empty() {
                    // ok: no index attached
                } else {
                    // ok: identity set; empty index may be attached
                    assert_eq!(indices.len(), 1);
                    let idx = &indices[0];
                    assert_eq!(idx.files.len(), 0);
                    assert_eq!(idx.num_rows, 0);
                }
            }
        }
    }

    #[tokio::test]
    async fn test_batch_ingest_empty_input_append_only() {
        let context = TestContext::new("batch_ingest_empty_append_only");
        let mut table = crate::storage::mooncake_table::test_utils::test_append_only_table(
            &context,
            "t_empty_append_only",
        )
        .await;

        let storage_config = crate::StorageConfig::FileSystem {
            root_directory: context.path().to_str().unwrap().to_string(),
            atomic_write_dir: None,
        };
        table.batch_ingest(vec![], storage_config, 700).await;

        match &table.next_snapshot_task.new_streaming_xact[0] {
            TransactionStreamOutput::Commit(commit) => {
                assert_eq!(commit.get_flushed_data_files().len(), 0);
                assert!(commit.get_file_indices().is_empty());
            }
            _ => panic!("unexpected"),
        }
    }

    #[tokio::test]
    async fn test_batch_ingest_empty_input_with_identity() {
        let context = TestContext::new("batch_ingest_empty_identity");
        let mut table = crate::storage::mooncake_table::test_utils::test_table(
            &context,
            "t_empty_identity",
            IdentityProp::Keys(vec![0]),
        )
        .await;

        let storage_config = crate::StorageConfig::FileSystem {
            root_directory: context.path().to_str().unwrap().to_string(),
            atomic_write_dir: None,
        };
        table.batch_ingest(vec![], storage_config, 800).await;

        match &table.next_snapshot_task.new_streaming_xact[0] {
            TransactionStreamOutput::Commit(commit) => {
                assert_eq!(commit.get_flushed_data_files().len(), 0);
                let indices = commit.get_file_indices();
                if indices.is_empty() {
                    // ok
                } else {
                    assert_eq!(indices.len(), 1);
                    let idx = &indices[0];
                    assert_eq!(idx.files.len(), 0);
                    assert_eq!(idx.num_rows, 0);
                }
            }
            _ => panic!("unexpected"),
        }
    }
}
