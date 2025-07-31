/// Manifest manager for deletion vectors, which correspond to one iceberg table, and one table snapshot.
use crate::storage::iceberg::deletion_vector::{
    DELETION_VECTOR_CADINALITY, DELETION_VECTOR_REFERENCED_DATA_FILE,
};
use crate::storage::iceberg::manifest_utils;
use crate::storage::iceberg::manifest_utils::ManifestEntryType;
use crate::storage::iceberg::puffin_writer_proxy::DataFileProxy;
use crate::storage::iceberg::puffin_writer_proxy::PuffinBlobMetadataProxy;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use iceberg::io::FileIO;
use iceberg::puffin::DELETION_VECTOR_V1;
use iceberg::spec::{
    DataContentType, DataFile, DataFileFormat, ManifestEntry, ManifestFile, ManifestMetadata,
    ManifestWriter, Struct, TableMetadata,
};
use iceberg::Result as IcebergResult;

pub(crate) struct DeletionVectorManifestManager<'a> {
    table_metadata: &'a TableMetadata,
    file_io: &'a FileIO,
    data_files_to_remove: &'a HashSet<String>,
    writer: Option<ManifestWriter>,
    // Map from referenced data file to deletion vector manifest entry.
    existing_deletion_vector_entries: HashMap<String, Arc<ManifestEntry>>,
}

impl<'a> DeletionVectorManifestManager<'a> {
    pub(crate) fn new(
        table_metadata: &'a TableMetadata,
        file_io: &'a FileIO,
        data_files_to_remove: &'a HashSet<String>,
    ) -> Self {
        DeletionVectorManifestManager {
            table_metadata,
            file_io,
            data_files_to_remove,
            writer: None,
            existing_deletion_vector_entries: HashMap::new(),
        }
    }

    fn init_writer_for_once(&mut self) -> IcebergResult<()> {
        if self.writer.is_some() {
            return Ok(());
        }
        let new_writer_builder =
            manifest_utils::create_manifest_writer_builder(self.table_metadata, self.file_io)?;
        let new_writer = new_writer_builder.build_v2_deletes();
        self.writer = Some(new_writer);
        Ok(())
    }

    pub(crate) fn add_manifest_entries(
        &mut self,
        manifest_entries: Vec<Arc<ManifestEntry>>,
        manifest_metadata: ManifestMetadata,
    ) -> IcebergResult<()> {
        assert_eq!(
            manifest_utils::get_manifest_entry_type(&manifest_entries, &manifest_metadata),
            ManifestEntryType::DeletionVector
        );
        for cur_manifest_entry in manifest_entries.into_iter() {
            // Skip deletion vectors which are requested to remove (due to compaction).
            let referenced_data_file = cur_manifest_entry
                .data_file()
                .referenced_data_file()
                .unwrap();
            if self.data_files_to_remove.contains(&referenced_data_file) {
                continue;
            }
            let old_entry = self.existing_deletion_vector_entries.insert(
                cur_manifest_entry
                    .data_file()
                    .referenced_data_file()
                    .unwrap(),
                cur_manifest_entry,
            );
            assert!(
                old_entry.is_none(),
                "Deletion vector for the same data file {:?} appeared for multiple times!",
                old_entry.unwrap().data_file().file_path()
            );
        }
        Ok(())
    }

    pub(crate) fn add_new_puffin_blobs(
        &mut self,
        deletion_vector_blobs_to_add: &HashMap<String, Vec<PuffinBlobMetadataProxy>>,
    ) -> IcebergResult<()> {
        for (puffin_filepath, blob_metadata) in deletion_vector_blobs_to_add.iter() {
            for cur_blob_metadata in blob_metadata.iter() {
                let (referenced_data_filepath, data_file) =
                    get_data_file_for_deletion_vector(puffin_filepath, cur_blob_metadata);
                self.existing_deletion_vector_entries
                    .remove(&referenced_data_filepath);
                self.init_writer_for_once()?;
                self.writer
                    .as_mut()
                    .unwrap()
                    .add_file(data_file, cur_blob_metadata.sequence_number)?;
            }
        }
        Ok(())
    }

    /// Finalize the current manifest file and return.
    pub(crate) async fn finalize(mut self) -> IcebergResult<Option<ManifestFile>> {
        if !self.existing_deletion_vector_entries.is_empty() {
            self.init_writer_for_once()?;
        }

        // Add old deletion vector entries which doesn't get overwritten.
        for (_, cur_manifest_entry) in self.existing_deletion_vector_entries.drain() {
            self.writer.as_mut().unwrap().add_file(
                cur_manifest_entry.data_file().clone(),
                cur_manifest_entry.sequence_number().unwrap(),
            )?;
        }

        if let Some(writer) = self.writer {
            let manifest_file = writer.write_manifest_file().await?;
            return Ok(Some(manifest_file));
        }
        Ok(None)
    }
}

/// Util function to get `DataFileProxy` for deletion vector puffin blob.
fn get_data_file_for_deletion_vector(
    puffin_filepath: &str,
    blob_metadata: &PuffinBlobMetadataProxy,
) -> (String /*referenced_data_filepath*/, DataFile) {
    assert_eq!(blob_metadata.r#type, DELETION_VECTOR_V1);
    let referenced_data_filepath = blob_metadata
        .properties
        .get(DELETION_VECTOR_REFERENCED_DATA_FILE)
        .unwrap()
        .clone();

    let data_file_proxy = DataFileProxy {
        content: DataContentType::PositionDeletes,
        file_path: puffin_filepath.to_string(),
        file_format: DataFileFormat::Puffin,
        partition: Struct::empty(),
        record_count: blob_metadata
            .properties
            .get(DELETION_VECTOR_CADINALITY)
            .unwrap()
            .parse()
            .unwrap(),
        file_size_in_bytes: 0, // TODO(hjiang): Not necessary for puffin blob, but worth double confirm.
        column_sizes: HashMap::new(),
        value_counts: HashMap::new(),
        null_value_counts: HashMap::new(),
        nan_value_counts: HashMap::new(),
        lower_bounds: HashMap::new(),
        upper_bounds: HashMap::new(),
        key_metadata: None,
        split_offsets: Vec::new(),
        equality_ids: Vec::new(),
        sort_order_id: None,
        first_row_id: None,
        partition_spec_id: 0,
        referenced_data_file: Some(referenced_data_filepath.clone()),
        content_offset: Some(blob_metadata.offset as i64),
        content_size_in_bytes: Some(blob_metadata.length as i64),
    };
    let data_file = unsafe { std::mem::transmute::<DataFileProxy, DataFile>(data_file_proxy) };
    (referenced_data_filepath, data_file)
}
