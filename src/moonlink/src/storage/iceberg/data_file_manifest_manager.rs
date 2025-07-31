use std::collections::HashSet;
use std::sync::Arc;

use iceberg::io::FileIO;
use iceberg::spec::{
    ManifestContentType, ManifestEntry, ManifestFile, ManifestMetadata, ManifestWriter,
    TableMetadata,
};
use iceberg::Result as IcebergResult;

use crate::storage::iceberg::manifest_utils;
use crate::storage::iceberg::manifest_utils::ManifestEntryType;

pub(crate) struct DataFileManifestManager<'a> {
    table_metadata: &'a TableMetadata,
    file_io: &'a FileIO,
    data_files_to_remove: &'a HashSet<String>,
    writer: Option<ManifestWriter>,
}

impl<'a> DataFileManifestManager<'a> {
    pub(crate) fn new(
        table_metadata: &'a TableMetadata,
        file_io: &'a FileIO,
        data_files_to_remove: &'a HashSet<String>,
    ) -> Self {
        DataFileManifestManager {
            table_metadata,
            file_io,
            data_files_to_remove,
            writer: None,
        }
    }

    fn init_writer_for_once(&mut self) -> IcebergResult<()> {
        if self.writer.is_some() {
            return Ok(());
        }
        let new_writer_builder =
            manifest_utils::create_manifest_writer_builder(self.table_metadata, self.file_io)?;
        let new_writer = new_writer_builder.build_v2_data();
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
            ManifestEntryType::DataFile
        );
        for cur_manifest_entry in manifest_entries.into_iter() {
            // Process data files, remove those been merged; and compact all data file entries into one manifest file.
            assert_eq!(*manifest_metadata.content(), ManifestContentType::Data);
            if self
                .data_files_to_remove
                .contains(cur_manifest_entry.data_file().file_path())
            {
                continue;
            }
            self.init_writer_for_once()?;
            self.writer.as_mut().unwrap().add_file(
                cur_manifest_entry.data_file().clone(),
                cur_manifest_entry.sequence_number().unwrap(),
            )?;
        }
        Ok(())
    }

    /// Finalize the current manifest file and return.
    pub(crate) async fn finalize(self) -> IcebergResult<Option<ManifestFile>> {
        if let Some(writer) = self.writer {
            let manifest_file = writer.write_manifest_file().await?;
            return Ok(Some(manifest_file));
        }
        Ok(None)
    }
}
