use std::collections::HashSet;
use std::sync::Arc;

use iceberg::io::FileIO;
use iceberg::spec::{
    ManifestContentType, ManifestEntry, ManifestFile, ManifestMetadata, ManifestWriter,
    TableMetadata,
};
use iceberg::Result as IcebergResult;

use crate::storage::table::iceberg::manifest_utils;
use crate::storage::table::iceberg::manifest_utils::ManifestEntryType;

/// Max number of manifest entries in a manifest file, which is expected be cap manifest file's max size ~50MiB.
pub(crate) const DEFAULT_MAX_MANIFEST_ENTRY_COUNT: usize = 25000;

pub(crate) struct DataFileManifestManager<'a> {
    table_metadata: &'a TableMetadata,
    file_io: &'a FileIO,
    data_files_to_remove: &'a HashSet<String>,
    writer: Option<ManifestWriter>,
    /// Number of manifest entries for the active manifest writer.
    cur_manifest_entries_num: usize,
    finalized_manifest_files: Vec<ManifestFile>,
    /// Entries per manifest file before rolling over.
    max_entries_per_manifest: usize,
}

#[cfg(test)]
thread_local! {
    /// Lets tests exercise manifest rollover without tens of thousands of data files.
    pub(crate) static MAX_MANIFEST_ENTRY_COUNT_FOR_TEST: std::cell::Cell<usize> =
        const { std::cell::Cell::new(DEFAULT_MAX_MANIFEST_ENTRY_COUNT) };
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
            cur_manifest_entries_num: 0,
            finalized_manifest_files: Vec::new(),
            #[cfg(not(test))]
            max_entries_per_manifest: DEFAULT_MAX_MANIFEST_ENTRY_COUNT,
            #[cfg(test)]
            max_entries_per_manifest: MAX_MANIFEST_ENTRY_COUNT_FOR_TEST.with(|c| c.get()),
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

    pub(crate) async fn add_manifest_entries(
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
            self.cur_manifest_entries_num += 1;

            // Check whether we need to rollover to a new manifest file.
            if self.cur_manifest_entries_num >= self.max_entries_per_manifest {
                if let Some(writer) = self.writer.take() {
                    let manifest_file = writer.write_manifest_file().await?;
                    self.finalized_manifest_files.push(manifest_file);
                }
                self.writer = None;
                self.cur_manifest_entries_num = 0;
            }
        }
        Ok(())
    }

    /// Finalize the current manifest file and return every manifest written, including the ones
    /// already rolled over.
    pub(crate) async fn finalize(mut self) -> IcebergResult<Vec<ManifestFile>> {
        if let Some(writer) = self.writer.take() {
            let manifest_file = writer.write_manifest_file().await?;
            self.finalized_manifest_files.push(manifest_file);
        }
        Ok(self.finalized_manifest_files)
    }
}
