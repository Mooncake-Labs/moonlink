use iceberg::io::FileIO;
use iceberg::spec::{
    DataFileFormat, ManifestContentType, ManifestEntry, ManifestMetadata, ManifestWriterBuilder,
    TableMetadata,
};
use iceberg::Result as IcebergResult;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ManifestEntryType {
    DataFile,
    DeletionVector,
    FileIndex,
}

/// Entry count for different types of manifest entries.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct ManifestEntryCount {
    /// Number of data files entries.
    pub(crate) data_file_entries: usize,
    /// Number of deletion vector entries.
    pub(crate) deletion_vector_entries: usize,
    /// Number of file indices entries.
    pub(crate) file_indices_entries: usize,
}

/// Util function to get type of the current manifest file.
/// Precondition: one manifest file only stores one type of manifest entries.
pub(crate) fn get_manifest_entry_type(
    manifest_entries: &[Arc<ManifestEntry>],
    manifest_metadata: &ManifestMetadata,
) -> ManifestEntryType {
    let file_format = manifest_entries.first().as_ref().unwrap().file_format();
    if *manifest_metadata.content() == ManifestContentType::Data
        && file_format == DataFileFormat::Parquet
    {
        return ManifestEntryType::DataFile;
    }
    if *manifest_metadata.content() == ManifestContentType::Deletes
        && file_format == DataFileFormat::Puffin
    {
        return ManifestEntryType::DeletionVector;
    }
    assert_eq!(*manifest_metadata.content(), ManifestContentType::Data);
    assert_eq!(file_format, DataFileFormat::Puffin);
    ManifestEntryType::FileIndex
}

/// Util function to create manifest write.
pub(crate) fn create_manifest_writer_builder(
    table_metadata: &TableMetadata,
    file_io: &FileIO,
) -> IcebergResult<ManifestWriterBuilder> {
    let manifest_writer_builder = ManifestWriterBuilder::new(
        file_io.new_output(format!(
            "{}/metadata/{}-m0.avro",
            table_metadata.location(),
            Uuid::now_v7()
        ))?,
        table_metadata.current_snapshot_id(),
        /*key_metadata=*/ None,
        table_metadata.current_schema().clone(),
        table_metadata.default_partition_spec().as_ref().clone(),
    );
    Ok(manifest_writer_builder)
}

/// Get manifest entry number for all types.
#[cfg(all(not(feature = "chaos-test"), any(test, debug_assertions)))]
pub(crate) async fn get_manifest_entries_number(
    table_metadata: &TableMetadata,
    file_io: FileIO,
) -> ManifestEntryCount {
    let mut entry_count = ManifestEntryCount::default();
    let current_snapshot = table_metadata.current_snapshot();
    if current_snapshot.is_none() {
        return entry_count;
    }
    let snapshot_meta = current_snapshot.unwrap();
    let manifest_list = snapshot_meta
        .load_manifest_list(&file_io, table_metadata)
        .await
        .unwrap();

    for manifest_file in manifest_list.entries().iter() {
        let manifest = manifest_file.load_manifest(&file_io).await.unwrap();
        let (manifest_entries, manifest_metadata) = manifest.into_parts();
        let entry_type = get_manifest_entry_type(&manifest_entries, &manifest_metadata);
        if entry_type == ManifestEntryType::DataFile {
            entry_count.data_file_entries += manifest_entries.len();
        } else if entry_type == ManifestEntryType::DeletionVector {
            entry_count.deletion_vector_entries += manifest_entries.len();
        } else {
            entry_count.file_indices_entries += manifest_entries.len();
        }
    }

    entry_count
}
