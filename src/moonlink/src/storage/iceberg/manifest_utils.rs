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
