// iceberg-rust currently doesn't support puffin related features, to write deletion vector into iceberg metadata, we need two things at least:
// 1. the start offset and blob size for each deletion vector
// 2. append blob metadata into manifest file
// So here to workaround the limitation and to avoid/reduce changes to iceberg-rust ourselves, we use a few proxy types to reinterpret the memory directly.
//
// deletion vector spec:
// issue collection: https://github.com/apache/iceberg/issues/11122
// deletion vector table spec: https://github.com/apache/iceberg/pull/11240
//
// puffin blob spec: https://iceberg.apache.org/puffin-spec/?h=deletion#deletion-vector-v1-blob-type
//
// TODO(hjiang): Add documentation on how we store puffin blobs inside of puffinf file, what's the relationship between puffin file and manifest file, etc.

use crate::storage::iceberg::manifest_utils::{self, ManifestEntryType};

use std::collections::{HashMap, HashSet};

use crate::storage::iceberg::data_file_manifest_manager::DataFileManifestManager;
use crate::storage::iceberg::deletion_vector_manifest_manager::DeletionVectorManifestManager;
use crate::storage::iceberg::file_index_manifest_manager::FileIndexManifestManager;
use iceberg::io::FileIO;
use iceberg::puffin::{CompressionCodec, PuffinWriter};
use iceberg::spec::{
    DataContentType, DataFileFormat, Datum, FormatVersion, ManifestListWriter, Snapshot, Struct,
    TableMetadata,
};
use iceberg::Result as IcebergResult;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
#[allow(dead_code)]
enum PuffinFlagProxy {
    FooterPayloadCompressed = 0,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct PuffinBlobMetadataProxy {
    pub(crate) r#type: String,
    pub(crate) fields: Vec<i32>,
    pub(crate) snapshot_id: i64,
    pub(crate) sequence_number: i64,
    pub(crate) offset: u64,
    pub(crate) length: u64,
    pub(crate) compression_codec: CompressionCodec,
    pub(crate) properties: HashMap<String, String>,
}

#[allow(dead_code)]
struct PuffinWriterProxy {
    writer: Box<dyn iceberg::io::FileWrite>,
    is_header_written: bool,
    num_bytes_written: u64,
    written_blobs_metadata: Vec<PuffinBlobMetadataProxy>,
    properties: HashMap<String, String>,
    footer_compression_codec: CompressionCodec,
    flags: std::collections::HashSet<PuffinFlagProxy>,
}

/// Data file carries data file path, partition tuple, metrics, …
#[derive(Debug, PartialEq, Clone, Eq)]
pub struct DataFileProxy {
    /// field id: 134
    ///
    /// Type of content stored by the data file: data, equality deletes,
    /// or position deletes (all v1 files are data files)
    pub(crate) content: DataContentType,
    /// field id: 100
    ///
    /// Full URI for the file with FS scheme
    pub(crate) file_path: String,
    /// field id: 101
    ///
    /// String file format name, `avro`, `orc`, `parquet`, or `puffin`
    pub(crate) file_format: DataFileFormat,
    /// field id: 102
    ///
    /// Partition data tuple, schema based on the partition spec output using
    /// partition field ids for the struct field ids
    pub(crate) partition: Struct,
    /// field id: 103
    ///
    /// Number of records in this file, or the cardinality of a deletion vector
    pub(crate) record_count: u64,
    /// field id: 104
    ///
    /// Total file size in bytes
    pub(crate) file_size_in_bytes: u64,
    /// field id: 108
    /// key field id: 117
    /// value field id: 118
    ///
    /// Map from column id to the total size on disk of all regions that
    /// store the column. Does not include bytes necessary to read other
    /// columns, like footers. Leave null for row-oriented formats (Avro)
    pub(crate) column_sizes: HashMap<i32, u64>,
    /// field id: 109
    /// key field id: 119
    /// value field id: 120
    ///
    /// Map from column id to number of values in the column (including null
    /// and NaN values)
    pub(crate) value_counts: HashMap<i32, u64>,
    /// field id: 110
    /// key field id: 121
    /// value field id: 122
    ///
    /// Map from column id to number of null values in the column
    pub(crate) null_value_counts: HashMap<i32, u64>,
    /// field id: 137
    /// key field id: 138
    /// value field id: 139
    ///
    /// Map from column id to number of NaN values in the column
    pub(crate) nan_value_counts: HashMap<i32, u64>,
    /// field id: 125
    /// key field id: 126
    /// value field id: 127
    ///
    /// Map from column id to lower bound in the column serialized as binary.
    /// Each value must be less than or equal to all non-null, non-NaN values
    /// in the column for the file.
    ///
    /// Reference:
    ///
    /// - [Binary single-value serialization](https://iceberg.apache.org/spec/#binary-single-value-serialization)
    pub(crate) lower_bounds: HashMap<i32, Datum>,
    /// field id: 128
    /// key field id: 129
    /// value field id: 130
    ///
    /// Map from column id to upper bound in the column serialized as binary.
    /// Each value must be greater than or equal to all non-null, non-Nan
    /// values in the column for the file.
    ///
    /// Reference:
    ///
    /// - [Binary single-value serialization](https://iceberg.apache.org/spec/#binary-single-value-serialization)
    pub(crate) upper_bounds: HashMap<i32, Datum>,
    /// field id: 131
    ///
    /// Implementation-specific key metadata for encryption
    pub(crate) key_metadata: Option<Vec<u8>>,
    /// field id: 132
    /// element field id: 133
    ///
    /// Split offsets for the data file. For example, all row group offsets
    /// in a Parquet file. Must be sorted ascending
    pub(crate) split_offsets: Vec<i64>,
    /// field id: 135
    /// element field id: 136
    ///
    /// Field ids used to determine row equality in equality delete files.
    /// Required when content is EqualityDeletes and should be null
    /// otherwise. Fields with ids listed in this column must be present
    /// in the delete file
    pub(crate) equality_ids: Vec<i32>,
    /// field id: 140
    ///
    /// ID representing sort order for this file.
    ///
    /// If sort order ID is missing or unknown, then the order is assumed to
    /// be unsorted. Only data files and equality delete files should be
    /// written with a non-null order id. Position deletes are required to be
    /// sorted by file and position, not a table order, and should set sort
    /// order id to null. Readers must ignore sort order id for position
    /// delete files.
    pub(crate) sort_order_id: Option<i32>,
    /// field id: 142
    ///
    /// The _row_id for the first row in the data file.
    /// For more details, refer to https://github.com/apache/iceberg/blob/main/format/spec.md#first-row-id-inheritance
    pub(crate) first_row_id: Option<i64>,
    /// This field is not included in spec. It is just store in memory representation used
    /// in process.
    pub(crate) partition_spec_id: i32,
    /// field id: 143
    ///
    /// Fully qualified location (URI with FS scheme) of a data file that all deletes reference.
    /// Position delete metadata can use `referenced_data_file` when all deletes tracked by the
    /// entry are in a single data file. Setting the referenced file is required for deletion vectors.
    pub(crate) referenced_data_file: Option<String>,
    /// field: 144
    ///
    /// The offset in the file where the content starts.
    /// The `content_offset` and `content_size_in_bytes` fields are used to reference a specific blob
    /// for direct access to a deletion vector. For deletion vectors, these values are required and must
    /// exactly match the `offset` and `length` stored in the Puffin footer for the deletion vector blob.
    pub(crate) content_offset: Option<i64>,
    /// field: 145
    ///
    /// The length of a referenced content stored in the file; required if `content_offset` is present
    pub(crate) content_size_in_bytes: Option<i64>,
}

/// Get puffin blob metadata within the puffin write, and close the writer.
/// This function is supposed to be called after all blobs added.
pub(crate) async fn get_puffin_metadata_and_close(
    puffin_writer: PuffinWriter,
) -> IcebergResult<Vec<PuffinBlobMetadataProxy>> {
    let puffin_writer_proxy =
        unsafe { std::mem::transmute::<PuffinWriter, PuffinWriterProxy>(puffin_writer) };
    let puffin_metadata = puffin_writer_proxy.written_blobs_metadata.clone();
    let puffin_writer =
        unsafe { std::mem::transmute::<PuffinWriterProxy, PuffinWriter>(puffin_writer_proxy) };
    puffin_writer.close().await?;
    Ok(puffin_metadata)
}

/// Util function to create manifest list writer and delete current one.
async fn create_new_manifest_list_writer(
    table_metadata: &TableMetadata,
    cur_snapshot: &Snapshot,
    file_io: &FileIO,
) -> IcebergResult<ManifestListWriter> {
    let manifest_list_outfile = file_io.new_output(cur_snapshot.manifest_list())?;

    let latest_seq_no = table_metadata.last_sequence_number();
    let manifest_list_writer = if table_metadata.format_version() == FormatVersion::V1 {
        ManifestListWriter::v1(
            manifest_list_outfile,
            cur_snapshot.snapshot_id(),
            /*parent_snapshot_id=*/ None,
        )
    } else {
        ManifestListWriter::v2(
            manifest_list_outfile,
            cur_snapshot.snapshot_id(),
            /*parent_snapshot_id=*/ None,
            latest_seq_no,
        )
    };
    Ok(manifest_list_writer)
}

/// Get all manifest files and entries,
/// - Data file entries: retain all entries except those marked for removal due to compaction.
/// - Deletion vector entries: remove entries referencing data files to be removed, and merge retained deletion vectors with the provided puffin deletion vector blob.
/// - File indices entries: retain all entries except those marked for removal due to index merging or data file compaction.
///
/// For more details, please refer to https://docs.google.com/document/d/1fIvrRfEHWBephsX0Br2G-Ils_30JIkmGkcdbFbovQjI/edit?usp=sharing
///
/// Note: this function should be called before catalog transaction commit.
///
/// # Arguments:
///
/// * data_files_to_remove: remote data file path, if non empty, both data file and deletion vector manifest entries should be updated.
/// * index_puffin_blobs_to_remove: remote file index puffin file path, if non empty, file index manifest entries should be updated.
///
/// TODO(hjiang):
/// 1. There're too many sequential IO operations to rewrite deletion vectors, need to optimize.
/// 2. Could optimize to avoid file indices manifest file to rewrite.
pub(crate) async fn append_puffin_metadata_and_rewrite(
    table_metadata: &TableMetadata,
    file_io: &FileIO,
    deletion_vector_blobs_to_add: &HashMap<String, Vec<PuffinBlobMetadataProxy>>,
    file_index_blobs_to_add: &HashMap<String, Vec<PuffinBlobMetadataProxy>>,
    data_files_to_remove: &HashSet<String>,
    index_puffin_blobs_to_remove: &HashSet<String>,
) -> IcebergResult<()> {
    if data_files_to_remove.is_empty()
        && deletion_vector_blobs_to_add.is_empty()
        && file_index_blobs_to_add.is_empty()
        && index_puffin_blobs_to_remove.is_empty()
    {
        return Ok(());
    }

    let cur_snapshot = table_metadata.current_snapshot().unwrap();
    let manifest_list = cur_snapshot
        .load_manifest_list(file_io, table_metadata)
        .await?;

    // Delete existing manifest list file and rewrite.
    let mut manifest_list_writer =
        create_new_manifest_list_writer(table_metadata, cur_snapshot, file_io).await?;

    // Manifest manager for data files, deletion vectors and file indices.
    let mut data_file_manifest_manager =
        DataFileManifestManager::new(table_metadata, file_io, data_files_to_remove);
    let mut deletion_vector_manifest_manager =
        DeletionVectorManifestManager::new(table_metadata, file_io, data_files_to_remove);
    let mut file_index_manifest_manager =
        FileIndexManifestManager::new(table_metadata, file_io, index_puffin_blobs_to_remove);

    // How to tell different manifest entry types:
    // - Data file: manifest content type `Data`, manifest entry file format `Parquet`
    // - Deletion vector: manifest content type `Deletes`, manifest entry file format `Puffin`
    // - File indices: manifest content type `Data`, manifest entry file format `Puffin`
    //
    // Precondition for manifest entries updates:
    // - Data file: [`data_files_to_remove`] is non empty.
    // - Deletion vector: [`deletion_vector_blobs_to_add`] is non empty, or [`data_files_to_remove`] is non empty.
    // - File index: [`file_index_blobs_to_add`] is non empty, or [`index_puffin_blobs_to_remove`] is non empty.
    for cur_manifest_file in manifest_list.entries() {
        let manifest = cur_manifest_file.load_manifest(file_io).await?;
        let (manifest_entries, manifest_metadata) = manifest.into_parts();

        // Assumption: we store all data file manifest entries in one manifest file.
        assert!(!manifest_entries.is_empty());

        // Check for data file entries, see if there're updates.
        let manifest_entry_type =
            manifest_utils::get_manifest_entry_type(&manifest_entries, &manifest_metadata);
        if manifest_entry_type == ManifestEntryType::DataFile && data_files_to_remove.is_empty() {
            manifest_list_writer.add_manifests([cur_manifest_file.clone()].into_iter())?;
            continue;
        }

        // Check for deletion vector entries, see if there're updates.
        if manifest_entry_type == ManifestEntryType::DeletionVector
            && deletion_vector_blobs_to_add.is_empty()
            && data_files_to_remove.is_empty()
        {
            manifest_list_writer.add_manifests([cur_manifest_file.clone()].into_iter())?;
            continue;
        }

        // Check for file index entries, see if there're updates.
        if manifest_entry_type == ManifestEntryType::FileIndex
            && file_index_blobs_to_add.is_empty()
            && index_puffin_blobs_to_remove.is_empty()
        {
            manifest_list_writer.add_manifests([cur_manifest_file.clone()].into_iter())?;
            continue;
        }

        match manifest_entry_type {
            ManifestEntryType::DataFile => {
                data_file_manifest_manager
                    .add_manifest_entries(manifest_entries, manifest_metadata)?;
            }
            ManifestEntryType::DeletionVector => {
                deletion_vector_manifest_manager
                    .add_manifest_entries(manifest_entries, manifest_metadata)?;
            }
            ManifestEntryType::FileIndex => {
                file_index_manifest_manager
                    .add_manifest_entries(manifest_entries, manifest_metadata)?;
            }
        }
    }

    // Append puffin blobs into existing manifest entries.
    deletion_vector_manifest_manager.add_new_puffin_blobs(deletion_vector_blobs_to_add)?;
    file_index_manifest_manager.add_new_puffin_blobs(file_index_blobs_to_add)?;

    // Attempt to finalize all existing manifest entries.
    if let Some(manifest_file) = data_file_manifest_manager.finalize().await? {
        manifest_list_writer.add_manifests(std::iter::once(manifest_file))?;
    }
    if let Some(manifest_file) = deletion_vector_manifest_manager.finalize().await? {
        manifest_list_writer.add_manifests(std::iter::once(manifest_file))?;
    }
    if let Some(manifest_file) = file_index_manifest_manager.finalize().await? {
        manifest_list_writer.add_manifests(std::iter::once(manifest_file))?;
    }

    // Flush the manifest list, there's no need to rewrite metadata.
    manifest_list_writer.close().await?;

    Ok(())
}
