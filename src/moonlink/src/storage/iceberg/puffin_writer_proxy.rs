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

use crate::storage::iceberg::deletion_vector::{
    DELETION_VECTOR_CADINALITY, DELETION_VECTOR_REFERENCED_DATA_FILE,
};
use crate::storage::iceberg::index::{MOONCAKE_HASH_INDEX_V1, MOONCAKE_HASH_INDEX_V1_CARDINALITY};

use std::collections::{HashMap, HashSet};
use uuid::Uuid;

use iceberg::io::FileIO;
use iceberg::puffin::{CompressionCodec, PuffinWriter, DELETION_VECTOR_V1};
use iceberg::spec::{
    DataContentType, DataFile, DataFileFormat, Datum, FormatVersion, ManifestContentType,
    ManifestListWriter, ManifestWriter, ManifestWriterBuilder, Snapshot, Struct, TableMetadata,
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
    r#type: String,
    fields: Vec<i32>,
    snapshot_id: i64,
    sequence_number: i64,
    offset: u64,
    length: u64,
    compression_codec: CompressionCodec,
    properties: HashMap<String, String>,
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
    content: DataContentType,
    /// field id: 100
    ///
    /// Full URI for the file with FS scheme
    file_path: String,
    /// field id: 101
    ///
    /// String file format name, `avro`, `orc`, `parquet`, or `puffin`
    file_format: DataFileFormat,
    /// field id: 102
    ///
    /// Partition data tuple, schema based on the partition spec output using
    /// partition field ids for the struct field ids
    partition: Struct,
    /// field id: 103
    ///
    /// Number of records in this file, or the cardinality of a deletion vector
    record_count: u64,
    /// field id: 104
    ///
    /// Total file size in bytes
    file_size_in_bytes: u64,
    /// field id: 108
    /// key field id: 117
    /// value field id: 118
    ///
    /// Map from column id to the total size on disk of all regions that
    /// store the column. Does not include bytes necessary to read other
    /// columns, like footers. Leave null for row-oriented formats (Avro)
    column_sizes: HashMap<i32, u64>,
    /// field id: 109
    /// key field id: 119
    /// value field id: 120
    ///
    /// Map from column id to number of values in the column (including null
    /// and NaN values)
    value_counts: HashMap<i32, u64>,
    /// field id: 110
    /// key field id: 121
    /// value field id: 122
    ///
    /// Map from column id to number of null values in the column
    null_value_counts: HashMap<i32, u64>,
    /// field id: 137
    /// key field id: 138
    /// value field id: 139
    ///
    /// Map from column id to number of NaN values in the column
    nan_value_counts: HashMap<i32, u64>,
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
    lower_bounds: HashMap<i32, Datum>,
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
    upper_bounds: HashMap<i32, Datum>,
    /// field id: 131
    ///
    /// Implementation-specific key metadata for encryption
    key_metadata: Option<Vec<u8>>,
    /// field id: 132
    /// element field id: 133
    ///
    /// Split offsets for the data file. For example, all row group offsets
    /// in a Parquet file. Must be sorted ascending
    split_offsets: Vec<i64>,
    /// field id: 135
    /// element field id: 136
    ///
    /// Field ids used to determine row equality in equality delete files.
    /// Required when content is EqualityDeletes and should be null
    /// otherwise. Fields with ids listed in this column must be present
    /// in the delete file
    equality_ids: Vec<i32>,
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
    sort_order_id: Option<i32>,
    /// field id: 142
    ///
    /// The _row_id for the first row in the data file.
    /// For more details, refer to https://github.com/apache/iceberg/blob/main/format/spec.md#first-row-id-inheritance
    pub(crate) first_row_id: Option<i64>,
    /// This field is not included in spec. It is just store in memory representation used
    /// in process.
    partition_spec_id: i32,
    /// field id: 143
    ///
    /// Fully qualified location (URI with FS scheme) of a data file that all deletes reference.
    /// Position delete metadata can use `referenced_data_file` when all deletes tracked by the
    /// entry are in a single data file. Setting the referenced file is required for deletion vectors.
    referenced_data_file: Option<String>,
    /// field: 144
    ///
    /// The offset in the file where the content starts.
    /// The `content_offset` and `content_size_in_bytes` fields are used to reference a specific blob
    /// for direct access to a deletion vector. For deletion vectors, these values are required and must
    /// exactly match the `offset` and `length` stored in the Puffin footer for the deletion vector blob.
    content_offset: Option<i64>,
    /// field: 145
    ///
    /// The length of a referenced content stored in the file; required if `content_offset` is present
    content_size_in_bytes: Option<i64>,
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

/// Util function to get `DataFileProxy` for new file index puffin blob.
fn get_data_file_for_file_index(
    puffin_filepath: &str,
    blob_metadata: &PuffinBlobMetadataProxy,
) -> DataFile {
    assert_eq!(blob_metadata.r#type, MOONCAKE_HASH_INDEX_V1);
    let data_file_proxy = DataFileProxy {
        content: DataContentType::Data,
        file_path: puffin_filepath.to_string(),
        file_format: DataFileFormat::Puffin,
        partition: Struct::empty(),
        record_count: blob_metadata
            .properties
            .get(MOONCAKE_HASH_INDEX_V1_CARDINALITY)
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
        referenced_data_file: None,
        content_offset: None,
        content_size_in_bytes: None,
    };
    unsafe { std::mem::transmute::<DataFileProxy, DataFile>(data_file_proxy) }
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

/// Util function to create manifest list writer and delete current one.
async fn create_new_manifest_list_writer(
    table_metadata: &TableMetadata,
    cur_snapshot: &Snapshot,
    file_io: &FileIO,
) -> IcebergResult<ManifestListWriter> {
    file_io.delete(cur_snapshot.manifest_list()).await?;
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

/// Util function to create manifest write.
fn create_manifest_writer_builder(
    table_metadata: &TableMetadata,
    file_io: &FileIO,
) -> IcebergResult<ManifestWriterBuilder> {
    let manifest_writer_builder = ManifestWriterBuilder::new(
        file_io.new_output(format!(
            "{}/metadata/{}-m0.avro",
            table_metadata.location(),
            Uuid::new_v4()
        ))?,
        table_metadata.current_snapshot_id(),
        /*key_metadata=*/ vec![],
        table_metadata.current_schema().clone(),
        table_metadata.default_partition_spec().as_ref().clone(),
    );
    Ok(manifest_writer_builder)
}

/// Get all manifest files, keep data files unchanged, and merge existing deletion vectors with puffion deletion vector blob and rewrite it.
/// For more details, please refer to https://docs.google.com/document/d/1fIvrRfEHWBephsX0Br2G-Ils_30JIkmGkcdbFbovQjI/edit?usp=sharing
///
/// Note: this function should be called before catalog transaction commit.
///
/// TODO(hjiang): There're too many sequential IO operations to rewrite deletion vectors, need to optimize.
pub(crate) async fn append_puffin_metadata_and_rewrite(
    table_metadata: &TableMetadata,
    file_io: &FileIO,
    puffin_blobs_to_add: &HashMap<String, Vec<PuffinBlobMetadataProxy>>,
    puffin_blobs_to_remove: &HashSet<String>,
) -> IcebergResult<()> {
    if puffin_blobs_to_add.is_empty() {
        return Ok(());
    }

    let cur_snapshot = table_metadata.current_snapshot().unwrap();
    let manifest_list = cur_snapshot
        .load_manifest_list(file_io, table_metadata)
        .await?;

    // Delete existing manifest list file and rewrite.
    let mut manifest_list_writer =
        create_new_manifest_list_writer(table_metadata, cur_snapshot, file_io).await?;

    // Rewrite the deletion vector manifest files.
    // TODO(hjiang): Double confirm for deletion vector manifest filename.
    let mut deletion_vector_manifest_writer: Option<ManifestWriter> = None;
    let mut file_index_manifest_writer: Option<ManifestWriter> = None;
    let init_deletion_vector_manifest_writer_for_once =
        |writer: &mut Option<ManifestWriter>| -> IcebergResult<()> {
            if writer.is_some() {
                return Ok(());
            }
            let new_writer_builder = create_manifest_writer_builder(table_metadata, file_io)?;
            let new_writer = new_writer_builder.build_v2_deletes();
            *writer = Some(new_writer);
            Ok(())
        };

    // Write new file index manifest files.
    let init_file_index_manifest_writer =
        |writer: &mut Option<ManifestWriter>| -> IcebergResult<()> {
            if writer.is_some() {
                return Ok(());
            }
            let new_writer_builder = create_manifest_writer_builder(table_metadata, file_io)?;
            let new_writer = new_writer_builder.build_v2_data();
            *writer = Some(new_writer);
            Ok(())
        };

    // Map from referenced data file to deletion vector manifest entry.
    let mut existing_deletion_vector_entries = HashMap::new();

    // Iterate through all manifest files, keep data manifest files, process hash index files, and merge all deletion vectors.
    for cur_manifest_file in manifest_list.entries() {
        let manifest = cur_manifest_file.load_manifest(file_io).await?;
        let (manifest_entries, manifest_metadata) = manifest.into_parts();

        // Keep data files unchanged.
        // Assumption: we store all data file manifest entries in one manifest file.
        assert!(!manifest_entries.is_empty());
        if *manifest_metadata.content() == ManifestContentType::Data
            && manifest_entries.first().unwrap().file_format() == DataFileFormat::Parquet
        {
            manifest_list_writer.add_manifests([cur_manifest_file.clone()].into_iter())?;
            continue;
        }

        // Process deletion vector puffin files.
        for cur_manifest_entry in manifest_entries.into_iter() {
            assert_eq!(cur_manifest_entry.file_format(), DataFileFormat::Puffin,);

            // Process file indices: skip those requested to remove, and keep those un-mentioned.
            if *manifest_metadata.content() == ManifestContentType::Data {
                // Skip file indices which are requested to remove.
                if puffin_blobs_to_remove.contains(cur_manifest_entry.data_file().file_path()) {
                    continue;
                }

                // Keep file indices which are not requested to remove.
                init_file_index_manifest_writer(&mut file_index_manifest_writer)?;
                file_index_manifest_writer.as_mut().unwrap().add_file(
                    cur_manifest_entry.data_file().clone(),
                    cur_manifest_entry.sequence_number().unwrap(),
                )?;
                continue;
            }

            // Process deletion vectors.
            assert_eq!(*manifest_metadata.content(), ManifestContentType::Deletes);
            let old_entry = existing_deletion_vector_entries.insert(
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
    }

    // Append puffin blobs into existing manifest entries.
    for (puffin_filepath, blob_metadata) in puffin_blobs_to_add.iter() {
        for cur_blob_metadata in blob_metadata.iter() {
            // Handle mooncake hash index v1.
            if cur_blob_metadata.r#type == MOONCAKE_HASH_INDEX_V1 {
                let data_file = get_data_file_for_file_index(puffin_filepath, cur_blob_metadata);
                init_file_index_manifest_writer(&mut file_index_manifest_writer)?;
                file_index_manifest_writer
                    .as_mut()
                    .unwrap()
                    .add_file(data_file, cur_blob_metadata.sequence_number)?;
                continue;
            }

            // Handle deletion vectors.
            let (referenced_data_filepath, data_file) =
                get_data_file_for_deletion_vector(puffin_filepath, cur_blob_metadata);
            existing_deletion_vector_entries.remove(&referenced_data_filepath);
            init_deletion_vector_manifest_writer_for_once(&mut deletion_vector_manifest_writer)?;
            deletion_vector_manifest_writer
                .as_mut()
                .unwrap()
                .add_file(data_file, cur_blob_metadata.sequence_number)?;
        }
    }

    // Add old deletion vector entries which doesn't get overwritten.
    for (_, cur_manifest_entry) in existing_deletion_vector_entries.drain() {
        init_deletion_vector_manifest_writer_for_once(&mut deletion_vector_manifest_writer)?;
        deletion_vector_manifest_writer.as_mut().unwrap().add_file(
            cur_manifest_entry.data_file().clone(),
            cur_manifest_entry.sequence_number().unwrap(),
        )?;
    }

    // Flush manifest file.
    if file_index_manifest_writer.is_some() {
        let index_file_manifest = file_index_manifest_writer
            .take()
            .unwrap()
            .write_manifest_file()
            .await?;
        manifest_list_writer.add_manifests(std::iter::once(index_file_manifest))?;
    }
    if deletion_vector_manifest_writer.is_some() {
        let deletion_vector_manifest = deletion_vector_manifest_writer
            .take()
            .unwrap()
            .write_manifest_file()
            .await?;
        manifest_list_writer.add_manifests(std::iter::once(deletion_vector_manifest))?;
    }

    // Flush the manifest list, there's no need to rewrite metadata.
    manifest_list_writer.close().await?;

    Ok(())
}
