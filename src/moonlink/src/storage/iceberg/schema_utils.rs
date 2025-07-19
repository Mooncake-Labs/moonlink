#[cfg(test)]
use crate::storage::mooncake_table::{
    IcebergSnapshotPayload, TableMetadata as MooncakeTableMetadata,
};
#[cfg(test)]
use crate::storage::storage_utils::MooncakeDataFileRef;
#[cfg(test)]
use iceberg::spec::Schema as IcebergSchema;

/// Schema related utils.
///
#[cfg(test)]
pub(crate) fn assert_is_same_schema(lhs: IcebergSchema, rhs: IcebergSchema) {
    let lhs_highest_field_id = lhs.highest_field_id();
    let rhs_highest_field_id = rhs.highest_field_id();
    assert_eq!(lhs_highest_field_id, rhs_highest_field_id);

    println!("left schema = {lhs:?}");
    println!("right schema = {rhs:?}");

    for cur_field_id in 0..=lhs_highest_field_id {
        let lhs_name = lhs.name_by_field_id(cur_field_id);
        let rhs_name = rhs.name_by_field_id(cur_field_id);
        assert_eq!(lhs_name, rhs_name);
    }
}

/// Validate the given parquet file matches the given schema.
///
/// Precondition: [`data_file`] is stored at local filesystem.
#[cfg(test)]
async fn assert_schema_consistent(
    data_file: &MooncakeDataFileRef,
    mooncake_table_metadata: &MooncakeTableMetadata,
) {
    let file = tokio::fs::File::open(data_file.file_path()).await.unwrap();
    let stream_builder = parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder::new(file)
        .await
        .unwrap();
    let parquet_schema = stream_builder.parquet_schema();
    let arrow_schema =
        parquet::arrow::parquet_to_arrow_schema(parquet_schema, /*key_value_metadata=*/ None)
            .unwrap();
    assert_eq!(arrow_schema, *mooncake_table_metadata.schema);
}

/// Validate all data files within iceberg snapshot payload matches the given schema.
#[cfg(test)]
pub(crate) async fn assert_iceberg_payload_schema_consistent(
    snapshot_payload: &IcebergSnapshotPayload,
    mooncake_table_metadata: &MooncakeTableMetadata,
) {
    // Assert import payload.
    let import_payload = &snapshot_payload.import_payload;
    for cur_data_file in import_payload.data_files.iter() {
        assert_schema_consistent(cur_data_file, mooncake_table_metadata).await;
    }

    // Assert data compaction payload.
    let data_compaction_payload = &snapshot_payload.data_compaction_payload;
    for cur_data_file in data_compaction_payload.new_data_files_to_import.iter() {
        assert_schema_consistent(cur_data_file, mooncake_table_metadata).await;
    }
}
