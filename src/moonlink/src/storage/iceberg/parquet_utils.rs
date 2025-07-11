// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Code adapted from iceberg-rust: https://github.com/apache/iceberg-rust

use iceberg::arrow::DEFAULT_MAP_FIELD_NAME;
use iceberg::io::FileIOBuilder;
use iceberg::spec::{
    visit_schema, DataContentType, DataFile, DataFileBuilder, DataFileFormat, ListType, MapType,
    NestedFieldRef, PrimitiveType, Schema, SchemaRef, SchemaVisitor, Struct, StructType,
    TableMetadata,
};
use iceberg::Result as IcebergResult;
use iceberg::{Error as IcebergError, ErrorKind};
use itertools::Itertools;
use parquet::file::metadata::ParquetMetaData;

use crate::storage::iceberg::parquet_metadata_utils;
use crate::storage::iceberg::parquet_stats_utils::MinMaxColAggregator;

use std::collections::HashMap;
use std::sync::Arc;

// ================================
// IndexByParquetPathName
// ================================
//
// A mapping from Parquet column path names to iceberg field id.
struct IndexByParquetPathName {
    name_to_id: HashMap<String, i32>,

    field_names: Vec<String>,

    field_id: i32,
}

impl IndexByParquetPathName {
    /// Creates a new, empty `IndexByParquetPathName`
    pub fn new() -> Self {
        Self {
            name_to_id: HashMap::new(),
            field_names: Vec::new(),
            field_id: 0,
        }
    }

    /// Retrieves the internal field ID
    pub fn get(&self, name: &str) -> Option<&i32> {
        self.name_to_id.get(name)
    }
}

impl Default for IndexByParquetPathName {
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaVisitor for IndexByParquetPathName {
    type T = ();

    fn before_struct_field(&mut self, field: &NestedFieldRef) -> IcebergResult<()> {
        self.field_names.push(field.name.to_string());
        self.field_id = field.id;
        Ok(())
    }

    fn after_struct_field(&mut self, _field: &NestedFieldRef) -> IcebergResult<()> {
        self.field_names.pop();
        Ok(())
    }

    fn before_list_element(&mut self, field: &NestedFieldRef) -> IcebergResult<()> {
        self.field_names.push(format!("list.{}", field.name));
        self.field_id = field.id;
        Ok(())
    }

    fn after_list_element(&mut self, _field: &NestedFieldRef) -> IcebergResult<()> {
        self.field_names.pop();
        Ok(())
    }

    fn before_map_key(&mut self, field: &NestedFieldRef) -> IcebergResult<()> {
        self.field_names
            .push(format!("{DEFAULT_MAP_FIELD_NAME}.key"));
        self.field_id = field.id;
        Ok(())
    }

    fn after_map_key(&mut self, _field: &NestedFieldRef) -> IcebergResult<()> {
        self.field_names.pop();
        Ok(())
    }

    fn before_map_value(&mut self, field: &NestedFieldRef) -> IcebergResult<()> {
        self.field_names
            .push(format!("{DEFAULT_MAP_FIELD_NAME}.value"));
        self.field_id = field.id;
        Ok(())
    }

    fn after_map_value(&mut self, _field: &NestedFieldRef) -> IcebergResult<()> {
        self.field_names.pop();
        Ok(())
    }

    fn schema(&mut self, _schema: &Schema, _value: Self::T) -> IcebergResult<Self::T> {
        Ok(())
    }

    fn field(&mut self, _field: &NestedFieldRef, _value: Self::T) -> IcebergResult<Self::T> {
        Ok(())
    }

    fn r#struct(&mut self, _struct: &StructType, _results: Vec<Self::T>) -> IcebergResult<Self::T> {
        Ok(())
    }

    fn list(&mut self, _list: &ListType, _value: Self::T) -> IcebergResult<Self::T> {
        Ok(())
    }

    fn map(
        &mut self,
        _map: &MapType,
        _key_value: Self::T,
        _value: Self::T,
    ) -> IcebergResult<Self::T> {
        Ok(())
    }

    fn primitive(&mut self, _p: &PrimitiveType) -> IcebergResult<Self::T> {
        let full_name = self.field_names.iter().map(String::as_str).join(".");
        let field_id = self.field_id;
        if let Some(existing_field_id) = self.name_to_id.get(full_name.as_str()) {
            return Err(IcebergError::new(
                ErrorKind::DataInvalid,
                format!(
                    "Invalid schema: multiple fields for name {full_name}: {field_id} and {existing_field_id}"
                ),
            ));
        } else {
            self.name_to_id.insert(full_name, field_id);
        }

        Ok(())
    }
}

// ================================
// parquet_to_data_file_builder
// ================================
//
// `ParquetMetadata` to data file builder
fn parquet_to_data_file_builder(
    schema: SchemaRef,
    metadata: Arc<ParquetMetaData>,
    written_size: usize,
    file_path: String,
    nan_value_counts: HashMap<i32, u64>,
) -> IcebergResult<DataFileBuilder> {
    let index_by_parquet_path = {
        let mut visitor = IndexByParquetPathName::new();
        visit_schema(&schema, &mut visitor)?;
        visitor
    };

    let (column_sizes, value_counts, null_value_counts, (lower_bounds, upper_bounds)) = {
        let mut per_col_size: HashMap<i32, u64> = HashMap::new();
        let mut per_col_val_num: HashMap<i32, u64> = HashMap::new();
        let mut per_col_null_val_num: HashMap<i32, u64> = HashMap::new();
        let mut min_max_agg = MinMaxColAggregator::new(schema);

        for row_group in metadata.row_groups() {
            for column_chunk_metadata in row_group.columns() {
                let parquet_path = column_chunk_metadata.column_descr().path().string();

                let Some(&field_id) = index_by_parquet_path.get(&parquet_path) else {
                    continue;
                };

                *per_col_size.entry(field_id).or_insert(0) +=
                    column_chunk_metadata.compressed_size() as u64;
                *per_col_val_num.entry(field_id).or_insert(0) +=
                    column_chunk_metadata.num_values() as u64;

                if let Some(statistics) = column_chunk_metadata.statistics() {
                    if let Some(null_count) = statistics.null_count_opt() {
                        *per_col_null_val_num.entry(field_id).or_insert(0) += null_count;
                    }

                    min_max_agg.update(field_id, statistics.clone())?;
                }
            }
        }
        (
            per_col_size,
            per_col_val_num,
            per_col_null_val_num,
            min_max_agg.produce(),
        )
    };

    let mut builder = DataFileBuilder::default();
    builder
        .content(DataContentType::Data)
        .file_path(file_path)
        .file_format(DataFileFormat::Parquet)
        .partition(Struct::empty())
        .record_count(metadata.file_metadata().num_rows() as u64)
        .file_size_in_bytes(written_size as u64)
        .column_sizes(column_sizes)
        .value_counts(value_counts)
        .null_value_counts(null_value_counts)
        .nan_value_counts(nan_value_counts)
        // # NOTE:
        // - We can ignore implementing distinct_counts due to this: https://lists.apache.org/thread/j52tsojv0x4bopxyzsp7m7bqt23n5fnd
        .lower_bounds(lower_bounds)
        .upper_bounds(upper_bounds)
        .split_offsets(
            metadata
                .row_groups()
                .iter()
                .filter_map(|group| group.file_offset())
                .collect(),
        );

    Ok(builder)
}

// ================================
// get_parquet_metadata
// ================================
//
// Get parquet metadata and file size for the given local parquet file.
async fn get_parquet_metadata(
    local_parquet_file: &str,
) -> IcebergResult<(ParquetMetaData, usize /*file size*/)> {
    let file_io = FileIOBuilder::new_fs_io().build().unwrap();
    let input_file = file_io.new_input(local_parquet_file)?;
    let file_metadata = input_file.metadata().await?;
    let file_size = file_metadata.size as usize;

    let parquet_metadata =
        parquet_metadata_utils::get_parquet_metadata(file_metadata, input_file).await?;

    Ok((parquet_metadata, file_size))
}

// ================================
// get_data_file_from_local_parquet_file
// ================================
//
// Get iceberg `DataFile` which is used to import into iceberg table from a local parquet file.
pub(crate) async fn get_data_file_from_local_parquet_file(
    local_parquet_file: &str,
    remote_parquet_file: String,
    table_metadata: &TableMetadata,
) -> IcebergResult<DataFile> {
    let (parquet_metadata, file_size) = get_parquet_metadata(local_parquet_file).await?;
    let mut builder = parquet_to_data_file_builder(
        table_metadata.current_schema().clone(),
        Arc::new(parquet_metadata),
        file_size,
        remote_parquet_file,
        // TODO: Implement nan_value_counts here
        HashMap::new(),
    )?;
    builder.partition_spec_id(table_metadata.default_partition_spec_id());

    builder.build().map_err(|e| {
        IcebergError::new(
            ErrorKind::Unexpected,
            format!("Failed to get data file because {e:?}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::record_batch::RecordBatch;
    use arrow_array::{Int32Array, Int64Array};
    use iceberg::arrow as IcebergArrow;
    use parquet::{arrow::AsyncArrowWriter, file::properties::WriterProperties};
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_get_data_file_from_parquet() {
        // Write parquet files to local filesystem.
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false)
                .with_metadata(HashMap::from([(
                    "PARQUET:field_id".to_string(),
                    "1".to_string(),
                )])),
            arrow::datatypes::Field::new("age", arrow::datatypes::DataType::Int64, false)
                .with_metadata(HashMap::from([(
                    "PARQUET:field_id".to_string(),
                    "2".to_string(),
                )])),
        ]));
        let record_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
            ],
        )
        .unwrap();

        let tmp_dir = tempdir().unwrap();
        let local_filepath = tmp_dir.path().join("src.parquet");
        let remote_filepath = tmp_dir.path().join("dst.parquet");
        let file = tokio::fs::File::create(&local_filepath).await.unwrap();
        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::UNCOMPRESSED)
            .build();

        let mut writer = AsyncArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
        writer.write(&record_batch).await.unwrap();
        writer.close().await.unwrap();

        // Get data file from local parquet file.
        let (parquet_metadata, file_size) = get_parquet_metadata(local_filepath.to_str().unwrap())
            .await
            .unwrap();
        let iceberg_schema = IcebergArrow::arrow_schema_to_schema(&schema).unwrap();
        let mut data_file_builder = parquet_to_data_file_builder(
            Arc::new(iceberg_schema),
            Arc::new(parquet_metadata),
            file_size,
            remote_filepath.to_str().unwrap().to_string(),
            /*nan_value_counts=*/ HashMap::new(),
        )
        .unwrap();
        data_file_builder.partition_spec_id(0);
        let data_file = data_file_builder.build().unwrap();

        assert_eq!(data_file.content_type(), DataContentType::Data);
        assert_eq!(data_file.file_path(), remote_filepath.to_str().unwrap());
        assert_eq!(data_file.file_format(), DataFileFormat::Parquet);
        assert_eq!(data_file.record_count(), 4);
    }
}
