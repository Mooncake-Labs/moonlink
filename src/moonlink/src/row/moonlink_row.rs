use super::moonlink_type::RowValue;
use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ArrowReaderBuilder;
use std::fs::File;

#[derive(Debug)]
pub struct MoonlinkRow {
    pub values: Vec<RowValue>,
}

impl MoonlinkRow {
    pub fn new(values: Vec<RowValue>) -> Self {
        Self { values }
    }

    pub fn equals_record_batch_at_offset(&self, batch: &RecordBatch, offset: usize) -> bool {
        if offset >= batch.num_rows() {
            panic!("Offset is out of bounds");
        }

        if self.values.len() != batch.num_columns() {
            panic!("MoonlinkRow has a different number of values than the RecordBatch");
        }

        for (value, column) in self.values.iter().zip(batch.columns()) {
            match value {
                RowValue::Int32(v) => {
                    if let Some(array) = column.as_any().downcast_ref::<arrow::array::Int32Array>()
                    {
                        if array.value(offset) != *v {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                RowValue::Int64(v) => {
                    if let Some(array) = column.as_any().downcast_ref::<arrow::array::Int64Array>()
                    {
                        if array.value(offset) != *v {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                RowValue::Float32(v) => {
                    if let Some(array) =
                        column.as_any().downcast_ref::<arrow::array::Float32Array>()
                    {
                        if array.value(offset) != *v {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                RowValue::Float64(v) => {
                    if let Some(array) =
                        column.as_any().downcast_ref::<arrow::array::Float64Array>()
                    {
                        if array.value(offset) != *v {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                RowValue::Decimal(v) => {
                    if let Some(array) = column
                        .as_any()
                        .downcast_ref::<arrow::array::Decimal128Array>()
                    {
                        if array.value(offset) != *v {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                RowValue::Bool(v) => {
                    if let Some(array) =
                        column.as_any().downcast_ref::<arrow::array::BooleanArray>()
                    {
                        if array.value(offset) != *v {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                RowValue::ByteArray(v) => {
                    if let Some(array) = column.as_any().downcast_ref::<arrow::array::BinaryArray>()
                    {
                        if array.value(offset) != v.as_slice() {
                            return false;
                        }
                    } else if let Some(array) =
                        column.as_any().downcast_ref::<arrow::array::StringArray>()
                    {
                        if array.value(offset).as_bytes() != v.as_slice() {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                RowValue::FixedLenByteArray(v) => {
                    if let Some(array) = column
                        .as_any()
                        .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
                    {
                        if array.value(offset) != v.as_slice() {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                RowValue::Null => {
                    if !column.is_null(offset) {
                        return false;
                    }
                }
            }
        }
        true
    }

    pub fn equals_parquet_at_offset(&self, file_name: &str, offset: usize) -> bool {
        let file = File::open(file_name).unwrap();
        let reader_builder = ArrowReaderBuilder::try_new(file).unwrap();
        let row_groups = reader_builder.metadata().row_groups();
        let mut target_row_group = 0;
        let mut row_count: usize = 0;
        for row_group in row_groups {
            if row_count + row_group.num_rows() as usize > offset {
                break;
            }
            row_count += row_group.num_rows() as usize;
            target_row_group += 1;
        }
        let mut reader = reader_builder
            .with_row_groups(vec![target_row_group])
            .with_offset(offset - row_count)
            .with_limit(1)
            .with_batch_size(1)
            .build()
            .unwrap();
        let batch = reader.next().unwrap().unwrap();
        self.equals_record_batch_at_offset(&batch, 0)
    }
}

impl PartialEq for MoonlinkRow {
    fn eq(&self, other: &Self) -> bool {
        self.values == other.values
    }
}
