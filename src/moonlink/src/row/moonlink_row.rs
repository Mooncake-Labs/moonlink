use super::moonlink_type::RowValue;
use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ArrowReaderBuilder;
use std::fs::File;
use std::mem::take;

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
        let min_len = self.values.len().min(other.values.len());
        self.values.iter().take(min_len).eq(other.values.iter().take(min_len))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Identity {
    IntPrimaryKey(usize),
    Keys(Vec<usize>),
    FullRow,
}

impl Identity {
    pub fn may_collide(&self) -> bool {
        match self {
            Identity::IntPrimaryKey(_) => false,
            Identity::Keys(_) => true,
            Identity::FullRow => true,
        }
    }

    pub fn extract_identify_columns(&self, mut row: MoonlinkRow) -> Option<MoonlinkRow> {
        match self {
            Identity::IntPrimaryKey(_) => None,
            Identity::Keys(keys) => {
                let mut identify_columns = Vec::new();
                for key in keys {
                    identify_columns.push(take(&mut row.values[*key]));
                }
                Some(MoonlinkRow::new(identify_columns))
            },
            Identity::FullRow => Some(row),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::RowValue;

    #[test]
    fn test_equals_by_value_prefix_various_types() {
        let row1 = MoonlinkRow::new(vec![
            RowValue::Int32(1),
            RowValue::Float32(2.0),
            RowValue::ByteArray(b"abc".to_vec()),
            RowValue::Null,
        ]);
        let row2 = MoonlinkRow::new(vec![
            RowValue::Int32(1),
            RowValue::Float32(2.0),
            RowValue::ByteArray(b"abc".to_vec()),
        ]);
        let row3 = MoonlinkRow::new(vec![
            RowValue::Int32(1),
            RowValue::Float32(2.0),
            RowValue::ByteArray(b"abcd".to_vec()), // different value
        ]);
        let row4 = MoonlinkRow::new(vec![
            RowValue::Int32(1),
            RowValue::Float32(2.0),
            RowValue::Int32(3), // different type
        ]);
        let row5 = MoonlinkRow::new(vec![
            RowValue::Int32(1),
            RowValue::Float32(2.0),
            RowValue::ByteArray(b"abc".to_vec()),
            RowValue::Null,
            RowValue::Int32(99),
        ]);
        let row_empty = MoonlinkRow::new(vec![]);

        // Prefix match (row2 is prefix of row1)
        assert!(row1 == row2);
        assert!(row2 == row1);

        // Full match
        assert!(row1 == row1);

        // Mismatch in value
        assert!(row1 != row3);
        assert!(row3 != row1);

        // Mismatch in type
        assert!(row1 != row4);
        assert!(row4 != row1);

        // row1 is prefix of row5
        assert!(row5 == row1);
        assert!(row1 == row5);

        // Empty row is prefix of any row
        assert!(row1 == row_empty);
        assert!(row_empty == row1);
    }
}
