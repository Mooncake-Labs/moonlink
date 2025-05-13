use super::moonlink_type::RowValue;
use ahash::AHasher;
use arrow::array::Array;
use arrow::datatypes::Field;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ArrowReaderBuilder;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::mem::take;

#[derive(Debug)]
pub struct MoonlinkRow {
    pub values: Vec<RowValue>,
}

impl MoonlinkRow {
    pub fn new(values: Vec<RowValue>) -> Self {
        Self { values }
    }

    pub fn equals_record_batch_at_offset(
        &self,
        batch: &RecordBatch,
        offset: usize,
        identity: &IdentityProp,
    ) -> bool {
        if offset >= batch.num_rows() {
            panic!("Offset is out of bounds");
        }

        // Helper closure to compare a value with a column at offset
        let value_matches_column = |value: &RowValue, column: &arrow::array::ArrayRef| -> bool {
            match value {
                RowValue::Int32(v) => {
                    if let Some(array) = column.as_any().downcast_ref::<arrow::array::Int32Array>()
                    {
                        array.value(offset) == *v
                    } else {
                        false
                    }
                }
                RowValue::Int64(v) => {
                    if let Some(array) = column.as_any().downcast_ref::<arrow::array::Int64Array>()
                    {
                        array.value(offset) == *v
                    } else {
                        false
                    }
                }
                RowValue::Float32(v) => {
                    if let Some(array) =
                        column.as_any().downcast_ref::<arrow::array::Float32Array>()
                    {
                        array.value(offset) == *v
                    } else {
                        false
                    }
                }
                RowValue::Float64(v) => {
                    if let Some(array) =
                        column.as_any().downcast_ref::<arrow::array::Float64Array>()
                    {
                        array.value(offset) == *v
                    } else {
                        false
                    }
                }
                RowValue::Decimal(v) => {
                    if let Some(array) = column
                        .as_any()
                        .downcast_ref::<arrow::array::Decimal128Array>()
                    {
                        array.value(offset) == *v
                    } else {
                        false
                    }
                }
                RowValue::Bool(v) => {
                    if let Some(array) =
                        column.as_any().downcast_ref::<arrow::array::BooleanArray>()
                    {
                        array.value(offset) == *v
                    } else {
                        false
                    }
                }
                RowValue::ByteArray(v) => {
                    if let Some(array) = column.as_any().downcast_ref::<arrow::array::BinaryArray>()
                    {
                        array.value(offset) == v.as_slice()
                    } else if let Some(array) =
                        column.as_any().downcast_ref::<arrow::array::StringArray>()
                    {
                        array.value(offset).as_bytes() == v.as_slice()
                    } else {
                        false
                    }
                }
                RowValue::FixedLenByteArray(v) => {
                    if let Some(array) = column
                        .as_any()
                        .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
                    {
                        array.value(offset) == v.as_slice()
                    } else {
                        false
                    }
                }
                RowValue::Array(_) => {
                    panic!("Array not supported");
                }
                RowValue::Struct(_) => {
                    panic!("Struct not supported");
                }
                RowValue::Null => column.is_null(offset),
            }
        };

        if let IdentityProp::Keys(keys) = identity {
            if keys.len() != batch.columns().len() {
                return self
                    .values
                    .iter()
                    .zip(keys.iter())
                    .all(|(value, &col_idx)| {
                        let column = batch.column(col_idx);
                        value_matches_column(value, column)
                    });
            }
        }
        self.values
            .iter()
            .zip(batch.columns())
            .all(|(value, column)| value_matches_column(value, column))
    }

    pub fn equals_parquet_at_offset(
        &self,
        file_name: &str,
        offset: usize,
        identity: &IdentityProp,
    ) -> bool {
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
        self.equals_record_batch_at_offset(&batch, 0, identity)
    }
}

impl MoonlinkRow {
    pub fn equals_full_row(&self, other: &Self, identity: &IdentityProp) -> bool {
        match identity {
            IdentityProp::Keys(keys) => {
                assert_eq!(self.values.len(), keys.len());
                self.values
                    .iter()
                    .eq(keys.iter().map(|k| &other.values[*k]))
            }
            IdentityProp::FullRow => self.values.iter().eq(other.values.iter()),
            IdentityProp::SinglePrimitiveKey(_) => {
                panic!("Never required for equality checkx")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentityProp {
    SinglePrimitiveKey(usize),
    Keys(Vec<usize>),
    FullRow,
}

impl IdentityProp {
    pub fn new_key(columns: Vec<usize>, fields: &[Field]) -> Self {
        if columns.len() == 1 {
            let width = fields[columns[0]].data_type().primitive_width();
            if width == Some(1) || width == Some(2) || width == Some(4) || width == Some(8) {
                IdentityProp::SinglePrimitiveKey(columns[0])
            } else {
                IdentityProp::Keys(columns)
            }
        } else {
            IdentityProp::Keys(columns)
        }
    }

    pub fn extract_identity_columns(&self, mut row: MoonlinkRow) -> Option<MoonlinkRow> {
        match self {
            IdentityProp::SinglePrimitiveKey(_) => None,
            IdentityProp::Keys(keys) => {
                let mut identity_columns = Vec::new();
                for key in keys {
                    identity_columns.push(take(&mut row.values[*key]));
                }
                Some(MoonlinkRow::new(identity_columns))
            }
            IdentityProp::FullRow => Some(row),
        }
    }

    pub fn extract_identity_for_key(&self, row: &MoonlinkRow) -> Option<MoonlinkRow> {
        if let IdentityProp::Keys(keys) = self {
            let mut identity_columns = Vec::new();
            for key in keys {
                identity_columns.push(row.values[*key].clone());
            }
            Some(MoonlinkRow::new(identity_columns))
        } else {
            None
        }
    }

    pub fn get_lookup_key(&self, row: &MoonlinkRow) -> u64 {
        match self {
            IdentityProp::SinglePrimitiveKey(key) => row.values[*key].to_u64_key(),
            IdentityProp::Keys(keys) => {
                let mut hasher = AHasher::default();
                for key in keys {
                    row.values[*key].hash(&mut hasher);
                }
                hasher.finish()
            }
            IdentityProp::FullRow => {
                let mut hasher = AHasher::default();
                for value in row.values.iter() {
                    value.hash(&mut hasher);
                }
                hasher.finish()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::RowValue;

    impl Clone for MoonlinkRow {
        fn clone(&self) -> Self {
            MoonlinkRow::new(self.values.clone())
        }
    }

    #[test]
    fn test_equals_full_row_with_identity() {
        use RowValue::*;

        let row1 = MoonlinkRow::new(vec![Int32(1), Float32(2.0), ByteArray(b"abc".to_vec())]);
        let row2 = MoonlinkRow::new(vec![Int32(1), Float32(2.0), ByteArray(b"abc".to_vec())]);
        let row3 = MoonlinkRow::new(vec![Int32(1), Float32(2.0), ByteArray(b"def".to_vec())]);
        let row4 = MoonlinkRow::new(vec![
            Int32(1),
            Float32(2.0),
            Int32(3), // different type
        ]);

        // Identity using all columns
        let identity_all = IdentityProp::FullRow;
        // Identity using only the first column
        let identity_first = IdentityProp::Keys(vec![0]);

        // All columns: identity_row and full_row
        let id_row1_all = identity_all.extract_identity_columns(row1.clone()).unwrap();

        assert!(id_row1_all.equals_full_row(&row2, &identity_all));
        assert!(!id_row1_all.equals_full_row(&row3, &identity_all));
        assert!(!id_row1_all.equals_full_row(&row4, &identity_all));

        // Only first column matters: all rows should be equal
        let id_row1_first = identity_first
            .extract_identity_columns(row1.clone())
            .unwrap();

        assert!(id_row1_first.equals_full_row(&row2, &identity_first));
        assert!(id_row1_first.equals_full_row(&row3, &identity_first));
        assert!(id_row1_first.equals_full_row(&row4, &identity_first));

        // You can also check that the identity_row equals its own full row
        assert!(id_row1_all.equals_full_row(&row1, &identity_all));
        assert!(id_row1_first.equals_full_row(&row1, &identity_first));
    }
}
