use crate::error::Result;
use arrow::array::BooleanArray;
use arrow::compute;
use arrow::record_batch::RecordBatch;
use arrow::util::bit_util;

struct DeletionRecord {
    _primary_key: i64,
}
/// Buffer for tracking deleted rows from a stream
///
pub struct DeletionBuffer {
    /// Boolean array tracking deletions (false = deleted, true = active)
    deletions: Vec<DeletionRecord>,
}

impl DeletionBuffer {
    pub fn new() -> Self {
        Self {
            deletions: Vec::new(),
        }
    }

    pub fn delete(&mut self, primary_key: i64) {
        self.deletions.push(DeletionRecord {
            _primary_key: primary_key,
        });
    }
}

/// Deletion vector for tracking deleted rows in a batch
pub struct BatchDeletionVector {
    /// Boolean array tracking deletions (false = deleted, true = active)
    deletion_vector: Option<Vec<u8>>,

    /// Maximum number of rows this buffer can track
    max_rows: usize,
}

impl BatchDeletionVector {
    /// Create a new delete buffer with the specified capacity
    pub fn new(max_rows: usize) -> Self {
        Self {
            deletion_vector: None,
            max_rows,
        }
    }

    /// Mark a row as deleted
    pub fn delete_row(&mut self, row_idx: usize) -> Result<()> {
        // Set the bit at row_idx to 1 (deleted)
        if self.deletion_vector.is_none() {
            self.deletion_vector = Some(vec![0xFF; self.max_rows / 8 + 1]);
            for i in self.max_rows..(self.max_rows / 8 + 1) * 8 {
                bit_util::unset_bit(self.deletion_vector.as_mut().unwrap(), i);
            }
        }
        bit_util::unset_bit(self.deletion_vector.as_mut().unwrap(), row_idx);
        Ok(())
    }

    /// Apply the deletion vector to filter a record batch
    pub fn apply_to_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        if self.deletion_vector.is_none() {
            return Ok(batch.clone());
        }
        let filter = BooleanArray::new_from_u8(self.deletion_vector.as_ref().unwrap())
            .slice(0, batch.num_rows());
        // Apply the filter to the batch
        let filtered_batch = compute::filter_record_batch(batch, &filter)?;
        Ok(filtered_batch)
    }

    #[cfg(test)]
    pub fn is_deleted(&self, row_idx: usize) -> bool {
        !bit_util::get_bit(self.deletion_vector.as_ref().unwrap(), row_idx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_delete_buffer() -> Result<()> {
        // Create a delete vector
        let mut buffer = BatchDeletionVector::new(5);
        // Delete some rows
        buffer.delete_row(1)?;
        buffer.delete_row(3)?;

        // Check deletion status
        assert!(!buffer.is_deleted(0));
        assert!(buffer.is_deleted(1));
        assert!(!buffer.is_deleted(2));
        assert!(buffer.is_deleted(3));
        assert!(!buffer.is_deleted(4));

        // Create a test batch
        let name_array = Arc::new(StringArray::from(vec!["A", "B", "C", "D", "E"])) as ArrayRef;
        let age_array = Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])) as ArrayRef;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("name", DataType::Utf8, false),
                Field::new("age", DataType::Int32, false),
            ])),
            vec![name_array, age_array],
        )?;

        // Apply deletion filter
        let filtered = buffer.apply_to_batch(&batch)?;

        // Check filtered batch
        assert_eq!(filtered.num_rows(), 3);
        let filtered_names = filtered
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let filtered_ages = filtered
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        assert_eq!(filtered_names.value(0), "A");
        assert_eq!(filtered_names.value(1), "C");
        assert_eq!(filtered_names.value(2), "E");

        assert_eq!(filtered_ages.value(0), 10);
        assert_eq!(filtered_ages.value(1), 30);
        assert_eq!(filtered_ages.value(2), 50);

        Ok(())
    }
}
