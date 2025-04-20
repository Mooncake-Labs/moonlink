use crate::error::Result;
use arrow::array::BooleanArray;
use arrow::compute;
use arrow::record_batch::RecordBatch;
use arrow::util::bit_util;

#[derive(Debug)]
pub(super) struct BatchDeletionVector {
    /// Boolean array tracking deletions (false = deleted, true = active)
    deletion_vector: Option<Vec<u8>>,

    /// Maximum number of rows this buffer can track
    max_rows: usize,
}

impl BatchDeletionVector {
    /// Create a new delete buffer with the specified capacity
    pub(super) fn new(max_rows: usize) -> Self {
        Self {
            deletion_vector: None,
            max_rows,
        }
    }

    /// Mark a row as deleted
    pub(super) fn delete_row(&mut self, row_idx: usize) -> bool {
        // Set the bit at row_idx to 1 (deleted)
        if self.deletion_vector.is_none() {
            self.deletion_vector = Some(vec![0xFF; self.max_rows / 8 + 1]);
            for i in self.max_rows..(self.max_rows / 8 + 1) * 8 {
                bit_util::unset_bit(self.deletion_vector.as_mut().unwrap(), i);
            }
        }
        let exist = bit_util::get_bit(self.deletion_vector.as_ref().unwrap(), row_idx);
        if exist {
            bit_util::unset_bit(self.deletion_vector.as_mut().unwrap(), row_idx);
        }
        exist
    }

    /// Apply the deletion vector to filter a record batch
    pub(super) fn apply_to_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        if self.deletion_vector.is_none() {
            return Ok(batch.clone());
        }
        let filter = BooleanArray::new_from_u8(self.deletion_vector.as_ref().unwrap())
            .slice(0, batch.num_rows());
        // Apply the filter to the batch
        let filtered_batch = compute::filter_record_batch(batch, &filter)?;
        Ok(filtered_batch)
    }

    pub(super) fn is_deleted(&self, row_idx: usize) -> bool {
        if self.deletion_vector.is_none() {
            false
        } else {
            !bit_util::get_bit(self.deletion_vector.as_ref().unwrap(), row_idx)
        }
    }

    pub(super) fn collect_active_rows(&self) -> Vec<usize> {
        let Some(bitmap) = &self.deletion_vector else {
            return (0..self.max_rows).collect();
        };
        (0..self.max_rows)
            .filter(move |i| bit_util::get_bit(bitmap, *i))
            .collect()
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
        buffer.delete_row(1);
        buffer.delete_row(3);

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

    #[test]
    fn test_into_iter() {
        // Create a delete vector
        let mut buffer = BatchDeletionVector::new(10);

        // Delete rows 1, 3, and 8
        buffer.delete_row(1);
        buffer.delete_row(3);
        buffer.delete_row(8);

        // Check that the iterator returns those positions
        let deleted_rows: Vec<usize> = buffer.collect_active_rows();
        assert_eq!(deleted_rows, vec![0, 2, 4, 5, 6, 7, 9]);
    }
}
