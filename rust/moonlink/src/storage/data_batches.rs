use crate::error::Result;
use crate::storage::column_array_builder::ColumnArrayBuilder;
use crate::storage::delete_buffer::BatchDeletionVector;
use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::Schema;
use pg_replicate::conversions::table_row::TableRow;
use std::sync::Arc;

/// A streaming buffered writer for column-oriented data.
/// Creates new buffers when the current one is full and links them together.
pub struct ColumnStoreBuffer {
    /// The Arrow schema defining the structure of the data
    schema: Arc<Schema>,
    /// Maximum number of rows per buffer before creating a new one
    max_rows_per_buffer: usize,
    /// Collection of record batches that have been filled
    filled_batches: Vec<RecordBatch>,
    /// Current batch being built, effectively filled_batches[n+1]
    current_rows: Vec<Box<ColumnArrayBuilder>>,
    /// Delete vector for each batch including the current one
    batch_dvs: Vec<BatchDeletionVector>,
    /// Current row count in the current buffer
    current_row_count: usize,
}

impl ColumnStoreBuffer {
    /// Initialize a new column store buffer with the given schema and buffer size.
    ///
    /// # Arguments
    ///
    /// * `schema` - The Arrow schema defining the structure of the data
    /// * `max_rows_per_buffer` - Maximum number of rows per buffer before creating a new one
    pub fn new(schema: Arc<Schema>, max_rows_per_buffer: usize) -> Self {
        let current_rows = schema
            .fields()
            .iter()
            .map(|field| {
                Box::new(ColumnArrayBuilder::new(
                    field.data_type().clone(),
                    max_rows_per_buffer,
                ))
            })
            .collect();

        Self {
            schema,
            max_rows_per_buffer,
            filled_batches: Vec::new(),
            current_rows,
            current_row_count: 0,
            batch_dvs: vec![BatchDeletionVector::new(max_rows_per_buffer)],
        }
    }

    /// Append a row of data to the buffer. If the current buffer is full,
    /// finalize it and start a new one.
    ///
    /// # Arguments
    ///
    /// * `row` - A vector of array references representing a single row
    pub fn append_row(&mut self, row: &TableRow) -> Result<(usize, usize)> {
        // Check if we need to finalize the current batch
        if self.current_row_count >= self.max_rows_per_buffer {
            self.finalize_current_batch()?;
        }

        row.values.iter().enumerate().for_each(|(i, cell)| {
            self.current_rows[i].append_value(cell);
        });
        self.current_row_count += 1;

        Ok((self.filled_batches.len(), self.current_row_count - 1))
    }

    /// Finalize the current batch, adding it to filled_batches and preparing for a new batch
    fn finalize_current_batch(&mut self) -> Result<()> {
        if self.current_row_count == 0 {
            return Ok(());
        }

        // Convert the current rows into a RecordBatch
        let columns: Vec<ArrayRef> = self
            .current_rows
            .iter_mut()
            .map(|builder| {
                // Finish the builder to get an array
                Arc::new(builder.finish()) as ArrayRef
            })
            .collect();

        let batch = RecordBatch::try_new(Arc::clone(&self.schema), columns)?;
        self.filled_batches.push(batch);
        self.batch_dvs
            .push(BatchDeletionVector::new(self.max_rows_per_buffer));
        // Reset the current batch
        self.current_row_count = 0;

        Ok(())
    }

    pub fn delete(&mut self, (seg_idx, row_idx): (usize, usize)) {
        self.batch_dvs[seg_idx].delete_row(row_idx).unwrap();
    }

    pub fn export(&self) -> Result<Vec<RecordBatch>> {
        let mut batches = Vec::new();
        // Convert the current rows into a RecordBatch
        let columns: Vec<ArrayRef> = self
            .current_rows
            .iter()
            .map(|builder| {
                // Finish the builder to get an array
                Arc::new(builder.finish_cloned()) as ArrayRef
            })
            .collect();

        batches.push(
            self.batch_dvs
                .last()
                .unwrap()
                .apply_to_batch(&RecordBatch::try_new(Arc::clone(&self.schema), columns)?)
                .unwrap(),
        );
        for (seg_idx, batch) in self.filled_batches.iter().enumerate() {
            let filtered_batch = self.batch_dvs[seg_idx].apply_to_batch(batch)?;
            batches.push(filtered_batch);
        }
        Ok(batches)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};
    use pg_replicate::conversions::table_row::TableRow;
    use pg_replicate::conversions::Cell;

    #[test]
    fn test_column_store_buffer() -> Result<()> {
        // Create a schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, false),
        ]));

        // Create a buffer with capacity for 2 rows
        let mut buffer = ColumnStoreBuffer::new(schema.clone(), 2);

        // Create some test data and append rows
        let row1 = TableRow {
            values: vec![Cell::String("John".to_string()), Cell::I32(25)],
        };
        let row2 = TableRow {
            values: vec![Cell::String("Jane".to_string()), Cell::I32(30)],
        };
        let row3 = TableRow {
            values: vec![Cell::String("Bob".to_string()), Cell::I32(40)],
        };

        buffer.append_row(&row1)?;
        buffer.append_row(&row2)?;

        // This should create a new buffer
        buffer.append_row(&row3)?;

        println!("{:?}", buffer.export().unwrap());

        Ok(())
    }
}
