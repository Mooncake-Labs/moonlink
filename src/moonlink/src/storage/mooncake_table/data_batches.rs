use super::column_array_builder::ColumnArrayBuilder;
use super::delete_vector::BatchDeletionVector;
use crate::error::Result;
use crate::row::MoonlinkRow;
use crate::storage::storage_utils::RecordLocation;
use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::Schema;
use std::sync::Arc;

#[derive(Debug)]
pub(super) struct InMemoryBatch {
    pub(super) data: Option<Arc<RecordBatch>>,
    pub(super) deletions: BatchDeletionVector,
}

impl InMemoryBatch {
    pub fn new(max_rows_per_buffer: usize) -> Self {
        Self {
            data: None,
            deletions: BatchDeletionVector::new(max_rows_per_buffer),
        }
    }

    pub fn get_filtered_batch(&self) -> Result<Option<RecordBatch>> {
        if self.data.is_none() {
            return Ok(None);
        }
        let batch = self
            .deletions
            .apply_to_batch(&self.data.as_ref().unwrap())?;
        Ok(Some(batch))
    }

    pub fn get_filtered_batch_with_limit(&self, row_limit: usize) -> Result<Option<RecordBatch>> {
        assert!(self.data.is_some());
        let batch = self
            .deletions
            .apply_to_batch(&self.data.as_ref().unwrap().slice(0, row_limit))?;
        Ok(Some(batch))
    }
}

#[derive(Debug)]
pub(super) struct BatchEntry {
    pub(super) id: u64,
    pub(super) batch: InMemoryBatch,
}

/// A streaming buffered writer for column-oriented data.
/// Creates new buffers when the current one is full and links them together.
pub(super) struct ColumnStoreBuffer {
    /// The Arrow schema defining the structure of the data
    schema: Arc<Schema>,
    /// Maximum number of rows per buffer before creating a new one
    max_rows_per_buffer: usize,
    /// Collection of record batches including the current one
    in_memory_batches: Vec<BatchEntry>,
    /// Current batch being built
    current_rows: Vec<Box<ColumnArrayBuilder>>,
    /// Current row count in the current buffer
    current_row_count: usize,
    /// Next batch id, each batch is assigned a unique id
    next_batch_id: u64,
}

impl ColumnStoreBuffer {
    /// Initialize a new column store buffer with the given schema and buffer size.
    ///
    pub(super) fn new(schema: Arc<Schema>, max_rows_per_buffer: usize) -> Self {
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
            in_memory_batches: vec![BatchEntry {
                id: 0,
                batch: InMemoryBatch::new(max_rows_per_buffer),
            }],
            current_rows,
            current_row_count: 0,
            next_batch_id: 0,
        }
    }

    /// Append a row of data to the buffer. If the current buffer is full,
    /// finalize it and start a new one.
    ///
    pub(super) fn append_row(
        &mut self,
        row: &MoonlinkRow,
    ) -> Result<(u64, usize, Option<(u64, Arc<RecordBatch>)>)> {
        let mut new_batch = None;
        // Check if we need to finalize the current batch
        if self.current_row_count >= self.max_rows_per_buffer {
            new_batch = self.finalize_current_batch()?;
        }

        row.values.iter().enumerate().for_each(|(i, cell)| {
            let _res = self.current_rows[i].append_value(cell);
            assert!(_res.is_ok());
        });
        self.current_row_count += 1;

        Ok((
            self.in_memory_batches.last().unwrap().id,
            self.current_row_count - 1,
            new_batch,
        ))
    }

    /// Finalize the current batch, adding it to filled_batches and preparing for a new batch
    ///
    pub(super) fn finalize_current_batch(&mut self) -> Result<Option<(u64, Arc<RecordBatch>)>> {
        if self.current_row_count == 0 {
            return Ok(None);
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

        let batch = Arc::new(RecordBatch::try_new(Arc::clone(&self.schema), columns)?);
        let last_batch = self.in_memory_batches.last_mut();
        last_batch.unwrap().batch.data = Some(batch.clone());
        self.next_batch_id += 1;
        self.in_memory_batches.push(BatchEntry {
            id: self.next_batch_id,
            batch: InMemoryBatch::new(self.max_rows_per_buffer),
        });
        // Reset the current batch
        self.current_row_count = 0;

        Ok(Some((self.next_batch_id - 1, batch)))
    }

    pub(super) fn delete_if_exists(&mut self, (batch_id, row_offset): (u64, usize)) -> bool {
        let idx = self
            .in_memory_batches
            .binary_search_by_key(&batch_id, |x| x.id)
            .unwrap();
        self.in_memory_batches[idx]
            .batch
            .deletions
            .delete_row(row_offset)
    }

    pub(super) fn drain(&mut self) -> Vec<BatchEntry> {
        assert!(self.current_row_count == 0);
        let last = self.in_memory_batches.pop();
        let current_batch = std::mem::take(&mut self.in_memory_batches);
        self.in_memory_batches.push(last.unwrap());
        current_batch
    }

    pub(super) fn get_num_rows(&self) -> usize {
        (self.in_memory_batches.len() - 1) * self.max_rows_per_buffer + self.current_row_count
    }

    pub(super) fn get_commit_check_point(&self) -> RecordLocation {
        RecordLocation::MemoryBatch(
            self.in_memory_batches.last().unwrap().id,
            self.current_row_count,
        )
    }
}

pub(super) fn create_batch_from_rows(
    rows: &[MoonlinkRow],
    schema: Arc<Schema>,
    deletions: &BatchDeletionVector,
) -> RecordBatch {
    let mut builders: Vec<Box<ColumnArrayBuilder>> = schema
        .fields()
        .iter()
        .map(|field| {
            Box::new(ColumnArrayBuilder::new(
                field.data_type().clone(),
                rows.len(),
            ))
        })
        .collect();
    for (i, row) in rows.iter().enumerate() {
        if !deletions.is_deleted(i) {
            for (j, _field) in schema.fields().iter().enumerate() {
                let _res = builders[j].append_value(&row.values[j]);
                assert!(_res.is_ok());
            }
        }
    }
    let columns: Vec<ArrayRef> = builders
        .iter_mut()
        .map(|builder| builder.finish())
        .collect();
    RecordBatch::try_new(Arc::clone(&schema), columns).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::RowValue;
    use arrow::datatypes::{DataType, Field};

    #[test]
    fn test_column_store_buffer() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, false),
        ]);

        let mut buffer = ColumnStoreBuffer::new(Arc::new(schema), 2);

        let row1 = MoonlinkRow::new(vec![
            RowValue::Int32(1),
            RowValue::ByteArray("John".as_bytes().to_vec()),
            RowValue::Int32(30),
        ]);

        let row2 = MoonlinkRow::new(vec![
            RowValue::Int32(2),
            RowValue::ByteArray("Jane".as_bytes().to_vec()),
            RowValue::Int32(25),
        ]);

        let row3 = MoonlinkRow::new(vec![
            RowValue::Int32(3),
            RowValue::ByteArray("Bob".as_bytes().to_vec()),
            RowValue::Int32(40),
        ]);

        buffer.append_row(&row1)?;
        buffer.append_row(&row2)?;

        // This should create a new buffer
        buffer.append_row(&row3)?;
        buffer.finalize_current_batch()?;

        let batches = buffer.drain();
        println!("batches: {:?}", batches);
        Ok(())
    }
}
