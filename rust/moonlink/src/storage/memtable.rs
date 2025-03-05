use crate::storage::data_batches::ColumnStoreBuffer;
use crate::storage::delete_buffer::DeletionBuffer;
use arrow::datatypes::Schema;
use pg_replicate::conversions::table_row::TableRow;
use std::collections::HashMap;
use std::sync::Arc;

struct MemIndex {
    /// Hash table mapping primary keys to seg_idx and row_idx
    ///
    hash_table: HashMap<i64, (usize, usize)>,
}

pub struct MemTable {
    /// Column store buffer for storing data
    ///
    pub column_store: ColumnStoreBuffer,

    /// Delete buffer for tracking deletions from the stream
    ///
    delete_buffer: DeletionBuffer,

    /// Mem index for the table
    ///
    mem_index: MemIndex,
}

impl MemTable {
    const MAX_ROWS_PER_BUFFER: usize = 2048;
    pub fn new(schema: Schema) -> Self {
        Self {
            column_store: ColumnStoreBuffer::new(Arc::new(schema), Self::MAX_ROWS_PER_BUFFER),
            delete_buffer: DeletionBuffer::new(),
            mem_index: MemIndex {
                hash_table: HashMap::new(),
            },
        }
    }

    pub fn delete(&mut self, primary_key: i64) {
        self.delete_buffer.delete(primary_key);
        self.column_store
            .delete(self.mem_index.hash_table[&primary_key]);
        self.mem_index.hash_table.remove(&primary_key);
    }

    pub fn append(&mut self, primary_key: i64, row: &TableRow) -> Result<(), crate::error::Error> {
        let (seg_idx, row_idx) = self.column_store.append_row(row)?;
        self.mem_index
            .hash_table
            .insert(primary_key, (seg_idx, row_idx));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Schema;
    use arrow::datatypes::{DataType, Field};
    use pg_replicate::conversions::Cell;

    #[test]
    fn test_mem_table() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, false),
        ]);
        let mut mem_table = MemTable::new(schema);

        // Create arrays properly
        mem_table
            .append(
                1,
                &TableRow {
                    values: vec![
                        Cell::I32(1),
                        Cell::String("John".to_string()),
                        Cell::I32(30),
                    ],
                },
            )
            .unwrap();

        mem_table
            .append(
                2,
                &TableRow {
                    values: vec![
                        Cell::I32(2),
                        Cell::String("Jane".to_string()),
                        Cell::I32(25),
                    ],
                },
            )
            .unwrap();

        mem_table
            .append(
                3,
                &TableRow {
                    values: vec![Cell::I32(3), Cell::String("foo".to_string()), Cell::I32(40)],
                },
            )
            .unwrap();

        mem_table.delete(2);

        assert_eq!(mem_table.mem_index.hash_table.len(), 2);

        println!("{:?}", mem_table.column_store.export().unwrap());
    }
}
