use crate::rest_ingest::json_converter::{JsonToMoonlinkRowConverter, JsonToMoonlinkRowError};
use arrow_schema::Schema;
use moonlink::row::MoonlinkRow;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::SystemTime;
use thiserror::Error;

pub type SrcTableId = u32;

#[derive(Debug, Error)]
pub enum RestSourceError {
    #[error("json conversion error: {0}")]
    JsonConversion(#[from] JsonToMoonlinkRowError),
    #[error("unknown table: {0}")]
    UnknownTable(String),
    #[error("invalid operation for table: {0}")]
    InvalidOperation(String),
}

#[derive(Debug, Clone)]
pub struct EventRequest {
    pub table_name: String,
    pub operation: EventOperation,
    pub payload: serde_json::Value,
    pub timestamp: SystemTime,
}

#[derive(Debug, Clone)]
pub enum EventOperation {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone)]
pub enum RestEvent {
    RowEvent {
        src_table_id: SrcTableId,
        operation: EventOperation,
        row: MoonlinkRow,
        lsn: u64,
        timestamp: SystemTime,
    },
    Commit {
        lsn: u64,
        timestamp: SystemTime,
    },
}

pub struct RestSource {
    table_schemas: HashMap<String, Arc<Schema>>,
    table_name_to_src_id: HashMap<String, SrcTableId>,
    lsn_generator: AtomicU64,
}

impl Default for RestSource {
    fn default() -> Self {
        Self::new()
    }
}

impl RestSource {
    pub fn new() -> Self {
        Self {
            table_schemas: HashMap::new(),
            table_name_to_src_id: HashMap::new(),
            lsn_generator: AtomicU64::new(1),
        }
    }

    pub fn add_table(&mut self, table_name: String, src_table_id: SrcTableId, schema: Arc<Schema>) {
        self.table_schemas.insert(table_name.clone(), schema);
        self.table_name_to_src_id.insert(table_name, src_table_id);
    }

    pub fn remove_table(&mut self, table_name: &str) {
        self.table_schemas.remove(table_name);
        self.table_name_to_src_id.remove(table_name);
    }

    pub fn process_request(
        &self,
        request: EventRequest,
    ) -> Result<Vec<RestEvent>, RestSourceError> {
        let schema = self
            .table_schemas
            .get(&request.table_name)
            .ok_or_else(|| RestSourceError::UnknownTable(request.table_name.clone()))?;

        let src_table_id = self
            .table_name_to_src_id
            .get(&request.table_name)
            .ok_or_else(|| RestSourceError::UnknownTable(request.table_name.clone()))?;

        let converter = JsonToMoonlinkRowConverter::new(schema.clone());
        let row = converter.convert(&request.payload)?;

        let row_lsn = self.lsn_generator.fetch_add(1, Ordering::SeqCst);
        let commit_lsn = self.lsn_generator.fetch_add(1, Ordering::SeqCst);

        // Generate both a row event and a commit event
        let events = vec![
            RestEvent::RowEvent {
                src_table_id: *src_table_id,
                operation: request.operation,
                row,
                lsn: row_lsn,
                timestamp: request.timestamp,
            },
            RestEvent::Commit {
                lsn: commit_lsn,
                timestamp: request.timestamp,
            },
        ];

        Ok(events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use moonlink::row::RowValue;
    use serde_json::json;
    use std::sync::Arc;

    fn make_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    #[test]
    fn test_rest_source_creation() {
        let mut source = RestSource::new();
        assert_eq!(source.table_schemas.len(), 0);
        assert_eq!(source.table_name_to_src_id.len(), 0);

        // Test adding table
        let schema = make_test_schema();
        source.add_table("test_table".to_string(), 1, schema.clone());
        assert_eq!(source.table_schemas.len(), 1);
        assert_eq!(source.table_name_to_src_id.len(), 1);
        assert!(source.table_schemas.contains_key("test_table"));
        assert_eq!(source.table_name_to_src_id.get("test_table"), Some(&1));

        // Test removing table
        source.remove_table("test_table");
        assert_eq!(source.table_schemas.len(), 0);
        assert_eq!(source.table_name_to_src_id.len(), 0);
    }

    #[test]
    fn test_process_request_success() {
        let mut source = RestSource::new();
        let schema = make_test_schema();
        source.add_table("test_table".to_string(), 1, schema);

        let request = EventRequest {
            table_name: "test_table".to_string(),
            operation: EventOperation::Insert,
            payload: json!({
                "id": 42,
                "name": "test"
            }),
            timestamp: SystemTime::now(),
        };

        let events = source.process_request(request).unwrap();
        assert_eq!(events.len(), 2); // Should have row event + commit event

        // Check row event (first event)
        match &events[0] {
            RestEvent::RowEvent {
                src_table_id,
                operation,
                row,
                lsn,
                ..
            } => {
                assert_eq!(*src_table_id, 1);
                assert!(matches!(operation, EventOperation::Insert));
                assert_eq!(row.values.len(), 2);
                assert_eq!(row.values[0], RowValue::Int32(42));
                assert_eq!(row.values[1], RowValue::ByteArray(b"test".to_vec()));
                assert_eq!(*lsn, 1);
            }
            _ => panic!("Expected RowEvent"),
        }

        // Check commit event (second event)
        match &events[1] {
            RestEvent::Commit { lsn, .. } => {
                assert_eq!(*lsn, 2);
            }
            _ => panic!("Expected Commit event"),
        }
    }

    #[test]
    fn test_process_request_unknown_table() {
        let source = RestSource::new();
        // No schema added

        let request = EventRequest {
            table_name: "unknown_table".to_string(),
            operation: EventOperation::Insert,
            payload: json!({"id": 1}),
            timestamp: SystemTime::now(),
        };

        let err = source.process_request(request).unwrap_err();
        match err {
            RestSourceError::UnknownTable(table_name) => {
                assert_eq!(table_name, "unknown_table");
            }
            _ => panic!("Expected UnknownTable error"),
        }
    }

    #[test]
    fn test_lsn_generation() {
        let mut source = RestSource::new();
        let schema = make_test_schema();
        source.add_table("test_table".to_string(), 1, schema);

        let request1 = EventRequest {
            table_name: "test_table".to_string(),
            operation: EventOperation::Insert,
            payload: json!({"id": 1, "name": "first"}),
            timestamp: SystemTime::now(),
        };

        let request2 = EventRequest {
            table_name: "test_table".to_string(),
            operation: EventOperation::Insert,
            payload: json!({"id": 2, "name": "second"}),
            timestamp: SystemTime::now(),
        };

        let events1 = source.process_request(request1).unwrap();
        let events2 = source.process_request(request2).unwrap();

        // Each request should generate 2 events (row + commit)
        assert_eq!(events1.len(), 2);
        assert_eq!(events2.len(), 2);

        // Check first request events
        match &events1[0] {
            RestEvent::RowEvent { lsn, .. } => assert_eq!(*lsn, 1),
            _ => panic!("Expected RowEvent"),
        }
        match &events1[1] {
            RestEvent::Commit { lsn, .. } => assert_eq!(*lsn, 2),
            _ => panic!("Expected Commit"),
        }

        // Check second request events (LSNs should continue incrementing)
        match &events2[0] {
            RestEvent::RowEvent { lsn, .. } => assert_eq!(*lsn, 3),
            _ => panic!("Expected RowEvent"),
        }
        match &events2[1] {
            RestEvent::Commit { lsn, .. } => assert_eq!(*lsn, 4),
            _ => panic!("Expected Commit"),
        }
    }
}
