use crate::rest_ingest::event_request::{
    EventRequest, FileEventOperation, FileEventRequest, RowEventOperation, RowEventRequest,
};
use crate::rest_ingest::json_converter::{JsonToMoonlinkRowConverter, JsonToMoonlinkRowError};
use crate::rest_ingest::rest_event::RestEvent;
use crate::Result;
use arrow_schema::Schema;
use bytes::Bytes;
use moonlink::row::MoonlinkRow;
use moonlink::{
    AccessorConfig, BaseFileSystemAccess, FileSystemAccessor, FsRetryConfig, FsTimeoutConfig,
    StorageConfig,
};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;

pub type SrcTableId = u32;

#[derive(Debug, Error)]
pub enum RestSourceError {
    #[error("json conversion error: {0}")]
    JsonConversion(#[from] JsonToMoonlinkRowError),
    #[error("unknown table: {0}")]
    UnknownTable(String),
    #[error("invalid operation for table: {0}")]
    InvalidOperation(String),
    #[error("duplicate table created: {0}")]
    DuplicateTable(String),
    #[error("non-existent table to remove: {0}")]
    NonExistentTable(String),
}

pub struct RestSource {
    table_schemas: HashMap<String, Arc<Schema>>,
    src_table_name_to_src_id: HashMap<String, SrcTableId>,
    lsn_generator: Arc<AtomicU64>,
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
            src_table_name_to_src_id: HashMap::new(),
            lsn_generator: Arc::new(AtomicU64::new(1)),
        }
    }

    /// # Arguments
    ///
    /// * persist_lsn: only assigned at recovery, used to indicate and update replication LSN.
    pub fn add_table(
        &mut self,
        src_table_name: String,
        src_table_id: SrcTableId,
        schema: Arc<Schema>,
        persist_lsn: Option<u64>,
    ) -> Result<()> {
        // Update LSN at recovery.
        if let Some(persist_lsn) = persist_lsn {
            let old_lsn = self.lsn_generator.load(Ordering::SeqCst);
            if persist_lsn > old_lsn {
                self.lsn_generator.store(persist_lsn, Ordering::SeqCst);
            }
        }

        // Update rest source states.
        if self
            .table_schemas
            .insert(src_table_name.clone(), schema)
            .is_some()
        {
            return Err(RestSourceError::DuplicateTable(src_table_name).into());
        }
        // Invariant sanity check.
        assert!(self
            .src_table_name_to_src_id
            .insert(src_table_name, src_table_id)
            .is_none());
        Ok(())
    }

    pub fn remove_table(&mut self, src_table_name: &str) -> Result<()> {
        if self.table_schemas.remove(src_table_name).is_none() {
            return Err(RestSourceError::NonExistentTable(src_table_name.to_string()).into());
        }
        // Invariant sanity check.
        assert!(self
            .src_table_name_to_src_id
            .remove(src_table_name)
            .is_some());
        Ok(())
    }

    /// Process an event request.
    pub fn process_request(&self, request: &EventRequest) -> Result<Vec<RestEvent>> {
        match request {
            EventRequest::FileRequest(request) => self.process_file_request(request),
            EventRequest::RowRequest(request) => self.process_row_request(request),
        }
    }

    /// Get arrow record batch reader from the given [`file_path`] and [`filesystem_accessor`].
    async fn read_record_batches(
        filesystem_accessor: Arc<dyn BaseFileSystemAccess>,
        file_path: String,
    ) -> Result<ParquetRecordBatchReader> {
        let content = filesystem_accessor.read_object(&file_path).await?;
        let content = Bytes::from(content);
        let builder = ParquetRecordBatchReaderBuilder::try_new(content)?;
        let record_batch_reader = builder.build()?;
        Ok(record_batch_reader)
    }

    /// Read parquet files and send parsed moonlink rows to channel.
    async fn generate_table_events_for_file_upload(
        src_table_id: SrcTableId,
        lsn_generator: Arc<AtomicU64>,
        storage_config: StorageConfig,
        parquet_files: Vec<String>,
        event_sender: tokio::sync::mpsc::UnboundedSender<Result<RestEvent>>,
    ) {
        let accessor_config = AccessorConfig {
            storage_config,
            timeout_config: FsTimeoutConfig::default(),
            retry_config: FsRetryConfig::default(),
            chaos_config: None,
        };
        let filesystem_accessor: Arc<dyn BaseFileSystemAccess> =
            Arc::new(FileSystemAccessor::new(accessor_config));
        // TODO(hjiang): Handle parallel read and error propagation.
        for cur_file in parquet_files.into_iter() {
            let res = Self::read_record_batches(filesystem_accessor.clone(), cur_file).await;
            if let Err(ref err) = res {
                event_sender.send(Err(err.clone())).unwrap();
            }

            // Send row append events.
            let record_batch_reader = res.unwrap();
            for batch in record_batch_reader {
                let batch = batch.unwrap();
                let moonlink_rows = MoonlinkRow::from_record_batch(&batch);
                for cur_row in moonlink_rows.into_iter() {
                    event_sender
                        .send(Ok(RestEvent::RowEvent {
                            src_table_id,
                            operation: RowEventOperation::Insert,
                            row: cur_row,
                            lsn: lsn_generator.fetch_add(1, Ordering::SeqCst),
                            timestamp: std::time::SystemTime::now(),
                        }))
                        .unwrap();
                }
            }

            // To avoid large transaction, send commit event after all events ingested.
            event_sender
                .send(Ok(RestEvent::Commit {
                    lsn: lsn_generator.fetch_add(1, Ordering::SeqCst),
                    timestamp: std::time::SystemTime::now(),
                }))
                .unwrap();
        }
    }

    /// Process an event request, which is operated on a file.
    fn process_file_request(&self, request: &FileEventRequest) -> Result<Vec<RestEvent>> {
        let src_table_id = self
            .src_table_name_to_src_id
            .get(&request.src_table_name)
            .ok_or_else(|| RestSourceError::UnknownTable(request.src_table_name.clone()))?;
        let src_table_id = *src_table_id;

        match request.operation {
            FileEventOperation::Insert => {
                let (file_upload_row_tx, file_upload_row_rx) =
                    tokio::sync::mpsc::unbounded_channel();
                let lsn_generator = self.lsn_generator.clone();
                let storage_config = request.storage_config.clone();
                let parquet_files = request.files.clone();

                tokio::task::spawn(async move {
                    Self::generate_table_events_for_file_upload(
                        src_table_id,
                        lsn_generator,
                        storage_config,
                        parquet_files,
                        file_upload_row_tx,
                    )
                    .await;
                });

                let file_upload_row_rx = Arc::new(Mutex::new(file_upload_row_rx));
                let file_rest_event = RestEvent::FileInsertEvent {
                    src_table_id,
                    table_events: file_upload_row_rx,
                };
                Ok(vec![file_rest_event])
            }
            FileEventOperation::Upload => {
                let lsn = self.lsn_generator.fetch_add(1, Ordering::SeqCst);
                let file_rest_event = RestEvent::FileUploadEvent {
                    src_table_id,
                    files: request.files.clone(),
                    lsn,
                };
                Ok(vec![file_rest_event])
            }
        }
    }

    /// Process an event request, which is operated on a row.
    fn process_row_request(&self, request: &RowEventRequest) -> Result<Vec<RestEvent>> {
        let schema = self
            .table_schemas
            .get(&request.src_table_name)
            .ok_or_else(|| RestSourceError::UnknownTable(request.src_table_name.clone()))?;

        let src_table_id = self
            .src_table_name_to_src_id
            .get(&request.src_table_name)
            .ok_or_else(|| RestSourceError::UnknownTable(request.src_table_name.clone()))?;

        let converter = JsonToMoonlinkRowConverter::new(schema.clone());
        let row = converter.convert(&request.payload)?;

        let row_lsn = self.lsn_generator.fetch_add(1, Ordering::SeqCst);
        let commit_lsn = self.lsn_generator.fetch_add(1, Ordering::SeqCst);

        // Generate both a row event and a commit event
        let events = vec![
            RestEvent::RowEvent {
                src_table_id: *src_table_id,
                operation: request.operation.clone(),
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
    use crate::{rest_ingest::event_request::FileEventRequest, Error};
    use arrow::record_batch::RecordBatch;
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use moonlink::row::RowValue;
    use parquet::arrow::AsyncArrowWriter;
    use serde_json::json;
    use std::{sync::Arc, time::SystemTime};
    use tempfile::TempDir;

    fn make_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    /// Test util function to generate a parquet under the given [`tempdir`].
    async fn generate_parquet_file(tempdir: &TempDir) -> String {
        let schema = make_test_schema();
        let ids = Int32Array::from(vec![1, 2, 3]);
        let names = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(names)]).unwrap();
        let file_path = tempdir.path().join("test.parquet");
        let file_path_str = file_path.to_str().unwrap().to_string();
        let file = tokio::fs::File::create(file_path).await.unwrap();
        let mut writer: AsyncArrowWriter<tokio::fs::File> =
            AsyncArrowWriter::try_new(file, schema, /*props=*/ None).unwrap();
        writer.write(&batch).await.unwrap();
        writer.close().await.unwrap();
        file_path_str
    }

    /// Test util function to get moonlink rows included in the generated parquet file.
    fn generate_moonlink_rows() -> Vec<MoonlinkRow> {
        let row_1 = MoonlinkRow::new(vec![
            RowValue::Int32(1),
            RowValue::ByteArray("Alice".as_bytes().to_vec()),
        ]);
        let row_2 = MoonlinkRow::new(vec![
            RowValue::Int32(2),
            RowValue::ByteArray("Bob".as_bytes().to_vec()),
        ]);
        let row_3 = MoonlinkRow::new(vec![
            RowValue::Int32(3),
            RowValue::ByteArray("Charlie".as_bytes().to_vec()),
        ]);
        vec![row_1, row_2, row_3]
    }

    /// Test util to get all rest events out of event channel.
    async fn get_all_rest_events(
        events_rx: Arc<Mutex<tokio::sync::mpsc::UnboundedReceiver<Result<RestEvent>>>>,
    ) -> Result<Vec<RestEvent>> {
        let mut rest_events = vec![];
        let mut guard = events_rx.lock().await;
        while let Some(event) = guard.recv().await {
            rest_events.push(event?);
        }
        Ok(rest_events)
    }

    #[test]
    fn test_rest_source_creation() {
        let mut source = RestSource::new();
        assert_eq!(source.table_schemas.len(), 0);
        assert_eq!(source.src_table_name_to_src_id.len(), 0);

        // Test adding table
        let schema = make_test_schema();
        source
            .add_table(
                "test_table".to_string(),
                1,
                schema.clone(),
                /*persist_lsn=*/ Some(0),
            )
            .unwrap();
        assert_eq!(source.table_schemas.len(), 1);
        assert_eq!(source.src_table_name_to_src_id.len(), 1);
        assert!(source.table_schemas.contains_key("test_table"));
        assert_eq!(source.src_table_name_to_src_id.get("test_table"), Some(&1));

        // Test removing table
        source.remove_table("test_table").unwrap();
        assert_eq!(source.table_schemas.len(), 0);
        assert_eq!(source.src_table_name_to_src_id.len(), 0);
    }

    #[test]
    fn test_process_row_append_request_success() {
        let mut source = RestSource::new();
        let schema = make_test_schema();
        source
            .add_table(
                "test_table".to_string(),
                1,
                schema,
                /*persist_lsn=*/ Some(0),
            )
            .unwrap();

        let request = RowEventRequest {
            src_table_name: "test_table".to_string(),
            operation: RowEventOperation::Insert,
            payload: json!({
                "id": 42,
                "name": "test"
            }),
            timestamp: SystemTime::now(),
            tx: None,
        };

        let events = source.process_row_request(&request).unwrap();
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
                assert!(matches!(operation, RowEventOperation::Insert));
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

    #[tokio::test]
    async fn test_process_file_insertion_request_success() {
        let tempdir = TempDir::new().unwrap();
        let filepath = generate_parquet_file(&tempdir).await;

        let mut source = RestSource::new();
        let schema = make_test_schema();
        source
            .add_table(
                /*src_table_name=*/ "test_table".to_string(),
                /*src_table_id=*/ 1,
                schema,
                /*persist_lsn=*/ Some(0),
            )
            .unwrap();

        let request = FileEventRequest {
            src_table_name: "test_table".to_string(),
            operation: FileEventOperation::Insert,
            storage_config: StorageConfig::FileSystem {
                root_directory: tempdir.path().to_str().unwrap().to_string(),
                atomic_write_dir: None,
            },
            files: vec![filepath],
            tx: None,
        };
        let events = source.process_file_request(&request).unwrap();
        assert_eq!(events.len(), 1);
        let moonlink_rows = generate_moonlink_rows();

        // Check file events.
        match &events[0] {
            RestEvent::FileInsertEvent { table_events, .. } => {
                let rest_events = get_all_rest_events(table_events.clone()).await.unwrap();
                // There're 3 append events and 1 commit event.
                assert_eq!(rest_events.len(), 4);

                for (idx, cur_rest_event) in rest_events.into_iter().enumerate() {
                    match cur_rest_event {
                        RestEvent::RowEvent {
                            src_table_id,
                            operation,
                            row,
                            lsn,
                            ..
                        } => {
                            // LSN starts with 1.
                            let expected_lsn = idx + 1;

                            assert_eq!(src_table_id, 1);
                            assert_eq!(operation, RowEventOperation::Insert);
                            assert_eq!(row, moonlink_rows[idx]);
                            assert_eq!(lsn, expected_lsn as u64);
                        }
                        RestEvent::Commit { lsn, .. } => {
                            // Three table append events are with LSN 1, 2, 3.
                            assert_eq!(lsn, 4);
                        }
                        _ => panic!("Receive unexpected rest event {cur_rest_event:?}"),
                    }
                }
            }
            _ => panic!("Receive unexpected rest event {:?}", events[0]),
        }
    }

    #[tokio::test]
    async fn test_process_file_upload_request_success() {
        let tempdir = TempDir::new().unwrap();
        let filepath = generate_parquet_file(&tempdir).await;

        let mut source = RestSource::new();
        let schema = make_test_schema();
        source
            .add_table(
                /*src_table_name=*/ "test_table".to_string(),
                /*src_table_id=*/ 1,
                schema,
                /*persist_lsn=*/ Some(0),
            )
            .unwrap();

        let request = FileEventRequest {
            src_table_name: "test_table".to_string(),
            operation: FileEventOperation::Upload,
            storage_config: StorageConfig::FileSystem {
                root_directory: tempdir.path().to_str().unwrap().to_string(),
                atomic_write_dir: None,
            },
            files: vec![filepath.clone()],
            tx: None,
        };
        let events = source.process_file_request(&request).unwrap();
        assert_eq!(events.len(), 1);

        // Check file events.
        match &events[0] {
            RestEvent::FileUploadEvent {
                src_table_id,
                files,
                lsn,
            } => {
                assert_eq!(*src_table_id, 1);
                assert_eq!(*files, vec![filepath]);
                assert_eq!(*lsn, 1);
            }
            _ => panic!("Receive unexpected rest event {:?}", events[0]),
        }
    }

    #[tokio::test]
    async fn test_process_file_upload_request_failed() {
        let mut source = RestSource::new();
        let schema = make_test_schema();
        source
            .add_table(
                /*src_table_name=*/ "test_table".to_string(),
                /*src_table_id=*/ 1,
                schema,
                /*persist_lsn=*/ Some(0),
            )
            .unwrap();

        let request = FileEventRequest {
            src_table_name: "test_table".to_string(),
            operation: FileEventOperation::Insert,
            storage_config: StorageConfig::FileSystem {
                root_directory: "/non_existent_dir".to_string(),
                atomic_write_dir: None,
            },
            files: vec!["non_existent_file".to_string()],
            tx: None,
        };
        let events = source.process_file_request(&request).unwrap();
        assert_eq!(events.len(), 1);

        // Check file events.
        match &events[0] {
            RestEvent::FileInsertEvent { table_events, .. } => {
                let rest_events = get_all_rest_events(table_events.clone()).await;
                assert!(rest_events.is_err());
            }
            _ => panic!("Receive unexpected rest event {:?}", events[0]),
        }
    }

    #[test]
    fn test_create_existing_table() {
        let mut source = RestSource::new();
        let schema = make_test_schema();
        source
            .add_table(
                "test_table".to_string(),
                /*src_table_id=*/ 1,
                schema.clone(),
                /*persist_lsn=*/ Some(0),
            )
            .unwrap();

        let res = source.add_table(
            "test_table".to_string(),
            /*src_table_id=*/ 1,
            schema,
            /*persist_lsn=*/ Some(0),
        );
        assert!(res.is_err());
    }

    #[test]
    fn test_drop_non_existent_table() {
        let mut source = RestSource::new();
        let res = source.remove_table("test_table");
        assert!(res.is_err());
    }

    #[test]
    fn test_process_request_unknown_table() {
        let source = RestSource::new();
        // No schema added

        let request = RowEventRequest {
            src_table_name: "unknown_table".to_string(),
            operation: RowEventOperation::Insert,
            payload: json!({"id": 1}),
            timestamp: SystemTime::now(),
            tx: None,
        };

        let err = source.process_row_request(&request).unwrap_err();
        match err {
            Error::RestSource(es) => {
                let inner = es
                    .source
                    .as_deref()
                    .and_then(|e| e.downcast_ref::<RestSourceError>())
                    .expect("expected inner RestSourceError");

                match inner {
                    RestSourceError::UnknownTable(table_name) => {
                        assert_eq!(table_name, "unknown_table");
                    }
                    other => panic!("Expected UnknownTable, got {other:?}"),
                }
            }
            other => panic!("Expected Error::RestSource, got {other:?}"),
        }
    }

    #[test]
    fn test_lsn_generation() {
        let mut source = RestSource::new();
        let schema = make_test_schema();
        source
            .add_table(
                "test_table".to_string(),
                1,
                schema,
                /*persist_lsn=*/ Some(0),
            )
            .unwrap();

        let request1 = RowEventRequest {
            src_table_name: "test_table".to_string(),
            operation: RowEventOperation::Insert,
            payload: json!({"id": 1, "name": "first"}),
            timestamp: SystemTime::now(),
            tx: None,
        };

        let request2 = RowEventRequest {
            src_table_name: "test_table".to_string(),
            operation: RowEventOperation::Insert,
            payload: json!({"id": 2, "name": "second"}),
            timestamp: SystemTime::now(),
            tx: None,
        };

        let events1 = source.process_row_request(&request1).unwrap();
        let events2 = source.process_row_request(&request2).unwrap();

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
