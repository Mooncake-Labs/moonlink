use crate::rest_ingest::event_request::{
    EventRequest, FileEventOperation, FileEventRequest, FlushRequest, IngestRequestPayload,
    RowEventOperation, RowEventRequest, SnapshotRequest,
};
use crate::rest_ingest::json_converter::{JsonToMoonlinkRowConverter, JsonToMoonlinkRowError};
use crate::rest_ingest::rest_event::RestEvent;
use crate::Result;
use arrow_schema::Schema;
use bytes::Bytes;
use futures::stream::Stream;
use moonlink::row::MoonlinkRow;
use moonlink::{
    AccessorConfig, BaseFileSystemAccess, FileSystemAccessor, FsRetryConfig, FsTimeoutConfig,
    StorageConfig,
};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};
use thiserror::Error;
use tokio::sync::mpsc;

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
    #[error("protobuf conversion error: {0}")]
    ProtobufDecoding(#[from] prost::DecodeError),
    #[error("moonlink row conversion error: {0}")]
    MoonlinkRowProtobufConversion(#[from] moonlink::row::ProtoToMoonlinkRowError),
}

pub struct RestSource {
    table_schemas: HashMap<String, Arc<Schema>>,
    src_table_name_to_src_id: HashMap<String, SrcTableId>,
    lsn_generator: Arc<AtomicU64>,
}

pub struct RestSourceStream {
    source: Arc<RwLock<RestSource>>,
    request_rx: mpsc::Receiver<EventRequest>,
}

impl Stream for RestSourceStream {
    type Item = (Option<mpsc::Sender<u64>>, Result<Vec<RestEvent>>);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.request_rx.poll_recv(cx) {
            Poll::Ready(Some(request)) => {
                let request_tx = request.get_request_tx();
                let result = {
                    let source = self.source.read().unwrap();
                    source.process_request_sync(request)
                };
                Poll::Ready(Some((request_tx, result)))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
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
    pub fn process_request(
        &self,
        request: EventRequest,
        results_tx: tokio::sync::mpsc::UnboundedSender<(
            Option<tokio::sync::mpsc::Sender<u64>>,
            Result<Vec<RestEvent>>,
        )>,
    ) {
        // Extract the request_tx from the request
        let request_tx = request.get_request_tx();

        match request {
            EventRequest::FileRequest(file_request) => {
                // Process file request synchronously and send to channel
                self.process_file_request_async(file_request, request_tx, results_tx);
            }
            EventRequest::RowRequest(row_request) => {
                // Use the async processing for row requests
                self.process_row_request_async(row_request, request_tx, results_tx);
            }
            EventRequest::SnapshotRequest(snapshot_request) => {
                // Process snapshot request synchronously and send to channel
                let result = self.process_snapshot_request(&snapshot_request);
                let _ = results_tx.send((request_tx, result));
            }
            EventRequest::FlushRequest(flush_request) => {
                // Process flush request synchronously and send to channel
                let result = self.process_flush_request(&flush_request);
                let _ = results_tx.send((request_tx, result));
            }
        }
    }

    /// Async helper for tests - processes row request and returns results directly
    #[cfg(test)]
    async fn process_row_request_test_helper(
        &self,
        request: &RowEventRequest,
    ) -> Result<Vec<RestEvent>> {
        let (results_tx, mut results_rx) = tokio::sync::mpsc::unbounded_channel();

        // Call the async method
        self.process_row_request_async(request.clone(), None, results_tx);

        // Wait for the result
        if let Some((_request_tx, result)) = results_rx.recv().await {
            result
        } else {
            Err(crate::Error::rest_api(
                "No result received".to_string(),
                None,
            ))
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
    ) -> Result<Vec<RestEvent>> {
        let accessor_config = AccessorConfig {
            storage_config,
            timeout_config: FsTimeoutConfig::default(),
            retry_config: FsRetryConfig::default(),
            chaos_config: None,
        };
        let filesystem_accessor: Arc<dyn BaseFileSystemAccess> =
            Arc::new(FileSystemAccessor::new(accessor_config));
        let mut rest_events = Vec::new();
        // TODO(hjiang): Handle parallel read and error propagation.
        for cur_file in parquet_files.into_iter() {
            let res = Self::read_record_batches(filesystem_accessor.clone(), cur_file).await;
            if let Err(ref err) = res {
                return Err(err.clone());
            }

            // Send row append events.
            let record_batch_reader = res.unwrap();
            for batch in record_batch_reader {
                let batch = batch.unwrap();
                let moonlink_rows = MoonlinkRow::from_record_batch(&batch);
                for cur_row in moonlink_rows.into_iter() {
                    rest_events.push(RestEvent::RowEvent {
                        src_table_id,
                        operation: RowEventOperation::Insert,
                        row: cur_row,
                        lsn: lsn_generator.fetch_add(1, Ordering::SeqCst),
                        timestamp: std::time::SystemTime::now(),
                    });
                }
            }

            // To avoid large transaction, send commit event after all events ingested.
            rest_events.push(RestEvent::Commit {
                lsn: lsn_generator.fetch_add(1, Ordering::SeqCst),
                timestamp: std::time::SystemTime::now(),
            })
        }
        Ok(rest_events)
    }

    /// Process an event request, which is operated on a file.
    fn process_file_request_async(
        &self,
        request: FileEventRequest,
        request_tx: Option<tokio::sync::mpsc::Sender<u64>>,
        results_tx: tokio::sync::mpsc::UnboundedSender<(
            Option<tokio::sync::mpsc::Sender<u64>>,
            Result<Vec<RestEvent>>,
        )>,
    ) {
        let src_table_id = match self
            .src_table_name_to_src_id
            .get(&request.src_table_name)
            .copied()
        {
            Some(id) => id,
            None => {
                let error = Err(crate::Error::rest_api(
                    format!("Unknown table: {}", request.src_table_name),
                    None,
                ));
                let _ = results_tx.send((request_tx, error));
                return;
            }
        };

        match request.operation {
            FileEventOperation::Insert => {
                let lsn_generator = self.lsn_generator.clone();
                let storage_config = request.storage_config.clone();
                let parquet_files = request.files.clone();

                tokio::task::spawn(async move {
                    // Spawn the file processing task
                    let events = Self::generate_table_events_for_file_upload(
                        src_table_id,
                        lsn_generator,
                        storage_config,
                        parquet_files,
                    )
                    .await;
                    let _ = results_tx.send((request_tx, events));
                });
            }
            FileEventOperation::Upload => {
                let lsn = self.lsn_generator.fetch_add(1, Ordering::SeqCst);
                let file_rest_event = RestEvent::FileUploadEvent {
                    src_table_id,
                    storage_config: request.storage_config,
                    files: request.files,
                    lsn,
                };
                let _ = results_tx.send((request_tx, Ok(vec![file_rest_event])));
            }
        }
    }

    /// Process an event request, which is operated on a row.
    fn process_row_request_async(
        &self,
        request: RowEventRequest,
        request_tx: Option<tokio::sync::mpsc::Sender<u64>>,
        results_tx: tokio::sync::mpsc::UnboundedSender<(
            Option<tokio::sync::mpsc::Sender<u64>>,
            Result<Vec<RestEvent>>,
        )>,
    ) {
        // Clone the data needed for async processing
        let schema = self.table_schemas.get(&request.src_table_name).cloned();
        let src_table_id = self
            .src_table_name_to_src_id
            .get(&request.src_table_name)
            .copied();
        let row_lsn = self.lsn_generator.fetch_add(1, Ordering::SeqCst);
        let commit_lsn = self.lsn_generator.fetch_add(1, Ordering::SeqCst);

        // Spawn task to process row request asynchronously
        tokio::spawn(async move {
            let result = (|| -> Result<Vec<RestEvent>> {
                let schema = schema
                    .ok_or_else(|| RestSourceError::UnknownTable(request.src_table_name.clone()))?;
                let src_table_id = src_table_id
                    .ok_or_else(|| RestSourceError::UnknownTable(request.src_table_name.clone()))?;

                // Decode based on payload type
                let row = match &request.payload {
                    IngestRequestPayload::Json(value) => {
                        let converter = JsonToMoonlinkRowConverter::new(schema);
                        converter.convert(value)?
                    }
                    IngestRequestPayload::Protobuf(bytes) => {
                        let p: moonlink_proto::moonlink::MoonlinkRow =
                            prost::Message::decode(bytes.as_slice())
                                .map_err(RestSourceError::ProtobufDecoding)?;
                        moonlink::row::proto_to_moonlink_row(p)?
                    }
                };

                // Generate both a row event and a commit event
                let events = vec![
                    RestEvent::RowEvent {
                        src_table_id,
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
            })();

            // Send result to queue with request_tx
            let _ = results_tx.send((request_tx, result));
        });
    }

    /// Create a stream from this RestSource
    pub fn create_stream(
        source: Arc<RwLock<RestSource>>,
        request_rx: mpsc::Receiver<EventRequest>,
    ) -> RestSourceStream {
        RestSourceStream { source, request_rx }
    }

    /// Process a request synchronously (used by the Stream implementation)
    fn process_request_sync(&self, request: EventRequest) -> Result<Vec<RestEvent>> {
        match request {
            EventRequest::RowRequest(row_request) => self.process_row_request_sync(row_request),
            EventRequest::FileRequest(file_request) => {
                match file_request.operation {
                    FileEventOperation::Insert => {
                        // For file insert, we need to do async work, but we're in a sync context
                        // This is a limitation - we'll return an error for now
                        Err(crate::Error::rest_api(
                            "File insert operations require async processing".to_string(),
                            None,
                        ))
                    }
                    FileEventOperation::Upload => self.process_file_upload_sync(file_request),
                }
            }
            EventRequest::SnapshotRequest(snapshot_request) => {
                self.process_snapshot_request(&snapshot_request)
            }
            EventRequest::FlushRequest(flush_request) => self.process_flush_request(&flush_request),
        }
    }

    /// Synchronous row processing
    fn process_row_request_sync(&self, request: RowEventRequest) -> Result<Vec<RestEvent>> {
        let schema = self
            .table_schemas
            .get(&request.src_table_name)
            .ok_or_else(|| RestSourceError::UnknownTable(request.src_table_name.clone()))?;

        let src_table_id = self
            .src_table_name_to_src_id
            .get(&request.src_table_name)
            .ok_or_else(|| RestSourceError::UnknownTable(request.src_table_name.clone()))?;

        // Decode based on payload type
        let row = match &request.payload {
            IngestRequestPayload::Json(value) => {
                let converter = JsonToMoonlinkRowConverter::new(schema.clone());
                converter.convert(value)?
            }
            IngestRequestPayload::Protobuf(bytes) => {
                let p: moonlink_proto::moonlink::MoonlinkRow =
                    prost::Message::decode(bytes.as_slice())
                        .map_err(RestSourceError::ProtobufDecoding)?;
                moonlink::row::proto_to_moonlink_row(p)?
            }
        };

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

    /// Synchronous file upload processing
    fn process_file_upload_sync(&self, request: FileEventRequest) -> Result<Vec<RestEvent>> {
        let src_table_id = self
            .src_table_name_to_src_id
            .get(&request.src_table_name)
            .ok_or_else(|| RestSourceError::UnknownTable(request.src_table_name.clone()))?;

        let lsn = self.lsn_generator.fetch_add(1, Ordering::SeqCst);
        let file_rest_event = RestEvent::FileUploadEvent {
            src_table_id: *src_table_id,
            storage_config: request.storage_config,
            files: request.files,
            lsn,
        };
        Ok(vec![file_rest_event])
    }

    /// Process a snapshot request
    fn process_snapshot_request(&self, request: &SnapshotRequest) -> Result<Vec<RestEvent>> {
        let src_table_id = self
            .src_table_name_to_src_id
            .get(&request.src_table_name)
            .ok_or_else(|| RestSourceError::UnknownTable(request.src_table_name.clone()))?;

        // Generate a snapshot request.
        let event = vec![RestEvent::Snapshot {
            src_table_id: *src_table_id,
            lsn: request.lsn,
        }];
        Ok(event)
    }

    /// Process a flush request.
    fn process_flush_request(&self, request: &FlushRequest) -> Result<Vec<RestEvent>> {
        let src_table_id = self
            .src_table_name_to_src_id
            .get(&request.src_table_name)
            .ok_or_else(|| RestSourceError::UnknownTable(request.src_table_name.clone()))?;

        // Generate flush request.
        let event = vec![RestEvent::Flush {
            src_table_id: *src_table_id,
        }];
        Ok(event)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        rest_ingest::event_request::{FileEventRequest, IngestRequestPayload},
        Error,
    };
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

    /// Test util to get all rest events from the new result channel.
    async fn get_all_rest_events_from_result_channel(
        mut events_rx: tokio::sync::mpsc::UnboundedReceiver<(
            Option<tokio::sync::mpsc::Sender<u64>>,
            Result<Vec<RestEvent>>,
        )>,
    ) -> Result<Vec<RestEvent>> {
        let mut all_events = vec![];
        while let Some((_request_tx, result)) = events_rx.recv().await {
            let events = result?;
            all_events.extend(events);
        }
        Ok(all_events)
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

    #[tokio::test]
    async fn test_process_row_append_request_success() {
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
            payload: IngestRequestPayload::Json(json!({
                "id": 42,
                "name": "test"
            })),
            timestamp: SystemTime::now(),
            tx: None,
        };

        let (events_tx, events_rx) = tokio::sync::mpsc::unbounded_channel();
        source.process_row_request_async(request, None, events_tx);

        let events = get_all_rest_events_from_result_channel(events_rx)
            .await
            .unwrap();
        assert_eq!(events.len(), 2); // Should have row event + commit event

        // Check the row event
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

        // Check the commit event
        match &events[1] {
            RestEvent::Commit { lsn, .. } => {
                assert_eq!(*lsn, 2);
            }
            _ => panic!("Expected Commit event"),
        }
    }

    #[tokio::test]
    async fn test_process_row_with_proto_row() {
        let mut source = RestSource::new();
        let schema = make_test_schema();
        source
            .add_table(
                "test_table".to_string(),
                /*src_table_id=*/ 1,
                schema,
                /*persist_lsn=*/ Some(0),
            )
            .unwrap();

        // Build a MoonlinkRow directly that would not match the JSON payload
        let direct_row = MoonlinkRow::new(vec![
            RowValue::Int32(123),
            RowValue::ByteArray(b"proto".to_vec()),
        ]);

        let request = RowEventRequest {
            src_table_name: "test_table".to_string(),
            operation: RowEventOperation::Insert,
            payload: {
                let p = moonlink::row::moonlink_row_to_proto(direct_row.clone());
                let mut buf = Vec::new();
                prost::Message::encode(&p, &mut buf).unwrap();
                IngestRequestPayload::Protobuf(buf)
            },
            timestamp: SystemTime::now(),
            tx: None,
        };

        let (events_tx, events_rx) = tokio::sync::mpsc::unbounded_channel();
        source.process_row_request_async(request, None, events_tx);

        let events = get_all_rest_events_from_result_channel(events_rx)
            .await
            .unwrap();
        match &events[0] {
            RestEvent::RowEvent { row, .. } => {
                assert_eq!(row, &direct_row);
            }
            other => panic!("expected RowEvent, got {other:?}"),
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

        let (results_tx, results_rx) = tokio::sync::mpsc::unbounded_channel();
        source.process_file_request_async(request, None, results_tx);
        let events = get_all_rest_events_from_result_channel(results_rx)
            .await
            .unwrap();

        let moonlink_rows = generate_moonlink_rows();
        // Should have 3 row events + 1 commit event = 4 events
        assert_eq!(events.len(), 4);

        // Check that we get the expected row events and commit
        for (idx, event) in events.iter().enumerate() {
            match event {
                RestEvent::RowEvent {
                    src_table_id,
                    operation,
                    row,
                    lsn,
                    ..
                } => {
                    // LSN starts with 1.
                    let expected_lsn = idx + 1;

                    assert_eq!(*src_table_id, 1);
                    assert_eq!(*operation, RowEventOperation::Insert);
                    assert_eq!(*row, moonlink_rows[idx]);
                    assert_eq!(*lsn, expected_lsn as u64);
                }
                RestEvent::Commit { lsn, .. } => {
                    // The commit event should be after all row events
                    assert_eq!(*lsn, 4);
                }
                _ => panic!("Unexpected event: {event:?}"),
            }
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

        let (results_tx, results_rx) = tokio::sync::mpsc::unbounded_channel();
        source.process_file_request_async(request, None, results_tx);
        let events = get_all_rest_events_from_result_channel(results_rx)
            .await
            .unwrap();
        assert_eq!(events.len(), 1);

        // Check file events.
        match &events[0] {
            RestEvent::FileUploadEvent {
                src_table_id,
                files,
                lsn,
                ..
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

        let (results_tx, mut results_rx) = tokio::sync::mpsc::unbounded_channel();
        source.process_file_request_async(request, None, results_tx);

        // Should receive an error result
        let (_request_tx, result) = results_rx.recv().await.unwrap();
        assert!(result.is_err());
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

    #[tokio::test]
    async fn test_process_request_unknown_table() {
        let source = RestSource::new();

        let request = RowEventRequest {
            src_table_name: "unknown_table".to_string(),
            operation: RowEventOperation::Insert,
            payload: IngestRequestPayload::Json(json!({"id": 1})),
            timestamp: SystemTime::now(),
            tx: None,
        };

        let err = source
            .process_row_request_test_helper(&request)
            .await
            .unwrap_err();
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

    #[tokio::test]
    async fn test_lsn_generation() {
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
            payload: IngestRequestPayload::Json(json!({"id": 1, "name": "first"})),
            timestamp: SystemTime::now(),
            tx: None,
        };

        let request2 = RowEventRequest {
            src_table_name: "test_table".to_string(),
            operation: RowEventOperation::Insert,
            payload: IngestRequestPayload::Json(json!({"id": 2, "name": "second"})),
            timestamp: SystemTime::now(),
            tx: None,
        };

        let events1 = source
            .process_row_request_test_helper(&request1)
            .await
            .unwrap();
        let events2 = source
            .process_row_request_test_helper(&request2)
            .await
            .unwrap();

        // Each request should generate 2 events (row + commit)
        assert_eq!(events1.len(), 2);
        assert_eq!(events2.len(), 2);

        // Check LSN ordering: first request should have LSNs 1,2 and second should have 3,4
        match (&events1[0], &events1[1]) {
            (RestEvent::RowEvent { lsn: lsn1, .. }, RestEvent::Commit { lsn: lsn2, .. }) => {
                assert_eq!(*lsn1, 1);
                assert_eq!(*lsn2, 2);
            }
            _ => panic!("Expected row event and commit event"),
        }

        match (&events2[0], &events2[1]) {
            (RestEvent::RowEvent { lsn: lsn1, .. }, RestEvent::Commit { lsn: lsn2, .. }) => {
                assert_eq!(*lsn1, 3);
                assert_eq!(*lsn2, 4);
            }
            _ => panic!("Expected row event and commit event"),
        }
    }
}
