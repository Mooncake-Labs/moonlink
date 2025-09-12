pub mod avro_converter;
pub mod datetime_utils;
pub mod decimal_utils;
pub mod event_request;
pub mod json_converter;
pub mod moonlink_rest_sink;
pub mod rest_event;
pub mod rest_source;
pub mod schema_util;

use crate::replication_state::ReplicationState;
use crate::rest_ingest::event_request::EventRequest;
use crate::rest_ingest::moonlink_rest_sink::RestSink;
use crate::rest_ingest::moonlink_rest_sink::TableStatus;
use crate::rest_ingest::rest_source::RestSource;
use crate::Result;
use apache_avro::schema::Schema as AvroSchema;
use arrow_schema::Schema;
use moonlink::TableEvent;
use moonlink::VisibilityLsn;
use more_asserts as ma;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tracing::{debug, error, warn};

pub type SrcTableId = u32;

/// Commands for the REST API event loop (similar to PostgresReplicationCommand)
#[derive(Debug)]
pub enum RestCommand {
    AddTable {
        src_table_name: String,
        src_table_id: SrcTableId,
        schema: Arc<Schema>,
        event_sender: mpsc::Sender<TableEvent>,
        visibility_tx: watch::Sender<VisibilityLsn>,
        flush_lsn_rx: watch::Receiver<u64>,
        wal_flush_lsn_rx: watch::Receiver<u64>,
        /// Persist LSN, only assigned for tables to recovery; used to indicate and update replication LSN.
        persist_lsn: Option<u64>,
    },
    SetAvroSchema {
        src_table_name: String,
        avro_schema: AvroSchema,
    },
    DropTable {
        src_table_name: String,
        src_table_id: SrcTableId,
    },
    Shutdown,
}

/// REST API connection following PostgreSQL pattern
pub struct RestApiConnection {
    rest_request_tx: mpsc::Sender<EventRequest>,
    cmd_tx: mpsc::Sender<RestCommand>,
    cmd_rx: Option<mpsc::Receiver<RestCommand>>,
    rest_request_rx: Option<mpsc::Receiver<EventRequest>>,
    next_src_table_id_generator: AtomicU32,
    replication_state: Arc<ReplicationState>,
}

impl RestApiConnection {
    pub async fn new() -> Result<Self> {
        let (rest_request_tx, rest_request_rx) = mpsc::channel(100);
        let (cmd_tx, cmd_rx) = mpsc::channel(8);

        Ok(Self {
            rest_request_tx,
            cmd_tx,
            cmd_rx: Some(cmd_rx),
            rest_request_rx: Some(rest_request_rx),
            next_src_table_id_generator: AtomicU32::new(1),
            replication_state: ReplicationState::new(),
        })
    }

    pub fn next_src_table_id(&self) -> SrcTableId {
        self.next_src_table_id_generator
            .fetch_add(1, Ordering::SeqCst)
    }

    pub fn get_rest_request_sender(&self) -> mpsc::Sender<EventRequest> {
        self.rest_request_tx.clone()
    }

    pub fn get_replication_state(&self) -> Arc<ReplicationState> {
        self.replication_state.clone()
    }

    /// Add a table to the REST source and sink (sends command to event loop)
    ///
    /// # Arguments
    ///
    /// * persist_lsn: only assigned at recovery, used to indicate and update replication LSN.
    #[allow(clippy::too_many_arguments)]
    pub async fn add_table(
        &self,
        src_table_name: String,
        src_table_id: SrcTableId,
        schema: Arc<Schema>,
        event_sender: mpsc::Sender<TableEvent>,
        visibility_tx: watch::Sender<VisibilityLsn>,
        flush_lsn_rx: watch::Receiver<u64>,
        wal_flush_lsn_rx: watch::Receiver<u64>,
        persist_lsn: Option<u64>,
    ) -> Result<()> {
        let command = RestCommand::AddTable {
            src_table_name,
            src_table_id,
            schema,
            event_sender,
            visibility_tx,
            flush_lsn_rx,
            wal_flush_lsn_rx,
            persist_lsn,
        };

        self.cmd_tx.send(command).await.map_err(|e| {
            crate::Error::rest_api(
                format!("Failed to send add table command: {e}"),
                Some(Arc::new(e.into())),
            )
        })?;

        Ok(())
    }

    /// Set Avro schema for an existing table
    pub async fn set_avro_schema(
        &self,
        src_table_name: String,
        avro_schema: AvroSchema,
    ) -> Result<()> {
        let command = RestCommand::SetAvroSchema {
            src_table_name,
            avro_schema,
        };

        self.cmd_tx.send(command).await.map_err(|e| {
            crate::Error::rest_api(
                format!("Failed to send set Avro schema command: {e}"),
                Some(Arc::new(e.into())),
            )
        })?;

        Ok(())
    }

    /// Drop a table from the REST source and sink (sends command to event loop)
    pub async fn drop_table(&self, src_table_id: SrcTableId, src_table_name: &str) -> Result<()> {
        let command = RestCommand::DropTable {
            src_table_name: src_table_name.to_string(),
            src_table_id,
        };

        self.cmd_tx.send(command).await.map_err(|e| {
            crate::Error::rest_api(
                format!("Failed to send drop table command: {e}"),
                Some(Arc::new(e.into())),
            )
        })?;

        Ok(())
    }

    /// Start REST API replication - returns the command receiver for the event loop
    pub fn start_replication(
        &mut self,
    ) -> (mpsc::Receiver<RestCommand>, mpsc::Receiver<EventRequest>) {
        (
            self.cmd_rx.take().unwrap(),
            self.rest_request_rx.take().unwrap(),
        )
    }

    pub async fn shutdown_replication(&mut self) -> Result<()> {
        self.cmd_tx.send(RestCommand::Shutdown).await.map_err(|e| {
            crate::Error::rest_api(
                format!("Failed to send shutdown command: {e}"),
                Some(Arc::new(e.into())),
            )
        })?;
        Ok(())
    }

    /// Spawn REST API event loop task (following PostgreSQL's spawn_replication_task pattern)
    pub async fn spawn_rest_task(&mut self) -> tokio::task::JoinHandle<Result<()>> {
        let sink = RestSink::new(self.replication_state.clone());
        let (cmd_rx, rest_request_rx) = self.start_replication();

        tokio::spawn(async move { run_rest_event_loop(sink, cmd_rx, rest_request_rx).await })
    }
}

/// REST API event loop (similar to PostgreSQL's run_event_loop)
#[tracing::instrument(name = "rest_event_loop", skip_all)]
pub async fn run_rest_event_loop(
    mut sink: RestSink,
    mut cmd_rx: mpsc::Receiver<RestCommand>,
    mut rest_request_rx: mpsc::Receiver<EventRequest>,
) -> Result<()> {
    debug!("REST API event loop started");

    // Create RestSource that we'll use for processing
    let mut rest_source = RestSource::new();

    // For processing errors happen inside of the eventloop, we simply log and proceed.
    // TODO(hjiang): implement exception safety guarantee.
    loop {
        tokio::select! {
            Some(cmd) = cmd_rx.recv() => match cmd {
                RestCommand::AddTable { src_table_name, src_table_id, schema, event_sender, visibility_tx, flush_lsn_rx, wal_flush_lsn_rx, persist_lsn } => {
                    debug!("Adding REST table '{}' with src_table_id {}", src_table_name, src_table_id);

                    // Add to sink (handles table events)
                    let table_status = TableStatus {
                        _wal_flush_lsn_rx: wal_flush_lsn_rx,
                        _flush_lsn_rx: flush_lsn_rx,
                        event_sender,
                        visibility_tx,
                    };
                    if let Err(e) = sink.add_table(src_table_id, table_status, persist_lsn) {
                        error!("Add table {src_table_name} with id {src_table_id} to sink failed: {e}");
                        continue;
                    }

                    // Add to source (handles schema and request processing)
                    if let Err(e) = rest_source.add_table(src_table_name.clone(), src_table_id, schema, persist_lsn) {
                        error!("Add table {src_table_name} with {src_table_id} to rest source failed: {e}");
                        continue;
                    }
                }
                RestCommand::SetAvroSchema { src_table_name, avro_schema } => {
                    debug!("Setting Avro schema for table '{}'", src_table_name);
                    if let Err(e) = rest_source.set_avro_schema(src_table_name.clone(), avro_schema) {
                        error!("Set avro schema failed for {src_table_name} failed: {e}");
                        continue;
                    }
                }
                RestCommand::DropTable { src_table_name, src_table_id } => {
                    debug!("Dropping REST table '{}' with src_table_id {}", src_table_name, src_table_id);

                    // Remove from sink
                    if let Err(e) = sink.drop_table(src_table_id) {
                        error!("Drop table {src_table_name} with id {src_table_id} failed: {e}");
                        continue;
                    }

                    // Remove from source
                    if let Err(e) = rest_source.remove_table(&src_table_name) {
                        error!("Remove table {src_table_name} failed: {e}");
                        continue;
                    }
                }
                RestCommand::Shutdown => {
                    debug!("received shutdown command");
                    break;
                }
            },
            // Process REST requests directly (similar to how PostgreSQL processes CDC events)
            Some(request) = rest_request_rx.recv() => {
                // TODO(hjiang): Handle recursive request like file insertion.
                let mut lsn = 0;

                // Process the request and generate events
                match rest_source.process_request(&request) {
                    Ok(rest_events) => {
                        // Send all events to be processed by the sink
                        for rest_event in rest_events {
                            if let Some(rest_lsn) = rest_event.lsn() {
                                ma::assert_gt!(rest_lsn, lsn);
                                lsn = rest_lsn;
                            }

                            // Process rest events.
                            let rest_event_proc_result = sink.process_rest_event(rest_event).await;
                            if let Err(e) = &rest_event_proc_result {
                                warn!(error = ?e, "failed to process REST event");
                                continue;
                            }
                        }

                        // Send back event response if applicable.
                        if let Some(rx) = request.get_request_tx() {
                            // Client connection could be cut down during request handling, so no guarantee send success.
                            let _ = rx.send(lsn).await;
                        }
                    }
                    Err(e) => {
                        warn!(error = ?e, "failed to process REST request {:?}", request);
                    }
                }
            }
            // All channels are closed.
            else => break,
        }
    }

    debug!("REST API event loop stopped");
    Ok(())
}
