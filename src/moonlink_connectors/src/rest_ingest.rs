pub mod datetime_utils;
pub mod decimal_utils;
pub mod json_converter;
pub mod moonlink_rest_sink;
pub mod rest_source;

use crate::replication_state::ReplicationState;
use crate::rest_ingest::moonlink_rest_sink::RestSink;
use crate::rest_ingest::rest_source::{EventRequest, RestSource};
use crate::{Error, Result};
use arrow_schema::Schema;
use moonlink::TableEvent;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};
use tokio::sync::{mpsc, watch};
use tracing::{debug, warn};

pub type SrcTableId = u32;

/// Commands for the REST API event loop (similar to PostgresReplicationCommand)
#[derive(Debug)]
pub enum RestCommand {
    AddTable {
        src_table_name: String,
        src_table_id: SrcTableId,
        schema: Arc<Schema>,
        event_sender: mpsc::Sender<TableEvent>,
        commit_lsn_tx: watch::Sender<u64>,
        flush_lsn_rx: watch::Receiver<u64>,
        wal_flush_lsn_rx: watch::Receiver<u64>,
        replication_state: Arc<ReplicationState>,
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
        })
    }

    pub fn next_src_table_id(&self) -> SrcTableId {
        self.next_src_table_id_generator
            .fetch_add(1, Ordering::SeqCst)
    }

    pub fn get_rest_request_sender(&self) -> mpsc::Sender<EventRequest> {
        self.rest_request_tx.clone()
    }

    /// Add a table to the REST source and sink (sends command to event loop)
    #[allow(clippy::too_many_arguments)]
    pub async fn add_table(
        &self,
        src_table_name: String,
        src_table_id: SrcTableId,
        schema: Arc<Schema>,
        event_sender: mpsc::Sender<TableEvent>,
        commit_lsn_tx: watch::Sender<u64>,
        flush_lsn_rx: watch::Receiver<u64>,
        wal_flush_lsn_rx: watch::Receiver<u64>,
        replication_state: Arc<ReplicationState>,
    ) -> Result<()> {
        let command = RestCommand::AddTable {
            src_table_name,
            src_table_id,
            schema,
            event_sender,
            commit_lsn_tx,
            flush_lsn_rx,
            wal_flush_lsn_rx,
            replication_state,
        };

        self.cmd_tx
            .send(command)
            .await
            .map_err(|e| crate::Error::RestApi(format!("Failed to send add table command: {e}")))?;

        Ok(())
    }

    /// Drop a table from the REST source and sink (sends command to event loop)
    pub async fn drop_table(&self, src_table_id: SrcTableId, src_table_name: &str) -> Result<()> {
        let command = RestCommand::DropTable {
            src_table_name: src_table_name.to_string(),
            src_table_id,
        };

        self.cmd_tx.send(command).await.map_err(|e| {
            crate::Error::RestApi(format!("Failed to send drop table command: {e}"))
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
        self.cmd_tx
            .send(RestCommand::Shutdown)
            .await
            .map_err(|e| crate::Error::RestApi(format!("Failed to send shutdown command: {e}")))?;
        Ok(())
    }

    /// Spawn REST API event loop task (following PostgreSQL's spawn_replication_task pattern)
    pub async fn spawn_rest_task(&mut self) -> tokio::task::JoinHandle<Result<()>> {
        let sink = RestSink::new();
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

    // UNDON, send status back for REST API if wait=true
    let mut flush_lsn_rxs: HashMap<SrcTableId, watch::Receiver<u64>> = HashMap::new();
    let mut wal_flush_lsn_rxs: HashMap<SrcTableId, watch::Receiver<u64>> = HashMap::new();
    let mut replication_states: HashMap<SrcTableId, Arc<ReplicationState>> = HashMap::new();

    loop {
        tokio::select! {
            Some(cmd) = cmd_rx.recv() => match cmd {
                RestCommand::AddTable { src_table_name, src_table_id, schema, event_sender, commit_lsn_tx, flush_lsn_rx, wal_flush_lsn_rx, replication_state } => {
                    debug!("Adding REST table '{}' with src_table_id {}", src_table_name, src_table_id);

                    // Add to sink (handles table events)
                    sink.add_table(src_table_id, event_sender, commit_lsn_tx)?;

                    // Add to source (handles schema and request processing)
                    rest_source.add_table(src_table_name.clone(), src_table_id, schema)?;

                    if flush_lsn_rxs.insert(src_table_id, flush_lsn_rx).is_some() {
                        return Err(Error::RestDuplicateTable(src_table_id));
                    }
                    // Invariant sanity check.
                    assert!(wal_flush_lsn_rxs.insert(src_table_id, wal_flush_lsn_rx).is_none());
                    assert!(replication_states.insert(src_table_id, replication_state).is_none());
                }
                RestCommand::DropTable { src_table_name, src_table_id } => {
                    debug!("Dropping REST table '{}' with src_table_id {}", src_table_name, src_table_id);

                    // Remove from sink
                    sink.drop_table(src_table_id)?;

                    // Remove from source
                    rest_source.remove_table(&src_table_name)?;

                    if flush_lsn_rxs.remove(&src_table_id).is_none() {
                        return Err(Error::RestNonExistentTable(src_table_id));
                    }
                    // Invariant sanity check.
                    assert!(wal_flush_lsn_rxs.remove(&src_table_id).is_some());
                    assert!(replication_states.remove(&src_table_id).is_some());
                }
                RestCommand::Shutdown => {
                    debug!("received shutdown command");
                    break;
                }
            },
            // Process REST requests directly (similar to how PostgreSQL processes CDC events)
            Some(request) = rest_request_rx.recv() => {
                // Process the request and generate events
                match rest_source.process_request(&request) {
                    Ok(rest_events) => {
                        // Send all events to be processed by the sink
                        for rest_event in rest_events {
                            let rest_event_proc_result = sink.process_rest_event(rest_event).await;
                            if let Err(e) = &rest_event_proc_result {
                                warn!(error = ?e, "failed to process REST event");
                                break; // Stop processing further events on error
                            }

                            // Update replication LSN if applicable.
                            let rest_event_proc_result = rest_event_proc_result.unwrap();
                            if let Some(commit_lsn) = rest_event_proc_result.commit_lsn {
                                replication_states.get(&rest_event_proc_result.src_table_id).as_ref().unwrap().mark(commit_lsn);
                            }
                        }
                    }
                    Err(e) => {
                        warn!(error = ?e, "failed to process REST request {:?}", request);
                    }
                }
            }
        }
    }

    debug!("REST API event loop stopped");
    Ok(())
}
