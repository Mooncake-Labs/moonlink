use crate::pg_replicate::util::PostgresTableRow;
use crate::pg_replicate::{
    conversions::{cdc_event::CdcEvent, table_row::TableRow},
    replication_state::ReplicationState,
    table::{SrcTableId, TableSchema},
};
use moonlink::TableEvent;
use postgres_replication::protocol::Column as ReplicationColumn;
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;
use tokio_postgres::types::PgLsn;
use tracing::{debug, warn};

#[derive(Default)]
struct TransactionState {
    final_lsn: u64,
    touched_tables: HashSet<SrcTableId>,
}

#[derive(Eq, PartialEq)]
struct ColumnInfo {
    name: String,
    typ: u32,
    modifier: i32,
}
pub struct Sink {
    event_senders: HashMap<SrcTableId, Sender<TableEvent>>,
    commit_lsn_txs: HashMap<SrcTableId, watch::Sender<u64>>,
    streaming_transactions_state: HashMap<u32, TransactionState>,
    transaction_state: TransactionState,
    replication_state: Arc<ReplicationState>,
    relation_cache: HashMap<SrcTableId, Vec<ColumnInfo>>,
}

impl Sink {
    pub fn new(replication_state: Arc<ReplicationState>) -> Self {
        Self {
            event_senders: HashMap::new(),
            commit_lsn_txs: HashMap::new(),
            streaming_transactions_state: HashMap::new(),
            transaction_state: TransactionState {
                final_lsn: 0,
                touched_tables: HashSet::new(),
            },
            replication_state,
            relation_cache: HashMap::new(),
        }
    }
}

pub struct SchemaChangeRequest(pub SrcTableId);

impl Sink {
    pub fn add_table(
        &mut self,
        src_table_id: SrcTableId,
        event_sender: Sender<TableEvent>,
        commit_lsn_tx: watch::Sender<u64>,
        table_schema: &TableSchema,
    ) {
        self.event_senders.insert(src_table_id, event_sender);
        self.commit_lsn_txs.insert(src_table_id, commit_lsn_tx);
        let columns = table_schema
            .column_schemas
            .iter()
            .map(|c| ColumnInfo {
                name: c.name.clone(),
                typ: c.typ.oid(),
                modifier: c.modifier,
            })
            .collect();
        self.relation_cache.insert(src_table_id, columns);
    }
    pub fn drop_table(&mut self, src_table_id: SrcTableId) {
        self.event_senders.remove(&src_table_id).unwrap();
        self.commit_lsn_txs.remove(&src_table_id).unwrap();
    }

    pub async fn alter_table(&mut self, src_table_id: SrcTableId, table_schema: &TableSchema) {
        let new_columns: Vec<ColumnInfo> = table_schema
            .column_schemas
            .iter()
            .map(|c| ColumnInfo {
                name: c.name.clone(),
                typ: c.typ.oid(),
                modifier: c.modifier,
            })
            .collect();
        let old_columns = self.relation_cache.get(&src_table_id).unwrap();
        let columns_to_drop = old_columns
            .iter()
            .filter(|c| !new_columns.contains(c))
            .map(|c| c.name.clone())
            .collect();
        if let Some(event_sender) = self.event_senders.get_mut(&src_table_id) {
            event_sender
                .send(TableEvent::AlterTable { columns_to_drop })
                .await
                .unwrap();
        }
        self.relation_cache.insert(src_table_id, new_columns);
    }
    /// Get final lsn for the current transaction.
    fn get_final_lsn(&mut self, table_id: SrcTableId, xact_id: Option<u32>) -> u64 {
        if let Some(xid) = xact_id {
            self.streaming_transactions_state
                .entry(xid)
                .or_default()
                .touched_tables
                .insert(table_id);
            self.streaming_transactions_state
                .get(&xid)
                .unwrap()
                .final_lsn
        } else {
            self.transaction_state.touched_tables.insert(table_id);
            self.transaction_state.final_lsn
        }
    }

    pub async fn process_cdc_event(
        &mut self,
        event: CdcEvent,
    ) -> Result<Option<SchemaChangeRequest>, Infallible> {
        match event {
            CdcEvent::Begin(begin_body) => {
                debug!(final_lsn = begin_body.final_lsn(), "begin transaction");
                self.transaction_state.final_lsn = begin_body.final_lsn();
            }
            CdcEvent::StreamStart(stream_start_body) => {
                debug!(stream_id = stream_start_body.xid(), "stream start");
            }
            CdcEvent::Commit(commit_body) => {
                debug!(end_lsn = commit_body.end_lsn(), "commit transaction");
                for table_id in &self.transaction_state.touched_tables {
                    let event_sender = self.event_senders.get(table_id).cloned();
                    if let Some(commit_lsn_tx) = self.commit_lsn_txs.get(table_id).cloned() {
                        if let Err(e) = commit_lsn_tx.send(commit_body.end_lsn()) {
                            warn!(error = ?e, "failed to send commit lsn");
                        }
                    }
                    if let Some(event_sender) = event_sender {
                        if let Err(e) = event_sender
                            .send(TableEvent::Commit {
                                lsn: commit_body.end_lsn(),
                                xact_id: None,
                            })
                            .await
                        {
                            warn!(error = ?e, "failed to send commit event");
                        }
                    }
                }
                self.transaction_state.touched_tables.clear();
                self.replication_state
                    .mark(PgLsn::from(commit_body.end_lsn()));
            }
            CdcEvent::StreamCommit(stream_commit_body) => {
                let xact_id = stream_commit_body.xid();
                debug!(
                    xact_id,
                    end_lsn = stream_commit_body.end_lsn(),
                    "stream commit"
                );
                if let Some(tables_in_txn) = self.streaming_transactions_state.get(&xact_id) {
                    for table_id in &tables_in_txn.touched_tables {
                        let event_sender = self.event_senders.get(table_id).cloned();
                        if let Some(commit_lsn_tx) = self.commit_lsn_txs.get(table_id).cloned() {
                            if let Err(e) = commit_lsn_tx.send(stream_commit_body.end_lsn()) {
                                warn!(error = ?e, "failed to send stream commit lsn");
                            }
                        }
                        if let Some(event_sender) = event_sender {
                            if let Err(e) = event_sender
                                .send(TableEvent::Commit {
                                    lsn: stream_commit_body.end_lsn(),
                                    xact_id: Some(xact_id),
                                })
                                .await
                            {
                                warn!(error = ?e, "failed to send stream commit event");
                            }
                        }
                    }
                    self.streaming_transactions_state.remove(&xact_id);
                }
                self.replication_state
                    .mark(PgLsn::from(stream_commit_body.end_lsn()));
            }
            CdcEvent::Insert((table_id, table_row, xact_id)) => {
                let final_lsn = self.get_final_lsn(table_id, xact_id);
                let event_sender = self.event_senders.get(&table_id).cloned();
                if let Some(event_sender) = event_sender {
                    if let Err(e) = event_sender
                        .send(TableEvent::Append {
                            row: PostgresTableRow(table_row).into(),
                            lsn: final_lsn,
                            xact_id,
                            is_copied: false,
                        })
                        .await
                    {
                        warn!(error = ?e, "failed to send append event");
                    }
                    if let Some(xid) = xact_id {
                        self.streaming_transactions_state
                            .entry(xid)
                            .or_default()
                            .touched_tables
                            .insert(table_id);
                    } else {
                        self.transaction_state.touched_tables.insert(table_id);
                    }
                }
            }
            CdcEvent::Update((table_id, old_table_row, new_table_row, xact_id)) => {
                let final_lsn = self.get_final_lsn(table_id, xact_id);
                let event_sender = self.event_senders.get(&table_id).cloned();
                if let Some(event_sender) = event_sender {
                    if let Err(e) = event_sender
                        .send(TableEvent::Delete {
                            row: PostgresTableRow(old_table_row.unwrap()).into(),
                            lsn: final_lsn,
                            xact_id,
                        })
                        .await
                    {
                        warn!(error = ?e, "failed to send delete event");
                    }
                    if let Err(e) = event_sender
                        .send(TableEvent::Append {
                            row: PostgresTableRow(new_table_row).into(),
                            lsn: final_lsn,
                            xact_id,
                            is_copied: false,
                        })
                        .await
                    {
                        warn!(error = ?e, "failed to send append event");
                    }
                }
            }
            CdcEvent::Delete((table_id, table_row, xact_id)) => {
                let final_lsn = self.get_final_lsn(table_id, xact_id);
                let event_sender = self.event_senders.get(&table_id).cloned();
                if let Some(event_sender) = event_sender {
                    if let Err(e) = event_sender
                        .send(TableEvent::Delete {
                            row: PostgresTableRow(table_row).into(),
                            lsn: final_lsn,
                            xact_id,
                        })
                        .await
                    {
                        warn!(error = ?e, "failed to send delete event");
                    }
                }
            }
            CdcEvent::Relation(relation_body) => {
                debug!(
                    relation_id = relation_body.rel_id(),
                    relation_name = relation_body.name().unwrap_or("unknown"),
                    "Relation"
                );
                let src_table_id = relation_body.rel_id();
                let cache_entry = self.relation_cache.get_mut(&src_table_id);
                if let Some(cache_entry) = cache_entry {
                    if cache_entry.len() != relation_body.columns().len() {
                        return Ok(Some(SchemaChangeRequest(src_table_id)));
                    }
                }
            }
            CdcEvent::Type(type_body) => {
                debug!(
                    type_id = type_body.id(),
                    type_xid = type_body.xid(),
                    type_name = type_body.name().unwrap_or("unknown"),
                    "Type"
                );
            }
            CdcEvent::PrimaryKeepAlive(primary_keepalive_body) => {
                self.replication_state
                    .mark(PgLsn::from(primary_keepalive_body.wal_end()));
            }
            CdcEvent::StreamStop(_stream_stop_body) => {
                debug!("Stream stop");
            }
            CdcEvent::StreamAbort(stream_abort_body) => {
                let xact_id = stream_abort_body.xid();
                warn!(xact_id, "stream transaction aborted");
                if let Some(tables_in_txn) = self.streaming_transactions_state.get(&xact_id) {
                    for table_id in &tables_in_txn.touched_tables {
                        let event_sender = self.event_senders.get(table_id).cloned();
                        if let Some(event_sender) = event_sender {
                            if let Err(e) =
                                event_sender.send(TableEvent::StreamAbort { xact_id }).await
                            {
                                warn!(error = ?e, "failed to send stream abort event");
                            }
                        }
                    }
                }
                self.streaming_transactions_state.remove(&xact_id);
            }
        }
        Ok(None)
    }
}
