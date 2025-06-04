use crate::pg_replicate::util::PostgresTableRow;
use crate::pg_replicate::{
    conversions::{cdc_event::CdcEvent, table_row::TableRow},
    replication_state::ReplicationState,
    table::TableId,
};
use moonlink::TableEvent;
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;
use tokio_postgres::types::PgLsn;

#[derive(Default)]
struct TransactionState {
    final_lsn: u64,
    touched_tables: HashSet<TableId>,
}

pub struct Sink {
    event_senders: HashMap<TableId, Sender<TableEvent>>,
    commit_lsn_txs: HashMap<TableId, watch::Sender<u64>>,
    streaming_transactions_state: HashMap<u32, TransactionState>,
    transaction_state: TransactionState,
    replication_state: Arc<ReplicationState>,
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
        }
    }
}

impl Sink {
    pub fn add_table(
        &mut self,
        table_id: TableId,
        event_sender: Sender<TableEvent>,
        commit_lsn_tx: watch::Sender<u64>,
    ) {
        self.event_senders.insert(table_id, event_sender);
        self.commit_lsn_txs.insert(table_id, commit_lsn_tx);
    }
    pub fn drop_table(&mut self, table_id: TableId) {
        self.event_senders.remove(&table_id).unwrap();
        self.commit_lsn_txs.remove(&table_id).unwrap();
    }

    pub async fn process_cdc_event(&mut self, event: CdcEvent) -> Result<PgLsn, Infallible> {
        match event {
            CdcEvent::Begin(begin_body) => {
                self.transaction_state.final_lsn = begin_body.final_lsn();
            }
            CdcEvent::StreamStart(_stream_start_body) => {}
            CdcEvent::Commit(commit_body) => {
                for table_id in &self.transaction_state.touched_tables {
                    let event_sender = self.event_senders.get(table_id).cloned();
                    if let Some(commit_lsn_tx) = self.commit_lsn_txs.get(table_id).cloned() {
                        let _ = commit_lsn_tx.send(commit_body.end_lsn());
                    }
                    if let Some(event_sender) = event_sender {
                        event_sender
                            .send(TableEvent::Commit {
                                lsn: commit_body.end_lsn(),
                                xact_id: None,
                            })
                            .await
                            .unwrap();
                    }
                }
                self.transaction_state.touched_tables.clear();
            }
            CdcEvent::StreamCommit(stream_commit_body) => {
                let xact_id = stream_commit_body.xid();
                if let Some(tables_in_txn) = self.streaming_transactions_state.get(&xact_id) {
                    for table_id in &tables_in_txn.touched_tables {
                        let event_sender = self.event_senders.get(table_id).cloned();
                        if let Some(commit_lsn_tx) = self.commit_lsn_txs.get(table_id).cloned() {
                            let _ = commit_lsn_tx.send(stream_commit_body.end_lsn());
                        }
                        if let Some(event_sender) = event_sender {
                            event_sender
                                .send(TableEvent::Commit {
                                    lsn: stream_commit_body.end_lsn(),
                                    xact_id: Some(xact_id),
                                })
                                .await
                                .unwrap();
                        }
                    }
                    self.streaming_transactions_state.remove(&xact_id);
                }
            }
            CdcEvent::Insert((table_id, table_row, xact_id)) => {
                let event_sender = self.event_senders.get(&table_id).cloned();
                if let Some(event_sender) = event_sender {
                    event_sender
                        .send(TableEvent::Append {
                            row: PostgresTableRow(table_row).into(),
                            xact_id,
                        })
                        .await
                        .unwrap();
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
                let final_lsn = if let Some(xid) = xact_id {
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
                };

                let event_sender = self.event_senders.get(&table_id).cloned();
                if let Some(event_sender) = event_sender {
                    event_sender
                        .send(TableEvent::Delete {
                            row: PostgresTableRow(old_table_row.unwrap()).into(),
                            lsn: final_lsn,
                            xact_id,
                        })
                        .await
                        .unwrap();
                    event_sender
                        .send(TableEvent::Append {
                            row: PostgresTableRow(new_table_row).into(),
                            xact_id,
                        })
                        .await
                        .unwrap();
                }
            }
            CdcEvent::Delete((table_id, table_row, xact_id)) => {
                let final_lsn = if let Some(xid) = xact_id {
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
                };

                let event_sender = self.event_senders.get(&table_id).cloned();
                if let Some(event_sender) = event_sender {
                    event_sender
                        .send(TableEvent::Delete {
                            row: PostgresTableRow(table_row).into(),
                            lsn: final_lsn,
                            xact_id,
                        })
                        .await
                        .unwrap();
                }
            }
            CdcEvent::Relation(relation_body) => println!("Relation {relation_body:?}"),
            CdcEvent::Type(type_body) => println!("Type {type_body:?}"),
            CdcEvent::PrimaryKeepAlive(primary_keepalive_body) => {
                self.replication_state
                    .mark(PgLsn::from(primary_keepalive_body.wal_end()));
            }
            CdcEvent::StreamStop(_stream_stop_body) => {}
            CdcEvent::StreamAbort(stream_abort_body) => {
                let xact_id = stream_abort_body.xid();
                if let Some(tables_in_txn) = self.streaming_transactions_state.get(&xact_id) {
                    for table_id in &tables_in_txn.touched_tables {
                        let event_sender = self.event_senders.get(table_id).cloned();
                        if let Some(event_sender) = event_sender {
                            event_sender
                                .send(TableEvent::StreamAbort { xact_id })
                                .await
                                .unwrap();
                        }
                    }
                }
                self.streaming_transactions_state.remove(&xact_id);
            }
        }
        Ok(PgLsn::from(0))
    }
}
