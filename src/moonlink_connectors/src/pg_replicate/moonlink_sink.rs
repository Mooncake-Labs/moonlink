use crate::pg_replicate::util::PostgresTableRow;
use crate::pg_replicate::{
    conversions::{cdc_event::CdcEvent, table_row::TableRow},
    table::{SrcTableId, TableSchema},
};
use crate::replication_state::ReplicationState;
use moonlink::TableEvent;
use postgres_replication::protocol::Column as ReplicationColumn;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::convert::Infallible;
use std::sync::Arc;
use tokio::sync::mpsc::{error::TrySendError, Sender};
use tokio::sync::{mpsc, watch};
use tokio_postgres::types::PgLsn;
use tracing::{debug, warn};

#[derive(Default)]
struct TransactionState {
    final_lsn: u64,
    /// Distinct tables touched in this transaction/stream, in first-touch order.
    touched_tables: Vec<SrcTableId>,
    /// Tracks the last table touched within
    /// this transaction/stream to avoid redundant inserts into `touched_tables` when consecutive rows
    /// target the same table.
    last_touched_table: Option<SrcTableId>,
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
    /// Cached sender for the last table used on the hot path.
    /// Avoids a HashMap lookup when consecutive rows target the same table.
    cached_event_sender: Option<(SrcTableId, Sender<TableEvent>)>,
    /// Streaming hot-path cache of the last processed (xid, table_id, lsn).
    /// Skips streaming state lookup when the next row has the same xid and table.
    streaming_last_key: Option<(u32, SrcTableId, u64)>,
}

impl Sink {
    #[inline(always)]
    async fn send_table_event(
        event_sender: &Sender<TableEvent>,
        event: TableEvent,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<TableEvent>> {
        match event_sender.try_send(event) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(event)) => {
                let permit = event_sender.reserve().await.expect("event receiver closed");
                permit.send(event);
                Ok(())
            }
            Err(TrySendError::Closed(event)) => Err(tokio::sync::mpsc::error::SendError(event)),
        }
    }
    pub fn new(replication_state: Arc<ReplicationState>) -> Self {
        Self {
            event_senders: HashMap::new(),
            commit_lsn_txs: HashMap::new(),
            streaming_transactions_state: HashMap::new(),
            transaction_state: TransactionState {
                final_lsn: 0,
                touched_tables: Vec::new(),
                last_touched_table: None,
            },
            replication_state,
            relation_cache: HashMap::new(),
            cached_event_sender: None,
            streaming_last_key: None,
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
        if let Some((cached_id, _)) = &self.cached_event_sender {
            if *cached_id == src_table_id {
                self.cached_event_sender = None;
            }
        }
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
        match xact_id {
            Some(xid) => {
                if let Some((last_xid, last_table, cached_lsn)) = self.streaming_last_key {
                    if last_xid == xid && last_table == table_id {
                        return cached_lsn;
                    }
                }
                let state = self.streaming_transactions_state.entry(xid).or_default();
                if state.last_touched_table != Some(table_id) {
                    if !state.touched_tables.contains(&table_id) {
                        state.touched_tables.push(table_id);
                    }
                    state.last_touched_table = Some(table_id);
                }
                let lsn = state.final_lsn;
                self.streaming_last_key = Some((xid, table_id, lsn));
                lsn
            }
            None => {
                if self.transaction_state.last_touched_table != Some(table_id) {
                    if !self.transaction_state.touched_tables.contains(&table_id) {
                        self.transaction_state.touched_tables.push(table_id);
                    }
                    self.transaction_state.last_touched_table = Some(table_id);
                }
                self.transaction_state.final_lsn
            }
        }
    }

    fn get_event_sender_for(&mut self, table_id: SrcTableId) -> Option<&Sender<TableEvent>> {
        if let Some((cached_id, _)) = &self.cached_event_sender {
            if *cached_id == table_id {
                return self.cached_event_sender.as_ref().map(|(_, s)| s);
            }
        }
        let sender = self.event_senders.get(&table_id).cloned();
        if let Some(sender) = sender {
            self.cached_event_sender = Some((table_id, sender));
            self.cached_event_sender.as_ref().map(|(_, s)| s)
        } else {
            None
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
                self.transaction_state.last_touched_table = None;
                self.streaming_last_key = None;
            }
            CdcEvent::StreamStart(stream_start_body) => {
                debug!(stream_id = stream_start_body.xid(), "stream start");
            }
            CdcEvent::Commit(commit_body) => {
                debug!(end_lsn = commit_body.end_lsn(), "commit transaction");
                let pg_lsn = PgLsn::from(commit_body.end_lsn());
                self.replication_state.mark(pg_lsn.into());
                for table_id in &self.transaction_state.touched_tables {
                    let event_sender = self.event_senders.get(table_id);
                    if let Some(commit_lsn_tx) = self.commit_lsn_txs.get(table_id).cloned() {
                        if let Err(e) = commit_lsn_tx.send(commit_body.end_lsn()) {
                            warn!(error = ?e, "failed to send commit lsn");
                        }
                    }
                    if let Some(event_sender) = event_sender {
                        if let Err(e) = Self::send_table_event(
                            event_sender,
                            TableEvent::Commit {
                                lsn: commit_body.end_lsn(),
                                xact_id: None,
                                is_recovery: false,
                            },
                        )
                        .await
                        {
                            warn!(error = ?e, "failed to send commit event");
                        }
                    }
                }
                self.transaction_state.touched_tables.clear();
                self.transaction_state.last_touched_table = None;
                self.streaming_last_key = None;
            }
            CdcEvent::StreamCommit(stream_commit_body) => {
                let xact_id = stream_commit_body.xid();
                debug!(
                    xact_id,
                    end_lsn = stream_commit_body.end_lsn(),
                    "stream commit"
                );
                let pg_lsn = PgLsn::from(stream_commit_body.end_lsn());
                self.replication_state.mark(pg_lsn.into());
                if let Some(tables_in_txn) = self.streaming_transactions_state.get(&xact_id) {
                    for table_id in &tables_in_txn.touched_tables {
                        let event_sender = self.event_senders.get(table_id);
                        if let Some(commit_lsn_tx) = self.commit_lsn_txs.get(table_id).cloned() {
                            if let Err(e) = commit_lsn_tx.send(stream_commit_body.end_lsn()) {
                                warn!(error = ?e, "failed to send stream commit lsn");
                            }
                        }
                        if let Some(event_sender) = event_sender {
                            if let Err(e) = Self::send_table_event(
                                event_sender,
                                TableEvent::Commit {
                                    lsn: stream_commit_body.end_lsn(),
                                    xact_id: Some(xact_id),
                                    is_recovery: false,
                                },
                            )
                            .await
                            {
                                warn!(error = ?e, "failed to send stream commit event");
                            }
                        }
                    }
                    self.streaming_transactions_state.remove(&xact_id);
                }
                self.streaming_last_key = None;
            }
            CdcEvent::Insert((table_id, table_row, xact_id)) => {
                let final_lsn = self.get_final_lsn(table_id, xact_id);
                if let Some(event_sender) = self.get_event_sender_for(table_id) {
                    if let Err(e) = Self::send_table_event(
                        event_sender,
                        TableEvent::Append {
                            row: PostgresTableRow(table_row).into(),
                            lsn: final_lsn,
                            xact_id,
                            is_copied: false,
                            is_recovery: false,
                        },
                    )
                    .await
                    {
                        warn!(error = ?e, "failed to send append event");
                    }
                }
            }
            CdcEvent::Update((table_id, old_table_row, new_table_row, xact_id)) => {
                let final_lsn = self.get_final_lsn(table_id, xact_id);
                if let Some(event_sender) = self.get_event_sender_for(table_id) {
                    if let Err(e) = Self::send_table_event(
                        event_sender,
                        TableEvent::Delete {
                            row: PostgresTableRow(old_table_row.unwrap()).into(),
                            lsn: final_lsn,
                            xact_id,
                            is_recovery: false,
                        },
                    )
                    .await
                    {
                        warn!(error = ?e, "failed to send delete event");
                    }
                    if let Err(e) = Self::send_table_event(
                        event_sender,
                        TableEvent::Append {
                            row: PostgresTableRow(new_table_row).into(),
                            lsn: final_lsn,
                            xact_id,
                            is_copied: false,
                            is_recovery: false,
                        },
                    )
                    .await
                    {
                        warn!(error = ?e, "failed to send append event");
                    }
                }
            }
            CdcEvent::Delete((table_id, table_row, xact_id)) => {
                let final_lsn = self.get_final_lsn(table_id, xact_id);
                if let Some(event_sender) = self.get_event_sender_for(table_id) {
                    if let Err(e) = Self::send_table_event(
                        event_sender,
                        TableEvent::Delete {
                            row: PostgresTableRow(table_row).into(),
                            lsn: final_lsn,
                            xact_id,
                            is_recovery: false,
                        },
                    )
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
                let pg_lsn = PgLsn::from(primary_keepalive_body.wal_end());
                self.replication_state.mark(pg_lsn.into());
            }
            CdcEvent::StreamStop(_stream_stop_body) => {
                debug!("Stream stop");
            }
            CdcEvent::StreamAbort(stream_abort_body) => {
                let xact_id = stream_abort_body.xid();
                warn!(xact_id, "stream transaction aborted");
                if let Some(tables_in_txn) = self.streaming_transactions_state.get(&xact_id) {
                    for table_id in &tables_in_txn.touched_tables {
                        if let Some(event_sender) = self.event_senders.get(table_id) {
                            if let Err(e) = Self::send_table_event(
                                event_sender,
                                TableEvent::StreamAbort {
                                    xact_id,
                                    is_recovery: false,
                                    closes_incomplete_wal_transaction: false,
                                },
                            )
                            .await
                            {
                                warn!(error = ?e, "failed to send stream abort event");
                            }
                        }
                    }
                }
                self.streaming_transactions_state.remove(&xact_id);
                self.streaming_last_key = None;
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pg_replicate::conversions::table_row::TableRow;
    use crate::pg_replicate::table::{ColumnSchema, LookupKey, TableName};
    use tokio::sync::{mpsc, watch};
    use tokio_postgres::types::Type;

    fn make_table_schema(src_table_id: SrcTableId) -> TableSchema {
        TableSchema {
            table_name: TableName {
                schema: "public".into(),
                name: format!("t_{src_table_id}"),
            },
            src_table_id,
            column_schemas: vec![ColumnSchema {
                name: "id".into(),
                typ: Type::INT4,
                modifier: 0,
                nullable: false,
            }],
            lookup_key: LookupKey::FullRow,
        }
    }

    #[tokio::test]
    async fn hot_path_streaming_caches_and_dedupes() {
        let replication_state = ReplicationState::new();
        let mut sink = Sink::new(replication_state);

        // Setup one table with event sender and commit lsn channel
        let table_id: SrcTableId = 1;
        let (tx, mut rx) = mpsc::channel::<TableEvent>(64);
        let (commit_tx, _commit_rx) = watch::channel::<u64>(0);
        let schema = make_table_schema(table_id);
        sink.add_table(table_id, tx, commit_tx, &schema);

        // Many inserts for the same (xid, table) pair
        let xid = Some(42u32);
        let rows = 10usize;
        for _ in 0..rows {
            let _ = sink
                .process_cdc_event(CdcEvent::Insert((
                    table_id,
                    TableRow { values: vec![] },
                    xid,
                )))
                .await
                .unwrap();
        }

        // Verify we only touched the table once in the streaming transaction state
        let state = sink
            .streaming_transactions_state
            .get(&xid.unwrap())
            .expect("streaming state present");
        assert_eq!(state.touched_tables, vec![table_id]);
        assert_eq!(state.last_touched_table, Some(table_id));

        // Verify micro-cache captured the last (xid, table)
        assert_eq!(sink.streaming_last_key, Some((xid.unwrap(), table_id, 0)));

        // Verify all Append events were emitted
        let mut append_count = 0;
        for _ in 0..rows {
            match rx.recv().await.expect("event") {
                TableEvent::Append { .. } => append_count += 1,
                ev => panic!("unexpected event: {ev:?}"),
            }
        }
        assert_eq!(append_count, rows);
    }

    #[tokio::test]
    async fn hot_path_non_streaming_vec_dedupe_across_tables() {
        let replication_state = ReplicationState::new();
        let mut sink = Sink::new(replication_state);

        // Two tables
        let a: SrcTableId = 11;
        let b: SrcTableId = 12;
        let (tx_a, mut rx_a) = mpsc::channel::<TableEvent>(8);
        let (tx_b, mut rx_b) = mpsc::channel::<TableEvent>(8);
        let (commit_tx_a, _rx_a) = watch::channel::<u64>(0);
        let (commit_tx_b, _rx_b) = watch::channel::<u64>(0);
        sink.add_table(a, tx_a, commit_tx_a, &make_table_schema(a));
        sink.add_table(b, tx_b, commit_tx_b, &make_table_schema(b));

        // Many inserts into A then into B within the same non-streaming transaction
        for _ in 0..5 {
            let _ = sink
                .process_cdc_event(CdcEvent::Insert((a, TableRow { values: vec![] }, None)))
                .await
                .unwrap();
        }
        for _ in 0..7 {
            let _ = sink
                .process_cdc_event(CdcEvent::Insert((b, TableRow { values: vec![] }, None)))
                .await
                .unwrap();
        }

        // Touched tables deduped: should contain both a and b exactly once
        assert_eq!(sink.transaction_state.touched_tables, vec![a, b]);
        assert_eq!(sink.transaction_state.last_touched_table, Some(b));

        // Drain a few events to ensure they were emitted to each table
        for _ in 0..5 {
            matches!(rx_a.recv().await.unwrap(), TableEvent::Append { .. });
        }
        for _ in 0..7 {
            matches!(rx_b.recv().await.unwrap(), TableEvent::Append { .. });
        }
    }

    #[tokio::test]
    async fn cached_sender_cleared_on_drop_table() {
        let replication_state = ReplicationState::new();
        let mut sink = Sink::new(replication_state);

        let table_id: SrcTableId = 21;
        let (tx, _rx) = mpsc::channel::<TableEvent>(4);
        let (commit_tx, _commit_rx) = watch::channel::<u64>(0);
        sink.add_table(table_id, tx, commit_tx, &make_table_schema(table_id));

        // Populate sender cache
        let _ = sink.get_event_sender_for(table_id);
        assert!(sink.cached_event_sender.is_some());

        // Drop table clears cache
        sink.drop_table(table_id);
        assert!(sink.cached_event_sender.is_none());
    }

    #[tokio::test]
    async fn interleaved_streams_do_not_use_stale_cache() {
        let replication_state = ReplicationState::new();
        let mut sink = Sink::new(replication_state);

        let table_id: SrcTableId = 31;
        let (tx, mut rx) = mpsc::channel::<TableEvent>(16);
        let (commit_tx, _commit_rx) = watch::channel::<u64>(0);
        sink.add_table(table_id, tx, commit_tx, &make_table_schema(table_id));

        let xid1 = Some(100u32);
        let xid2 = Some(200u32);

        // First insert with xid1 establishes cache
        let _ = sink
            .process_cdc_event(CdcEvent::Insert((
                table_id,
                TableRow { values: vec![] },
                xid1,
            )))
            .await
            .unwrap();
        assert_eq!(sink.streaming_last_key, Some((xid1.unwrap(), table_id, 0)));
        // Insert with xid2 must not use xid1 cache; updates cache to xid2
        let _ = sink
            .process_cdc_event(CdcEvent::Insert((
                table_id,
                TableRow { values: vec![] },
                xid2,
            )))
            .await
            .unwrap();
        assert_eq!(sink.streaming_last_key, Some((xid2.unwrap(), table_id, 0)));
        // Back to xid1 updates cache back to xid1
        let _ = sink
            .process_cdc_event(CdcEvent::Insert((
                table_id,
                TableRow { values: vec![] },
                xid1,
            )))
            .await
            .unwrap();
        assert_eq!(sink.streaming_last_key, Some((xid1.unwrap(), table_id, 0)));

        // Drain events to avoid channel overflow
        for _ in 0..3 {
            matches!(rx.recv().await.unwrap(), TableEvent::Append { .. });
        }

        // Both xids have independent touched_tables containing table_id exactly once
        assert_eq!(
            sink.streaming_transactions_state
                .get(&xid1.unwrap())
                .map(|s| s.touched_tables.clone()),
            Some(vec![table_id])
        );
        assert_eq!(
            sink.streaming_transactions_state
                .get(&xid2.unwrap())
                .map(|s| s.touched_tables.clone()),
            Some(vec![table_id])
        );
    }

    #[tokio::test]
    async fn cache_updates_on_table_change_same_xid() {
        let replication_state = ReplicationState::new();
        let mut sink = Sink::new(replication_state);

        let a: SrcTableId = 41;
        let b: SrcTableId = 42;
        let (tx_a, mut rx_a) = mpsc::channel::<TableEvent>(8);
        let (tx_b, mut rx_b) = mpsc::channel::<TableEvent>(8);
        let (commit_tx_a, _rx_a) = watch::channel::<u64>(0);
        let (commit_tx_b, _rx_b) = watch::channel::<u64>(0);
        sink.add_table(a, tx_a, commit_tx_a, &make_table_schema(a));
        sink.add_table(b, tx_b, commit_tx_b, &make_table_schema(b));

        let xid = Some(777u32);
        // A then B under same xid
        let _ = sink
            .process_cdc_event(CdcEvent::Insert((a, TableRow { values: vec![] }, xid)))
            .await
            .unwrap();
        assert_eq!(sink.streaming_last_key, Some((xid.unwrap(), a, 0)));
        let _ = sink
            .process_cdc_event(CdcEvent::Insert((b, TableRow { values: vec![] }, xid)))
            .await
            .unwrap();
        assert_eq!(sink.streaming_last_key, Some((xid.unwrap(), b, 0)));

        // Dedup holds each table once
        let state = sink
            .streaming_transactions_state
            .get(&xid.unwrap())
            .expect("state");
        assert_eq!(state.touched_tables, vec![a, b]);
        assert_eq!(state.last_touched_table, Some(b));

        // Drain events
        matches!(rx_a.recv().await.unwrap(), TableEvent::Append { .. });
        matches!(rx_b.recv().await.unwrap(), TableEvent::Append { .. });
    }

    #[tokio::test]
    async fn sender_cache_persists_across_xid_and_stream_like_boundaries() {
        let replication_state = ReplicationState::new();
        let mut sink = Sink::new(replication_state);

        let table_id: SrcTableId = 51;
        let (tx, mut rx) = mpsc::channel::<TableEvent>(8);
        let (commit_tx, _commit_rx) = watch::channel::<u64>(0);
        sink.add_table(table_id, tx, commit_tx, &make_table_schema(table_id));

        let xid1 = Some(1u32);
        let xid2 = Some(2u32);

        // First insert populates sender cache
        let _ = sink
            .process_cdc_event(CdcEvent::Insert((
                table_id,
                TableRow { values: vec![] },
                xid1,
            )))
            .await
            .unwrap();
        assert!(sink.cached_event_sender.is_some());

        // Simulate a chunk boundary by doing nothing; next insert with different xid should reuse sender cache
        let _ = sink
            .process_cdc_event(CdcEvent::Insert((
                table_id,
                TableRow { values: vec![] },
                xid2,
            )))
            .await
            .unwrap();
        assert!(sink.cached_event_sender.is_some());

        // Drain events
        for _ in 0..2 {
            matches!(rx.recv().await.unwrap(), TableEvent::Append { .. });
        }
    }

    #[tokio::test]
    async fn non_streaming_state_resets_between_transactions() {
        let replication_state = ReplicationState::new();
        let mut sink = Sink::new(replication_state);

        let table_id: SrcTableId = 61;
        let (tx, mut rx) = mpsc::channel::<TableEvent>(8);
        let (commit_tx, _commit_rx) = watch::channel::<u64>(0);
        sink.add_table(table_id, tx, commit_tx, &make_table_schema(table_id));

        // First transaction: several inserts (non-streaming)
        for _ in 0..3 {
            let _ = sink
                .process_cdc_event(CdcEvent::Insert((
                    table_id,
                    TableRow { values: vec![] },
                    None,
                )))
                .await
                .unwrap();
        }
        assert_eq!(sink.transaction_state.touched_tables, vec![table_id]);
        assert_eq!(sink.transaction_state.last_touched_table, Some(table_id));
        assert!(sink.cached_event_sender.is_some());
        for _ in 0..3 {
            matches!(rx.recv().await.unwrap(), TableEvent::Append { .. });
        }

        // Simulate commit boundary: reset non-streaming state
        sink.transaction_state.touched_tables.clear();
        sink.transaction_state.last_touched_table = None;

        // Second transaction: inserts again, should behave like fresh state
        for _ in 0..2 {
            let _ = sink
                .process_cdc_event(CdcEvent::Insert((
                    table_id,
                    TableRow { values: vec![] },
                    None,
                )))
                .await
                .unwrap();
        }
        assert_eq!(sink.transaction_state.touched_tables, vec![table_id]);
        assert_eq!(sink.transaction_state.last_touched_table, Some(table_id));
        // Sender cache should remain usable across transactions
        assert!(sink.cached_event_sender.is_some());
        for _ in 0..2 {
            matches!(rx.recv().await.unwrap(), TableEvent::Append { .. });
        }
    }

    #[tokio::test]
    async fn test_send_table_event_ok() {
        let (tx, mut rx) = mpsc::channel(1);
        let res = Sink::send_table_event(&tx, TableEvent::DropTable).await;
        assert!(res.is_ok());
        let msg = rx.recv().await;
        assert!(matches!(msg, Some(TableEvent::DropTable)));
    }

    #[tokio::test]
    async fn test_send_table_event_full_reserve_path() {
        let (tx, mut rx) = mpsc::channel(1);
        tx.try_send(TableEvent::DropTable).unwrap();

        let send_future = Sink::send_table_event(&tx, TableEvent::DropTable);
        let drain_future = async {
            // Give the sender a moment to hit the reserve path
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            // Free one slot so reserve() can complete
            let first = rx.recv().await;
            assert!(matches!(first, Some(TableEvent::DropTable)));
            // Receive the second event that was sent via the permit
            let second = rx.recv().await;
            assert!(matches!(second, Some(TableEvent::DropTable)));
        };

        let (send_res, _) = tokio::join!(send_future, drain_future);
        assert!(send_res.is_ok());
    }

    #[tokio::test]
    async fn test_send_table_event_closed() {
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        let res = Sink::send_table_event(&tx, TableEvent::DropTable).await;
        assert!(res.is_err());
    }
}
