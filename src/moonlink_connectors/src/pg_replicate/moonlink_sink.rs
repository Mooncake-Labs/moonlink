use crate::pg_replicate::util::PostgresTableRow;
use crate::pg_replicate::{
    conversions::{cdc_event::CdcEvent, table_row::TableRow},
    table::{SrcTableId, TableSchema},
};
use moonlink::TableEvent;
use moonlink::{CommitState, ReplicationState};
use postgres_replication::protocol::Column as ReplicationColumn;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::convert::Infallible;
use std::sync::Arc;
use tokio::sync::mpsc::{error::TrySendError, Sender};
use tokio::sync::{mpsc, watch};
use tokio_postgres::types::PgLsn;
use tracing::{debug, error, warn};

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

pub struct Sink {
    event_senders: HashMap<SrcTableId, Sender<TableEvent>>,
    commit_lsn_txs: HashMap<SrcTableId, Arc<CommitState>>,
    streaming_transactions_state: HashMap<u32, TransactionState>,
    transaction_state: TransactionState,
    replication_state: Arc<ReplicationState>,
    /// Source table names, for logs.
    table_names: HashMap<SrcTableId, String>,
    /// Tables truncated inside a streamed transaction that has not committed yet.
    pending_stream_truncates: HashMap<u32, Vec<SrcTableId>>,
    /// Cached sender for the last table used on the hot path.
    /// Avoids a HashMap lookup when consecutive rows target the same table.
    cached_event_sender: Option<(SrcTableId, Sender<TableEvent>)>,
    /// Streaming hot-path cache of the last processed (xid, table_id, lsn).
    /// Skips streaming state lookup when the next row has the same xid and table.
    streaming_last_key: Option<(u32, SrcTableId, u64)>,
    /// Tracks the maximum LSN observed from primary keepalive messages.
    /// Used to assert that subsequent LSN-bearing CDC events are not older.
    max_keepalive_lsn_seen: u64,
    /// Tables whose initial copy is running, with the highest commit LSN streamed for them
    /// meanwhile. Commits up to the copy's boundary are in the copy and dropped by the table
    /// handler, so the table's commit LSN is only published once the boundary is known.
    copying_tables: HashMap<SrcTableId, u64>,
    /// Commit LSNs (copy boundaries) to publish once the replication LSN reaches them: a reader
    /// must never see a commit LSN the replication LSN does not cover yet.
    deferred_commit_marks: HashMap<SrcTableId, u64>,
}

impl Sink {
    #[inline(always)]
    async fn send_table_event(
        event_sender: &Sender<TableEvent>,
        event: TableEvent,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<TableEvent>> {
        match event_sender.try_send(event) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(event)) => match event_sender.reserve().await {
                Ok(permit) => {
                    permit.send(event);
                    Ok(())
                }
                // The table handler is gone (table dropped or failed); nothing to deliver to.
                Err(_) => Err(tokio::sync::mpsc::error::SendError(event)),
            },
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
            table_names: HashMap::new(),
            pending_stream_truncates: HashMap::new(),
            cached_event_sender: None,
            streaming_last_key: None,
            max_keepalive_lsn_seen: 0,
            copying_tables: HashMap::new(),
            deferred_commit_marks: HashMap::new(),
        }
    }

    /// Publish that `table_id` committed at `lsn`; held back while its initial copy runs.
    fn mark_table_commit(&mut self, table_id: SrcTableId, lsn: u64) {
        if let Some(highest) = self.copying_tables.get_mut(&table_id) {
            *highest = (*highest).max(lsn);
            return;
        }
        if let Some(commit_lsn_tx) = self.commit_lsn_txs.get(&table_id) {
            commit_lsn_tx.mark(lsn);
        }
    }

    /// What the stream has applied so far (commits and keepalives).
    pub fn replication_lsn(&self) -> u64 {
        self.replication_state.now()
    }

    /// Advance the replication LSN, first publishing the deferred commit LSNs it now covers.
    fn mark_replication(&mut self, lsn: u64) {
        let commit_lsn_txs = &self.commit_lsn_txs;
        self.deferred_commit_marks.retain(|table_id, commit_lsn| {
            if *commit_lsn > lsn {
                return true;
            }
            if let Some(commit_lsn_tx) = commit_lsn_txs.get(table_id) {
                commit_lsn_tx.mark(*commit_lsn);
            }
            false
        });
        self.replication_state.mark(lsn);
    }

    /// The initial copy of `table_id` finished at `boundary_lsn`. Its commit LSN becomes the
    /// boundary if rows were copied (an empty copy creates no snapshot to read at it), or the
    /// latest commit streamed after the boundary; never a commit the boundary dropped.
    pub fn finish_initial_copy(
        &mut self,
        table_id: SrcTableId,
        boundary_lsn: u64,
        rows_copied: bool,
    ) {
        let Some(highest_streamed) = self.copying_tables.remove(&table_id) else {
            return;
        };
        let mut commit_lsn = if rows_copied { boundary_lsn } else { 0 };
        if highest_streamed > boundary_lsn {
            commit_lsn = commit_lsn.max(highest_streamed);
        }
        if commit_lsn == 0 {
            return;
        }
        if commit_lsn <= self.replication_state.now() {
            if let Some(commit_lsn_tx) = self.commit_lsn_txs.get(&table_id) {
                commit_lsn_tx.mark(commit_lsn);
            }
        } else {
            self.deferred_commit_marks.insert(table_id, commit_lsn);
        }
    }

    /// Postgres never sends a change older than a keepalive it already sent on the same stream.
    /// If it ever does, log it rather than stopping replication for every table.
    fn warn_if_before_keepalive(&self, lsn: u64, what: &str) {
        if lsn < self.max_keepalive_lsn_seen {
            warn!(
                lsn,
                keepalive_lsn = self.max_keepalive_lsn_seen,
                "{what} LSN is older than an earlier keepalive"
            );
        }
    }

    /// Reset the per-connection keepalive floor. Should be called after establishing a new CDC stream.
    pub fn reset_keepalive_floor(&mut self) {
        self.max_keepalive_lsn_seen = 0;
    }

    /// Postgres re-sends transactions that were still streaming when replication reconnects, so
    /// their partially received rows are dropped.
    pub async fn abort_in_progress_streams(&mut self) {
        // Postgres re-sends these transactions from the start, truncates included.
        self.pending_stream_truncates.clear();
        for (xact_id, state) in std::mem::take(&mut self.streaming_transactions_state) {
            for table_id in state.touched_tables {
                if let Some(event_sender) = self.event_senders.get(&table_id) {
                    if let Err(e) =
                        Self::send_table_event(event_sender, TableEvent::StreamAbort { xact_id })
                            .await
                    {
                        warn!(error = ?e, xact_id, "failed to abort in-progress stream");
                    }
                }
            }
        }
        self.transaction_state = TransactionState::default();
        self.streaming_last_key = None;
    }
}

impl Sink {
    /// Register a table; with `initial_copy`, its commit LSN is held back until
    /// [`Self::finish_initial_copy`].
    pub fn add_table(
        &mut self,
        src_table_id: SrcTableId,
        event_sender: Sender<TableEvent>,
        commit_lsn_tx: Arc<CommitState>,
        table_schema: &TableSchema,
        initial_copy: bool,
    ) {
        if initial_copy {
            self.copying_tables.insert(src_table_id, 0);
        }
        self.event_senders.insert(src_table_id, event_sender);
        self.commit_lsn_txs.insert(src_table_id, commit_lsn_tx);
        self.table_names
            .insert(src_table_id, table_schema.table_name.to_string());
    }

    /// Stop forwarding events for a table. A table that was failed earlier is already detached.
    pub fn drop_table(&mut self, src_table_id: SrcTableId) {
        self.detach_table(src_table_id);
        self.table_names.remove(&src_table_id);
    }

    fn detach_table(&mut self, src_table_id: SrcTableId) -> Option<Sender<TableEvent>> {
        self.commit_lsn_txs.remove(&src_table_id);
        self.copying_tables.remove(&src_table_id);
        self.deferred_commit_marks.remove(&src_table_id);
        if let Some((cached_id, _)) = &self.cached_event_sender {
            if *cached_id == src_table_id {
                self.cached_event_sender = None;
            }
        }
        self.event_senders.remove(&src_table_id)
    }

    /// Mirrors do not apply TRUNCATE (a delete of every row would rewrite the whole table); a
    /// truncated source leaves its mirror out of sync, so the mirror enters the errored state.
    async fn fail_truncated_tables(&mut self, src_table_ids: Vec<SrcTableId>) {
        for src_table_id in src_table_ids {
            let table_name = self
                .table_names
                .get(&src_table_id)
                .cloned()
                .unwrap_or_else(|| src_table_id.to_string());
            self.fail_table(
                src_table_id,
                format!("source table {table_name} was truncated"),
            )
            .await;
        }
    }

    /// Put one mirror into the errored state because CDC can no longer apply its source's changes
    /// faithfully. Its later events are dropped; every other table keeps replicating.
    pub async fn fail_table(&mut self, src_table_id: SrcTableId, reason: String) {
        let table_name = self
            .table_names
            .get(&src_table_id)
            .cloned()
            .unwrap_or_else(|| src_table_id.to_string());
        let Some(event_sender) = self.detach_table(src_table_id) else {
            return;
        };
        error!(table = %table_name, %reason, "mirror can no longer follow its source table");
        if event_sender
            .send(TableEvent::EnterErrored { reason })
            .await
            .is_err()
        {
            warn!(table = %table_name, "table handler already stopped");
        }
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

    pub async fn process_cdc_event(&mut self, event: CdcEvent) -> Result<(), Infallible> {
        match event {
            CdcEvent::Begin(begin_body) => {
                debug!(final_lsn = begin_body.final_lsn(), "begin transaction");
                self.warn_if_before_keepalive(begin_body.final_lsn(), "begin");
                self.transaction_state.final_lsn = begin_body.final_lsn();
                self.transaction_state.last_touched_table = None;
                self.streaming_last_key = None;
            }
            CdcEvent::StreamStart(stream_start_body) => {
                debug!(stream_id = stream_start_body.xid(), "stream start");
            }
            CdcEvent::Commit(commit_body) => {
                debug!(end_lsn = commit_body.end_lsn(), "commit transaction");
                self.warn_if_before_keepalive(commit_body.end_lsn(), "commit");
                // Readers rely on this order (union_read::LsnView): mark table commit LSNs before the replication LSN.
                for table_id in self.transaction_state.touched_tables.clone() {
                    self.mark_table_commit(table_id, commit_body.end_lsn());
                    let event_sender = self.event_senders.get(&table_id);
                    if let Some(event_sender) = event_sender {
                        if let Err(e) = Self::send_table_event(
                            event_sender,
                            TableEvent::Commit {
                                lsn: commit_body.end_lsn(),
                                xact_id: None,
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
                let pg_lsn = PgLsn::from(commit_body.end_lsn());
                self.mark_replication(pg_lsn.into());
            }
            CdcEvent::StreamCommit(stream_commit_body) => {
                let xact_id = stream_commit_body.xid();
                debug!(
                    xact_id,
                    end_lsn = stream_commit_body.end_lsn(),
                    "stream commit"
                );
                self.warn_if_before_keepalive(stream_commit_body.end_lsn(), "stream commit");
                // Readers rely on this order (union_read::LsnView): mark table commit LSNs before the replication LSN.
                if let Some(tables_in_txn) = self.streaming_transactions_state.remove(&xact_id) {
                    for table_id in tables_in_txn.touched_tables {
                        self.mark_table_commit(table_id, stream_commit_body.end_lsn());
                        let event_sender = self.event_senders.get(&table_id);
                        if let Some(event_sender) = event_sender {
                            if let Err(e) = Self::send_table_event(
                                event_sender,
                                TableEvent::Commit {
                                    lsn: stream_commit_body.end_lsn(),
                                    xact_id: Some(xact_id),
                                },
                            )
                            .await
                            {
                                warn!(error = ?e, "failed to send stream commit event");
                            }
                        }
                    }
                }
                self.streaming_last_key = None;
                if let Some(truncated) = self.pending_stream_truncates.remove(&xact_id) {
                    self.fail_truncated_tables(truncated).await;
                }
                let pg_lsn = PgLsn::from(stream_commit_body.end_lsn());
                self.mark_replication(pg_lsn.into());
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
                            row: PostgresTableRow(old_table_row).into(),
                            lsn: final_lsn,
                            xact_id,
                            delete_if_exists: false,
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
                            delete_if_exists: false,
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
                // Schema changes are detected by the stream, in order, as TableUnsupported.
            }
            CdcEvent::TableUnsupported {
                src_table_id,
                reason,
            } => {
                self.fail_table(src_table_id, reason).await;
            }
            // Checked by the replication loop, which has a connection to the source.
            CdcEvent::RelationSeen { .. } => {}
            CdcEvent::Truncate {
                src_table_ids,
                xact_id,
            } => match xact_id {
                // A streamed transaction can still abort; act on its truncates at commit.
                Some(xact_id) => self
                    .pending_stream_truncates
                    .entry(xact_id)
                    .or_default()
                    .extend(src_table_ids),
                None => self.fail_truncated_tables(src_table_ids).await,
            },
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
                let wal_end = primary_keepalive_body.wal_end();
                self.warn_if_before_keepalive(wal_end, "keepalive");
                if wal_end > self.max_keepalive_lsn_seen {
                    self.max_keepalive_lsn_seen = wal_end;
                }
                self.mark_replication(pg_lsn.into());
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
                                TableEvent::StreamAbort { xact_id },
                            )
                            .await
                            {
                                warn!(error = ?e, "failed to send stream abort event");
                            }
                        }
                    }
                }
                self.streaming_transactions_state.remove(&xact_id);
                self.pending_stream_truncates.remove(&xact_id);
                self.streaming_last_key = None;
            }
        }
        Ok(())
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

    fn copying_sink(replication: &Arc<ReplicationState>) -> (Sink, Arc<CommitState>) {
        let mut sink = Sink::new(replication.clone());
        let (tx, _rx) = mpsc::channel(8);
        let commit = CommitState::new();
        sink.add_table(
            1,
            tx,
            commit.clone(),
            &make_table_schema(1),
            /*initial_copy=*/ true,
        );
        (sink, commit)
    }

    #[test]
    fn copy_boundary_ahead_of_the_stream_is_published_when_the_stream_reaches_it() {
        let replication = ReplicationState::new();
        let (mut sink, commit) = copying_sink(&replication);
        sink.mark_replication(50);
        sink.finish_initial_copy(1, 100, /*rows_copied=*/ true);
        // Never ahead of the replication LSN, which the copy path no longer touches.
        assert_eq!((commit.now(), replication.now()), (0, 50));
        sink.mark_replication(99);
        assert_eq!(commit.now(), 0);
        sink.mark_replication(120);
        assert_eq!((commit.now(), replication.now()), (100, 120));
    }

    #[test]
    fn commits_dropped_by_the_copy_boundary_are_never_published() {
        let replication = ReplicationState::new();
        let (mut sink, commit) = copying_sink(&replication);
        sink.mark_table_commit(1, 80);
        sink.mark_replication(80);
        assert_eq!(commit.now(), 0);
        // An empty copy has no snapshot at the boundary to read.
        sink.finish_initial_copy(1, 100, /*rows_copied=*/ false);
        sink.mark_replication(200);
        assert_eq!(commit.now(), 0);
        // Later commits are published as usual.
        sink.mark_table_commit(1, 210);
        assert_eq!(commit.now(), 210);
    }

    #[test]
    fn commits_after_the_boundary_streamed_during_the_copy_are_published() {
        let replication = ReplicationState::new();
        let (mut sink, commit) = copying_sink(&replication);
        sink.mark_table_commit(1, 150);
        sink.mark_replication(150);
        sink.finish_initial_copy(1, 100, /*rows_copied=*/ false);
        assert_eq!(commit.now(), 150);
    }

    #[tokio::test]
    async fn hot_path_streaming_caches_and_dedupes() {
        let replication_state = ReplicationState::new();
        let mut sink = Sink::new(replication_state);

        // Setup one table with event sender and commit lsn channel
        let table_id: SrcTableId = 1;
        let (tx, mut rx) = mpsc::channel::<TableEvent>(64);
        let commit_state = CommitState::new();
        let schema = make_table_schema(table_id);
        sink.add_table(table_id, tx, commit_state, &schema, false);

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
        let commit_state_a = CommitState::new();
        let commit_state_b = CommitState::new();
        sink.add_table(
            a,
            tx_a,
            commit_state_a.clone(),
            &make_table_schema(a),
            false,
        );
        sink.add_table(
            b,
            tx_b,
            commit_state_b.clone(),
            &make_table_schema(b),
            false,
        );

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
        let commit_state = CommitState::new();
        let mut sink = Sink::new(replication_state);

        let table_id: SrcTableId = 21;
        let (tx, _rx) = mpsc::channel::<TableEvent>(4);
        sink.add_table(
            table_id,
            tx,
            commit_state.clone(),
            &make_table_schema(table_id),
            false,
        );

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
        let commit_state = CommitState::new();
        let mut sink = Sink::new(replication_state);

        let table_id: SrcTableId = 31;
        let (tx, mut rx) = mpsc::channel::<TableEvent>(16);
        let (commit_tx, _commit_rx) = watch::channel::<u64>(0);
        sink.add_table(
            table_id,
            tx,
            commit_state.clone(),
            &make_table_schema(table_id),
            false,
        );

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
        let commit_state = CommitState::new();
        let mut sink = Sink::new(replication_state);

        let a: SrcTableId = 41;
        let b: SrcTableId = 42;
        let (tx_a, mut rx_a) = mpsc::channel::<TableEvent>(8);
        let (tx_b, mut rx_b) = mpsc::channel::<TableEvent>(8);
        let commit_state_a = CommitState::new();
        let commit_state_b = CommitState::new();
        sink.add_table(
            a,
            tx_a,
            commit_state_a.clone(),
            &make_table_schema(a),
            false,
        );
        sink.add_table(
            b,
            tx_b,
            commit_state_b.clone(),
            &make_table_schema(b),
            false,
        );

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
        let commit_state = CommitState::new();
        let mut sink = Sink::new(replication_state);

        let table_id: SrcTableId = 51;
        let (tx, mut rx) = mpsc::channel::<TableEvent>(8);
        sink.add_table(
            table_id,
            tx,
            commit_state.clone(),
            &make_table_schema(table_id),
            false,
        );

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
        let commit_state = CommitState::new();
        let mut sink = Sink::new(replication_state);

        let table_id: SrcTableId = 61;
        let (tx, mut rx) = mpsc::channel::<TableEvent>(8);
        sink.add_table(
            table_id,
            tx,
            commit_state.clone(),
            &make_table_schema(table_id),
            false,
        );

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
        Sink::send_table_event(&tx, TableEvent::DropTable)
            .await
            .unwrap();
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
