/// This file implements chaos test for table handler.
///
/// System invariants:
/// - Begin events happen only after end events
/// - End events happen only after begin events
/// - Rows to delete comes from appended ones
/// - LSN always increases
use crate::event_sync::create_table_event_syncer;
use crate::row::{MoonlinkRow, RowValue};
use crate::storage::mooncake_table::table_creation_test_utils::*;
use crate::table_handler::{TableEvent, TableHandler};
use crate::union_read::ReadStateManager;
use crate::ObjectStorageCache;
use crate::TableEventManager;

use rand::prelude::*;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::{tempdir, TempDir};
use tokio::sync::mpsc::{self, Sender};
use tokio::sync::watch;

#[derive(Debug)]
struct ChaosState {
    current_lsn: u64,
    has_begun: bool,
    // Used to generate rows to insert.
    next_id: i32,
    inserted_rows: VecDeque<MoonlinkRow>,
}

fn create_row(id: i32, name: &str, age: i32) -> MoonlinkRow {
    MoonlinkRow::new(vec![
        RowValue::Int32(id),
        RowValue::ByteArray(name.as_bytes().to_vec()),
        RowValue::Int32(age),
    ])
}

impl ChaosState {
    fn new() -> Self {
        Self {
            current_lsn: 0,
            has_begun: false,
            next_id: 0,
            inserted_rows: VecDeque::new(),
        }
    }

    fn next_lsn(&mut self) -> u64 {
        self.current_lsn += 1;
        self.current_lsn
    }

    fn next_row(&mut self) -> MoonlinkRow {
        let row = create_row(self.next_id, /*name=*/ "user", self.next_id % 5);
        self.next_id += 1;
        row
    }
}

fn generate_random_events(state: &mut ChaosState, rng: &mut impl Rng) -> Vec<TableEvent> {
    #[derive(Debug, Clone)]
    enum EventKind {
        Begin,
        Append,
        Delete,
        EndWithFlush,
        EndNoFlush,
    }

    let mut choices = vec![];

    if !state.has_begun {
        choices.push(EventKind::Begin);
    } else {
        choices.push(EventKind::Append);
        if !state.inserted_rows.is_empty() {
            choices.push(EventKind::Delete);
        }
        choices.push(EventKind::EndWithFlush);
        choices.push(EventKind::EndNoFlush);
    }

    match *choices.choose(rng).unwrap() {
        EventKind::Begin => {
            state.has_begun = true;
            vec![TableEvent::Append {
                row: state.next_row(),
                xact_id: None,
                lsn: state.next_lsn(),
                is_copied: false,
            }]
        }
        EventKind::Append => {
            let row = state.next_row();
            state.inserted_rows.push_back(row.clone());
            vec![TableEvent::Append {
                row,
                xact_id: None,
                lsn: state.next_lsn(),
                is_copied: false,
            }]
        }
        EventKind::Delete => {
            let idx = rng.random_range(0..state.inserted_rows.len());
            let row = state.inserted_rows.remove(idx).unwrap();
            vec![TableEvent::Delete {
                row,
                xact_id: None,
                lsn: state.next_lsn(),
            }]
        }
        EventKind::EndWithFlush => {
            let lsn = state.next_lsn();
            state.has_begun = false;
            vec![
                TableEvent::Commit { lsn, xact_id: None },
                TableEvent::Flush { lsn },
            ]
        }
        EventKind::EndNoFlush => {
            let lsn = state.next_lsn();
            state.has_begun = false;
            vec![TableEvent::Commit { lsn, xact_id: None }]
        }
    }
}

#[allow(dead_code)]
struct TestEnvironment {
    iceberg_temp_dir: TempDir,
    cache_temp_dir: TempDir,
    table_temp_dir: TempDir,
    object_storage_cache: ObjectStorageCache,
    read_state_manager: ReadStateManager,
    table_event_manager: TableEventManager,
    table_handler: TableHandler,
    event_sender: Sender<TableEvent>,
    event_replay_rx: mpsc::UnboundedReceiver<TableEvent>,
}

impl TestEnvironment {
    async fn new() -> Self {
        let iceberg_temp_dir = tempdir().unwrap();
        let iceberg_table_config = get_iceberg_table_config(&iceberg_temp_dir);

        let table_temp_dir = tempdir().unwrap();
        let mooncake_table_metadata = create_test_table_metadata_with_index_merge(
            table_temp_dir.path().to_str().unwrap().to_string(),
        );

        // Local filesystem to store read-through cache.
        let cache_temp_dir = tempdir().unwrap();
        let object_storage_cache = ObjectStorageCache::default_for_test(&cache_temp_dir);

        // Create mooncake table and table event notification receiver.
        let table = create_mooncake_table(
            mooncake_table_metadata,
            iceberg_table_config,
            object_storage_cache.clone(),
        )
        .await;
        let (_, replication_rx) = watch::channel(0u64);
        let (_, last_commit_rx) = watch::channel(0u64);
        let read_state_manager =
            ReadStateManager::new(&table, replication_rx.clone(), last_commit_rx);
        let (table_event_sync_sender, table_event_sync_receiver) = create_table_event_syncer();
        let (event_replay_tx, event_replay_rx) = mpsc::unbounded_channel();
        let table_handler = TableHandler::new(
            table,
            table_event_sync_sender,
            replication_rx.clone(),
            Some(event_replay_tx),
        )
        .await;
        let table_event_manager =
            TableEventManager::new(table_handler.get_event_sender(), table_event_sync_receiver);
        let event_sender = table_handler.get_event_sender();

        Self {
            iceberg_temp_dir,
            cache_temp_dir,
            table_temp_dir,
            object_storage_cache,
            table_event_manager,
            read_state_manager,
            table_handler,
            event_sender,
            event_replay_rx,
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_chaos() {
    let mut env = TestEnvironment::new().await;
    let event_sender = env.event_sender.clone();

    let task = tokio::spawn(async move {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let mut rng = StdRng::seed_from_u64(nanos as u64);
        let mut state = ChaosState::new();

        // TODO(hjiang): Make iteration count a CLI configurable constant.
        for _ in 0..100 {
            let events = generate_random_events(&mut state, &mut rng);
            for cur_event in events.into_iter() {
                event_sender.send(cur_event).await.unwrap();
            }
        }

        // TODO(hjiang): Temporarily hard code a sleep time to trigger background tasks.
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;

        // If anything bad happens in the eventloop, drop table would fail.
        event_sender.send(TableEvent::DropTable).await.unwrap();
    });

    // Await the task directly and handle its result.
    let task_result = task.await;

    // Print out events in order if chaos test fails.
    if let Err(e) = task_result {
        // Display all enqueued events for debugging and replay.
        while let Some(cur_event) = env.event_replay_rx.recv().await {
            println!("{cur_event:?}");
        }
        // Propagate the panic to fail the test.
        if let Ok(panic) = e.try_into_panic() {
            std::panic::resume_unwind(panic);
        }
    }
}
