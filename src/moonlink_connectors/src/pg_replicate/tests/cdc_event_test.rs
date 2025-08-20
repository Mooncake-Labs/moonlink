#![cfg(feature = "connector-pg")]

use super::test_utils::{
    create_publication_for_table, create_replication_client_and_slot, database_url,
    fetch_table_schema, set_replica_identity_full, setup_connection, spawn_sql_executor,
    TestResources,
};
use crate::pg_replicate::conversions::cdc_event::CdcEvent;
use crate::pg_replicate::conversions::Cell;
use crate::pg_replicate::postgres_source::{CdcStreamConfig, PostgresSource};
use futures::StreamExt;
use serial_test::serial;
use std::collections::VecDeque;
use std::time::Duration;

const STREAM_NEXT_TIMEOUT_MS: u64 = 100;
const EVENT_COLLECTION_SECS: u64 = 5;
// Common helpers moved to tests/test_utils.rs

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_composite_types() {
    let client = setup_connection().await;

    let table_name = format!("test_composite_cdc");
    let publication = format!("test_composite_pub");
    let slot_name = format!("test_composite_slot");

    let mut resources = TestResources::new(client);
    resources.add_table(table_name.clone());
    resources.add_publication(publication.clone());
    resources.add_slot(slot_name.clone());

    // Create composite types
    resources.client()
        .simple_query(
            "CREATE TYPE test_address AS (street TEXT, city TEXT, zip INTEGER);
             CREATE TYPE test_point AS (x FLOAT8, y FLOAT8);
             CREATE TYPE test_location AS (name TEXT, point test_point);
             CREATE TYPE test_person AS (name TEXT, addresses test_address[], location test_location);",
        )
        .await
        .unwrap();
    resources.add_type("test_person");
    resources.add_type("test_location");
    resources.add_type("test_point");
    resources.add_type("test_address");

    // Create test table
    resources
        .client()
        .simple_query(&format!(
            "CREATE TABLE {table_name} (
                 id INTEGER PRIMARY KEY,
                 basic_addr test_address,
                 nested_loc test_location,
                 complex_person test_person,
                 addr_array test_address[]
             );"
        ))
        .await
        .unwrap();

    // Ensure replica identity is FULL so replication uses full row images
    set_replica_identity_full(resources.client(), &table_name).await;

    // Create publication
    create_publication_for_table(resources.client(), &publication, &table_name).await;
    resources
        .client()
        .simple_query(&format!(
            "INSERT INTO {table_name} VALUES 
             (1, 
              ROW('123 Main St', 'NYC', 10001)::test_address,
              ROW('Home', ROW(40.7, -74.0)::test_point)::test_location,
              ROW('Alice', 
                  ARRAY[ROW('123 Main St', 'NYC', 10001)::test_address, 
                        ROW('456 Oak Ave', 'LA', 90210)::test_address],
                  ROW('Work', ROW(34.0, -118.0)::test_point)::test_location
              )::test_person,
              ARRAY[ROW('789 Pine St', 'Chicago', 60601)::test_address,
                    ROW('321 Elm St', 'Boston', 02101)::test_address]
             );"
        ))
        .await
        .unwrap();

    let (replication_client, confirmed_flush_lsn) =
        create_replication_client_and_slot(&slot_name).await;

    // Create CDC stream configuration
    let cdc_config = CdcStreamConfig {
        publication: publication.clone(),
        slot_name: slot_name.clone(),
        confirmed_flush_lsn: confirmed_flush_lsn,
    };

    // Create CDC stream (converted events) and attach table schema for conversion
    let mut cdc_stream = PostgresSource::create_cdc_stream(replication_client, cdc_config.clone())
        .await
        .unwrap();

    // Fetch and add the table schema so composite parsing works
    let table_schema = fetch_table_schema(&publication, &table_name).await;
    let mut pinned_stream = Box::pin(cdc_stream);
    pinned_stream.as_mut().add_table_schema(table_schema);

    // Start a background SQL executor and submit changes via channel
    let sql_tx = spawn_sql_executor(database_url());
    resources.set_sql_tx(sql_tx.clone());
    sql_tx
        .send(format!(
            "INSERT INTO {table_name} VALUES \
             (2, ROW('999 Test St', 'Test City', 12345)::test_address, NULL, NULL, NULL);"
        ))
        .unwrap();
    sql_tx
        .send(format!(
            "UPDATE {table_name} \
             SET basic_addr = ROW('Updated St', 'Updated City', 54321)::test_address \
             WHERE id = 1;"
        ))
        .unwrap();
    sql_tx
        .send(format!(
            "INSERT INTO {table_name} VALUES\
             (3,\
              ROW('A St', 'A City', 11111)::test_address,\
              ROW('Place', ROW(1.5, 2.5)::test_point)::test_location,\
              ROW('Bob',\
                  ARRAY[ROW('X Ave', 'X City', 22222)::test_address,\
                        ROW('Y Blvd', 'Y City', 33333)::test_address],\
                  ROW('Office', ROW(3.25, 4.75)::test_point)::test_location\
              )::test_person,\
              ARRAY[ROW('Z Rd', 'Z City', 44444)::test_address,\
                    ROW('W Way', 'W City', 55555)::test_address]\
             );"
        ))
        .unwrap();
    sql_tx
        .send(format!("DELETE FROM {table_name} WHERE id = 2;"))
        .unwrap();

    // Collect CDC events for a limited time
    let mut events = Vec::new();
    let timeout = Duration::from_secs(EVENT_COLLECTION_SECS);
    let start_time = std::time::Instant::now();

    while start_time.elapsed() < timeout {
        match tokio::time::timeout(
            Duration::from_millis(STREAM_NEXT_TIMEOUT_MS),
            pinned_stream.next(),
        )
        .await
        {
            Ok(Some(Ok(event))) => {
                events.push(event);
            }
            Ok(Some(Err(e))) => panic!("Error in CDC stream: {:?}", e),
            Ok(None) => {
                // Stream ended
                break;
            }
            Err(_) => {
                // Timeout, continue
                continue;
            }
        }
    }

    assert!(!events.is_empty(), "No CDC events were received");

    // the events might include other types of events, eg. BEGIN, PrimaryKeepAlive, etc.
    // so using deque to pop the events in the order of execution sequence
    let mut q: VecDeque<_> = events.into_iter().collect();

    // Expect Insert id=2 first
    let inserted_row = loop {
        let ev = q.pop_front().expect("expected insert for id=2");
        if let CdcEvent::Insert((_, row, _)) = ev {
            if matches!(row.values.get(0), Some(Cell::I32(2))) {
                break row;
            }
        }
    };

    // basic_addr should be a composite (street, city, zip)
    match inserted_row.values.get(1) {
        Some(Cell::Composite(fields)) => {
            assert!(matches!(fields.get(0), Some(Cell::String(s)) if s == "999 Test St"));
            assert!(matches!(fields.get(1), Some(Cell::String(s)) if s == "Test City"));
            assert!(matches!(fields.get(2), Some(Cell::I32(12345))));
        }
        other => panic!("unexpected basic_addr cell: {:?}", other),
    }

    // validate update for id=1 next
    let update_row = loop {
        let ev = q.pop_front().expect("expected update for id=1");
        if let CdcEvent::Update((_, _, row, _)) = ev {
            if matches!(row.values.get(0), Some(Cell::I32(1))) {
                break row;
            }
        }
    };
    match update_row.values.get(1) {
        Some(Cell::Composite(fields)) => {
            assert!(matches!(fields.get(0), Some(Cell::String(s)) if s == "Updated St"));
            assert!(matches!(fields.get(1), Some(Cell::String(s)) if s == "Updated City"));
            assert!(matches!(fields.get(2), Some(Cell::I32(54321))));
        }
        other => panic!("unexpected updated basic_addr cell: {:?}", other),
    }

    // Expect insert for id=3 next
    let complex_row = loop {
        let ev = q.pop_front().expect("expected insert for id=3");
        if let CdcEvent::Insert((_, row, _)) = ev {
            if matches!(row.values.get(0), Some(Cell::I32(3))) {
                break row;
            }
        }
    };

    // Next expect delete for id=2
    let deleted_row = loop {
        let ev = q.pop_front().expect("expected delete for id=2");
        if let CdcEvent::Delete((_, row, _)) = ev {
            if matches!(row.values.get(0), Some(Cell::I32(2))) {
                break row;
            }
        }
    };

    assert!(matches!(deleted_row.values.get(0), Some(Cell::I32(2))));

    // basic_addr
    match complex_row.values.get(1) {
        Some(Cell::Composite(fields)) => {
            assert!(matches!(fields.get(0), Some(Cell::String(s)) if s == "A St"));
            assert!(matches!(fields.get(1), Some(Cell::String(s)) if s == "A City"));
            assert!(matches!(fields.get(2), Some(Cell::I32(11111))));
        }
        other => panic!("unexpected basic_addr cell: {:?}", other),
    }

    // nested_loc: (name TEXT, point test_point(F64,F64))
    match complex_row.values.get(2) {
        Some(Cell::Composite(fields)) => {
            assert!(matches!(fields.get(0), Some(Cell::String(s)) if s == "Place"));
            match fields.get(1) {
                Some(Cell::Composite(point)) => {
                    assert!(matches!(point.get(0), Some(Cell::F64(x)) if (*x - 1.5).abs() < 1e-9));
                    assert!(matches!(point.get(1), Some(Cell::F64(y)) if (*y - 2.5).abs() < 1e-9));
                }
                other => panic!("unexpected point in nested_loc: {:?}", other),
            }
        }
        other => panic!("unexpected nested_loc cell: {:?}", other),
    }

    // complex_person: (name TEXT, addresses test_address[], location test_location)
    match complex_row.values.get(3) {
        Some(Cell::Composite(fields)) => {
            // name
            assert!(matches!(fields.get(0), Some(Cell::String(s)) if s == "Bob"));
            // addresses array of composites
            match fields.get(1) {
                Some(Cell::Array(crate::pg_replicate::conversions::ArrayCell::Composite(arr))) => {
                    assert_eq!(arr.len(), 2);
                    // First address
                    let first = arr[0].as_ref().expect("first address should be Some");
                    assert!(matches!(first.get(0), Some(Cell::String(s)) if s == "X Ave"));
                    assert!(matches!(first.get(1), Some(Cell::String(s)) if s == "X City"));
                    assert!(matches!(first.get(2), Some(Cell::I32(22222))));
                    // Second address
                    let second = arr[1].as_ref().expect("second address should be Some");
                    assert!(matches!(second.get(0), Some(Cell::String(s)) if s == "Y Blvd"));
                    assert!(matches!(second.get(1), Some(Cell::String(s)) if s == "Y City"));
                    assert!(matches!(second.get(2), Some(Cell::I32(33333))));
                }
                other => panic!("unexpected addresses array: {:?}", other),
            }
            // location composite
            match fields.get(2) {
                Some(Cell::Composite(loc)) => {
                    assert!(matches!(loc.get(0), Some(Cell::String(s)) if s == "Office"));
                    match loc.get(1) {
                        Some(Cell::Composite(point)) => {
                            assert!(
                                matches!(point.get(0), Some(Cell::F64(x)) if (*x - 3.25).abs() < 1e-9)
                            );
                            assert!(
                                matches!(point.get(1), Some(Cell::F64(y)) if (*y - 4.75).abs() < 1e-9)
                            );
                        }
                        other => panic!("unexpected location.point: {:?}", other),
                    }
                }
                other => panic!("unexpected complex_person.location: {:?}", other),
            }
        }
        other => panic!("unexpected complex_person cell: {:?}", other),
    }

    // addr_array: array of test_address composites
    match complex_row.values.get(4) {
        Some(Cell::Array(crate::pg_replicate::conversions::ArrayCell::Composite(arr))) => {
            assert_eq!(arr.len(), 2);
            let a0 = arr[0].as_ref().expect("addr_array[0]");
            assert!(matches!(a0.get(0), Some(Cell::String(s)) if s == "Z Rd"));
            assert!(matches!(a0.get(1), Some(Cell::String(s)) if s == "Z City"));
            assert!(matches!(a0.get(2), Some(Cell::I32(44444))));
            let a1 = arr[1].as_ref().expect("addr_array[1]");
            assert!(matches!(a1.get(0), Some(Cell::String(s)) if s == "W Way"));
            assert!(matches!(a1.get(1), Some(Cell::String(s)) if s == "W City"));
            assert!(matches!(a1.get(2), Some(Cell::I32(55555))));
        }
        other => panic!("unexpected addr_array cell: {:?}", other),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_null() {
    // CdcEvent already imported above in this test module

    let client = setup_connection().await;

    let table_name = format!("test_cdc_string_null");
    let publication = format!("test_cdc_string_null_pub");
    let slot_name = format!("test_cdc_string_null_slot");

    let mut resources = TestResources::new(client);
    resources.add_table(table_name.clone());
    resources.add_publication(publication.clone());
    resources.add_slot(slot_name.clone());

    // Simple schema with text columns to verify stringified "null"
    resources
        .client()
        .simple_query(&format!(
            "CREATE TABLE {table_name} (
                id INTEGER PRIMARY KEY,
                t1 TEXT,
                t2 TEXT
            );"
        ))
        .await
        .unwrap();
    set_replica_identity_full(resources.client(), &table_name).await;

    // Publication
    create_publication_for_table(resources.client(), &publication, &table_name).await;

    let (replication_client, confirmed_flush_lsn) =
        create_replication_client_and_slot(&slot_name).await;

    let cdc_config = CdcStreamConfig {
        publication: publication.clone(),
        slot_name: slot_name.clone(),
        confirmed_flush_lsn: confirmed_flush_lsn,
    };

    let mut cdc_stream = PostgresSource::create_cdc_stream(replication_client, cdc_config.clone())
        .await
        .unwrap();

    let table_schema = fetch_table_schema(&publication, &table_name).await;
    let mut pinned_stream = Box::pin(cdc_stream);
    pinned_stream.as_mut().add_table_schema(table_schema);

    // Background executor
    let sql_tx = spawn_sql_executor(database_url());
    resources.set_sql_tx(sql_tx.clone());

    // Insert rows with literal 'null' strings
    sql_tx
        .send(format!(
            "INSERT INTO {table_name} VALUES
                (1, 'null', 'NULL'),
                (2, 'nuLL', 'not null'),
                (3, NULL, 'null');"
        ))
        .unwrap();

    // Collect events then search for inserts like test_composite_types
    let mut events = Vec::new();
    let timeout = Duration::from_secs(EVENT_COLLECTION_SECS);
    let start_time = std::time::Instant::now();
    while start_time.elapsed() < timeout {
        match tokio::time::timeout(
            Duration::from_millis(STREAM_NEXT_TIMEOUT_MS),
            pinned_stream.next(),
        )
        .await
        {
            Ok(Some(Ok(ev))) => events.push(ev),
            Ok(Some(Err(e))) => panic!("Error in CDC stream: {:?}", e),
            Ok(None) => break,
            Err(_) => continue,
        }
    }

    // Find inserted rows by id and validate stringified null handling
    let find_insert = |id: i32| -> Option<&crate::pg_replicate::conversions::table_row::TableRow> {
        events.iter().find_map(|e| {
            if let CdcEvent::Insert((_, row, _)) = e {
                if matches!(row.values.get(0), Some(Cell::I32(v)) if *v == id) {
                    return Some(row);
                }
            }
            None
        })
    };

    let row1 = find_insert(1).expect("missing insert id=1");
    assert!(matches!(row1.values.get(1), Some(Cell::String(s)) if s == "null"));
    assert!(matches!(row1.values.get(2), Some(Cell::String(s)) if s == "NULL"));

    let row2 = find_insert(2).expect("missing insert id=2");
    assert!(matches!(row2.values.get(1), Some(Cell::String(s)) if s == "nuLL"));
    assert!(matches!(row2.values.get(2), Some(Cell::String(s)) if s == "not null"));

    let row3 = find_insert(3).expect("missing insert id=3");
    assert!(matches!(row3.values.get(1), Some(Cell::Null)));
    assert!(matches!(row3.values.get(2), Some(Cell::String(s)) if s == "null"));
}
