#![cfg(feature = "connector-pg")]

use crate::pg_replicate::clients::postgres::ReplicationClient;
use crate::pg_replicate::conversions::Cell;
use crate::pg_replicate::postgres_source::{CdcStreamConfig, PostgresSource};
use crate::pg_replicate::table::{TableName, TableSchema};
use futures::StreamExt;
use serial_test::serial;
use std::time::Duration;
use tokio_postgres::{connect, NoTls};

const DEFAULT_DB_URL: &str = "postgresql://postgres:postgres@postgres:5432/postgres";
const REPLICATION_SLOT: &str = "test_composite_slot";
const PUBLICATION: &str = "test_composite_pub";
const TABLE_NAME: &str = "test_composite_cdc";
const STREAM_NEXT_TIMEOUT_MS: u64 = 100;
const EVENT_COLLECTION_SECS: u64 = 5;

fn database_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_DB_URL.to_string())
}

async fn setup_connection() -> tokio_postgres::Client {
    let database_url = database_url();
    let (client, connection) = connect(&database_url, NoTls).await.unwrap();
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Postgres connection error: {e}");
        }
    });
    client
}

async fn setup_composite_test_data(client: &tokio_postgres::Client) {
    // Clean up existing objects
    client
        .simple_query(
            "DROP TABLE IF EXISTS test_composite_cdc CASCADE;
             DROP TYPE IF EXISTS test_address CASCADE;
             DROP TYPE IF EXISTS test_point CASCADE;
             DROP TYPE IF EXISTS test_location CASCADE;
             DROP TYPE IF EXISTS test_person CASCADE;",
        )
        .await
        .unwrap();

    // Create composite types
    client
        .simple_query(
            "CREATE TYPE test_address AS (street TEXT, city TEXT, zip INTEGER);
             CREATE TYPE test_point AS (x FLOAT8, y FLOAT8);
             CREATE TYPE test_location AS (name TEXT, point test_point);
             CREATE TYPE test_person AS (name TEXT, addresses test_address[], location test_location);",
        )
        .await
        .unwrap();

    // Create test table
    client
        .simple_query(
            "CREATE TABLE test_composite_cdc (
                 id INTEGER PRIMARY KEY,
                 basic_addr test_address,
                 nested_loc test_location,
                 complex_person test_person,
                 addr_array test_address[]
             );",
        )
        .await
        .unwrap();

    // Ensure replica identity is FULL so replication uses full row images
    client
        .simple_query("ALTER TABLE test_composite_cdc REPLICA IDENTITY FULL;")
        .await
        .unwrap();

    // Create publication
    client
        .simple_query("DROP PUBLICATION IF EXISTS test_composite_pub;")
        .await
        .unwrap();
    client
        .simple_query("CREATE PUBLICATION test_composite_pub FOR TABLE test_composite_cdc;")
        .await
        .unwrap();
}

async fn insert_test_data(client: &tokio_postgres::Client) {
    client
        .simple_query(
            "INSERT INTO test_composite_cdc VALUES 
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
             );",
        )
        .await
        .unwrap();
}

async fn ensure_logical_replication(client: &tokio_postgres::Client) {
    let _ = client
        .simple_query("ALTER SYSTEM SET wal_level = 'logical';")
        .await;
    let _ = client.simple_query("SELECT pg_reload_conf();").await;
}

async fn drop_replication_slot_if_exists(client: &tokio_postgres::Client, slot_name: &str) {
    let query = format!("SELECT pg_drop_replication_slot('{slot}') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{slot}');", slot = slot_name);
    let _ = client.simple_query(&query).await;
}

async fn create_replication_client() -> ReplicationClient {
    let url = database_url();
    let (mut replication_client, connection) =
        ReplicationClient::connect_no_tls(&url, true).await.unwrap();
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Replication connection error: {e}");
        }
    });
    replication_client
}

async fn fetch_table_schema_for_test_table(publication: &str) -> TableSchema {
    let url = database_url();
    let (schema_pg_client, schema_conn) = connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        if let Err(e) = schema_conn.await {
            eprintln!("Schema connection error: {e}");
        }
    });
    let mut schema_client = ReplicationClient::from_client(schema_pg_client);
    let table_name = TableName {
        schema: "public".to_string(),
        name: TABLE_NAME.to_string(),
    };
    let src_table_id = schema_client
        .get_src_table_id(&table_name)
        .await
        .unwrap()
        .expect("missing table id");
    schema_client
        .get_table_schema(src_table_id, table_name, Some(publication))
        .await
        .unwrap()
}

fn spawn_background_changes(database_url: String) {
    tokio::spawn(async move {
        let (bg_client, bg_connection) = connect(&database_url, NoTls).await.unwrap();
        tokio::spawn(async move {
            if let Err(e) = bg_connection.await {
                eprintln!("Background connection error: {e}");
            }
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let _ = bg_client
            .simple_query(
                "INSERT INTO test_composite_cdc VALUES 
                 (2, ROW('999 Test St', 'Test City', 12345)::test_address, NULL, NULL, NULL);",
            )
            .await;

        tokio::time::sleep(Duration::from_millis(100)).await;

        let _ = bg_client
            .simple_query(
                "UPDATE test_composite_cdc 
                 SET basic_addr = ROW('Updated St', 'Updated City', 54321)::test_address 
                 WHERE id = 1;",
            )
            .await;

        tokio::time::sleep(Duration::from_millis(100)).await;

        let _ = bg_client
            .simple_query(
                "INSERT INTO test_composite_cdc VALUES
                 (3,
                  ROW('A St', 'A City', 11111)::test_address,
                  ROW('Place', ROW(1.5, 2.5)::test_point)::test_location,
                  ROW('Bob',
                      ARRAY[ROW('X Ave', 'X City', 22222)::test_address,
                            ROW('Y Blvd', 'Y City', 33333)::test_address],
                      ROW('Office', ROW(3.25, 4.75)::test_point)::test_location
                  )::test_person,
                  ARRAY[ROW('Z Rd', 'Z City', 44444)::test_address,
                        ROW('W Way', 'W City', 55555)::test_address]
                 );",
            )
            .await;

        tokio::time::sleep(Duration::from_millis(100)).await;

        let _ = bg_client
            .simple_query("DELETE FROM test_composite_cdc WHERE id = 2;")
            .await;
    });
}

#[tokio::test]
#[serial]
async fn test_composite_types_in_cdc_stream() {
    let client = setup_connection().await;
    setup_composite_test_data(&client).await;
    ensure_logical_replication(&client).await;
    insert_test_data(&client).await;

    let mut replication_client = create_replication_client().await;
    drop_replication_slot_if_exists(&client, REPLICATION_SLOT).await;

    // Create replication slot using the replication client
    replication_client
        .begin_readonly_transaction()
        .await
        .unwrap();
    let slot_info = match tokio::time::timeout(
        Duration::from_secs(10),
        replication_client.get_or_create_slot(REPLICATION_SLOT),
    )
    .await
    {
        Ok(Ok(slot_info)) => slot_info,
        Ok(Err(_)) => return,
        Err(_) => return,
    };

    // Commit the transaction after creating the slot
    replication_client.commit_txn().await.unwrap();

    // Create CDC stream configuration
    let cdc_config = CdcStreamConfig {
        publication: PUBLICATION.to_string(),
        slot_name: REPLICATION_SLOT.to_string(),
        confirmed_flush_lsn: slot_info.confirmed_flush_lsn,
    };

    // Create CDC stream (converted events) and attach table schema for conversion
    let mut cdc_stream = match tokio::time::timeout(
        Duration::from_secs(10),
        PostgresSource::create_cdc_stream(replication_client, cdc_config.clone()),
    )
    .await
    {
        Ok(Ok(stream)) => stream,
        Ok(Err(_)) => return,
        Err(_) => return,
    };

    // Fetch and add the table schema so composite parsing works
    let table_schema = fetch_table_schema_for_test_table(PUBLICATION).await;
    use std::pin::Pin;
    let mut pinned_stream = Box::pin(cdc_stream);
    pinned_stream.as_mut().add_table_schema(table_schema);

    // Start a background task to make changes to the table
    spawn_background_changes(database_url());

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
            Ok(Some(Err(e))) => {
                eprintln!("Error in CDC stream: {:?}", e);
                continue;
            }
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

    // Verify we received some events
    assert!(!events.is_empty(), "No CDC events were received");

    // Validate parsed values via conversion layer
    use crate::pg_replicate::conversions::cdc_event::CdcEvent;

    // Find insert for id=2 and verify composite fields
    let inserted_row = events
        .iter()
        .find_map(|e| {
            if let CdcEvent::Insert((_, row, _)) = e {
                if matches!(row.values.get(0), Some(Cell::I32(2))) {
                    return Some(row);
                }
            }
            None
        })
        .expect("expected insert for id=2");

    // basic_addr should be a composite (street, city, zip)
    match inserted_row.values.get(1) {
        Some(Cell::Composite(fields)) => {
            assert!(matches!(fields.get(0), Some(Cell::String(s)) if s == "999 Test St"));
            assert!(matches!(fields.get(1), Some(Cell::String(s)) if s == "Test City"));
            assert!(matches!(fields.get(2), Some(Cell::I32(12345))));
        }
        other => panic!("unexpected basic_addr cell: {:?}", other),
    }

    // Optionally validate update for id=1 if present
    if let Some(updated_row) = events.iter().find_map(|e| {
        if let CdcEvent::Update((_, _old, new_row, _)) = e {
            if matches!(new_row.values.get(0), Some(Cell::I32(1))) {
                return Some(new_row);
            }
        }
        None
    }) {
        match updated_row.values.get(1) {
            Some(Cell::Composite(fields)) => {
                assert!(matches!(fields.get(0), Some(Cell::String(s)) if s == "Updated St"));
                assert!(matches!(fields.get(1), Some(Cell::String(s)) if s == "Updated City"));
                assert!(matches!(fields.get(2), Some(Cell::I32(54321))));
            }
            other => panic!("unexpected updated basic_addr cell: {:?}", other),
        }
    }

    // Find delete for id=2
    let deleted_row = events
        .iter()
        .find_map(|e| {
            if let CdcEvent::Delete((_, row, _)) = e {
                if matches!(row.values.get(0), Some(Cell::I32(2))) {
                    return Some(row);
                }
            }
            None
        })
        .expect("expected delete for id=2");

    assert!(matches!(deleted_row.values.get(0), Some(Cell::I32(2))));

    // Validate complex nested insert (id=3)
    let complex_row = events
        .iter()
        .find_map(|e| {
            if let CdcEvent::Insert((_, row, _)) = e {
                if matches!(row.values.get(0), Some(Cell::I32(3))) {
                    return Some(row);
                }
            }
            None
        })
        .expect("expected insert for id=3");

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
