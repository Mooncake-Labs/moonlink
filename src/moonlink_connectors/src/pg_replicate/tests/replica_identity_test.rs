#![cfg(feature = "connector-pg")]

use super::test_utils::{
    create_replication_client_and_slot, database_url, fetch_table_schema, setup_connection,
    spawn_sql_executor, TestResources,
};
use crate::pg_replicate::conversions::{cdc_event::CdcEvent, Cell};
use crate::pg_replicate::postgres_source::{CdcStreamConfig, PostgresSource};
use crate::pg_replicate::PostgresConnection;
use futures::StreamExt;
use moonlink::ReplicationState;
use serial_test::serial;
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_replica_identity_on_existing_partitions() {
    let mut resources = TestResources::new(setup_connection().await);
    resources.add_table("test_identity_root");
    resources.add_table("\"test identity's ordinary table\"");
    resources.client().simple_query(
        "CREATE TABLE test_identity_root (id integer PRIMARY KEY, payload text) PARTITION BY RANGE (id);
         CREATE TABLE test_identity_nested PARTITION OF test_identity_root FOR VALUES FROM (0) TO (10) PARTITION BY RANGE (id);
         CREATE TABLE test_identity_leaf0 PARTITION OF test_identity_nested FOR VALUES FROM (0) TO (10);
         CREATE TABLE test_identity_leaf1 PARTITION OF test_identity_root FOR VALUES FROM (10) TO (20);
         CREATE TABLE \"test identity's ordinary table\" (id integer, payload text);
         INSERT INTO test_identity_root VALUES (1, 'old first'), (11, 'old second');"
    ).await.unwrap();

    // Exercise the control operation without creating its shared publication/slot.
    let uri = database_url();
    let (cmd_tx, cmd_rx) = mpsc::channel(8);
    let mut connection = PostgresConnection {
        postgres_client: setup_connection().await,
        source: Arc::new(PostgresSource::new(&uri, None, None, false).await.unwrap()),
        uri,
        slot_name: String::new(),
        cmd_tx,
        cmd_rx: Some(cmd_rx),
        replication_state: ReplicationState::new(),
        retry_handles: vec![],
    };
    connection
        .alter_table_replica_identity("public.test_identity_root")
        .await
        .unwrap();
    connection
        .alter_table_replica_identity("public.test identity's ordinary table")
        .await
        .unwrap();

    let identities = resources
        .client()
        .query(
            "SELECT relreplident::text FROM pg_class WHERE oid IN
         (SELECT relid FROM pg_partition_tree('test_identity_root'))
         OR oid = '\"test identity''s ordinary table\"'::regclass",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(identities.len(), 5);
    assert!(identities.iter().all(|row| row.get::<_, String>(0) == "f"));

    let publication = "test_identity_pub";
    let slot = "test_identity_slot";
    resources.client().simple_query(
        "CREATE PUBLICATION test_identity_pub FOR TABLE test_identity_root WITH (publish_via_partition_root = true);"
    ).await.unwrap();
    resources.add_publication(publication);
    let (client, confirmed_flush_lsn) = create_replication_client_and_slot(slot).await;
    resources.add_slot(slot);
    let stream = PostgresSource::create_cdc_stream(
        client,
        CdcStreamConfig {
            publication: publication.to_owned(),
            slot_name: slot.to_owned(),
            confirmed_flush_lsn,
        },
    )
    .await
    .unwrap();
    let mut stream = Box::pin(stream);
    stream
        .as_mut()
        .add_table_schema(fetch_table_schema(publication, "test_identity_root").await);
    let sender = spawn_sql_executor(database_url());
    resources.set_sql_tx(sender.clone());
    sender.send("UPDATE test_identity_root SET payload = 'updated' WHERE id = 1; DELETE FROM test_identity_root WHERE id = 11;".to_owned()).unwrap();

    tokio::time::timeout(Duration::from_secs(10), async {
        let (mut updated, mut deleted) = (false, false);
        while !(updated && deleted) {
            match stream.next().await.unwrap().unwrap() {
                CdcEvent::Update((_, old, new, _)) => {
                    let old = old.expect("partition UPDATE must contain the full old row");
                    assert!(matches!(&old.values[..], [Cell::I32(1), Cell::String(s)] if s == "old first"));
                    assert!(matches!(&new.values[..], [Cell::I32(1), Cell::String(s)] if s == "updated"));
                    updated = true;
                }
                CdcEvent::Delete((_, old, _)) => {
                    assert!(matches!(&old.values[..], [Cell::I32(11), Cell::String(s)] if s == "old second"));
                    deleted = true;
                }
                _ => {}
            }
        }
    }).await.expect("partition UPDATE and DELETE events must arrive");
    drop(stream);
}
