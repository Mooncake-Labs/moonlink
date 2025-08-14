mod common;

#[cfg(test)]
mod tests {
    use super::common::{
        current_wal_lsn, ids_from_state, ids_from_state_with_deletes, TestGuard, SRC_URI, TABLE_ID,
    };
    use serial_test::serial;
    use std::collections::HashSet;
    use std::sync::Arc;
    use tokio_postgres::{connect, NoTls};

    // Initial copy tests can be added here
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_initial_copy_handles_existing_rows() {
        // First, create our own PostgreSQL client to pre-populate data
        let (initial_client, connection) = connect(SRC_URI, NoTls).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let table_name = "copy_test";

        // Create the PostgreSQL table and pre-populate it with existing rows
        initial_client
            .simple_query(&format!(
                "DROP TABLE IF EXISTS {table_name};
                 CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, name TEXT);"
            ))
            .await
            .unwrap();
        initial_client
            .simple_query(&format!(
                "INSERT INTO {table_name} VALUES (1,'old_a'),(2,'old_b');"
            ))
            .await
            .unwrap();

        // Create the backend with no tables
        let (guard, _) = TestGuard::new(None, true).await;
        let backend = guard.backend();

        // Register the table - this kicks off *initial copy* in the background
        backend
            .create_table(
                guard.database_id,
                TABLE_ID,
                format!("public.{table_name}"),
                SRC_URI.to_string(),
                guard.get_serialized_table_config(),
                None, /* input_schema */
            )
            .await
            .unwrap();

        // While copy is in-flight, send an additional row that must be *buffered*
        initial_client
            .simple_query(&format!("INSERT INTO {table_name} VALUES (3,'new_c');"))
            .await
            .unwrap();

        let lsn_after_insert = current_wal_lsn(&initial_client).await;

        let ids = ids_from_state(
            &backend
                .scan_table(guard.database_id, TABLE_ID, Some(lsn_after_insert))
                .await
                .unwrap(),
        );

        assert_eq!(ids, HashSet::from([1, 2, 3]));

        // Manually drop the table we created
        initial_client
            .simple_query(&format!("DROP TABLE IF EXISTS {table_name};"))
            .await
            .unwrap();
        let _ = backend.drop_table(guard.database_id, TABLE_ID).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_initial_copy_handles_large_existing_rows() {
        // First, create our own PostgreSQL client to pre-populate data
        let (initial_client, connection) = connect(SRC_URI, NoTls).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let table_name = "copy_test";

        // Create the PostgreSQL table and pre-populate it with existing rows
        initial_client
            .simple_query(&format!(
                "DROP TABLE IF EXISTS {table_name};
                 CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, name TEXT);"
            ))
            .await
            .unwrap();

        let row_count = 1024i64;
        initial_client
            .simple_query(&format!(
                "INSERT INTO {table_name}
                 SELECT gs, 'base'
                 FROM generate_series(1, {row_count}) AS gs;",
            ))
            .await
            .unwrap();

        // Create the backend with no tables
        let (guard, _) = TestGuard::new(None, true).await;
        let backend = guard.backend();

        // Register the table - this kicks off *initial copy* in the background
        backend
            .create_table(
                guard.database_id,
                TABLE_ID,
                format!("public.{table_name}"),
                SRC_URI.to_string(),
                guard.get_serialized_table_config(),
                None, /* input_schema */
            )
            .await
            .unwrap();

        // sleep for 5 seconds
        // tokio::time::sleep(Duration::from_secs(5)).await;

        let lsn_after_insert = current_wal_lsn(&initial_client).await;

        let ids = ids_from_state(
            &backend
                .scan_table(guard.database_id, TABLE_ID, Some(lsn_after_insert))
                .await
                .unwrap(),
        );

        let expected: HashSet<i64> = (1..=row_count).collect();
        assert_eq!(ids, expected);

        // Manually drop the table we created
        initial_client
            .simple_query(&format!("DROP TABLE IF EXISTS {table_name};"))
            .await
            .unwrap();
        let _ = backend.drop_table(guard.database_id, TABLE_ID).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_initial_copy_handles_inserts_during_copy() {
        let (initial_client, connection) = connect(SRC_URI, NoTls).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let table_name = "copy_insert_during";

        // Prepare a table with many rows so the copy takes some time
        let row_count = 10000i64;
        initial_client
            .simple_query(&format!(
                "DROP TABLE IF EXISTS {table_name};
                 CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, name TEXT);
                 INSERT INTO {table_name}
                 SELECT gs, 'base'
                 FROM generate_series(1, {row_count}) AS gs;",
            ))
            .await
            .unwrap();

        let (guard, _) = TestGuard::new(None, true).await;
        let backend = Arc::clone(guard.backend());

        // Start create_table without awaiting so we can modify data during copy
        let backend_clone = Arc::clone(&backend);
        let table_config = guard.get_serialized_table_config();
        let create_handle = tokio::spawn(async move {
            backend_clone
                .create_table(
                    guard.database_id,
                    TABLE_ID,
                    format!("public.{table_name}"),
                    SRC_URI.to_string(),
                    table_config,
                    None, /* input_schema */
                )
                .await
                .unwrap();
        });

        // Insert a brand new row while copy is running
        initial_client
            .simple_query(&format!(
                "INSERT INTO {table_name} VALUES ({},'extra');",
                row_count + 1,
                table_name = table_name
            ))
            .await
            .unwrap();

        let lsn = current_wal_lsn(&initial_client).await;
        create_handle.await.unwrap();
        let ids = ids_from_state(
            &backend
                .scan_table(guard.database_id, TABLE_ID, Some(lsn))
                .await
                .unwrap(),
        );

        let mut expected: HashSet<i64> = (1..=row_count).collect();
        expected.insert(row_count + 1); // inserted row

        assert_eq!(ids.len(), expected.len());
        assert_eq!(ids, expected);

        initial_client
            .simple_query(&format!("DROP TABLE IF EXISTS {table_name};"))
            .await
            .unwrap();
        let _ = backend.drop_table(guard.database_id, TABLE_ID).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_initial_copy_handles_updates_during_copy() {
        let (initial_client, connection) = connect(SRC_URI, NoTls).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let table_name = "copy_update_during";

        // Prepare a table with many rows so the copy takes some time
        let row_count = 10000i64;
        initial_client
            .simple_query(&format!(
                "DROP TABLE IF EXISTS {table_name};
                 CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, name TEXT);
                 INSERT INTO {table_name}
                 SELECT gs, 'base'
                 FROM generate_series(1, {row_count}) AS gs;",
            ))
            .await
            .unwrap();

        let (guard, _) = TestGuard::new(None, true).await;
        let backend = Arc::clone(guard.backend());

        // Start create_table without awaiting so we can modify data during copy
        let backend_clone = Arc::clone(&backend);
        let table_config = guard.get_serialized_table_config();
        let create_handle = tokio::spawn(async move {
            backend_clone
                .create_table(
                    guard.database_id,
                    TABLE_ID,
                    format!("public.{table_name}"),
                    SRC_URI.to_string(),
                    table_config,
                    None, /* input_schema */
                )
                .await
                .unwrap();
        });

        // Perform various mutations while copy is running
        // Update id 1 -> row_count + 1
        initial_client
            .simple_query(&format!(
                "UPDATE {table_name} SET id = {new_id} WHERE id = 1;",
                table_name = table_name,
                new_id = row_count + 1
            ))
            .await
            .unwrap();
        // Delete id 2
        initial_client
            .simple_query(&format!("DELETE FROM {table_name} WHERE id = 2;"))
            .await
            .unwrap();
        // Insert a brand new row
        initial_client
            .simple_query(&format!(
                "INSERT INTO {table_name} VALUES ({},'extra');",
                row_count + 2,
                table_name = table_name
            ))
            .await
            .unwrap();

        // Wait for the copy to finish
        create_handle.await.unwrap();

        // Insert another row after copy completes
        initial_client
            .simple_query(&format!(
                "INSERT INTO {table_name} VALUES ({},'after');",
                row_count + 3,
                table_name = table_name
            ))
            .await
            .unwrap();

        let lsn = current_wal_lsn(&initial_client).await;
        let ids = ids_from_state(
            &backend
                .scan_table(guard.database_id, TABLE_ID, Some(lsn))
                .await
                .unwrap(),
        );

        let mut expected: HashSet<i64> = (3..=row_count).collect();
        expected.insert(row_count + 1); // updated id
        expected.insert(row_count + 2); // inserted during copy
        expected.insert(row_count + 3); // inserted after copy

        assert_eq!(ids, expected);

        initial_client
            .simple_query(&format!("DROP TABLE IF EXISTS {table_name};"))
            .await
            .unwrap();
        let _ = backend.drop_table(guard.database_id, TABLE_ID).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_initial_copy_handles_deletes_during_copy() {
        let (initial_client, connection) = connect(SRC_URI, NoTls).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let table_name = "copy_delete_during";

        initial_client
            .simple_query(&format!(
                "DROP TABLE IF EXISTS {table_name};
                 CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, name TEXT);"
            ))
            .await
            .unwrap();

        let row_count = 10_000i64;
        initial_client
            .simple_query(&format!(
                "INSERT INTO {table_name}
                 SELECT gs, 'base'
                 FROM generate_series(1, {row_count}) AS gs;"
            ))
            .await
            .unwrap();

        let (guard, new_client) = TestGuard::new(None, true).await;
        let backend = Arc::clone(guard.backend());

        let backend_clone = Arc::clone(&backend);
        backend_clone
            .create_table(
                guard.database_id,
                TABLE_ID,
                format!("public.{table_name}"),
                SRC_URI.to_string(),
                guard.get_serialized_table_config(),
                None, /* input_schema */
            )
            .await
            .unwrap();

        // Delete one of the rows while copy is executing
        new_client
            .simple_query(&format!("DELETE FROM {table_name} WHERE id = 1;"))
            .await
            .unwrap();

        // Add another row after copy finishes
        initial_client
            .simple_query(&format!(
                "INSERT INTO {table_name} VALUES ({},'c');",
                row_count + 1,
                table_name = table_name
            ))
            .await
            .unwrap();

        let lsn = current_wal_lsn(&initial_client).await;
        let ids = ids_from_state_with_deletes(
            &backend
                .scan_table(guard.database_id, TABLE_ID, Some(lsn))
                .await
                .unwrap(),
        )
        .await;

        let mut expected: HashSet<i64> = (2..=row_count).collect();
        expected.insert(row_count + 1);
        assert!(!ids.contains(&1));
        assert_eq!(ids, expected);

        initial_client
            .simple_query(&format!("DROP TABLE IF EXISTS {table_name};"))
            .await
            .unwrap();
        let _ = backend.drop_table(guard.database_id, TABLE_ID).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_initial_copy_insert_then_delete_during_copy() {
        let (initial_client, connection) = connect(SRC_URI, NoTls).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let table_name = "copy_insert_delete";
        let row_count = 10_000i64;

        initial_client
            .simple_query(&format!(
                "DROP TABLE IF EXISTS {table_name};
                 CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, name TEXT);
                 INSERT INTO {table_name}
                 SELECT gs, 'base'
                 FROM generate_series(1, {row_count}) AS gs;",
            ))
            .await
            .unwrap();

        let (guard, new_client) = TestGuard::new(None, true).await;
        let backend = Arc::clone(guard.backend());

        let backend_clone = Arc::clone(&backend);
        backend_clone
            .create_table(
                guard.database_id,
                TABLE_ID,
                format!("public.{table_name}"),
                SRC_URI.to_string(),
                guard.get_serialized_table_config(),
                None, /* input_schema */
            )
            .await
            .unwrap();

        // Delete one of the rows currently being copied
        new_client
            .simple_query(&format!(
                "DELETE FROM {table_name} WHERE id = {};",
                1,
                table_name = table_name
            ))
            .await
            .unwrap();

        let lsn = current_wal_lsn(&initial_client).await;
        let ids = ids_from_state_with_deletes(
            &backend
                .scan_table(guard.database_id, TABLE_ID, Some(lsn))
                .await
                .unwrap(),
        )
        .await;

        let expected: HashSet<i64> = (2..=row_count).collect();

        assert!(!ids.contains(&1));
        assert_eq!(ids.len(), expected.len());
        assert_eq!(ids, expected);

        initial_client
            .simple_query(&format!("DROP TABLE IF EXISTS {table_name};"))
            .await
            .unwrap();
        let _ = backend.drop_table(guard.database_id, TABLE_ID).await;
    }

    /// A kitchen-sink stress test that:
    ///  * copies 50 k rows
    ///  * inserts 100 new rows while the copy is in-flight
    ///  * updates a PK (id = 1 → row_count + 101) mid-copy
    ///  * bulk-deletes 20 % of the original table mid-copy
    ///  * attempts (and then rolls back) an additional big insertion
    ///  * verifies the final state exactly matches what succeeded & was committed
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn stress_test_initial_copy_heavy_mutations() {
        let (initial_client, connection) = connect(SRC_URI, NoTls).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let table_name = "copy_stress";
        let row_count: i64 = 500_000;

        // (Re)create table and seed a large baseline.
        initial_client
            .simple_query(&format!(
                "DROP TABLE IF EXISTS {table_name};
                 CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, name TEXT);
                 INSERT INTO {table_name}
                 SELECT gs, 'base'
                 FROM generate_series(1, {row_count}) AS gs;"
            ))
            .await
            .unwrap();

        // Spin up backend & kick off the initial copy in its own task.
        let (guard, new_client) = TestGuard::new(None, true).await;
        let backend = Arc::clone(guard.backend());

        let backend_clone = Arc::clone(&backend);
        backend_clone
            .create_table(
                guard.database_id,
                TABLE_ID,
                format!("public.{table_name}"),
                SRC_URI.to_string(),
                guard.get_serialized_table_config(),
                None, /* input_schema */
            )
            .await
            .unwrap();

        // ===== 1. Massive concurrent mutations while COPY is still running =====
        // (a) Insert 100 fresh rows that must be buffered then applied.
        new_client
            .simple_query(&format!(
                "INSERT INTO {table_name}
                 SELECT gs, 'hot_insert'
                 FROM generate_series({start_id}, {end_id}) AS gs;",
                table_name = table_name,
                start_id = row_count + 1,
                end_id = row_count + 100
            ))
            .await
            .unwrap();

        // // (b) Update a primary key (forces delete+insert under logical replication).
        new_client
            .simple_query(&format!(
                "UPDATE {table_name}
                 SET id = {new_id}
                 WHERE id = 1;",
                table_name = table_name,
                new_id = row_count + 101
            ))
            .await
            .unwrap();

        // (c) Delete 20 % of the original rows (ids divisible by 5).
        new_client
            .simple_query(&format!(
                "DELETE FROM {table_name}
                 WHERE id % 5::BIGINT = 0;"
            ))
            .await
            .unwrap();

        // (d) Aborted transaction (should have **zero** effect downstream).
        new_client
            .simple_query(&format!(
                "BEGIN;
                 INSERT INTO {table_name}
                 SELECT gs, 'rolled_back'
                 FROM generate_series({rb_start}, {rb_end}) AS gs;
                 ROLLBACK;",
                table_name = table_name,
                rb_start = row_count + 1000,
                rb_end = row_count + 1100
            ))
            .await
            .unwrap();

        // ===== 2. Final verification =====
        let lsn = current_wal_lsn(&initial_client).await;
        let observed_ids = ids_from_state_with_deletes(
            &backend
                .scan_table(guard.database_id, TABLE_ID, Some(lsn))
                .await
                .unwrap(),
        )
        .await;

        // Build the *exact* expected ID set.
        let mut expected: HashSet<i64> = (1..=row_count) // all original rows …
            .filter(|id| id % 5 != 0) // … except those we deleted
            .collect();

        // The PK update removed `1` and inserted `row_count + 101`.
        expected.remove(&1);
        expected.insert(row_count + 101);

        // Inserted 100 fresh rows.
        expected.extend((row_count + 1)..=row_count + 100);

        // The deleted rows should not appear.
        expected.retain(|id| id % 5 != 0);

        // Nothing from the rolled-back transaction should appear.

        assert_eq!(
            observed_ids.len(),
            expected.len(),
            "Initial copy + heavy concurrent mutations produced unexpected state"
        );

        assert_eq!(
            observed_ids, expected,
            "Initial copy + heavy concurrent mutations produced unexpected state"
        );

        // Clean-up.
        initial_client
            .simple_query(&format!("DROP TABLE IF EXISTS {table_name};"))
            .await
            .unwrap();
        let _ = backend.drop_table(guard.database_id, TABLE_ID).await;
    }
}
