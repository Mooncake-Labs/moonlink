mod common;

#[cfg(test)]
mod tests {
    use super::common::{current_wal_lsn, ids_from_state, TestGuard, URI};
    use serial_test::serial;
    use std::collections::HashSet;
    use std::sync::Arc;
    use tokio_postgres::{connect, NoTls};

    // Initial copy tests can be added here
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_initial_copy_captures_existing_and_buffered_rows() {
        // First, create our own PostgreSQL client to pre-populate data
        let (initial_client, connection) = connect(URI, NoTls).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let table_name = "copy_test";

        // Create the PostgreSQL table and pre-populate it with existing rows
        initial_client
            .simple_query(&format!(
                "DROP TABLE IF EXISTS {table_name};
                 CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, name TEXT);",
                table_name = table_name
            ))
            .await
            .unwrap();
        initial_client
            .simple_query(&format!(
                "INSERT INTO {table_name} VALUES (1,'old_a'),(2,'old_b');",
                table_name = table_name
            ))
            .await
            .unwrap();

        // Create the backend with no tables
        let (guard, _) = TestGuard::new(None).await;
        let backend = guard.backend();

        // Register the table - this kicks off *initial copy* in the background
        backend
            .create_table(table_name, &format!("public.{table_name}"), URI)
            .await
            .unwrap();

        // While copy is in-flight, send an additional row that must be *buffered*
        initial_client
            .simple_query(&format!(
                "INSERT INTO {table_name} VALUES (3,'new_c');",
                table_name = table_name
            ))
            .await
            .unwrap();

        let lsn_after_insert = current_wal_lsn(&initial_client).await;

        let ids = ids_from_state(
            &backend
                .scan_table(&table_name, Some(lsn_after_insert))
                .await
                .unwrap(),
        );

        assert_eq!(ids, HashSet::from([1, 2, 3]));

        // Manually drop the table we created
        initial_client
            .simple_query(&format!(
                "DROP TABLE IF EXISTS {table_name};",
                table_name = table_name
            ))
            .await
            .unwrap();
        let _ = backend.drop_table(table_name).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_initial_copy_handles_updates_during_copy() {
        let (initial_client, connection) = connect(URI, NoTls).await.unwrap();
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
                table_name = table_name,
                row_count = row_count,
            ))
            .await
            .unwrap();

        let (guard, _) = TestGuard::new(None).await;
        let backend = Arc::clone(guard.backend());

        // Start create_table without awaiting so we can modify data during copy
        let backend_clone = Arc::clone(&backend);
        let create_handle = tokio::spawn(async move {
            backend_clone
                .create_table(table_name, &format!("public.{table_name}"), URI)
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
            .simple_query(&format!(
                "DELETE FROM {table_name} WHERE id = 2;",
                table_name = table_name
            ))
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
        let ids = ids_from_state(&backend.scan_table(&table_name, Some(lsn)).await.unwrap());

        let mut expected: HashSet<i64> = (3..=row_count).collect();
        expected.insert(row_count + 1); // updated id
        expected.insert(row_count + 2); // inserted during copy
        expected.insert(row_count + 3); // inserted after copy

        assert_eq!(ids, expected);

        initial_client
            .simple_query(&format!(
                "DROP TABLE IF EXISTS {table_name};",
                table_name = table_name
            ))
            .await
            .unwrap();
        let _ = backend.drop_table(table_name).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_initial_copy_handles_deletes_during_copy() {
        let (initial_client, connection) = connect(URI, NoTls).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let table_name = "copy_delete_during";

        initial_client
            .simple_query(&format!(
                "DROP TABLE IF EXISTS {table_name};
                 CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, name TEXT);",
                table_name = table_name
            ))
            .await
            .unwrap();

        // Initial rows
        initial_client
            .simple_query(&format!(
                "INSERT INTO {table_name} VALUES (1,'a'),(2,'b');",
                table_name = table_name
            ))
            .await
            .unwrap();

        let (guard, _) = TestGuard::new(None).await;
        let backend = Arc::clone(guard.backend());

        let backend_clone = Arc::clone(&backend);
        let create_handle = tokio::spawn(async move {
            backend_clone
                .create_table(table_name, &format!("public.{table_name}"), URI)
                .await
                .unwrap();
        });

        // Delete one of the rows while copy is executing
        initial_client
            .simple_query(&format!(
                "DELETE FROM {table_name} WHERE id = 1;",
                table_name = table_name
            ))
            .await
            .unwrap();

        create_handle.await.unwrap();

        // Add another row after copy finishes
        initial_client
            .simple_query(&format!(
                "INSERT INTO {table_name} VALUES (3,'c');",
                table_name = table_name
            ))
            .await
            .unwrap();

        let lsn = current_wal_lsn(&initial_client).await;
        let ids = ids_from_state(&backend.scan_table(&table_name, Some(lsn)).await.unwrap());

        assert_eq!(ids, HashSet::from([2, 3]));

        initial_client
            .simple_query(&format!(
                "DROP TABLE IF EXISTS {table_name};",
                table_name = table_name
            ))
            .await
            .unwrap();
        let _ = backend.drop_table(table_name).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_initial_copy_insert_then_delete_during_copy() {
        let (initial_client, connection) = connect(URI, NoTls).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let table_name = "copy_insert_delete";
        let row_count = 10000i64;

        initial_client
            .simple_query(&format!(
                "DROP TABLE IF EXISTS {table_name};
                 CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, name TEXT);
                 INSERT INTO {table_name}
                 SELECT gs, 'base'
                 FROM generate_series(1, {row_count}) AS gs;",
                table_name = table_name,
                row_count = row_count,
            ))
            .await
            .unwrap();

        let (guard, _) = TestGuard::new(None).await;
        let backend = Arc::clone(guard.backend());

        let backend_clone = Arc::clone(&backend);
        let create_handle = tokio::spawn(async move {
            backend_clone
                .create_table(table_name, &format!("public.{table_name}"), URI)
                .await
                .unwrap();
        });

        // Insert then immediately delete a row while copy runs
        initial_client
            .simple_query(&format!(
                "INSERT INTO {table_name} VALUES ({},'tmp');",
                row_count + 1,
                table_name = table_name
            ))
            .await
            .unwrap();
        initial_client
            .simple_query(&format!(
                "DELETE FROM {table_name} WHERE id = {};",
                row_count + 1,
                table_name = table_name
            ))
            .await
            .unwrap();

        create_handle.await.unwrap();

        // Final insert after copy
        initial_client
            .simple_query(&format!(
                "INSERT INTO {table_name} VALUES ({},'after');",
                row_count + 2,
                table_name = table_name
            ))
            .await
            .unwrap();

        let lsn = current_wal_lsn(&initial_client).await;
        let ids = ids_from_state(&backend.scan_table(&table_name, Some(lsn)).await.unwrap());

        let mut expected: HashSet<i64> = (1..=row_count).collect();
        expected.insert(row_count + 2);

        assert_eq!(ids, expected);

        initial_client
            .simple_query(&format!(
                "DROP TABLE IF EXISTS {table_name};",
                table_name = table_name
            ))
            .await
            .unwrap();
        let _ = backend.drop_table(table_name).await;
    }
}
