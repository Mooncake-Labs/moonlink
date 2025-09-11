mod common;

#[cfg(test)]
mod tests {
    use super::common::{
        connect_to_postgres, get_database_uri, get_serialized_table_config, DATABASE,
    };
    use moonlink_backend::MoonlinkBackend;
    use serial_test::serial;
    use tempfile::TempDir;
    use tokio_postgres::Client;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_drop_colstore_succeeds_when_rowstore_dropped() {
        // Setup backend without auto-creating a default table.
        let tmp_dir = TempDir::new().unwrap();
        let uri = get_database_uri();

        // Initialize backend with a sqlite metadata store in temp dir.
        let metadata_store = moonlink_metadata_store::SqliteMetadataStore::new_with_directory(
            tmp_dir.path().to_str().unwrap(),
        )
        .await
        .unwrap();
        let backend = MoonlinkBackend::new(
            tmp_dir.path().to_str().unwrap().to_string(),
            /*data_server_uri=*/ None,
            Box::new(metadata_store),
        )
        .await
        .unwrap();

        // Connect to Postgres and create two source tables: `c` (colstore) and `r` (rowstore).
        let (client, _h) = connect_to_postgres(&uri).await;
        create_pg_table(&client, "c").await;
        create_pg_table(&client, "r").await;

        // Register ONLY the Mooncake table `c` with Moonlink, sourcing from Postgres rowstore `public.r`.
        backend
            .create_table(
                DATABASE.to_string(),
                "c".to_string(),
                "public.r".to_string(),
                uri.clone(),
                get_serialized_table_config(&tmp_dir),
                None,
            )
            .await
            .unwrap();

        // Simulate user mistake: drop the Postgres rowstore table `r` directly.
        client
            .simple_query("DROP TABLE IF EXISTS r;")
            .await
            .unwrap();

        // Now drop the Mooncake columnstore `c` via backend; this should still succeed and clean up state.
        backend
            .drop_table(DATABASE.to_string(), "c".to_string())
            .await
            .unwrap();

        // Assert the Mooncake table directory for `c` has been removed.
        let base_path = backend.get_base_path();
        let c_path = format!("{}/{}.{}", base_path, DATABASE, "c");
        assert!(!tokio::fs::try_exists(&c_path).await.unwrap());

        // Calling drop again should be a no-op and succeed.
        backend
            .drop_table(DATABASE.to_string(), "c".to_string())
            .await
            .unwrap();
    }

    async fn create_pg_table(client: &Client, name: &str) {
        let create_stmt = format!(
            "DROP TABLE IF EXISTS {name};\n             CREATE TABLE {name} (id BIGINT PRIMARY KEY, name TEXT);"
        );
        client.simple_query(&create_stmt).await.unwrap();
    }
}
