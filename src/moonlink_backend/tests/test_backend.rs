#[cfg(test)]
mod tests {
    use moonlink_backend::recreate_directory;
    use moonlink_backend::MoonlinkBackend;
    use moonlink_backend::DEFAULT_MOONLINK_TEMP_FILE_PATH;
    use tempfile::TempDir;
    use tokio_postgres::{connect, Client, NoTls};

    #[test]
    fn test_recreate_directory() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("temp_file.txt");
        let file = std::fs::File::create(&file_path).unwrap();
        drop(file);
        assert!(std::fs::exists(&file_path).unwrap());

        // Re-create an exising directory.
        recreate_directory(temp_dir.path().to_str().unwrap()).unwrap();
        assert!(!std::fs::exists(&file_path).unwrap());

        // Re-create a non-existent directory.
        let internal_dir = temp_dir.path().join("internal_dir");
        assert!(!std::fs::exists(&internal_dir).unwrap());
        recreate_directory(internal_dir.to_str().unwrap()).unwrap();
        assert!(std::fs::exists(&internal_dir).unwrap());
    }

    /// Test util function to create a table and attempt basic sql statements to verify creation success.
    async fn test_table_creation_impl(
        service: &MoonlinkBackend<&'static str>,
        client: &Client,
        uri: &str,
    ) {
        client.simple_query("DROP TABLE IF EXISTS test; CREATE TABLE test (id bigint PRIMARY KEY, name VARCHAR(255));").await.unwrap();
        service
            .create_table("test", "public.test", uri)
            .await
            .unwrap();
        client
            .simple_query("INSERT INTO test VALUES (1 ,'foo');")
            .await
            .unwrap();
        client
            .simple_query("INSERT INTO test VALUES (2 ,'bar');")
            .await
            .unwrap();
        let old = service.scan_table(&"test", None).await.unwrap();
        // wait 2 second
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let new = service.scan_table(&"test", None).await.unwrap();
        assert_ne!(old.data, new.data);

        // Clean up temporary files directory after test.
        recreate_directory(DEFAULT_MOONLINK_TEMP_FILE_PATH).unwrap();
    }

    // Test table creation and drop.
    #[tokio::test]
    async fn test_moonlink_service() {
        let temp_dir = TempDir::new().expect("tempdir failed");
        let uri = "postgresql://postgres:postgres@postgres:5432/postgres";
        let service =
            MoonlinkBackend::<&'static str>::new(temp_dir.path().to_str().unwrap().to_string());
        // connect to postgres and create a table
        let (client, connection) = connect(uri, NoTls).await.unwrap();
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        test_table_creation_impl(&service, &client, uri).await;
        service.drop_table("test").await.unwrap();
        test_table_creation_impl(&service, &client, uri).await;
    }
}
