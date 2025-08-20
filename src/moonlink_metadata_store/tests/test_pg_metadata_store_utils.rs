use tokio_postgres::{connect, NoTls};

/// Test connection string.
const URI: &str = "postgresql://postgres:postgres@postgres:5432/postgres";

#[cfg(test)]
mod tests {
    use super::*;

    use serial_test::serial;

    use moonlink_metadata_store::PgUtils;

    /// Util function to get database URI.
    fn get_table_uri() -> String {
        std::env::var("DATABASE_URL").unwrap_or_else(|_| URI.to_string())
    }

    #[tokio::test]
    #[serial]
    async fn test_table_exists() {
        const EXISTENT_TABLE: &str = "existent_table";
        const NON_EXISTENT_TABLE: &str = "non_existent_table";

        let (postgres_client, connection) = connect(&get_table_uri(), NoTls).await.unwrap();

        // Spawn connection driver in background to keep eventloop alive.
        let _pg_connection = tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Postgres connection error: {e}");
            }
        });

        // Drop table.
        postgres_client
            .simple_query(&format!("DROP TABLE IF EXISTS {EXISTENT_TABLE} CASCADE;"))
            .await
            .unwrap();
        postgres_client
            .simple_query(&format!(
                "DROP TABLE IF EXISTS {NON_EXISTENT_TABLE} CASCADE;"
            ))
            .await
            .unwrap();

        // Check table existence.
        assert!(!PgUtils::table_exists(&postgres_client, EXISTENT_TABLE)
            .await
            .unwrap());
        assert!(!PgUtils::table_exists(&postgres_client, NON_EXISTENT_TABLE)
            .await
            .unwrap());

        // Check table existent after table creation.
        postgres_client
            .simple_query(&format!("CREATE TABLE {EXISTENT_TABLE} (id INT);",))
            .await
            .unwrap();
        assert!(PgUtils::table_exists(&postgres_client, EXISTENT_TABLE)
            .await
            .unwrap());
    }
}
