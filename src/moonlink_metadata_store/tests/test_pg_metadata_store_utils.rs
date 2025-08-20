mod common;

use common::test_environment::get_postgres_client;

/// Test connection string.
#[cfg(not(feature = "test-tls"))]
const URI: &str = "postgresql://postgres:postgres@postgres:5432/postgres?sslmode=disable";
#[cfg(feature = "test-tls")]
const URI: &str = "postgresql://postgres:postgres@postgres:5432/postgres?sslmode=verify-full";

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

        let (postgres_client, _connection_handle) = get_postgres_client(&get_table_uri()).await;

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
