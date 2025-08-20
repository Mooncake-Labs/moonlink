mod common;

use common::test_environment::*;
use common::test_utils::*;
use moonlink_metadata_store::base_metadata_store::MetadataStoreTrait;
use moonlink_metadata_store::PgMetadataStore;

/// Test connection string.
const SRC_TABLE_URI: &str = "postgresql://postgres:postgres@postgres:5432/postgres";
/// Test table name.
const SRC_TABLE_NAME: &str = "table";
/// Test destination database name.
const DATABASE: &str = "dst-database";
/// Test destination table name.
const TABLE: &str = "dst-schema.dst-table";

#[cfg(test)]
mod tests {
    use super::*;

    use serial_test::serial;

    /// Util function to get database URI.
    fn get_table_uri() -> String {
        std::env::var("DATABASE_URL").unwrap_or_else(|_| SRC_TABLE_URI.to_string())
    }

    /// Test util function to get table metadata entries, and check whether it matches written one.
    ///
    /// TODO(hjiang): Refactor to take a trait.
    async fn check_persisted_metadata(pg_metadata_store: &PgMetadataStore) {
        let metadata_entries = pg_metadata_store
            .get_all_table_metadata_entries()
            .await
            .unwrap();
        assert_eq!(metadata_entries.len(), 1);
        let table_metadata_entry = &metadata_entries[0];
        assert_eq!(table_metadata_entry.table, TABLE);
        assert_eq!(table_metadata_entry.src_table_name, SRC_TABLE_NAME);
        assert_eq!(table_metadata_entry.src_table_uri, get_table_uri());
        assert_eq!(
            table_metadata_entry.moonlink_table_config,
            get_moonlink_table_config()
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_table_metadata_store_and_load() {
        let table_uri = get_table_uri();
        let _test_environment = TestEnvironment::new(&table_uri).await;
        // Unused metadata storage, used to check it could be initialized for multiple times idempotently.
        let _ = PgMetadataStore::new(table_uri.clone()).unwrap();
        // Initialize for the second time.
        let metadata_store = PgMetadataStore::new(table_uri.clone()).unwrap();
        let moonlink_table_config = get_moonlink_table_config();

        // Store moonlink table config to metadata storage.
        metadata_store
            .store_table_metadata(
                DATABASE,
                TABLE,
                SRC_TABLE_NAME,
                &table_uri,
                moonlink_table_config.clone(),
            )
            .await
            .unwrap();

        // Load moonlink table config from metadata config.
        check_persisted_metadata(&metadata_store).await;
    }

    /// Test scenario: load from non-existent schema.
    #[tokio::test]
    #[serial]
    async fn test_table_metadata_load_from_non_existent_schema() {
        let table_uri = get_table_uri();
        let test_environment = TestEnvironment::new(&table_uri).await;
        let metadata_store = PgMetadataStore::new(table_uri).unwrap();

        // Delete moonlink schema.
        test_environment.delete_mooncake_schema().await;

        // Load moonlink table config from metadata config.
        let res = metadata_store.get_all_table_metadata_entries().await;
        assert!(res.is_err());
    }

    /// Test scenario: load from non-existent table.
    #[tokio::test]
    #[serial]
    async fn test_table_metadata_load_from_non_existent_table() {
        let table_uri = get_table_uri();
        let _test_environment = TestEnvironment::new(&table_uri).await;
        let metadata_store = PgMetadataStore::new(table_uri).unwrap();

        // Load moonlink table config from metadata config.
        let res = metadata_store.get_all_table_metadata_entries().await;
        assert!(res.is_err());
    }

    /// Test scenario: store for duplicate table ids.
    #[tokio::test]
    #[serial]
    async fn test_table_metadata_store_for_duplicate_tables() {
        let table_uri = get_table_uri();
        let _test_environment = TestEnvironment::new(&table_uri).await;
        let metadata_store = PgMetadataStore::new(table_uri.clone()).unwrap();
        let moonlink_table_config = get_moonlink_table_config();

        // Store moonlink table config to metadata storage.
        metadata_store
            .store_table_metadata(
                DATABASE,
                TABLE,
                SRC_TABLE_NAME,
                &table_uri,
                moonlink_table_config.clone(),
            )
            .await
            .unwrap();

        // Store moonlink table config to metadata storage.
        let res = metadata_store
            .store_table_metadata(
                DATABASE,
                TABLE,
                SRC_TABLE_NAME,
                &table_uri,
                moonlink_table_config.clone(),
            )
            .await;
        assert!(res.is_err());
    }

    /// Test scenario: delete table metadata store.
    #[tokio::test]
    #[serial]
    async fn test_delete_table_metadata_store() {
        let table_uri = get_table_uri();
        let _test_environment = TestEnvironment::new(&table_uri).await;
        let metadata_store = PgMetadataStore::new(table_uri.clone()).unwrap();
        let moonlink_table_config = get_moonlink_table_config();

        // Store moonlink table metadata to metadata storage.
        metadata_store
            .store_table_metadata(
                DATABASE,
                TABLE,
                SRC_TABLE_NAME,
                &table_uri,
                moonlink_table_config.clone(),
            )
            .await
            .unwrap();

        // Load and check moonlink table config from metadata config.
        check_persisted_metadata(&metadata_store).await;

        // Delete moonlink table config to metadata storage and check.
        metadata_store
            .delete_table_metadata(DATABASE, TABLE)
            .await
            .unwrap();
        let metadata_entries = metadata_store
            .get_all_table_metadata_entries()
            .await
            .unwrap();
        assert_eq!(metadata_entries.len(), 0);

        // Delete for the second time also fails.
        let res = metadata_store.delete_table_metadata(DATABASE, TABLE).await;
        assert!(res.is_err());
    }
}
