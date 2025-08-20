use crate::base_metadata_store::MetadataStoreTrait;
use crate::sqlite::sqlite_metadata_store::SqliteMetadataStore;
use moonlink::{AccessorConfig, IcebergTableConfig, MoonlinkTableConfig, StorageConfig};

use tempfile::{tempdir, TempDir};

/// Source table uri.
const SRC_TABLE_URI: &str = "postgresql://postgres:postgres@postgres:5432/postgres";
/// Test table name.
const SRC_TABLE_NAME: &str = "src_table";
/// Test destination database.
const DATABASE: &str = "dst_database";
/// Test destination table name.
const TABLE: &str = "dst_schema.dst_table";

/// Create a filesystem config for test.
fn get_storage_config() -> StorageConfig {
    #[allow(unreachable_code)]
    #[cfg(feature = "storage-gcs")]
    {
        return StorageConfig::Gcs {
            project: "project".to_string(),
            region: "region".to_string(),
            bucket: "bucket".to_string(),
            access_key_id: "access_key_id".to_string(),
            secret_access_key: "secret_access_key".to_string(),
            endpoint: None,
            disable_auth: false,
            multipart_upload_threshold: None,
        };
    }

    #[allow(unreachable_code)]
    #[cfg(feature = "storage-s3")]
    {
        return StorageConfig::S3 {
            access_key_id: "access_key_id".to_string(),
            secret_access_key: "secret_access_key".to_string(),
            region: "region".to_string(),
            bucket: "bucket".to_string(),
            endpoint: None,
        };
    }

    #[allow(unreachable_code)]
    #[cfg(feature = "storage-fs")]
    {
        return StorageConfig::FileSystem {
            root_directory: "/tmp/test_warehouse_uri".to_string(),
            atomic_write_dir: None,
        };
    }

    #[allow(unreachable_code)]
    {
        panic!("No storage backend feature enabled");
    }
}

fn get_accessor_config() -> AccessorConfig {
    AccessorConfig::new_with_storage_config(get_storage_config())
}

/// Create a moonlink table config for test.
pub(crate) fn get_moonlink_table_config() -> MoonlinkTableConfig {
    MoonlinkTableConfig {
        iceberg_table_config: IcebergTableConfig {
            namespace: vec!["namespace".to_string()],
            table_name: "table".to_string(),
            accessor_config: get_accessor_config(),
        },
        ..Default::default()
    }
}

async fn check_persisted_metadata(sqlite_metadata_store: &SqliteMetadataStore) {
    let metadata_entries = sqlite_metadata_store
        .get_all_table_metadata_entries()
        .await
        .unwrap();
    assert_eq!(metadata_entries.len(), 1);
    let table_metadata_entry = &metadata_entries[0];
    assert_eq!(table_metadata_entry.table, TABLE);
    assert_eq!(table_metadata_entry.src_table_name, SRC_TABLE_NAME);
    assert_eq!(table_metadata_entry.src_table_uri, SRC_TABLE_URI);
    assert_eq!(
        table_metadata_entry.moonlink_table_config,
        get_moonlink_table_config()
    );
}

/// Test util function to get sqlite database filepath.
fn get_sqlite_database_filepath(tmp_dir: &TempDir) -> String {
    format!(
        "sqlite://{}/sqlite_metadata_store.db",
        tmp_dir.path().to_str().unwrap()
    )
}

#[tokio::test]
async fn test_metadata_table_exists() {
    let tmp_dir = tempdir().unwrap();
    let sqlite_path = get_sqlite_database_filepath(&tmp_dir);

    let metadata_store = SqliteMetadataStore::new(sqlite_path.clone()).await.unwrap();
    let moonlink_table_config = get_moonlink_table_config();

    // Check metadata table existence.
    let exists = metadata_store.metadata_table_exists().await.unwrap();
    assert!(!exists);

    // Store moonlink table config to metadata storage.
    metadata_store
        .store_table_metadata(
            DATABASE,
            TABLE,
            SRC_TABLE_NAME,
            SRC_TABLE_URI,
            moonlink_table_config.clone(),
        )
        .await
        .unwrap();

    // Load moonlink table config from metadata config.
    let exists = metadata_store.metadata_table_exists().await.unwrap();
    assert!(exists);
    check_persisted_metadata(&metadata_store).await;
}

#[tokio::test]
async fn test_table_metadata_store_and_load() {
    let tmp_dir = tempdir().unwrap();
    let sqlite_path = get_sqlite_database_filepath(&tmp_dir);

    let metadata_store = SqliteMetadataStore::new(sqlite_path.clone()).await.unwrap();
    let moonlink_table_config = get_moonlink_table_config();

    // Store moonlink table config to metadata storage.
    metadata_store
        .store_table_metadata(
            DATABASE,
            TABLE,
            SRC_TABLE_NAME,
            SRC_TABLE_URI,
            moonlink_table_config.clone(),
        )
        .await
        .unwrap();

    // Load moonlink table config from metadata config.
    check_persisted_metadata(&metadata_store).await;
}

/// Test scenario: store for duplicate table ids.
#[tokio::test]
async fn test_table_metadata_store_for_duplicate_tables() {
    let tmp_dir = tempdir().unwrap();
    let sqlite_path = get_sqlite_database_filepath(&tmp_dir);

    let metadata_store = SqliteMetadataStore::new(sqlite_path.clone()).await.unwrap();
    let moonlink_table_config = get_moonlink_table_config();

    // Store moonlink table config to metadata storage.
    metadata_store
        .store_table_metadata(
            DATABASE,
            TABLE,
            SRC_TABLE_NAME,
            SRC_TABLE_URI,
            moonlink_table_config.clone(),
        )
        .await
        .unwrap();

    // Load and check moonlink table config from metadata config.
    let res = metadata_store
        .store_table_metadata(
            DATABASE,
            TABLE,
            SRC_TABLE_NAME,
            SRC_TABLE_URI,
            moonlink_table_config.clone(),
        )
        .await;
    assert!(res.is_err());
}

/// Test scenario: load from non-existent table.
#[tokio::test]
async fn test_table_metadata_load_from_non_existent_table() {
    let tmp_dir = tempdir().unwrap();
    let sqlite_path = get_sqlite_database_filepath(&tmp_dir);
    let metadata_store = SqliteMetadataStore::new(sqlite_path.clone()).await.unwrap();

    // Load moonlink table config from metadata config.
    let res = metadata_store.get_all_table_metadata_entries().await;
    assert!(res.is_err());
}

/// Test scenario: delete table metadata store.
#[tokio::test]
async fn test_delete_table_metadata_store() {
    let tmp_dir = tempdir().unwrap();
    let sqlite_path = get_sqlite_database_filepath(&tmp_dir);

    let metadata_store = SqliteMetadataStore::new(sqlite_path.clone()).await.unwrap();
    let moonlink_table_config = get_moonlink_table_config();

    // Store moonlink table config to metadata storage.
    metadata_store
        .store_table_metadata(
            DATABASE,
            TABLE,
            SRC_TABLE_NAME,
            SRC_TABLE_URI,
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
