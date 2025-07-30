use crate::storage::filesystem::accessor::filesystem_accessor::FileSystemAccessor;
use crate::storage::filesystem::storage_config::StorageConfig;
use crate::storage::filesystem::test_utils::object_storage_test_utils::*;
use crate::storage::filesystem::{
    accessor::base_filesystem_accessor::BaseFileSystemAccess, accessor_config::AccessorConfig,
};

use std::sync::Arc;
use std::time::Duration;

use base64::engine::general_purpose::STANDARD as base64;
use base64::Engine;
use chrono::Utc;
use hmac::{Hmac, Mac};
use iceberg::{Error as IcebergError, Result as IcebergResult};
use sha1::Sha1;

use backon::{ExponentialBuilder, Retryable};
use tokio::time::sleep;

type HmacSha1 = Hmac<Sha1>;

/// Minio related constants.
///
/// Local minio warehouse needs special handling, so we simply prefix with special token.
pub(crate) static S3_TEST_BUCKET_PREFIX: &str = "test-minio-warehouse-";
pub(crate) static S3_TEST_WAREHOUSE_URI_PREFIX: &str = "s3://test-minio-warehouse-";
pub(crate) static S3_TEST_ACCESS_KEY_ID: &str = "minioadmin";
pub(crate) static S3_TEST_SECRET_ACCESS_KEY: &str = "minioadmin";
pub(crate) static S3_TEST_ENDPOINT: &str = "http://s3.local:9000";

/// Create a S3 catalog config.
pub(crate) fn create_s3_storage_config(warehouse_uri: &str) -> AccessorConfig {
    let bucket = get_bucket_from_warehouse_uri(warehouse_uri);
    let storage_config = StorageConfig::S3 {
        access_key_id: S3_TEST_ACCESS_KEY_ID.to_string(),
        secret_access_key: S3_TEST_SECRET_ACCESS_KEY.to_string(),
        region: "auto".to_string(), // minio doesn't care about region.
        bucket: bucket.to_string(),
        endpoint: Some(S3_TEST_ENDPOINT.to_string()),
    };
    AccessorConfig::new_with_storage_config(storage_config)
}

pub(crate) fn get_test_s3_bucket_and_warehouse() -> (String, String) {
    get_bucket_and_warehouse(S3_TEST_BUCKET_PREFIX, S3_TEST_WAREHOUSE_URI_PREFIX)
}

async fn create_test_s3_bucket_impl(bucket: Arc<String>) -> IcebergResult<()> {
    let date = Utc::now().format("%a, %d %b %Y %T GMT").to_string();
    let string_to_sign = format!("PUT\n\n\n{date}\n/{bucket}");

    let mut mac = HmacSha1::new_from_slice(S3_TEST_SECRET_ACCESS_KEY.as_bytes()).unwrap();
    mac.update(string_to_sign.as_bytes());
    let signature = base64.encode(mac.finalize().into_bytes());

    let auth_header = format!("AWS {S3_TEST_ACCESS_KEY_ID}:{signature}");
    let url = format!("{S3_TEST_ENDPOINT}/{bucket}");
    let client = reqwest::Client::new();

    client
        .put(&url)
        .header("Authorization", auth_header)
        .header("Date", date)
        .send()
        .await
        .map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Failed to create bucket {bucket} in minio with url {url}: {e}"),
            )
        })?;

    Ok(())
}

async fn delete_s3_bucket_objects(bucket: &str) -> IcebergResult<()> {
    let config = create_s3_storage_config(&format!("s3://{bucket}"));
    let accessor = FileSystemAccessor::new(config);
    accessor.remove_directory("/").await.map_err(|e| {
        IcebergError::new(
            iceberg::ErrorKind::Unexpected,
            format!("Failed to remove directory in bucket {bucket}: {e}"),
        )
    })?;
    Ok(())
}

async fn delete_test_s3_bucket_impl(bucket: Arc<String>) -> IcebergResult<()> {
    // Delete all objects in the bucket first.
    delete_s3_bucket_objects(&bucket).await?;

    // Now delete the bucket.
    let date = Utc::now().format("%a, %d %b %Y %T GMT").to_string();
    let string_to_sign = format!("DELETE\n\n\n{date}\n/{bucket}");

    let mut mac = HmacSha1::new_from_slice(S3_TEST_SECRET_ACCESS_KEY.as_bytes()).unwrap();
    mac.update(string_to_sign.as_bytes());
    let signature = base64.encode(mac.finalize().into_bytes());

    let auth_header = format!("AWS {S3_TEST_ACCESS_KEY_ID}:{signature}");
    let url = format!("{S3_TEST_ENDPOINT}/{bucket}");
    let client = reqwest::Client::new();

    client
        .delete(&url)
        .header("Authorization", auth_header)
        .header("Date", date)
        .send()
        .await
        .map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Failed to delete bucket {bucket} in minio: {e}"),
            )
        })?;

    Ok(())
}

/// Creates the provided bucket with exponential backoff retry; this function assumes the bucket doesn't exist, otherwise it will return error.
pub(crate) async fn create_test_s3_bucket(bucket: String) -> IcebergResult<()> {
    let bucket = Arc::new(bucket);
    let backoff = ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(TEST_RETRY_INIT_MILLISEC))
        .with_max_times(TEST_RETRY_COUNT);

    (move || {
        let bucket = Arc::clone(&bucket);
        async move { create_test_s3_bucket_impl(bucket).await }
    })
    .retry(backoff)
    .sleep(sleep)
    .when(|e: &IcebergError| {
        matches!(
            e.kind(),
            iceberg::ErrorKind::Unexpected | iceberg::ErrorKind::CatalogCommitConflicts
        )
    })
    .await
}

pub(crate) async fn delete_test_s3_bucket(bucket: String) {
    let bucket = Arc::new(bucket);
    let backoff = ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(TEST_RETRY_INIT_MILLISEC))
        .with_max_times(TEST_RETRY_COUNT);

    let _ = (move || {
        let bucket = Arc::clone(&bucket);
        async move { delete_test_s3_bucket_impl(bucket).await }
    })
    .retry(backoff)
    .sleep(sleep)
    .when(|e: &IcebergError| {
        matches!(
            e.kind(),
            iceberg::ErrorKind::Unexpected | iceberg::ErrorKind::CatalogCommitConflicts
        )
    })
    .await;
}
