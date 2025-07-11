use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::filesystem::accessor::filesystem_accessor::FileSystemAccessor;
use crate::storage::filesystem::filesystem_config::FileSystemConfig;
use crate::storage::filesystem::test_utils::object_storage_test_utils::*;
use crate::storage::iceberg::tokio_retry_utils;

use std::sync::Arc;

use base64::engine::general_purpose::STANDARD as base64;
use base64::Engine;
use chrono::Utc;
use hmac::{Hmac, Mac};
use iceberg::Error as IcebergError;
use iceberg::Result as IcebergResult;
use sha1::Sha1;
use tokio_retry2::strategy::{jitter, ExponentialBackoff};
use tokio_retry2::Retry;

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
pub(crate) fn create_s3_filesystem_config(warehouse_uri: &str) -> FileSystemConfig {
    let bucket = get_bucket_from_warehouse_uri(warehouse_uri);
    FileSystemConfig::S3 {
        access_key_id: S3_TEST_ACCESS_KEY_ID.to_string(),
        secret_access_key: S3_TEST_SECRET_ACCESS_KEY.to_string(),
        region: "auto".to_string(), // minio doesn't care about region.
        bucket: bucket.to_string(),
        endpoint: Some(S3_TEST_ENDPOINT.to_string()),
    }
}

pub(crate) fn get_test_s3_bucket_and_warehouse(
) -> (String /*bucket_name*/, String /*warehouse_url*/) {
    get_bucket_and_warehouse(S3_TEST_BUCKET_PREFIX, S3_TEST_WAREHOUSE_URI_PREFIX)
}

/// Create test bucket in minio server.
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

/// Creates the provided bucket with exponential backoff retry; this function assumes the bucket doesn't exist, otherwise it will return error.
pub(crate) async fn create_test_s3_bucket(bucket: String) -> IcebergResult<()> {
    let retry_strategy = ExponentialBackoff::from_millis(TEST_RETRY_INIT_MILLISEC)
        .map(jitter)
        .take(TEST_RETRY_COUNT);

    Retry::spawn(retry_strategy, {
        let bucket_name = Arc::new(bucket);
        move || {
            let bucket_name = Arc::clone(&bucket_name);
            async move {
                create_test_s3_bucket_impl(bucket_name)
                    .await
                    .map_err(tokio_retry_utils::iceberg_to_tokio_retry_error)
            }
        }
    })
    .await?;
    Ok(())
}

/// Util function to delete all objects in a S3 bucket.
async fn delete_s3_bucket_objects(bucket: &str) -> IcebergResult<()> {
    let filesystem_config = create_s3_filesystem_config(&format!("s3://{bucket}"));
    let filesystem_accessor = FileSystemAccessor::new(filesystem_config);
    filesystem_accessor
        .remove_directory("/")
        .await
        .map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Failed to remove directory in bucket {bucket}: {e}"),
            )
        })?;
    Ok(())
}

/// Delete test bucket in minio server.
pub async fn delete_test_s3_bucket_impl(bucket: Arc<String>) -> IcebergResult<()> {
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
                format!("Failed to delete bucket {bucket} in minio {e}"),
            )
        })?;

    Ok(())
}

/// Delete the provided bucket with exponential backoff retry; this function assume bucket already exists and return error if not.
pub(crate) async fn delete_test_s3_bucket(bucket: String) {
    let retry_strategy = ExponentialBackoff::from_millis(TEST_RETRY_INIT_MILLISEC)
        .map(jitter)
        .take(TEST_RETRY_COUNT);

    Retry::spawn(retry_strategy, {
        let bucket_name = Arc::new(bucket);
        move || {
            let bucket_name = Arc::clone(&bucket_name);
            async move {
                delete_test_s3_bucket_impl(bucket_name)
                    .await
                    .map_err(tokio_retry_utils::iceberg_to_tokio_retry_error)
            }
        }
    })
    .await
    .unwrap();
}
