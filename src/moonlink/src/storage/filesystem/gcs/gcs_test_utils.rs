use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::filesystem::filesystem_config::FileSystemConfig;
use crate::storage::filesystem::test_utils::object_storage_test_utils::*;
use crate::storage::iceberg::tokio_retry_utils;
use crate::FileSystemAccessor;

use std::sync::Arc;

use iceberg::{Error as IcebergError, Result as IcebergResult};
use reqwest::StatusCode;
use tokio_retry2::strategy::{jitter, ExponentialBackoff};
use tokio_retry2::Retry;

/// Fake GCS related constants.
///
pub(crate) static GCS_TEST_BUCKET_PREFIX: &str = "test-gcs-warehouse-";
pub(crate) static GCS_TEST_WAREHOUSE_URI_PREFIX: &str = "gs://test-gcs-warehouse-";
pub(crate) static GCS_TEST_ENDPOINT: &str = "http://gcs.local:4443";
pub(crate) static GCS_TEST_PROJECT: &str = "fake-project";

pub(crate) fn create_gcs_filesystem_config(warehouse_uri: &str) -> FileSystemConfig {
    let bucket = get_bucket_from_warehouse_uri(warehouse_uri);
    FileSystemConfig::Gcs {
        bucket: bucket.to_string(),
        endpoint: Some(GCS_TEST_ENDPOINT.to_string()),
        disable_auth: true,
        project: GCS_TEST_PROJECT.to_string(),
        cred_path: None,
    }
}

pub(crate) fn get_test_gcs_bucket_and_warehouse() -> (String /*bucket*/, String /*warehouse_uri*/) {
    get_bucket_and_warehouse(GCS_TEST_BUCKET_PREFIX, GCS_TEST_WAREHOUSE_URI_PREFIX)
}

async fn create_gcs_bucket_impl(bucket: Arc<String>) -> IcebergResult<()> {
    let client = reqwest::Client::new();
    let url = format!("{GCS_TEST_ENDPOINT}/storage/v1/b?project={GCS_TEST_PROJECT}");
    let res = client
        .post(&url)
        .json(&serde_json::json!({ "name": *bucket }))
        .send()
        .await?;
    if res.status() != StatusCode::OK {
        return Err(IcebergError::new(
            iceberg::ErrorKind::Unexpected,
            format!(
                "Failed to create bucket {} in fake-gcs-server: HTTP {}",
                bucket,
                res.status()
            ),
        ));
    }
    Ok(())
}

/// Util function to delete all objects in a GCS bucket.
async fn delete_gcs_bucket_objects(bucket: &str) -> IcebergResult<()> {
    let filesystem_config = create_gcs_filesystem_config(&format!("gs://{bucket}"));
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

async fn delete_gcs_bucket_impl(bucket: Arc<String>) -> IcebergResult<()> {
    // Fake GCS server doesn't support bucket deletion if it contains objects, so need to delete all objects first.
    delete_gcs_bucket_objects(&bucket).await?;

    // Now delete the bucket.
    let client = reqwest::Client::new();
    let url = format!("{GCS_TEST_ENDPOINT}/storage/v1/b/{bucket}");
    let res = client.delete(&url).send().await.map_err(|e| {
        IcebergError::new(
            iceberg::ErrorKind::Unexpected,
            format!("Failed to delete bucket {bucket} in fake-gcs-server: {e}"),
        )
    })?;

    if res.status() != StatusCode::OK {
        return Err(IcebergError::new(
            iceberg::ErrorKind::Unexpected,
            format!(
                "Failed to delete bucket {} in fake-gcs-server: HTTP {}",
                bucket,
                res.status()
            ),
        ));
    }
    Ok(())
}

pub(crate) async fn create_test_gcs_bucket(bucket: String) -> IcebergResult<()> {
    let retry_strategy = ExponentialBackoff::from_millis(TEST_RETRY_INIT_MILLISEC)
        .map(jitter)
        .take(TEST_RETRY_COUNT);

    Retry::spawn(retry_strategy, {
        let bucket_name = Arc::new(bucket);
        move || {
            let bucket_name = Arc::clone(&bucket_name);
            async move {
                create_gcs_bucket_impl(bucket_name)
                    .await
                    .map_err(tokio_retry_utils::iceberg_to_tokio_retry_error)
            }
        }
    })
    .await?;
    Ok(())
}

pub(crate) async fn delete_test_gcs_bucket(bucket: String) {
    let retry_strategy = ExponentialBackoff::from_millis(TEST_RETRY_INIT_MILLISEC)
        .map(jitter)
        .take(TEST_RETRY_COUNT);

    Retry::spawn(retry_strategy, {
        let bucket_name = Arc::new(bucket);
        move || {
            let bucket_name = Arc::clone(&bucket_name);
            async move {
                delete_gcs_bucket_impl(bucket_name)
                    .await
                    .map_err(tokio_retry_utils::iceberg_to_tokio_retry_error)
            }
        }
    })
    .await
    .unwrap();
}
