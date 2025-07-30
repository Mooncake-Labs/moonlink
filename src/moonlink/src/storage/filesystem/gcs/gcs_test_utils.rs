use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::filesystem::accessor_config::AccessorConfig;
use crate::storage::filesystem::storage_config::StorageConfig;
use crate::storage::filesystem::test_utils::object_storage_test_utils::*;
use crate::FileSystemAccessor;

use std::sync::Arc;
use std::time::Duration;

use backon::{ExponentialBuilder, Retryable};
use iceberg::{Error as IcebergError, Result as IcebergResult};
use reqwest::StatusCode;

/// Fake GCS related constants.
pub(crate) static GCS_TEST_BUCKET_PREFIX: &str = "test-gcs-warehouse-";
pub(crate) static GCS_TEST_WAREHOUSE_URI_PREFIX: &str = "gs://test-gcs-warehouse-";
pub(crate) static GCS_TEST_ENDPOINT: &str = "http://gcs.local:4443";
pub(crate) static GCS_TEST_PROJECT: &str = "fake-project";

pub(crate) fn create_gcs_storage_config(warehouse_uri: &str) -> AccessorConfig {
    let bucket = get_bucket_from_warehouse_uri(warehouse_uri);
    let storage_config = StorageConfig::Gcs {
        bucket: bucket.to_string(),
        endpoint: Some(GCS_TEST_ENDPOINT.to_string()),
        disable_auth: true,
        project: GCS_TEST_PROJECT.to_string(),
        region: "".to_string(),
        access_key_id: "".to_string(),
        secret_access_key: "".to_string(),
    };
    AccessorConfig::new_with_storage_config(storage_config)
}

pub(crate) fn get_test_gcs_bucket_and_warehouse() -> (String, String) {
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

async fn delete_gcs_bucket_objects(bucket: &str) -> IcebergResult<()> {
    let accessor_config = create_gcs_storage_config(&format!("gs://{bucket}"));
    let accessor = FileSystemAccessor::new(accessor_config);
    accessor.remove_directory("/").await.map_err(|e| {
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
    let bucket = Arc::new(bucket);
    let backoff = ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(TEST_RETRY_INIT_MILLISEC))
        .with_max_times(TEST_RETRY_COUNT);

    (move || {
        let bucket = Arc::clone(&bucket);
        async move { create_gcs_bucket_impl(bucket).await }
    })
    .retry(backoff)
    .sleep(tokio::time::sleep)
    .when(|e: &IcebergError| {
        matches!(
            e.kind(),
            iceberg::ErrorKind::Unexpected | iceberg::ErrorKind::CatalogCommitConflicts
        )
    })
    .await
}

pub(crate) async fn delete_test_gcs_bucket(bucket: String) {
    let bucket = Arc::new(bucket);
    let backoff = ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(TEST_RETRY_INIT_MILLISEC))
        .with_max_times(TEST_RETRY_COUNT);

    let _ = (move || {
        let bucket = Arc::clone(&bucket);
        async move { delete_gcs_bucket_impl(bucket).await }
    })
    .retry(backoff)
    .sleep(tokio::time::sleep)
    .when(|e: &IcebergError| {
        matches!(
            e.kind(),
            iceberg::ErrorKind::Unexpected | iceberg::ErrorKind::CatalogCommitConflicts
        )
    })
    .await;
}
