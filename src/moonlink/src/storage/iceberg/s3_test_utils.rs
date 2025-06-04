/// This module provides a few test util functions.
use crate::storage::iceberg::file_catalog::{CatalogConfig, FileCatalog};

use rand::Rng;

/// Minio related constants.
///
/// Local minio warehouse needs special handling, so we simply prefix with special token.
#[allow(dead_code)]
pub(crate) static MINIO_TEST_BUCKET_PREFIX: &str = "test-minio-warehouse-";
#[allow(dead_code)]
pub(crate) static MINIO_TEST_WAREHOUSE_URI_PREFIX: &str = "s3://test-minio-warehouse-";
#[allow(dead_code)]
pub(crate) static MINIO_ACCESS_KEY_ID: &str = "minioadmin";
#[allow(dead_code)]
pub(crate) static MINIO_SECRET_ACCESS_KEY: &str = "minioadmin";
#[allow(dead_code)]
pub(crate) static MINIO_ENDPOINT: &str = "http://minio:9000";
#[allow(dead_code)]
static TEST_RETRY_COUNT: usize = 5;
#[allow(dead_code)]
static TEST_RETRY_INIT_MILLISEC: u64 = 100;
#[allow(dead_code)]
static TEST_BUCKET_NAME_LEN: usize = 10;

/// Create a S3 catalog, which communicates with local minio server.
#[allow(dead_code)]
pub(crate) fn create_minio_s3_catalog(bucket: &str, warehouse_uri: &str) -> FileCatalog {
    let catalog_config = CatalogConfig::S3 {
        access_key_id: MINIO_ACCESS_KEY_ID.to_string(),
        secret_access_key: MINIO_SECRET_ACCESS_KEY.to_string(),
        region: "auto".to_string(), // minio doesn't care about region.
        bucket: bucket.to_string(),
        endpoint: MINIO_ENDPOINT.to_string(),
    };
    FileCatalog::new(warehouse_uri.to_string(), catalog_config).unwrap()
}

#[allow(dead_code)]
pub(crate) fn get_test_minio_bucket_and_warehouse(
) -> (String /*bucket_name*/, String /*warehouse_url*/) {
    // minio bucket name only allows lowercase case letters, digits and hyphen.
    const TEST_BUCKET_NAME_LEN: usize = 12;
    const ALLOWED_CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789-";
    let mut rng = rand::rng();
    let random_string: String = (0..TEST_BUCKET_NAME_LEN)
        .map(|_| {
            let idx = rng.random_range(0..ALLOWED_CHARS.len());
            ALLOWED_CHARS[idx] as char
        })
        .collect();
    (
        format!("{}{}", MINIO_TEST_BUCKET_PREFIX, random_string),
        format!("{}{}", MINIO_TEST_WAREHOUSE_URI_PREFIX, random_string),
    )
}

#[allow(dead_code)]
pub(crate) fn get_test_minio_bucket(warehouse_uri: &str) -> String {
    let random_string = warehouse_uri
        .strip_prefix(MINIO_TEST_WAREHOUSE_URI_PREFIX)
        .unwrap()
        .to_string();
    format!("{}{}", MINIO_TEST_BUCKET_PREFIX, random_string)
}

#[cfg(test)]
pub(crate) mod object_store_test_utils {
    use super::*;

    use crate::storage::iceberg::tokio_retry_utils;

    use std::sync::Arc;

    use aws_sdk_s3::config::{Credentials, Region};
    use aws_sdk_s3::types::{Delete, ObjectIdentifier};
    use aws_sdk_s3::Client as S3Client;

    use iceberg::Error as IcebergError;
    use iceberg::Result as IcebergResult;
    use tokio_retry2::strategy::{jitter, ExponentialBackoff};
    use tokio_retry2::Retry;

    /// Create s3 client to connect minio.
    #[allow(dead_code)]
    async fn create_s3_client() -> S3Client {
        let creds = Credentials::new(
            MINIO_ACCESS_KEY_ID.to_string(),
            MINIO_SECRET_ACCESS_KEY.to_string(),
            /*session_token=*/ None,
            /*expires_after=*/ None,
            /*provider_name=*/ "local-credentials",
        );
        let config = aws_sdk_s3::Config::builder()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .credentials_provider(creds.clone())
            .region(Region::new("us-east-1"))
            .endpoint_url(MINIO_ENDPOINT.to_string())
            .force_path_style(true)
            .build();
        S3Client::from_conf(config.clone())
    }

    /// Create test bucket in minio server.
    #[allow(dead_code)]
    async fn create_test_s3_bucket_impl(bucket: Arc<String>) -> IcebergResult<()> {
        let s3_client = create_s3_client().await;
        s3_client
            .create_bucket()
            .bucket(bucket.to_string())
            .send()
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to create the test bucket in minio {}", e),
                )
            })?;
        Ok(())
    }

    /// Creates the provided bucket with exponential backoff retry; this function assumes the bucket doesn't exist, otherwise it will return error.
    #[allow(dead_code)]
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

    /// Delete test bucket in minio server.
    #[allow(dead_code)]
    async fn delete_test_s3_bucket_impl(bucket: Arc<String>) -> IcebergResult<()> {
        let s3_client = create_s3_client().await;
        let objects = s3_client
            .list_objects_v2()
            .bucket(bucket.to_string())
            .send()
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to list objects under test bucket in minio {}", e),
                )
            })?;

        if let Some(contents) = objects.contents {
            let delete_objects: Vec<ObjectIdentifier> = contents
                .into_iter()
                .filter_map(|o| o.key)
                .map(|key| {
                    ObjectIdentifier::builder()
                        .key(key)
                        .build()
                        .expect("Failed to build ObjectIdentifier")
                })
                .collect();

            if !delete_objects.is_empty() {
                s3_client
                    .delete_objects()
                    .bucket(bucket.to_string())
                    .delete(
                        Delete::builder()
                            .set_objects(Some(delete_objects))
                            .build()
                            .expect("Failed to build Delete object"),
                    )
                    .send()
                    .await
                    .map_err(|e| {
                        IcebergError::new(
                            iceberg::ErrorKind::Unexpected,
                            format!("Failed to delete objects under test bucket in minio {}", e),
                        )
                    })?;
            }

            s3_client
                .delete_bucket()
                .bucket(bucket.to_string())
                .send()
                .await
                .map_err(|e| {
                    IcebergError::new(
                        iceberg::ErrorKind::Unexpected,
                        format!("Failed to delete the test bucket in minio {}", e),
                    )
                })?;
        }

        Ok(())
    }

    /// Delete the provided bucket with exponential backoff retry; this function assume bucket already exists and return error if not.
    #[allow(dead_code)]
    pub(crate) async fn delete_test_s3_bucket(bucket: String) -> IcebergResult<()> {
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
        .await?;
        Ok(())
    }
}
