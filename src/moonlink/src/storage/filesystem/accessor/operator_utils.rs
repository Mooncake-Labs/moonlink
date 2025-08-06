use crate::storage::filesystem::accessor::filesystem_accessor_chaos_wrapper::ChaosLayer;
use crate::storage::filesystem::accessor_config::AccessorConfig;
use crate::storage::filesystem::accessor_config::RetryConfig;
use crate::storage::filesystem::accessor_config::TimeoutConfig;
use crate::storage::filesystem::storage_config::StorageConfig;
use crate::Result;

use opendal::layers::RetryLayer;
use opendal::layers::TimeoutLayer;
use opendal::services;
use opendal::Operator;

fn create_opendal_operator_impl(storage_config: &StorageConfig) -> Result<Operator> {
    match storage_config {
        #[cfg(feature = "storage-fs")]
        StorageConfig::FileSystem {
            root_directory,
            atomic_write_dir,
        } => {
            let mut builder = services::Fs::default().root(root_directory);
            if let Some(atomic_write_dir) = atomic_write_dir {
                builder = builder.atomic_write_dir(atomic_write_dir);
            }
            Ok(Operator::new(builder)?.finish())
        }
        #[cfg(feature = "storage-gcs")]
        StorageConfig::Gcs {
            region,
            bucket,
            endpoint,
            access_key_id,
            secret_access_key,
            disable_auth,
            ..
        } => {
            // Test environment.
            if *disable_auth {
                let builder = services::Gcs::default()
                    .root("/")
                    .bucket(bucket)
                    .endpoint(endpoint.as_ref().unwrap())
                    .disable_config_load()
                    .disable_vm_metadata()
                    .allow_anonymous();
                return Ok(Operator::new(builder)?.finish());
            }

            let builder = services::S3::default()
                .root("/")
                .region(region)
                .bucket(bucket)
                .endpoint("https://storage.googleapis.com")
                .access_key_id(access_key_id)
                .secret_access_key(secret_access_key);
            Ok(Operator::new(builder)?.finish())
        }
        #[cfg(feature = "storage-s3")]
        StorageConfig::S3 {
            access_key_id,
            secret_access_key,
            region,
            bucket,
            endpoint,
        } => {
            let mut builder = services::S3::default()
                .bucket(bucket)
                .region(region)
                .access_key_id(access_key_id)
                .secret_access_key(secret_access_key);
            if let Some(endpoint) = endpoint {
                builder = builder.endpoint(endpoint);
            }
            Ok(Operator::new(builder)?.finish())
        }
    }
}

fn create_retry_layer(retry_config: &RetryConfig) -> RetryLayer {
    RetryLayer::new()
        .with_max_times(retry_config.max_count)
        .with_factor(retry_config.delay_factor)
        .with_min_delay(retry_config.min_delay)
        .with_max_delay(retry_config.max_delay)
        .with_jitter()
}

fn create_timeout_layer(timeout_config: &TimeoutConfig) -> TimeoutLayer {
    TimeoutLayer::new()
        .with_io_timeout(timeout_config.timeout)
        .with_timeout(timeout_config.timeout)
}

/// Util function to create opendal operator from filesystem config.
pub(crate) async fn create_opendal_operator(accessor_config: &AccessorConfig) -> Result<Operator> {
    let storage_config = accessor_config.storage_config.clone();
    // Operator creation might involve synchronous blocking IO operation, schedule to dedicated tokio executors.
    let mut op = tokio::task::spawn_blocking(move || create_opendal_operator_impl(&storage_config))
        .await??;

    // Apply chaos layer.
    if let Some(chaos_config) = &accessor_config.chaos_config {
        let chaos_layer = ChaosLayer::new(chaos_config.clone());
        op = op.layer(chaos_layer);
    }
    // Apply retry layer.
    let retry_layer = create_retry_layer(&accessor_config.retry_config);
    op = op.layer(retry_layer);
    // Apply timeout layer.
    let timeout_layer = create_timeout_layer(&accessor_config.timeout_config);
    op = op.layer(timeout_layer);

    Ok(op)
}
