use crate::storage::filesystem::accessor::filesystem_accessor_chaos_wrapper::ChaosLayer;
use crate::storage::filesystem::accessor_config::AccessorConfig;
use crate::storage::filesystem::accessor_config::RetryConfig;
use crate::storage::filesystem::accessor_config::ThrottleConfig;
use crate::storage::filesystem::accessor_config::TimeoutConfig;
use crate::storage::filesystem::storage_config::StorageConfig;
use crate::Result;

use opendal::layers::RetryLayer;
use opendal::layers::ThrottleLayer;
use opendal::layers::TimeoutLayer;
use opendal::services;
use opendal::Operator;

/// Temp-file directory for atomic writes, created under the filesystem root. Hidden (dot) entries
/// are never treated as namespaces or tables.
pub(crate) const DEFAULT_ATOMIC_WRITE_SUBDIR: &str = ".moonlink_atomic_tmp";

/// Delete temp files a crash or a failed write left in an atomic-write directory. Runs once per
/// directory and process, before the first operator of this process writes through it.
#[cfg(feature = "storage-fs")]
fn remove_leftover_atomic_write_files(dir: &str) {
    static SWEPT: std::sync::LazyLock<std::sync::Mutex<std::collections::HashSet<String>>> =
        std::sync::LazyLock::new(Default::default);
    if !SWEPT.lock().unwrap().insert(dir.to_string()) {
        return;
    }
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let (mut files, mut bytes) = (0usize, 0u64);
    for entry in entries.flatten() {
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        if metadata.is_file() && std::fs::remove_file(entry.path()).is_ok() {
            files += 1;
            bytes += metadata.len();
        }
    }
    if files > 0 {
        tracing::info!(
            dir,
            files,
            bytes,
            "removed temp files left by interrupted writes"
        );
    }
}

fn create_opendal_operator_impl(storage_config: &StorageConfig) -> Result<Operator> {
    match storage_config {
        #[cfg(feature = "storage-fs")]
        StorageConfig::FileSystem {
            root_directory,
            atomic_write_dir,
        } => {
            // Always write through a temp file + rename, so a crash or a full disk never leaves a
            // truncated metadata file or version hint behind. The default temp directory lives
            // under the root, i.e. on the same filesystem, which keeps the rename atomic.
            let atomic_write_dir = atomic_write_dir.clone().unwrap_or_else(|| {
                std::path::Path::new(root_directory)
                    .join(DEFAULT_ATOMIC_WRITE_SUBDIR)
                    .to_string_lossy()
                    .to_string()
            });
            remove_leftover_atomic_write_files(&atomic_write_dir);
            let builder = services::Fs::default()
                .root(root_directory)
                .atomic_write_dir(&atomic_write_dir);
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
                .secret_access_key(secret_access_key)
                .disable_config_load()
                .disable_ec2_metadata();
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

fn create_throttle_layer(throttle_config: &ThrottleConfig) -> ThrottleLayer {
    ThrottleLayer::new(throttle_config.bandwidth, throttle_config.burst)
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
    // Apply throttle layer.
    if let Some(throttle_config) = &accessor_config.throttle_config {
        let throttle_layer = create_throttle_layer(throttle_config);
        op = op.layer(throttle_layer);
    }
    // Apply retry layer.
    let retry_layer = create_retry_layer(&accessor_config.retry_config);
    op = op.layer(retry_layer);
    // Apply timeout layer.
    let timeout_layer = create_timeout_layer(&accessor_config.timeout_config);
    op = op.layer(timeout_layer);

    Ok(op)
}
