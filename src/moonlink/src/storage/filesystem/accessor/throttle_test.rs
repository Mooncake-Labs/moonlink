/// Integration tests for ThrottleConfig with OpenDAL ThrottleLayer
///
/// Note: These are smoke tests to verify throttle integration works.
/// Precise bandwidth enforcement is not tested due to OpenDAL ThrottleLayer
/// implementation details (CPU busy-wait instead of proper async sleep).
use std::time::{Duration, Instant};
use tempfile::tempdir;
use tokio::time::timeout;

use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::filesystem::accessor::filesystem_accessor::FileSystemAccessor;
use crate::storage::filesystem::accessor_config::{AccessorConfig, ThrottleConfig};
use crate::StorageConfig;

const TEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Create accessor with throttle configuration
fn create_throttle_accessor(
    temp_dir: &tempfile::TempDir,
    bandwidth_mbps: f64,
    burst_mb: f64,
) -> std::sync::Arc<dyn BaseFileSystemAccess> {
    let throttle_config = Some(ThrottleConfig {
        bandwidth: (bandwidth_mbps * 1024.0 * 1024.0) as u32,
        burst: (burst_mb * 1024.0 * 1024.0) as u32,
    });

    let accessor_config = AccessorConfig {
        storage_config: StorageConfig::FileSystem {
            root_directory: temp_dir.path().to_string_lossy().to_string(),
            atomic_write_dir: None,
        },
        retry_config: Default::default(),
        timeout_config: Default::default(),
        throttle_config,
        chaos_config: None,
    };

    std::sync::Arc::new(FileSystemAccessor::new(accessor_config))
}

#[tokio::test]
async fn test_throttle_sequential_writes() {
    let temp_dir = tempdir().unwrap();
    let test_future = async {
        let file_size = 1024 * 1024; // 1 MB per file
        let num_files = 6;
        let test_data = vec![b'x'; file_size];

        // Test with throttle configuration
        let throttled_accessor = create_throttle_accessor(&temp_dir, 1.0, 2.0); // 1 MB/s, 2 MB burst
        let start_time = Instant::now();
        for i in 0..num_files {
            throttled_accessor
                .write_object(&format!("throttled_{i}.dat"), test_data.clone())
                .await
                .unwrap();
        }
        let throttled_duration = start_time.elapsed();

        // Test without throttle (high limits)
        let baseline_accessor = create_throttle_accessor(&temp_dir, 1000.0, 1000.0);
        let start_time = Instant::now();
        for i in 0..num_files {
            baseline_accessor
                .write_object(&format!("baseline_{i}.dat"), test_data.clone())
                .await
                .unwrap();
        }
        let baseline_duration = start_time.elapsed();
        println!("Sequential writes test:");
        println!(
            "  Throttled (1MB/s, 2MB burst): {:.3}s for {}MB",
            throttled_duration.as_secs_f64(),
            (num_files * file_size) as f64 / (1024.0 * 1024.0)
        );
        println!(
            "  Baseline (1000MB/s): {:.3}s",
            baseline_duration.as_secs_f64()
        );
        println!(
            "  Slowdown ratio: {:.2}x",
            throttled_duration.as_secs_f64() / baseline_duration.as_secs_f64()
        );
        // Throttled operations should take longer than baseline
        assert!(
            throttled_duration > baseline_duration,
            "Throttled operations should be slower than baseline: throttled={:.3}s, baseline={:.3}s",
            throttled_duration.as_secs_f64(),
            baseline_duration.as_secs_f64()
        );
    };

    timeout(TEST_TIMEOUT, test_future).await.unwrap();
}

#[tokio::test]
async fn test_throttle_parallel_writes() {
    let temp_dir = tempdir().unwrap();
    let test_future = async {
        let file_size = 1024 * 1024; // 1 MB per file
        let num_parallel = 4;
        let files_per_task = 2;
        let test_data = vec![b'x'; file_size];

        // Test with throttle configuration - parallel writes
        let throttled_accessor = create_throttle_accessor(&temp_dir, 1.0, 2.0); // 1.0 MB/s, 2 MB burst
        let start_time = Instant::now();

        let mut handles = Vec::new();
        for task_id in 0..num_parallel {
            let accessor = throttled_accessor.clone();
            let data = test_data.clone();
            let handle = tokio::spawn(async move {
                for file_id in 0..files_per_task {
                    accessor
                        .write_object(
                            &format!("throttled_task{task_id}_file{file_id}.dat"),
                            data.clone(),
                        )
                        .await
                        .unwrap();
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }
        let throttled_duration = start_time.elapsed();

        // Test without throttle - parallel writes
        let baseline_accessor = create_throttle_accessor(&temp_dir, 1000.0, 1000.0);
        let start_time = Instant::now();

        let mut handles = Vec::new();
        for task_id in 0..num_parallel {
            let accessor = baseline_accessor.clone();
            let data = test_data.clone();
            let handle = tokio::spawn(async move {
                for file_id in 0..files_per_task {
                    accessor
                        .write_object(
                            &format!("baseline_task{task_id}_file{file_id}.dat"),
                            data.clone(),
                        )
                        .await
                        .unwrap();
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }
        let baseline_duration = start_time.elapsed();
        println!("Parallel writes test:");
        println!(
            "  Throttled (1.0MB/s, 2MB burst): {:.3}s for {}MB across {} tasks",
            throttled_duration.as_secs_f64(),
            (num_parallel * files_per_task * file_size) as f64 / (1024.0 * 1024.0),
            num_parallel
        );
        println!(
            "  Baseline (1000MB/s): {:.3}s",
            baseline_duration.as_secs_f64()
        );
        println!(
            "  Slowdown ratio: {:.2}x",
            throttled_duration.as_secs_f64() / baseline_duration.as_secs_f64()
        );
        // Parallel throttled operations should take longer than baseline
        assert!(
            throttled_duration > baseline_duration,
            "Parallel throttled operations should be slower than baseline: throttled={:.3}s, baseline={:.3}s",
            throttled_duration.as_secs_f64(),
            baseline_duration.as_secs_f64()
        );
    };

    timeout(TEST_TIMEOUT, test_future).await.unwrap();
}

#[tokio::test]
async fn test_throttle_insufficient_capacity() {
    let temp_dir = tempdir().unwrap();
    let test_future = async {
        let oversized_data = vec![b'y'; 2 * 1024 * 1024]; // 2 MB > 1 MB burst
        let throttled_accessor = create_throttle_accessor(&temp_dir, 1.0, 1.0); // 1 MB/s, 1 MB burst

        // Single write larger than burst capacity should fail
        let result = throttled_accessor
            .write_object("oversized.dat", oversized_data)
            .await;
        println!("Insufficient capacity test:");
        println!("  Attempted 2MB write with 1MB burst capacity");
        // Should get an error when write size exceeds burst capacity
        assert!(result.is_err(), "Expected error for oversized write");
    };

    timeout(TEST_TIMEOUT, test_future).await.unwrap();
}
