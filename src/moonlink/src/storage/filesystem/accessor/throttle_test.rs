/// Tests for ThrottleConfig integration with OpenDAL ThrottleLayer
use std::time::Instant;
use tempfile::tempdir;

use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::filesystem::accessor::filesystem_accessor::FileSystemAccessor;
use crate::storage::filesystem::accessor_config::{AccessorConfig, ThrottleConfig};
use crate::StorageConfig;
use more_asserts as ma;

/// Create accessor with optional throttle configuration
fn create_test_accessor(
    temp_dir: &tempfile::TempDir,
    throttle_config: Option<ThrottleConfig>,
) -> std::sync::Arc<dyn BaseFileSystemAccess> {
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
    let file_size = 1024 * 1024; // 1 MiB per file
    let num_files = 6;
    let test_data = vec![b'x'; file_size];

    // Test with throttle configuration
    let throttled_accessor = create_test_accessor(
        &temp_dir,
        Some(ThrottleConfig {
            bandwidth: 1024 * 1024, // 1 MiB/s
            burst: 2 * 1024 * 1024, // 2 MiB burst
        }),
    );
    let start_time = Instant::now();
    for i in 0..num_files {
        throttled_accessor
            .write_object(&format!("throttled_{i}.dat"), test_data.clone())
            .await
            .unwrap();
    }
    let throttled_duration = start_time.elapsed();

    // Test without throttle
    let baseline_accessor = create_test_accessor(&temp_dir, None);
    let start_time = Instant::now();
    for i in 0..num_files {
        baseline_accessor
            .write_object(&format!("baseline_{i}.dat"), test_data.clone())
            .await
            .unwrap();
    }
    let baseline_duration = start_time.elapsed();
    // Throttled operations should take longer than baseline
    ma::assert_gt!(
        throttled_duration,
        baseline_duration,
        "Throttled operations should be slower than baseline",
    );
}

#[tokio::test]
async fn test_throttle_parallel_writes() {
    let temp_dir = tempdir().unwrap();
    let file_size = 1024 * 1024; // 1 MiB per file
    let concurrent_tasks = 4; // Number of concurrent tasks to test parallel throttling
    let files_per_task = 2; // Files written by each task (total: 4×2=8 files)
    let test_data = vec![b'x'; file_size];

    // Test with throttle configuration - parallel writes
    let throttled_accessor = create_test_accessor(
        &temp_dir,
        Some(ThrottleConfig {
            bandwidth: 1024 * 1024, // 1 MiB/s
            burst: 1024 * 1024,     // 1 MiB burst
        }),
    );
    let start_time = Instant::now();

    let mut handles = Vec::new();
    for task_id in 0..concurrent_tasks {
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
    let baseline_accessor = create_test_accessor(&temp_dir, None);
    let start_time = Instant::now();

    let mut handles = Vec::new();
    for task_id in 0..concurrent_tasks {
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
    // Parallel throttled operations should take longer than baseline
    ma::assert_gt!(
        throttled_duration,
        baseline_duration,
        "Parallel throttled operations should be slower than baseline",
    );
}

#[tokio::test]
async fn test_throttle_insufficient_capacity() {
    let temp_dir = tempdir().unwrap();
    let oversized_data = vec![b'y'; 2 * 1024 * 1024]; // 2 MiB > 1 MiB burst
    let throttled_accessor = create_test_accessor(
        &temp_dir,
        Some(ThrottleConfig {
            bandwidth: 1024 * 1024, // 1 MiB/s
            burst: 1024 * 1024,     // 1 MiB burst
        }),
    );

    // Single write larger than burst capacity should fail
    let result = throttled_accessor
        .write_object("oversized.dat", oversized_data)
        .await;
    // Should get an error when write size exceeds burst capacity
    assert!(result.is_err(), "Expected error for oversized write");
}
