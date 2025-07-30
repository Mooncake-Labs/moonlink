use crate::storage::filesystem::accessor::factory::create_filesystem_accessor;
use crate::storage::filesystem::accessor::operator_utils;
use crate::storage::filesystem::accessor::test_utils::*;
use crate::storage::filesystem::accessor::unbuffered_stream_writer::UnbufferedStreamWriter;
use crate::storage::filesystem::s3::s3_test_utils::*;
use crate::storage::filesystem::s3::test_guard::TestGuard;
use crate::storage::filesystem::test_utils::writer_test_utils::*;

use futures::StreamExt;
use rstest::rstest;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_stats_object() {
    let (bucket, warehouse_uri) = get_test_s3_bucket_and_warehouse();
    let _test_guard = TestGuard::new(bucket.clone()).await;
    let s3_storage_config = create_s3_storage_config(&warehouse_uri);
    let filesystem_accessor = create_filesystem_accessor(s3_storage_config);

    const DST_FILENAME: &str = "target";
    const TARGET_FILESIZE: usize = 10;

    // Write object.
    let random_content = create_random_string(TARGET_FILESIZE);
    filesystem_accessor
        .write_object(DST_FILENAME, random_content.as_bytes().to_vec())
        .await
        .unwrap();

    // Stats object.
    let metadata = filesystem_accessor
        .stats_object(DST_FILENAME)
        .await
        .unwrap();
    assert_eq!(metadata.content_length(), TARGET_FILESIZE as u64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_conditional_write() {
    let (bucket, warehouse_uri) = get_test_s3_bucket_and_warehouse();
    let _test_guard = TestGuard::new(bucket.clone()).await;
    let s3_storage_config = create_s3_storage_config(&warehouse_uri);
    let filesystem_accessor = create_filesystem_accessor(s3_storage_config);

    const DST_FILENAME: &str = "target";
    const TARGET_FILESIZE: usize = 10;
    const FAKE_ETAG: &str = "etag";

    // Write object conditionally, with destination file doesn't exist.
    let random_content = create_random_string(TARGET_FILESIZE);
    let metadata = filesystem_accessor
        .conditional_write_object(
            DST_FILENAME,
            random_content.as_bytes().to_vec(),
            /*etag=*/ None,
        )
        .await
        .unwrap();
    // Read object and check.
    let actual_content = filesystem_accessor.read_object(DST_FILENAME).await.unwrap();
    assert_eq!(actual_content, random_content.as_bytes().to_vec());

    // Write object conditionally, with a fake etag value which doesn't match.
    let random_content = create_random_string(TARGET_FILESIZE);
    let res = filesystem_accessor
        .conditional_write_object(
            DST_FILENAME,
            random_content.as_bytes().to_vec(),
            /*etag=*/ Some(FAKE_ETAG.to_string()),
        )
        .await;
    assert!(res.is_err());

    // Write object conditionally, with the matching etag filled in.
    let random_content = create_random_string(TARGET_FILESIZE);
    let etag = metadata.etag().map(|etag| etag.to_string());
    filesystem_accessor
        .conditional_write_object(DST_FILENAME, random_content.as_bytes().to_vec(), etag)
        .await
        .unwrap();
    // Read object and check.
    let actual_content = filesystem_accessor.read_object(DST_FILENAME).await.unwrap();
    assert_eq!(actual_content, random_content.as_bytes().to_vec());

    // Write object conditionally, with no etag filled in.
    let res = filesystem_accessor
        .conditional_write_object(
            DST_FILENAME,
            random_content.as_bytes().to_vec(),
            /*etag=*/ None,
        )
        .await;
    assert!(res.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[rstest]
#[case(10)]
#[case(18 * 1024 * 1024)]
async fn test_stream_read(#[case] file_size: usize) {
    let (bucket, warehouse_uri) = get_test_s3_bucket_and_warehouse();
    let _test_guard = TestGuard::new(bucket.clone()).await;
    let gcs_storage_config = create_s3_storage_config(&warehouse_uri);

    // Prepare remote file.
    let remote_filepath = format!("{warehouse_uri}/remote");
    let expected_content =
        create_remote_file(&remote_filepath, gcs_storage_config.clone(), file_size).await;

    // Stream read from destination path.
    let mut actual_content = vec![];
    let filesystem_accessor = create_filesystem_accessor(gcs_storage_config);
    let mut read_stream = filesystem_accessor
        .stream_read(&remote_filepath)
        .await
        .unwrap();
    while let Some(chunk) = read_stream.next().await {
        let data = chunk.unwrap();
        actual_content.extend_from_slice(&data);
    }

    // Validate destination file content.
    let actual_content = String::from_utf8(actual_content).unwrap();
    assert_eq!(actual_content.len(), expected_content.len());
    assert_eq!(actual_content, expected_content);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[rstest]
#[case(10)]
#[case(18 * 1024 * 1024)]
async fn test_copy_from_local_to_remote(#[case] file_size: usize) {
    // Prepare src file.
    let temp_dir = tempfile::tempdir().unwrap();
    let root_directory = temp_dir.path().to_str().unwrap().to_string();
    let src_filepath = format!("{}/src", &root_directory);
    let expected_content = create_local_file(&src_filepath, file_size).await;

    let (bucket, warehouse_uri) = get_test_s3_bucket_and_warehouse();
    let _test_guard = TestGuard::new(bucket.clone()).await;
    let s3_storage_config = create_s3_storage_config(&warehouse_uri);

    // Copy from src to dst.
    let filesystem_accessor = create_filesystem_accessor(s3_storage_config);
    let dst_filepath = format!("{warehouse_uri}/dst");
    filesystem_accessor
        .copy_from_local_to_remote(&src_filepath, &dst_filepath)
        .await
        .unwrap();

    // Validate destination file content.
    let actual_content = filesystem_accessor
        .read_object_as_string(&dst_filepath)
        .await
        .unwrap();
    assert_eq!(actual_content, expected_content);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[rstest]
#[case(10)]
#[case(18 * 1024 * 1024)]
async fn test_copy_from_remote_to_local(#[case] file_size: usize) {
    let temp_dir = tempfile::tempdir().unwrap();
    let root_directory = temp_dir.path().to_str().unwrap().to_string();
    let dst_filepath = format!("{}/dst", &root_directory);

    let (bucket, warehouse_uri) = get_test_s3_bucket_and_warehouse();
    let _test_guard = TestGuard::new(bucket.clone()).await;
    let s3_storage_config = create_s3_storage_config(&warehouse_uri);

    // Prepare src file.
    let src_filepath = format!("{warehouse_uri}/src");
    let expected_content =
        create_remote_file(&src_filepath, s3_storage_config.clone(), file_size).await;

    // Copy from src to dst.
    let filesystem_accessor = create_filesystem_accessor(s3_storage_config);
    filesystem_accessor
        .copy_from_remote_to_local(&src_filepath, &dst_filepath)
        .await
        .unwrap();

    // Validate destination file content.
    let actual_content = tokio::fs::read_to_string(dst_filepath).await.unwrap();
    assert_eq!(actual_content, expected_content);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_unbuffered_stream_writer() {
    let dst_filename = "dst".to_string();
    let (bucket, warehouse_uri) = get_test_s3_bucket_and_warehouse();
    let _test_guard = TestGuard::new(bucket.clone()).await;
    let s3_storage_config = create_s3_storage_config(&warehouse_uri);
    let operator = operator_utils::create_opendal_operator(&s3_storage_config).unwrap();

    // Create writer and append in blocks.
    let writer =
        Box::new(UnbufferedStreamWriter::new(operator.clone(), dst_filename.clone()).unwrap());
    test_unbuffered_stream_writer_impl(writer, dst_filename, s3_storage_config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_unbuffered_stream_write_with_filesystem_accessor() {
    let (bucket, warehouse_uri) = get_test_s3_bucket_and_warehouse();
    let _test_guard = TestGuard::new(bucket.clone()).await;
    let s3_storage_config = create_s3_storage_config(&warehouse_uri);
    let filesystem_accessor = create_filesystem_accessor(s3_storage_config.clone());

    let dst_filename = "dst".to_string();
    let dst_filepath = format!("{}/{}", &warehouse_uri, dst_filename);
    let writer = filesystem_accessor
        .create_unbuffered_stream_writer(&dst_filepath)
        .await
        .unwrap();
    test_unbuffered_stream_writer_impl(writer, dst_filename, s3_storage_config).await;
}
