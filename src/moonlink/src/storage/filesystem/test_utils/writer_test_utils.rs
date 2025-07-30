use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::filesystem::accessor::base_unbuffered_stream_writer::BaseUnbufferedStreamWriter;
use crate::storage::filesystem::accessor::filesystem_accessor::FileSystemAccessor;
use crate::storage::filesystem::accessor_config::AccessorConfig;

/// Util function to test stream writer.
pub(crate) async fn test_unbuffered_stream_writer_impl(
    mut writer: Box<dyn BaseUnbufferedStreamWriter>,
    dst_filename: String,
    accessor_config: AccessorConfig,
) {
    const FILE_SIZE: usize = 10;
    const CONTENT: &str = "helloworld";

    writer
        .append_non_blocking(CONTENT.as_bytes()[..FILE_SIZE / 2].to_vec())
        .await
        .unwrap();
    writer
        .append_non_blocking(CONTENT.as_bytes()[FILE_SIZE / 2..].to_vec())
        .await
        .unwrap();
    writer.finalize().await.unwrap();

    // Verify content.
    let filesystem_accessor = FileSystemAccessor::new(accessor_config);
    let actual_content = filesystem_accessor
        .read_object_as_string(&dst_filename)
        .await
        .unwrap();
    assert_eq!(actual_content, CONTENT);
}
