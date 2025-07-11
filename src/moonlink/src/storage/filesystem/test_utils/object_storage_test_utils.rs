/// Testing utils for object storage.
#[allow(dead_code)]
pub(crate) const TEST_RETRY_COUNT: usize = 2;
#[allow(dead_code)]
pub(crate) const TEST_RETRY_INIT_MILLISEC: u64 = 100;
#[allow(dead_code)]
pub(crate) const TEST_BUCKET_NAME_LEN: usize = 10;

use rand::Rng;

/// Get object storage bucket name from warehouse uri.
#[allow(dead_code)]
pub(crate) fn get_bucket_from_warehouse_uri(warehouse_uri: &str) -> String {
    // Try to parse with url::Url
    if let Ok(url) = url::Url::parse(warehouse_uri) {
        if let Some(bucket) = url.host_str() {
            return bucket.to_string();
        }
    }

    // Fallback: strip scheme manually (e.g., "s3://bucket/dir1/dir2")
    warehouse_uri
        .strip_prefix("s3://")
        .or_else(|| warehouse_uri.strip_prefix("gs://"))
        .and_then(|rest| rest.split('/').next())
        .unwrap_or_default()
        .to_string()
}

#[allow(dead_code)]
pub(crate) fn get_bucket_and_warehouse(
    bucket_prefix: &str,
    warehouse_uri_prefix: &str,
) -> (String /*bucket*/, String /*warehouse_uri*/) {
    const TEST_BUCKET_NAME_LEN: usize = 12;
    const ALLOWED_CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::rng();
    let random_string: String = (0..TEST_BUCKET_NAME_LEN)
        .map(|_| {
            let idx = rng.random_range(0..ALLOWED_CHARS.len());
            ALLOWED_CHARS[idx] as char
        })
        .collect();
    (
        format!("{bucket_prefix}{random_string}"),
        format!("{warehouse_uri_prefix}{random_string}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_bucket_from_warehouse_uri() {
        assert_eq!(
            get_bucket_from_warehouse_uri("s3://my-bucket/dir1/dir2/abc.parquet"),
            "my-bucket"
        );
        assert_eq!(
            get_bucket_from_warehouse_uri("gs://another-bucket/folder/file"),
            "another-bucket"
        );
    }
}
