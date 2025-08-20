use std::time::Duration;

use crate::storage::cache::metadata::base_cache::MetadataCacheTrait;
use crate::storage::cache::metadata::test_utils::MokaCacheTestBuilder;

#[tokio::test]
async fn test_get_values() {
    let cache = MokaCacheTestBuilder::new().build();

    cache
        .initialize_for_test(vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ])
        .await;

    assert_eq!(
        cache.get(&"key1".to_string()).await,
        Some("value1".to_string())
    );
}

#[tokio::test]
async fn test_evict_by_ttl() {
    let cache = MokaCacheTestBuilder::new()
        .ttl(Duration::from_secs(0))
        .build();

    cache
        .initialize_for_test(vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ])
        .await;

    assert_eq!(cache.len().await, 0);
}

#[tokio::test]
async fn test_put_values() {
    let cache = MokaCacheTestBuilder::new().build();

    cache.put("key1".to_string(), "value1".to_string()).await;
    cache.put("key2".to_string(), "value2".to_string()).await;

    let all_entries = cache.dump_all_for_test().await;

    assert_eq!(cache.len().await, 2);
    assert!(all_entries.contains(&("key1".to_string(), "value1".to_string())));
    assert!(all_entries.contains(&("key2".to_string(), "value2".to_string())));
}

#[tokio::test]
async fn test_replace_entry_when_max_size_exceeds() {
    let cache = MokaCacheTestBuilder::new().build();

    cache
        .initialize_for_test(vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ])
        .await;

    cache.put("key3".to_string(), "value3".to_string()).await;
    cache.force_cleanup_for_test().await;

    let all_entries = cache.dump_all_for_test().await;

    assert_eq!(cache.len().await, 2);
    assert!(all_entries.contains(&("key2".to_string(), "value2".to_string())));
    assert!(all_entries.contains(&("key3".to_string(), "value3".to_string())));
}

#[tokio::test]
async fn test_evict_value() {
    let cache = MokaCacheTestBuilder::new().build();

    cache
        .initialize_for_test(vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ])
        .await;

    let removed = cache.evict(&"key1".to_string()).await;

    cache.force_cleanup_for_test().await;

    let all_entries = cache.dump_all_for_test().await;
    assert_eq!(removed, Some("value1".to_string()));
    assert_eq!(cache.len().await, 1);
    assert!(all_entries.contains(&("key2".to_string(), "value2".to_string())));
}

#[tokio::test]
async fn test_evict_non_existing_key() {
    use crate::storage::cache::metadata::test_utils::MokaCacheTestBuilder;

    let cache = MokaCacheTestBuilder::new().build::<String, String>();

    cache
        .initialize_for_test(vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ])
        .await;

    let removed = cache.evict(&"not_exist".to_string()).await;

    let all_entries = cache.dump_all_for_test().await;
    assert_eq!(cache.len().await, 2);
    assert_eq!(removed, None);
    assert!(all_entries.contains(&("key1".to_string(), "value1".to_string())));
    assert!(all_entries.contains(&("key2".to_string(), "value2".to_string())));
}

#[tokio::test]
async fn test_clear_all_values() {
    let cache = MokaCacheTestBuilder::new().build();

    cache
        .initialize_for_test(vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ])
        .await;

    cache.clear().await;
    cache.force_cleanup_for_test().await;

    assert_eq!(cache.len().await, 0);
}
