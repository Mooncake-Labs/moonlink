//! Size of the table-opening future.
use super::test_utils::*;
use super::*;
use crate::storage::mooncake_table::table_creation_test_utils::*;

/// Opening a table is awaited from deep in the recovery chain (backend start, replication
/// manager, connection, table init), and in debug builds each of those async callers keeps a
/// copy of its child future on the stack. A large `MooncakeTable::new` future overflowed the
/// 2 MiB stack of the thread recovering a mirror. The large parts (loading with its garbage
/// collection, the index rebuild) are boxed; keep it that way.
#[tokio::test]
async fn test_table_open_future_stays_small() {
    let context = TestContext::new("open_future");
    let iceberg_table_config = test_iceberg_table_config(&context, "open_future");
    let open = MooncakeTable::new(
        (*create_test_arrow_schema()).clone(),
        "open_future".to_string(),
        1,
        context.path(),
        iceberg_table_config.clone(),
        test_mooncake_table_config(&context),
        create_test_object_storage_cache(&context.temp_dir),
        create_test_filesystem_accessor(&iceberg_table_config),
    );
    let size = std::mem::size_of_val(&open);
    assert!(
        size <= 16 << 10,
        "MooncakeTable::new future is {size} bytes"
    );
}
