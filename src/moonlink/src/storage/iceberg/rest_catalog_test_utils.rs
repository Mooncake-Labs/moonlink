use crate::storage::iceberg::iceberg_table_config::RestCatalogConfig;
use crate::storage::mooncake_table::test_utils_commons::REST_CATALOG_TEST_URI;
use crate::{AccessorConfig, FsRetryConfig, FsTimeoutConfig, IcebergTableConfig, StorageConfig};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::TableCreation;
use rand::{distr::Alphanumeric, Rng};
use std::collections::HashMap;
use tempfile::TempDir;

const DEFAULT_REST_CATALOG_NAME: &str = "test";
const DEFAULT_WAREHOUSE_PATH: &str = "/tmp/moonlink_iceberg";

pub(crate) fn get_random_string() -> String {
    let rng = rand::rng();
    rng.sample_iter(&Alphanumeric)
        .take(10)
        .map(char::from)
        .collect()
}

pub(crate) fn default_accessor_config() -> AccessorConfig {
    let storage_config = StorageConfig::FileSystem {
        root_directory: DEFAULT_WAREHOUSE_PATH.to_string(),
        atomic_write_dir: None,
    };
    AccessorConfig::new_with_storage_config(storage_config)
}

pub(crate) fn default_rest_catalog_config() -> RestCatalogConfig {
    RestCatalogConfig {
        name: format!("{}-{}", DEFAULT_REST_CATALOG_NAME, get_random_string()),
        uri: REST_CATALOG_TEST_URI.to_string(),
        warehouse: DEFAULT_WAREHOUSE_PATH.to_string(),
        props: HashMap::new(),
    }
}

pub(crate) fn get_accessor_config(tmp_dir: &TempDir) -> AccessorConfig {
    let storage_config = StorageConfig::FileSystem {
        root_directory: tmp_dir.path().to_str().unwrap().to_string(),
        atomic_write_dir: None,
    };
    AccessorConfig {
        storage_config,
        retry_config: FsRetryConfig::default(),
        timeout_config: FsTimeoutConfig::default(),
        chaos_config: None,
    }
}

pub(crate) fn get_rest_iceberg_table_config(tmp_dir: &TempDir) -> IcebergTableConfig {
    IcebergTableConfig {
        namespace: vec![get_random_string()],
        table_name: get_random_string(),
        data_accessor_config: get_accessor_config(tmp_dir),
        metadata_accessor_config: crate::IcebergCatalogConfig::Rest {
            rest_catalog_config: default_rest_catalog_config(),
        },
    }
}

pub(crate) fn default_table_creation(table_name: String) -> TableCreation {
    TableCreation::builder()
        .name(table_name)
        .schema(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
                ])
                .build()
                .unwrap(),
        )
        .build()
}
