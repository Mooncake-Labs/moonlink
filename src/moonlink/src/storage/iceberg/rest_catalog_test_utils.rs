use crate::storage::iceberg::iceberg_table_config::RestCatalogConfig;
use crate::storage::mooncake_table::test_utils_commons::REST_CATALOG_TEST_URI;
use crate::{AccessorConfig, StorageConfig};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::TableCreation;
use rand::{distr::Alphanumeric, Rng};
use std::collections::HashMap;

const DEFAULT_REST_CATALOG_NAME: &str = "test";
const DEFAULT_WAREHOUSE_PATH: &str = "warehouse";

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
