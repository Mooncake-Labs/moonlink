use crate::storage::iceberg::iceberg_table_config::RestCatalogConfig;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::TableCreation;
use rand::{distr::Alphanumeric, Rng};
use std::collections::HashMap;

const DEFAULT_REST_CATALOG_NAME: &str = "test";
const DEFAULT_REST_CATALOG_URI: &str = "http://localhost:8181";
const DEFAULT_WAREHOUSE_PATH: &str = "warehouse";

pub(crate) fn get_random_string() -> String {
    let rng = rand::rng();
    rng.sample_iter(&Alphanumeric)
        .take(10)
        .map(char::from)
        .collect()
}

pub(crate) fn default_rest_catalog_config() -> RestCatalogConfig {
    RestCatalogConfig {
        name: DEFAULT_REST_CATALOG_NAME.to_string(),
        uri: DEFAULT_REST_CATALOG_URI.to_string(),
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
