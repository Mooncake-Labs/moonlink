use crate::storage::iceberg::{iceberg_table_config::RestCatalogConfig, rest_catalog::RestCatalog};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, NamespaceIdent, TableCreation};
use rand::{distr::Alphanumeric, Rng};
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::iter::FromIterator;
use std::vec;

const DEFAULT_REST_CATALOG_NAME: &str = "test";
const DEFAULT_REST_CATALOG_URI: &str = "http://iceberg-rest.local:8181";
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

/// Util function to transaction vec into set
pub(crate) fn to_set<T: Eq + Hash>(vec: Vec<T>) -> HashSet<T> {
    HashSet::from_iter(vec)
}

/// Util function to test multiple_namespaces
pub(crate) async fn create_namespaces(
    catalog: &RestCatalog,
    namespace_idents: &Vec<&NamespaceIdent>,
) {
    for namespace_ident in namespace_idents {
        let _ = catalog
            .create_namespace(namespace_ident, HashMap::new())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "Namespace creation failed, namespace={}",
                    &namespace_ident.to_url_string()
                )
            });
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
