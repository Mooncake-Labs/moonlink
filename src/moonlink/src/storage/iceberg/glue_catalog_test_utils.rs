use crate::storage::filesystem::s3::s3_test_utils::*;
use crate::storage::iceberg::cloud_security_config::{AwsSecurityConfig, CloudSecurityConfig};
use crate::storage::iceberg::file_catalog_test_utils::get_test_schema;
use crate::storage::iceberg::glue_catalog::GlueCatalog;
use crate::storage::iceberg::iceberg_table_config::GlueCatalogConfig;
use crate::storage::iceberg::moonlink_catalog::CatalogAccess;
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use rand::{distr::Alphanumeric, Rng};
use std::collections::HashMap;

/// Test AWS access id.
pub(crate) const TEST_AWS_ACCESS_ID: &str = "moonlink_test_access_id";
/// Test AWS secret.
pub(crate) const TEST_AWS_ACCESS_SECRET: &str = "moonlink_test_secret";
/// Test AWS region.
pub(crate) const TEST_AWS_RETION: &str = "us-east-1";
/// Test glue endpoint.
pub(crate) const TEST_GLUE_ENDPOINT: &str = "http://moto-glue.local:5000";

/// Test util function to get glue catalog name.
pub(crate) fn get_random_glue_catalog_name() -> String {
    format!("glue-catalog-{}", uuid::Uuid::new_v4())
}

/// Test util function to get a random string.
fn get_random_string() -> String {
    let rng = rand::rng();
    rng.sample_iter(&Alphanumeric)
        .take(10)
        .map(char::from)
        .collect()
}

/// Test util function to get a random namespace.
pub(crate) fn get_random_namespace() -> String {
    get_random_string()
}

/// Test util function to get a random table.
pub(crate) fn get_random_table() -> String {
    get_random_string()
}

/// Test util function to create aws security config.
pub(crate) fn create_aws_cloud_security_config() -> CloudSecurityConfig {
    let aws_security_config = AwsSecurityConfig {
        access_key_id: TEST_AWS_ACCESS_ID.to_string(),
        security_access_key: TEST_AWS_ACCESS_SECRET.to_string(),
        region: TEST_AWS_RETION.to_string(),
    };
    CloudSecurityConfig::Aws(aws_security_config)
}

/// Test util function to create a glue catalog.
pub(crate) async fn create_glue_catalog(warehouse_uri: String) -> GlueCatalog {
    let glue_config = GlueCatalogConfig {
        // AWS security config.
        cloud_secret_config: create_aws_cloud_security_config(),
        // Glue configs.
        name: get_random_glue_catalog_name(),
        uri: TEST_GLUE_ENDPOINT.to_string(),
        catalog_id: None,
        warehouse: warehouse_uri.clone(),
        s3_endpoint: Some(S3_TEST_ENDPOINT.to_string()),
    };
    let accessor_config = create_s3_storage_config(&warehouse_uri);
    let glue_catalog = GlueCatalog::new(glue_config, accessor_config, get_test_schema())
        .await
        .unwrap();
    glue_catalog
}

/// Test util function to create a namespace for the given glue catalog.
pub(crate) async fn create_namespace(glue_catalog: &GlueCatalog, namespace_ident: NamespaceIdent) {
    glue_catalog
        .create_namespace(&namespace_ident, /*properties=*/ HashMap::new())
        .await
        .unwrap();
}
/// Test util function to create a table for the given glue catalog.
pub(crate) async fn create_table(
    glue_catalog: &GlueCatalog,
    namespace_ident: NamespaceIdent,
    table_ident: TableIdent,
) {
    let table_creation = TableCreation::builder()
        .name(table_ident.name().to_string())
        .location(format!(
            "{}/{}/{}",
            glue_catalog.get_warehouse_location(),
            namespace_ident.to_url_string(),
            table_ident.name(),
        ))
        .schema(get_test_schema())
        .build();
    glue_catalog
        .create_table(&namespace_ident, table_creation)
        .await
        .unwrap();
}
