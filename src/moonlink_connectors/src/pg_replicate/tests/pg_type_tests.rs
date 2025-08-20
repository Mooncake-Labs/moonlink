#![cfg(feature = "connector-pg")]

use crate::pg_replicate::clients::postgres::ReplicationClient;
use crate::pg_replicate::conversions::text::TextFormatConverter;
use crate::pg_replicate::table::{ColumnSchema, TableName};
use serial_test::serial;
use tokio_postgres::types::{Kind, Type};
use tokio_postgres::{connect, NoTls};

async fn setup_connection() -> tokio_postgres::Client {
    // Use DATABASE_URL env var if set (for CI), otherwise use devcontainer default
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://postgres:postgres@postgres:5432/postgres".to_string());

    let (client, connection) = connect(&database_url, NoTls).await.unwrap();
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Postgres connection error: {e}");
        }
    });
    client
}

async fn get_table_id(client: &tokio_postgres::Client, table_name: &str) -> u32 {
    let query = format!(
        "SELECT oid FROM pg_class WHERE relname = '{}' 
         AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')",
        table_name
    );

    for message in client.simple_query(&query).await.unwrap() {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = message {
            return row.get("oid").unwrap().parse().unwrap();
        }
    }
    panic!("Table not found: {}", table_name);
}

#[tokio::test]
#[serial]
async fn test_basic_composite_type() {
    let client = setup_connection().await;

    // Create basic composite type
    client
        .simple_query(
            "DROP TYPE IF EXISTS test_address CASCADE;
             CREATE TYPE test_address AS (street TEXT, city TEXT, zip INTEGER);
             
             DROP TABLE IF EXISTS test_basic_composite CASCADE;
             CREATE TABLE test_basic_composite (
                 id INTEGER PRIMARY KEY,
                 addr test_address
             );
             
             INSERT INTO test_basic_composite VALUES 
             (1, ROW('123 Main St', 'NYC', 10001)::test_address);",
        )
        .await
        .unwrap();

    let table_id = get_table_id(&client, "test_basic_composite").await;
    let table_name = TableName {
        schema: "public".to_string(),
        name: "test_basic_composite".to_string(),
    };

    let replication_client = ReplicationClient::from_client(client);
    let schema = replication_client
        .get_table_schema(table_id, table_name, /*publication=*/ None)
        .await
        .unwrap();

    let addr_col = schema
        .column_schemas
        .iter()
        .find(|c| c.name == "addr")
        .unwrap();

    if let Kind::Composite(fields) = addr_col.typ.kind() {
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0].name(), "street");
        assert_eq!(*fields[0].type_(), Type::TEXT);
        assert_eq!(fields[1].name(), "city");
        assert_eq!(*fields[1].type_(), Type::TEXT);
        assert_eq!(fields[2].name(), "zip");
        assert_eq!(*fields[2].type_(), Type::INT4);
    } else {
        panic!("Expected composite type");
    }

    // Test parsing
    let test_value = r#"("123 Main St",NYC,10001)"#;
    assert!(TextFormatConverter::try_from_str(&addr_col.typ, test_value).is_ok());
}

#[tokio::test]
#[serial]
async fn test_nested_composite_types() {
    let client = setup_connection().await;

    // Create nested composite types
    client
        .simple_query(
            "DROP TYPE IF EXISTS test_point CASCADE;
             DROP TYPE IF EXISTS test_location CASCADE;
             CREATE TYPE test_point AS (x FLOAT8, y FLOAT8);
             CREATE TYPE test_location AS (name TEXT, point test_point);
             
             DROP TABLE IF EXISTS test_nested CASCADE;
             CREATE TABLE test_nested (
                 id INTEGER PRIMARY KEY,
                 loc test_location
             );
             
             INSERT INTO test_nested VALUES 
             (1, ROW('Home', ROW(1.5, 2.5)::test_point)::test_location);",
        )
        .await
        .unwrap();

    let table_id = get_table_id(&client, "test_nested").await;
    let replication_client = ReplicationClient::from_client(client);
    let schema = replication_client
        .get_table_schema(
            table_id,
            TableName {
                schema: "public".to_string(),
                name: "test_nested".to_string(),
            },
            /*publication=*/ None,
        )
        .await
        .unwrap();

    let loc_col = schema
        .column_schemas
        .iter()
        .find(|c| c.name == "loc")
        .unwrap();

    if let Kind::Composite(loc_fields) = loc_col.typ.kind() {
        let point_field = loc_fields.iter().find(|f| f.name() == "point").unwrap();

        if let Kind::Composite(point_fields) = point_field.type_().kind() {
            assert_eq!(point_fields.len(), 2);
            assert_eq!(*point_fields[0].type_(), Type::FLOAT8);
            assert_eq!(*point_fields[1].type_(), Type::FLOAT8);
        } else {
            panic!("Expected nested composite");
        }
    } else {
        panic!("Expected composite type");
    }

    let test_value = r#"(Home,"(1.5,2.5)")"#;
    assert!(TextFormatConverter::try_from_str(&loc_col.typ, test_value).is_ok());
}

#[tokio::test]
#[serial]
async fn test_array_of_composite_types() {
    let client = setup_connection().await;

    // Create composite type for array testing
    client
        .simple_query(
            "DROP TYPE IF EXISTS test_item CASCADE;
             CREATE TYPE test_item AS (name TEXT, value INTEGER);
             
             DROP TABLE IF EXISTS test_array_composite CASCADE;
             CREATE TABLE test_array_composite (
                 id INTEGER PRIMARY KEY,
                 items test_item[]
             );
             
             INSERT INTO test_array_composite VALUES 
             (1, ARRAY[ROW('Item1', 100)::test_item, ROW('Item2', 200)::test_item]);",
        )
        .await
        .unwrap();

    let table_id = get_table_id(&client, "test_array_composite").await;
    let replication_client = ReplicationClient::from_client(client);
    let schema = replication_client
        .get_table_schema(
            table_id,
            TableName {
                schema: "public".to_string(),
                name: "test_array_composite".to_string(),
            },
            /*publication=*/ None,
        )
        .await
        .unwrap();

    let items_col = schema
        .column_schemas
        .iter()
        .find(|c| c.name == "items")
        .unwrap();

    if let Kind::Array(element_type) = items_col.typ.kind() {
        if let Kind::Composite(fields) = element_type.kind() {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].name(), "name");
            assert_eq!(*fields[0].type_(), Type::TEXT);
            assert_eq!(fields[1].name(), "value");
            assert_eq!(*fields[1].type_(), Type::INT4);
        } else {
            panic!("Expected array element to be composite");
        }
    } else {
        panic!("Expected array type");
    }

    // Test parsing - arrays use curly braces and quotes for composite elements
    let test_value = r#"{"(Item1,100)","(Item2,200)"}"#;
    let result = TextFormatConverter::try_from_str(&items_col.typ, test_value);
    assert!(
        result.is_ok(),
        "Failed to parse array of composites: {:?}",
        result
    );
}

#[tokio::test]
#[serial]
async fn test_composite_with_nested_array() {
    let client = setup_connection().await;

    // Create composite type with array field
    client
        .simple_query(
            "DROP TYPE IF EXISTS test_skill CASCADE;
             DROP TYPE IF EXISTS test_person CASCADE;
             CREATE TYPE test_skill AS (name TEXT, level INTEGER);
             CREATE TYPE test_person AS (name TEXT, skills test_skill[]);
             
             DROP TABLE IF EXISTS test_nested_array CASCADE;
             CREATE TABLE test_nested_array (
                 id INTEGER PRIMARY KEY,
                 person test_person
             );
             
             INSERT INTO test_nested_array VALUES 
             (1, ROW('Alice', ARRAY[ROW('Python', 5)::test_skill, ROW('Rust', 3)::test_skill])::test_person);",
        )
        .await
        .unwrap();

    let table_id = get_table_id(&client, "test_nested_array").await;
    let replication_client = ReplicationClient::from_client(client);
    let schema = replication_client
        .get_table_schema(
            table_id,
            TableName {
                schema: "public".to_string(),
                name: "test_nested_array".to_string(),
            },
            /*publication=*/ None,
        )
        .await
        .unwrap();

    let person_col = schema
        .column_schemas
        .iter()
        .find(|c| c.name == "person")
        .unwrap();

    if let Kind::Composite(person_fields) = person_col.typ.kind() {
        let skills_field = person_fields.iter().find(|f| f.name() == "skills").unwrap();

        if let Kind::Array(skill_type) = skills_field.type_().kind() {
            if let Kind::Composite(skill_fields) = skill_type.kind() {
                assert_eq!(skill_fields.len(), 2);
                assert_eq!(*skill_fields[0].type_(), Type::TEXT);
                assert_eq!(*skill_fields[1].type_(), Type::INT4);
            } else {
                panic!("Expected skill array element to be composite");
            }
        } else {
            panic!("Expected skills to be array");
        }
    } else {
        panic!("Expected person to be composite");
    }

    // Test parsing - composite with nested array of composites
    // Format: outer composite uses parens, array uses braces with escaped quotes for composite elements
    let test_value = r#"(Alice,"{\"(Python,5)\",\"(Rust,3)\"}")"#;
    let result = TextFormatConverter::try_from_str(&person_col.typ, test_value);
    assert!(
        result.is_ok(),
        "Failed to parse composite with nested array: {:?}",
        result
    );
}

#[tokio::test]
#[serial]
async fn test_deeply_nested_composites() {
    let client = setup_connection().await;

    // Create deeply nested composite types (3+ levels)
    client
        .simple_query(
            "DROP TYPE IF EXISTS test_coords CASCADE;
             DROP TYPE IF EXISTS test_address CASCADE;
             DROP TYPE IF EXISTS test_location CASCADE;
             DROP TYPE IF EXISTS test_venue CASCADE;
             
             CREATE TYPE test_coords AS (lat FLOAT8, lon FLOAT8);
             CREATE TYPE test_address AS (street TEXT, coords test_coords);
             CREATE TYPE test_location AS (name TEXT, addr test_address);
             CREATE TYPE test_venue AS (id INTEGER, loc test_location);
             
             DROP TABLE IF EXISTS test_deep CASCADE;
             CREATE TABLE test_deep (
                 id INTEGER PRIMARY KEY,
                 venue test_venue
             );
             
             INSERT INTO test_deep VALUES 
             (1, ROW(100, ROW('Stadium', ROW('123 Main', ROW(40.7, -74.0)::test_coords)::test_address)::test_location)::test_venue);",
        )
        .await
        .unwrap();

    let table_id = get_table_id(&client, "test_deep").await;
    let replication_client = ReplicationClient::from_client(client);
    let schema = replication_client
        .get_table_schema(
            table_id,
            TableName {
                schema: "public".to_string(),
                name: "test_deep".to_string(),
            },
            /*publication=*/ None,
        )
        .await
        .unwrap();

    let venue_col = schema
        .column_schemas
        .iter()
        .find(|c| c.name == "venue")
        .unwrap();

    // Verify 4 levels of nesting
    if let Kind::Composite(venue_fields) = venue_col.typ.kind() {
        let loc_field = venue_fields.iter().find(|f| f.name() == "loc").unwrap();

        if let Kind::Composite(loc_fields) = loc_field.type_().kind() {
            let addr_field = loc_fields.iter().find(|f| f.name() == "addr").unwrap();

            if let Kind::Composite(addr_fields) = addr_field.type_().kind() {
                let coords_field = addr_fields.iter().find(|f| f.name() == "coords").unwrap();

                if let Kind::Composite(coords_fields) = coords_field.type_().kind() {
                    assert_eq!(coords_fields.len(), 2);
                    assert_eq!(*coords_fields[0].type_(), Type::FLOAT8);
                } else {
                    panic!("Expected coords to be composite");
                }
            } else {
                panic!("Expected addr to be composite");
            }
        } else {
            panic!("Expected loc to be composite");
        }
    } else {
        panic!("Expected venue to be composite");
    }

    // Test parsing - deeply nested composites require proper escaping
    // Each level of nesting doubles the quotes
    let test_value = r#"(100,"(Stadium,\"(123 Main,\\\"(40.7,-74.0)\\\")\")")"#;
    let result = TextFormatConverter::try_from_str(&venue_col.typ, test_value);
    assert!(
        result.is_ok(),
        "Failed to parse deeply nested composite: {:?}",
        result
    );
}

#[tokio::test]
#[serial]
async fn test_complex_mixed_nesting() {
    let client = setup_connection().await;

    // Test array of composites with nested arrays
    client
        .simple_query(
            "DROP TYPE IF EXISTS test_tag CASCADE;
             DROP TYPE IF EXISTS test_meta CASCADE;
             DROP TYPE IF EXISTS test_doc CASCADE;
             
             CREATE TYPE test_tag AS (name TEXT, score INTEGER);
             CREATE TYPE test_meta AS (tags test_tag[], author TEXT);
             CREATE TYPE test_doc AS (title TEXT, meta test_meta, refs INTEGER[]);
             
             DROP TABLE IF EXISTS test_mixed CASCADE;
             CREATE TABLE test_mixed (
                 id INTEGER PRIMARY KEY,
                 docs test_doc[]
             );
             
             INSERT INTO test_mixed VALUES 
             (1, ARRAY[
                 ROW('Doc1', ROW(ARRAY[ROW('tag1', 5)::test_tag], 'Alice')::test_meta, ARRAY[1,2])::test_doc
             ]);",
        )
        .await
        .unwrap();

    let table_id = get_table_id(&client, "test_mixed").await;
    let replication_client = ReplicationClient::from_client(client);
    let schema = replication_client
        .get_table_schema(
            table_id,
            TableName {
                schema: "public".to_string(),
                name: "test_mixed".to_string(),
            },
            /*publication=*/ None,
        )
        .await
        .unwrap();

    let docs_col = schema
        .column_schemas
        .iter()
        .find(|c| c.name == "docs")
        .unwrap();

    // Verify: Array[Composite[Composite[Array[Composite]], Array]]
    if let Kind::Array(doc_type) = docs_col.typ.kind() {
        if let Kind::Composite(doc_fields) = doc_type.kind() {
            let meta_field = doc_fields.iter().find(|f| f.name() == "meta").unwrap();

            if let Kind::Composite(meta_fields) = meta_field.type_().kind() {
                let tags_field = meta_fields.iter().find(|f| f.name() == "tags").unwrap();

                if let Kind::Array(tag_type) = tags_field.type_().kind() {
                    assert!(matches!(tag_type.kind(), Kind::Composite(_)));
                } else {
                    panic!("Expected tags to be array");
                }
            } else {
                panic!("Expected meta to be composite");
            }

            // Verify refs is a simple array
            let refs_field = doc_fields.iter().find(|f| f.name() == "refs").unwrap();
            assert!(matches!(refs_field.type_().kind(), Kind::Array(_)));
        } else {
            panic!("Expected doc array element to be composite");
        }
    } else {
        panic!("Expected docs to be array");
    }
}
