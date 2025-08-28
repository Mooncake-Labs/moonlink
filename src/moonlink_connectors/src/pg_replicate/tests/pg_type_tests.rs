#![cfg(feature = "connector-pg")]

use super::test_utils::{create_replication_client, setup_connection, TestResources};
use crate::pg_replicate::conversions::text::TextFormatConverter;
use crate::pg_replicate::table::TableName;
use serial_test::serial;
use tokio_postgres::types::{Kind, Type};

// Type constants
const TYPE_ADDRESS: &str = "test_address";
const TYPE_POINT: &str = "test_point";
const TYPE_LOCATION: &str = "test_location";
const TYPE_ITEM: &str = "test_item";
const TYPE_SKILL: &str = "test_skill";
const TYPE_PERSON: &str = "test_person";
const TYPE_COORDS: &str = "test_coords";
const TYPE_VENUE: &str = "test_venue";
const TYPE_TAG: &str = "test_tag";
const TYPE_META: &str = "test_meta";
const TYPE_DOC: &str = "test_doc";

// Table constants
const TABLE_BASIC_COMPOSITE: &str = "test_basic_composite";
const TABLE_NESTED: &str = "test_nested";
const TABLE_ARRAY_COMPOSITE: &str = "test_array_composite";
const TABLE_NESTED_ARRAY: &str = "test_nested_array";
const TABLE_DEEP: &str = "test_deep";
const TABLE_MIXED: &str = "test_mixed";

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_basic_composite_type() {
    let client = setup_connection().await;
    let mut resources = TestResources::new(client);

    // Create basic composite type
    resources
        .client()
        .simple_query(&format!(
            "DROP TYPE IF EXISTS {} CASCADE;
             CREATE TYPE {} AS (street TEXT, city TEXT, zip INTEGER);
             
             DROP TABLE IF EXISTS {} CASCADE;
             CREATE TABLE {} (
                 id INTEGER PRIMARY KEY,
                 addr {}
             );
             
             INSERT INTO {} VALUES 
             (1, ROW('123 Main St', 'NYC', 10001)::{});",
            TYPE_ADDRESS,
            TYPE_ADDRESS,
            TABLE_BASIC_COMPOSITE,
            TABLE_BASIC_COMPOSITE,
            TYPE_ADDRESS,
            TABLE_BASIC_COMPOSITE,
            TYPE_ADDRESS
        ))
        .await
        .unwrap();

    // Register resources for cleanup
    resources.add_type(TYPE_ADDRESS);
    resources.add_table(TABLE_BASIC_COMPOSITE);

    let table_id = get_table_id(resources.client(), TABLE_BASIC_COMPOSITE).await;
    let table_name = TableName {
        schema: "public".to_string(),
        name: TABLE_BASIC_COMPOSITE.to_string(),
    };

    let replication_client = create_replication_client().await;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_nested_composite_types() {
    let client = setup_connection().await;
    let mut resources = TestResources::new(client);

    // Create nested composite types
    resources
        .client()
        .simple_query(&format!(
            "DROP TYPE IF EXISTS {} CASCADE;
             DROP TYPE IF EXISTS {} CASCADE;
             CREATE TYPE {} AS (x FLOAT8, y FLOAT8);
             CREATE TYPE {} AS (name TEXT, point {});
             
             DROP TABLE IF EXISTS {} CASCADE;
             CREATE TABLE {} (
                 id INTEGER PRIMARY KEY,
                 loc {}
             );
             
             INSERT INTO {} VALUES 
             (1, ROW('Home', ROW(1.5, 2.5)::{})::{});",
            TYPE_POINT,
            TYPE_LOCATION,
            TYPE_POINT,
            TYPE_LOCATION,
            TYPE_POINT,
            TABLE_NESTED,
            TABLE_NESTED,
            TYPE_LOCATION,
            TABLE_NESTED,
            TYPE_POINT,
            TYPE_LOCATION
        ))
        .await
        .unwrap();

    // Register resources for cleanup
    resources.add_type(TYPE_POINT);
    resources.add_type(TYPE_LOCATION);
    resources.add_table(TABLE_NESTED);

    let table_id = get_table_id(resources.client(), TABLE_NESTED).await;
    let replication_client = create_replication_client().await;
    let schema = replication_client
        .get_table_schema(
            table_id,
            TableName {
                schema: "public".to_string(),
                name: TABLE_NESTED.to_string(),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_array_of_composite_types() {
    let client = setup_connection().await;
    let mut resources = TestResources::new(client);

    // Create composite type for array testing
    resources
        .client()
        .simple_query(&format!(
            "DROP TYPE IF EXISTS {} CASCADE;
             CREATE TYPE {} AS (name TEXT, value INTEGER);
             
             DROP TABLE IF EXISTS {} CASCADE;
             CREATE TABLE {} (
                 id INTEGER PRIMARY KEY,
                 items {}[]
             );
             
             INSERT INTO {} VALUES 
             (1, ARRAY[ROW('Item1', 100)::{}, ROW('Item2', 200)::{}]);",
            TYPE_ITEM,
            TYPE_ITEM,
            TABLE_ARRAY_COMPOSITE,
            TABLE_ARRAY_COMPOSITE,
            TYPE_ITEM,
            TABLE_ARRAY_COMPOSITE,
            TYPE_ITEM,
            TYPE_ITEM
        ))
        .await
        .unwrap();

    // Register resources for cleanup
    resources.add_type(TYPE_ITEM);
    resources.add_table(TABLE_ARRAY_COMPOSITE);

    let table_id = get_table_id(resources.client(), TABLE_ARRAY_COMPOSITE).await;
    let replication_client = create_replication_client().await;
    let schema = replication_client
        .get_table_schema(
            table_id,
            TableName {
                schema: "public".to_string(),
                name: TABLE_ARRAY_COMPOSITE.to_string(),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_composite_with_nested_array() {
    let client = setup_connection().await;
    let mut resources = TestResources::new(client);

    // Create composite type with array field
    resources
        .client()
        .simple_query(&format!(
            "DROP TYPE IF EXISTS {} CASCADE;
             DROP TYPE IF EXISTS {} CASCADE;
             CREATE TYPE {} AS (name TEXT, level INTEGER);
             CREATE TYPE {} AS (name TEXT, skills {}[]);
             
             DROP TABLE IF EXISTS {} CASCADE;
             CREATE TABLE {} (
                 id INTEGER PRIMARY KEY,
                 person {}
             );
             
             INSERT INTO {} VALUES 
             (1, ROW('Alice', ARRAY[ROW('Python', 5)::{}, ROW('Rust', 3)::{}])::{});",
            TYPE_SKILL,
            TYPE_PERSON,
            TYPE_SKILL,
            TYPE_PERSON,
            TYPE_SKILL,
            TABLE_NESTED_ARRAY,
            TABLE_NESTED_ARRAY,
            TYPE_PERSON,
            TABLE_NESTED_ARRAY,
            TYPE_SKILL,
            TYPE_SKILL,
            TYPE_PERSON
        ))
        .await
        .unwrap();

    // Register resources for cleanup
    resources.add_type(TYPE_SKILL);
    resources.add_type(TYPE_PERSON);
    resources.add_table(TABLE_NESTED_ARRAY);

    let table_id = get_table_id(resources.client(), TABLE_NESTED_ARRAY).await;
    let replication_client = create_replication_client().await;
    let schema = replication_client
        .get_table_schema(
            table_id,
            TableName {
                schema: "public".to_string(),
                name: TABLE_NESTED_ARRAY.to_string(),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_deeply_nested_composites() {
    let client = setup_connection().await;
    let mut resources = TestResources::new(client);

    // Create deeply nested composite types (3+ levels)
    resources
        .client()
        .simple_query(&format!(
            "DROP TYPE IF EXISTS {} CASCADE;
             DROP TYPE IF EXISTS {} CASCADE;
             DROP TYPE IF EXISTS {} CASCADE;
             DROP TYPE IF EXISTS {} CASCADE;
             
             CREATE TYPE {} AS (lat FLOAT8, lon FLOAT8);
             CREATE TYPE {} AS (street TEXT, coords {});
             CREATE TYPE {} AS (name TEXT, addr {});
             CREATE TYPE {} AS (id INTEGER, loc {});
             
             DROP TABLE IF EXISTS {} CASCADE;
             CREATE TABLE {} (
                 id INTEGER PRIMARY KEY,
                 venue {}
             );
             
             INSERT INTO {} VALUES 
             (1, ROW(100, ROW('Stadium', ROW('123 Main', ROW(40.7, -74.0)::{})::{})::{})::{});",
            TYPE_COORDS,
            TYPE_ADDRESS,
            TYPE_LOCATION,
            TYPE_VENUE,
            TYPE_COORDS,
            TYPE_ADDRESS,
            TYPE_COORDS,
            TYPE_LOCATION,
            TYPE_ADDRESS,
            TYPE_VENUE,
            TYPE_LOCATION,
            TABLE_DEEP,
            TABLE_DEEP,
            TYPE_VENUE,
            TABLE_DEEP,
            TYPE_COORDS,
            TYPE_ADDRESS,
            TYPE_LOCATION,
            TYPE_VENUE
        ))
        .await
        .unwrap();

    // Register resources for cleanup
    resources.add_type(TYPE_COORDS);
    resources.add_type(TYPE_ADDRESS);
    resources.add_type(TYPE_LOCATION);
    resources.add_type(TYPE_VENUE);
    resources.add_table(TABLE_DEEP);

    let table_id = get_table_id(resources.client(), TABLE_DEEP).await;
    let replication_client = create_replication_client().await;
    let schema = replication_client
        .get_table_schema(
            table_id,
            TableName {
                schema: "public".to_string(),
                name: TABLE_DEEP.to_string(),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_complex_mixed_nesting() {
    let client = setup_connection().await;
    let mut resources = TestResources::new(client);

    // Test array of composites with nested arrays
    resources
        .client()
        .simple_query(&format!(
            "DROP TYPE IF EXISTS {} CASCADE;
             DROP TYPE IF EXISTS {} CASCADE;
             DROP TYPE IF EXISTS {} CASCADE;
             
             CREATE TYPE {} AS (name TEXT, score INTEGER);
             CREATE TYPE {} AS (tags {}[], author TEXT);
             CREATE TYPE {} AS (title TEXT, meta {}, refs INTEGER[]);
             
             DROP TABLE IF EXISTS {} CASCADE;
             CREATE TABLE {} (
                 id INTEGER PRIMARY KEY,
                 docs {}[]
             );
             
             INSERT INTO {} VALUES 
             (1, ARRAY[
                 ROW('Doc1', ROW(ARRAY[ROW('tag1', 5)::{}], 'Alice')::{}, ARRAY[1,2])::{}
             ]);",
            TYPE_TAG,
            TYPE_META,
            TYPE_DOC,
            TYPE_TAG,
            TYPE_META,
            TYPE_TAG,
            TYPE_DOC,
            TYPE_META,
            TABLE_MIXED,
            TABLE_MIXED,
            TYPE_DOC,
            TABLE_MIXED,
            TYPE_TAG,
            TYPE_META,
            TYPE_DOC
        ))
        .await
        .unwrap();

    // Register resources for cleanup
    resources.add_type(TYPE_TAG);
    resources.add_type(TYPE_META);
    resources.add_type(TYPE_DOC);
    resources.add_table(TABLE_MIXED);

    let table_id = get_table_id(resources.client(), TABLE_MIXED).await;
    let replication_client = create_replication_client().await;
    let schema = replication_client
        .get_table_schema(
            table_id,
            TableName {
                schema: "public".to_string(),
                name: TABLE_MIXED.to_string(),
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
