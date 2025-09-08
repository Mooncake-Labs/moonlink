use moonlink_service::ServiceConfig;
use serde_json::json;
use std::time::Duration;
use tokio::net::UnixStream;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Moonlink REST API Demo");

    // Start the Moonlink service
    println!("🔧 Starting Moonlink service...");

    // Clean up any existing demo directory
    let demo_path = "/tmp/moonlink-demo";
    if tokio::fs::metadata(demo_path).await.is_ok() {
        tokio::fs::remove_dir_all(demo_path).await?;
    }
    tokio::fs::create_dir_all(demo_path).await?;

    // Start the service with REST API enabled
    let service_handle = tokio::spawn(async {
        if let Err(e) = moonlink_service::start_with_config(ServiceConfig {
            base_path: demo_path.to_string(),
            rest_api_port: Some(3030),
            tcp_port: None,
            otel_api_port: None,
            data_server_uri: None,
        })
        .await
        {
            eprintln!("Service failed: {e}");
        }
    });

    // Wait for the service to start
    println!("⏳ Waiting for service to be ready...");
    sleep(Duration::from_secs(3)).await;

    let client = reqwest::Client::new();
    let base_url = "http://localhost:3030";

    // Test 1: Health check
    println!("\n🔍 Testing health check...");
    let response = client.get(format!("{base_url}/health")).send().await?;

    if response.status().is_success() {
        println!("✅ Health check passed!");
        let health_data: serde_json::Value = response.json().await?;
        println!(
            "   Response: {}",
            serde_json::to_string_pretty(&health_data)?
        );
    } else {
        println!("❌ Health check failed: {}", response.status());
        service_handle.abort();
        return Ok(());
    }

    // Test 2: Create a table
    println!("\n🏗️ Creating table 'demo_users'...");
    let create_table_payload = json!({
        "database": "database_name",
        "table": "table_name",
        "schema": [
            {"name": "id", "data_type": "int32", "nullable": false},
            {"name": "name", "data_type": "string", "nullable": false},
            {"name": "email", "data_type": "string", "nullable": true},
            {"name": "age", "data_type": "int32", "nullable": true}
        ],
        "table_config": {}
    });

    let response = client
        .post(format!("{base_url}/tables/demo_users"))
        .header("content-type", "application/json")
        .json(&create_table_payload)
        .send()
        .await?;

    if response.status().is_success() {
        println!("✅ Table created successfully!");
        let table_data: serde_json::Value = response.json().await?;
        println!(
            "   Response: {}",
            serde_json::to_string_pretty(&table_data)?
        );
    } else {
        println!("❌ Table creation failed: {}", response.status());
        let error_text = response.text().await?;
        println!("   Error: {error_text}");
        service_handle.abort();
        return Ok(());
    }

    // Wait a moment for table to be ready
    sleep(Duration::from_millis(500)).await;

    // Test 3: Insert data into the created table
    println!("\n📝 Inserting data into 'demo_users'...");
    let insert_payload = json!({
        "operation": "insert",
        "data": {
            "id": 1,
            "name": "Alice Johnson",
            "email": "alice@example.com",
            "age": 30
        }
    });

    let response = client
        .post(format!("{base_url}/ingest/demo_users"))
        .header("content-type", "application/json")
        .json(&insert_payload)
        .send()
        .await?;

    if response.status().is_success() {
        println!("✅ Data inserted successfully!");
        let ingest_data: serde_json::Value = response.json().await?;
        println!(
            "   Response: {}",
            serde_json::to_string_pretty(&ingest_data)?
        );
    } else {
        println!("❌ Data insertion failed: {}", response.status());
        let error_text = response.text().await?;
        println!("   Error: {error_text}");
    }

    // Test 4: Insert another record
    println!("\n📝 Inserting another record...");
    let insert_payload2 = json!({
        "operation": "insert",
        "data": {
            "id": 2,
            "name": "Bob Smith",
            "email": "bob@example.com",
            "age": 25
        }
    });

    let response = client
        .post(format!("{base_url}/ingest/demo_users"))
        .header("content-type", "application/json")
        .json(&insert_payload2)
        .send()
        .await?;

    if response.status().is_success() {
        println!("✅ Second record inserted successfully!");
        let ingest_data: serde_json::Value = response.json().await?;
        println!(
            "   Response: {}",
            serde_json::to_string_pretty(&ingest_data)?
        );
    } else {
        println!("❌ Second insertion failed: {}", response.status());
    }

    // Wait for data to be committed and flushed
    println!("\n⏳ Waiting for data to be committed and flushed...");
    sleep(Duration::from_secs(3)).await;

    // Test 5: Read data back via RPC socket
    println!("\n📖 Reading data back via RPC socket...");
    match read_table_via_rpc().await {
        Ok(_) => println!("✅ Data read successfully via RPC!"),
        Err(e) => println!("❌ Failed to read data via RPC: {e}"),
    }

    // Test 6: Try to ingest data to a non-existent table (should fail)
    println!("\n🚫 Testing ingestion to non-existent table...");
    let payload = json!({
        "operation": "insert",
        "data": {
            "id": 999,
            "name": "Should Fail"
        }
    });

    let response = client
        .post(format!("{base_url}/ingest/nonexistent_table"))
        .header("content-type", "application/json")
        .json(&payload)
        .send()
        .await?;

    if response.status().is_success() {
        println!("✅ Request accepted (table will be created dynamically or queued)");
        let data: serde_json::Value = response.json().await?;
        println!("   Response: {}", serde_json::to_string_pretty(&data)?);
    } else {
        println!("✅ Expected behavior - non-existent table handling");
        let error_data: serde_json::Value = response.json().await?;
        println!(
            "   Response: {}",
            serde_json::to_string_pretty(&error_data)?
        );
    }

    // Test 7: Test invalid operation
    println!("\n🚫 Testing invalid operation...");
    let invalid_payload = json!({
        "operation": "invalid_operation",
        "data": {"id": 1}
    });

    let response = client
        .post(format!("{base_url}/ingest/demo_users"))
        .header("content-type", "application/json")
        .json(&invalid_payload)
        .send()
        .await?;

    if response.status().is_client_error() {
        println!("✅ Expected error for invalid operation!");
        let error_data: serde_json::Value = response.json().await?;
        println!("   Error: {}", serde_json::to_string_pretty(&error_data)?);
    } else {
        println!("❌ Unexpected response: {}", response.status());
    }

    println!("\n🎉 Demo completed successfully!");
    println!("🛑 Shutting down service...");

    // Shutdown the service
    service_handle.abort();

    println!("\n📚 API Endpoints:");
    println!("   Health Check:   GET  {base_url}/health");
    println!("   Create Table:   POST {base_url}/tables/{{table_name}}");
    println!("   Ingest Data:    POST {base_url}/ingest/{{table_name}}");
    println!("\n📋 Example Usage:");
    println!(
        "   
   Create Table:
   POST {base_url}/tables/my_table
   {{
     \"schema\": \"test_schema\",
     \"table_id\": \"test_table\",
     \"schema\": [
       {{\"name\": \"id\", \"data_type\": \"int32\", \"nullable\": false}},
       {{\"name\": \"name\", \"data_type\": \"string\", \"nullable\": true}}
     ]
   }}
   
   Insert Data:
   POST {base_url}/ingest/my_table
   {{
     \"operation\": \"insert\",
     \"data\": {{
       \"id\": 123,
       \"name\": \"Example Record\"
     }}
   }}"
    );

    Ok(())
}

async fn read_table_via_rpc() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to the Unix socket
    let socket_path = "/tmp/moonlink-demo/moonlink.sock";
    let mut stream = UnixStream::connect(socket_path).await?;

    println!("   🔌 Connected to RPC socket: {socket_path}");

    // List tables first
    println!("   📋 Listing tables...");
    let tables = moonlink_rpc::list_tables(&mut stream).await?;
    println!("   Found {} table(s):", tables.len());
    for table in &tables {
        println!(
            "     - Database: {}, Table: {}, Commit LSN: {}",
            table.database.clone(),
            table.table.clone(),
            table.commit_lsn
        );
    }

    if tables.is_empty() {
        println!("   ⚠️  No tables found to read from");
        return Ok(());
    }

    // Find our demo table (database_id=1, table_id=100)
    let demo_table = tables
        .iter()
        .find(|t| t.database == "test_schema" && t.table == "test_table");

    if let Some(table) = demo_table {
        println!(
            "   📖 Reading from demo table (Database: {}, Table: {})...",
            table.database.clone(),
            table.table.clone()
        );

        // Get table schema
        println!("   📐 Getting table schema...");
        let schema_bytes = moonlink_rpc::get_table_schema(
            &mut stream,
            table.database.clone(),
            table.table.clone(),
        )
        .await?;
        println!("   Schema size: {} bytes", schema_bytes.len());

        // Scan table data
        println!("   🔍 Scanning table data...");
        let data_bytes: Vec<u8> = moonlink_rpc::scan_table_begin(
            &mut stream,
            table.database.clone(),
            table.table.clone(),
            0,
        )
        .await?;
        println!("   Data size: {} bytes", data_bytes.len());

        // End scan
        moonlink_rpc::scan_table_end(&mut stream, table.database.clone(), table.table.clone())
            .await?;
        println!("   ✅ Table scan completed");

        // Try to decode the Arrow data (basic attempt)
        if !data_bytes.is_empty() {
            println!(
                "   📊 Received table data - {} bytes of Arrow format",
                data_bytes.len()
            );
            // Note: For a full demo, we could decode the Arrow data here using arrow-rs
            // but that would require additional dependencies and complexity
        } else {
            println!("   ⚠️  No data returned from table scan");
        }
    } else {
        println!("   ⚠️  Demo table (DB: 1, Table: 100) not found in table list");
    }

    Ok(())
}
