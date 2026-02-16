<div align="center">

# Moonlink 🥮
managed ingestion engine for Apache Iceberg

[![License](https://img.shields.io/badge/License-BSL-blue)](https://github.com/Mooncake-Labs/moonlink/blob/main/LICENSE)
[![Slack](https://img.shields.io/badge/Mooncake%20Devs-purple?logo=slack)](https://join.slack.com/t/mooncake-devs/shared_invite/zt-2sepjh5hv-rb9jUtfYZ9bvbxTCUrsEEA)
[![Twitter](https://img.shields.io/twitter/url?url=https%3A%2F%2Fx.com%2Fmooncakelabs&label=%40mooncakelabs)](https://x.com/mooncakelabs)
[![Docs](https://img.shields.io/badge/docs-moonlink?style=flat&logo=readthedocs&logoColor=white)](https://docs.mooncake.dev/moonlink/intro)

</div>

## Overview

Moonlink is an Iceberg-native ingestion engine bringing streaming inserts and upserts to your lakehouse.

Ingest Postgres CDC, event streams (Kafka), and OTEL into Iceberg **without** complex maintenance and compaction. 

Moonlink buffers, caches, and indexes data so Iceberg tables stay read-optimized.

```

             ┌──────────moonlink───────────┐                         
             │  ┌───────────────────────┐  │  ┌───────Iceberg───────┐
             │  │                       │  │  │      obj. store     │
Postgres ───►│  │┌ ─ ─ ─ ─ ┐ ┌ ─ ─ ─ ─ ┐│  │  │┌───────┐ ┌─────────┐│
             │  │                       │  │  ││       │ │         ││
Kafka    ───►│  ││  index  │ │  cache  ││  ├──►│ index │ │ parquet ││
             │  │                       │  │  ││       │ │         ││
Events   ───►│  │└ ─ ─ ─ ─ ┘ └ ─ ─ ─ ─ ┘│  │  │└───────┘ └─────────┘│
             │  │                  nvme │  │  │                     │
             │  └───────────────────────┘  │  └─────────────────────┘
             └─────────────────────────────┘                         
```

> **Note:** Moonlink is in preview. Expect changes. Join our [Community](https://join.slack.com/t/mooncakelabs/shared_invite/zt-2sepjh5hv-rb9jUtfYZ9bvbxTCUrsEEA) to stay updated!

## Why Moonlink?

Traditional ingestion tools write data and metadata files per update into Iceberg. That's fine for slow-changing data, but on real-time streams it causes:

- **Tiny data files** — frequent commits create thousands of small Parquet files  
- **Metadata explosion** — equality-deletes compound this problem

which leads to:
- **Slow read performance** — query planning overhead scales with file count
- **Manual maintenance** — periodic Spark jobs for compaction/cleanup

Moonlink minimizes write amplification and metadata churn by buffering incoming data, building indexes and caches on NVMe, and committing read-optimized files and deletion vectors to Iceberg.

**Inserts** are buffered and flushed as size-tuned Parquet

```
          ┌───moonlink───┐  ┌────iceberg───┐
          │┌─ ─ ─ ─ ─ ─ ┐│  │┌─ ─ ─ ─ ─ ─ ┐│
raw insert│              │  │              │
────────► ││   Arrow    │├─►││   Parquet  ││
          │              │  │              │
          │└─ ─ ─ ─ ─ ─ ┘│  │└─ ─ ─ ─ ─ ─ ┘│
          └──────────────┘  └──────────────┘
```

**Deletes** are mapped to deletion vectors using an index  built on row positions

```
           ┌───moonlink───┐   ┌────iceberg───┐
           │┌─ ─ ─── ─ ─ ┐│   │┌─ ─ ─ ─ ─ ─ ┐│
raw deletes│              │   │              │
  ────────►││   index    │├──►││  deletion  ││
           │              │   │   vectors    │
           │└─ ─ ─── ─ ─ ┘│   │└─ ─ ─ ─ ─ ─ ┘│
           └──────────────┘   └──────────────┘
```

## Write Paths

Moonlink supports multiple input sources for ingest:

1. **PostgreSQL CDC** — ingest via logical replication with millisecond-level latency  
2. **REST API** — simple HTTP endpoint for direct event ingestion  
3. **Kafka** — sink support coming soon  
4. **OTEL** — sink support on the roadmap  

## Read Path

Moonlink commits data as **Iceberg v3 tables with deletion vectors**. These tables can be queried from any Iceberg-compatible engine.

**Engines**
1. **DuckDB**   
2. **Apache Spark**
3. **Postgres** with `pg_duckdb` or  `pg_mooncake`

**Catalogs**
1. **AWS Glue** — coming soon  
2. **Unity Catalog** — coming soon  

---

### Real-Time Reads (<s freshness)

For workloads requiring sub-second visibility into new data, Moonlink supports real-time querying:

1. **DuckDB** — with the [`duckdb_mooncake`](https://github.com/Mooncake-Labs/duckdb_mooncake) extension.
2. **Postgres** — with the [`pg_mooncake`](https://github.com/Mooncake-Labs/pg_mooncake) extension.
3. **DataFusion** – with [`Moonlink Datafusion`](https://github.com/Mooncake-Labs/moonlink/tree/main/src/moonlink_datafusion)

 
## Quick Start

### 1. Clone & Build

Clone the repository and build the service binary:

```bash
git clone https://github.com/Mooncake-Labs/moonlink.git
cd moonlink
cargo build --release --bin moonlink_service
```

## 2. Start the Moonlink Service

Start the Moonlink service, which will store data in the `./data` directory:

```bash
./target/release/moonlink_service ./data
```

### 3. Verify Service Health

Check that the service is running properly:

```bash
curl http://localhost:3030/health
```

### 4. Create a Table

Create a table with a defined schema. Here's an example creating a `users` table:

```bash
curl -X POST http://localhost:3030/tables/users \
  -H "Content-Type: application/json" \
  -d '{
    "database": "my_database",
    "table": "users",
    "schema": [
      {"name": "id", "data_type": "int32", "nullable": false},
      {"name": "name", "data_type": "string", "nullable": false},
      {"name": "email", "data_type": "string", "nullable": true},
      {"name": "age", "data_type": "int32", "nullable": true},
      {"name": "created_at", "data_type": "date32", "nullable": true}
    ],
    "table_config": {"mooncake": {"append_only": true}}
  }'
```

### 5. Insert Data

Insert data into the created table:

```bash
curl -X POST http://localhost:3030/ingest/users \
  -H "Content-Type: application/json" \
  -d '{
    "operation": "insert",
    "request_mode": "async",
    "data": {
      "id": 1,
      "name": "Alice Johnson",
      "email": "alice@example.com",
      "age": 30,
      "created_at": "2024-01-01"
    }
  }'
```

## Roadmap and Contributing
Roadmap (near‑term):
1. Kafka sink preview
2. Schema evolution from Postgres and Kafka
3. Catalog integrations (AWS Glue, Unity Catalog)
4. REST API stabilization (Insert, Upsert into Iceberg directly)

We’re grateful for our contributors. If you'd like to help improve Moonlink, join our [community](https://join.slack.com/t/mooncake-devs/shared_invite/zt-2sepjh5hv-rb9jUtfYZ9bvbxTCUrsEEA).

🥮
