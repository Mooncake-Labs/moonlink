<div align="center">

# Moonlink 🥮
managed ingestion engine for Apache Iceberg

[![License](https://img.shields.io/badge/License-BSL-blue)](https://github.com/Mooncake-Labs/moonlink/blob/main/LICENSE)
[![Slack](https://img.shields.io/badge/Mooncake%20Devs-purple?logo=slack)](https://join.slack.com/t/mooncake-devs/shared_invite/zt-2sepjh5hv-rb9jUtfYZ9bvbxTCUrsEEA)
[![Twitter](https://img.shields.io/twitter/url?url=https%3A%2F%2Fx.com%2Fmooncakelabs&label=%40mooncakelabs)](https://x.com/mooncakelabs)
[![Docs](https://img.shields.io/badge/docs-moonlink?style=flat&logo=readthedocs&logoColor=white)](https://docs.mooncake.dev/moonlink/intro)

</div>

<div align="left">

## Overview

Moonlink is an Iceberg-native ingestion engine bringing streaming inserts and upserts to your lakehouse.

Ingest Postgres CDC, event streams (Kafka), and OTEL into Iceberg **without** complex maintenance and compaction. 

Moonlink buffers, caches, and indexes data so Iceberg tables stay read-optimized.

```

             ┌──────────moonlink───────────┐                         
             │  ┌───────────────────────┐  │  ┌───────Iceberg───────┐
             │  │                       │  │  │         s3          │
Postgres ───►│  │┌ ─ ─ ─ ─ ┐ ┌ ─ ─ ─ ─ ┐│  │  │┌───────┐ ┌─────────┐│
             │  │                       │  │  ││       │ │         ││
Kafka    ───►│  ││  index  │ │  cache  ││  ├──►│─index │ │ parquet ││
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

**Inserts** are buffered and flushed as size-tuned Parquet:

```
          ┌───moonlink───┐  ┌────iceberg───┐
          │┌─ ─ ─ ─ ─ ─ ┐│  │┌─ ─ ─ ─ ─ ─ ┐│
raw insert│              │  │              │
────────► ││   Arrow    │├─►││   Parquet  ││
          │              │  │              │
          │└─ ─ ─ ─ ─ ─ ┘│  │└─ ─ ─ ─ ─ ─ ┘│
          └──────────────┘  └──────────────┘
```

**Deletes** are indexed and mapped to deletion vectors:

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

1. **DuckDB** — with the Mooncake extension  
2. **Postgres** — with the `pg_mooncake`.

 
## Quick Start

Prereqs: Rust (cargo) and Docker.

1) Clone & build the service binary
```bash
git clone https://github.com/Mooncake-Labs/moonlink.git
cd moonlink
cargo build --release --bin moonlink_service
```

2) Start nginx to serve your data dir

Pick a local folder where Moonlink will store table data. Use the same folder for Moonlink and nginx:
```bash
mkdir -p ~/data/mooncake
docker run -d --name nginx-read-local-ssd \
  -p 8080:80 \
  -v ~/data/mooncake:/usr/share/nginx/html:ro \
  --restart unless-stopped \
  nginx:alpine

# Optional: verify nginx is serving the directory
curl -I http://localhost:8080/ | head -n 1
```

3) Run Moonlink

Point Moonlink at the same data dir and the nginx endpoint:
```bash
cargo run --release --bin moonlink_service -- \
  ~/data/mooncake \
  --data-server-uri http://localhost:8080
```

4) Verify
You should see logs like:
```text
Moonlink service started successfully
RPC server listening on TCP: 0.0.0.0:3031
RPC server listening on Unix socket: ".../moonlink.sock"
Starting REST API server on 0.0.0.0:3030
```

For advanced configuration, see the [docs](https://docs.mooncake.dev/moonlink/intro).


## Roadmap and Contributing
Roadmap (near‑term):
1. Kafka sink preview
2. Schema evolution from Postgres and Kafka
3. Catalog integrations (AWS Glue, Unity Catalog)
4. REST API stabilization (Insert, Upsert into Iceberg directly)

We’re grateful for our contributors. If you'd like to help improve Moonlink, join our [community](https://join.slack.com/t/mooncake-devs/shared_invite/zt-2sepjh5hv-rb9jUtfYZ9bvbxTCUrsEEA).

🥮