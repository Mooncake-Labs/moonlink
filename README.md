<div align="center">

# Moonlink 🥮
Postgres-to-Iceberg mirroring engine for pg_mooncake

[![License](https://img.shields.io/badge/License-BSL-blue)](https://github.com/Mooncake-Labs/moonlink/blob/main/LICENSE)
[![Slack](https://img.shields.io/badge/Mooncake%20Devs-purple?logo=slack)](https://join.slack.com/t/mooncake-devs/shared_invite/zt-2sepjh5hv-rb9jUtfYZ9bvbxTCUrsEEA)
[![Twitter](https://img.shields.io/twitter/url?url=https%3A%2F%2Fx.com%2Fmooncakelabs&label=%40mooncakelabs)](https://x.com/mooncakelabs)
[![Docs](https://img.shields.io/badge/docs-moonlink?style=flat&logo=readthedocs&logoColor=white)](https://docs.mooncake.dev/moonlink/intro)

</div>

## Overview

Moonlink is the engine behind [`pg_mooncake`](https://github.com/Mooncake-Labs/pg_mooncake): it mirrors Postgres tables into Iceberg (columnstore) tables via logical replication, **without** separate maintenance and compaction jobs.

It runs inside a Postgres background worker started by pg_mooncake and is controlled over a unix socket (`moonlink_rpc`). Moonlink buffers, caches, and indexes data so Iceberg tables stay read-optimized.

```

             ┌──────────moonlink───────────┐                         
             │  ┌───────────────────────┐  │  ┌───────Iceberg───────┐
             │  │                       │  │  │      obj. store     │
             │  │┌ ─ ─ ─ ─ ┐ ┌ ─ ─ ─ ─ ┐│  │  │┌───────┐ ┌─────────┐│
             │  │                       │  │  ││       │ │         ││
Postgres ───►│  ││  index  │ │  cache  ││  ├──►│ index │ │ parquet ││
             │  │                       │  │  ││       │ │         ││
             │  │└ ─ ─ ─ ─ ┘ └ ─ ─ ─ ─ ┘│  │  │└───────┘ └─────────┘│
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

## Write Path

**PostgreSQL CDC**: tables are initially copied, then kept in sync via logical replication with millisecond-level latency. The source Postgres needs `wal_level = logical`.

## Read Path

Moonlink commits data as **Iceberg v3 tables with deletion vectors** (filesystem catalog), which can be queried from any Iceberg-compatible engine.

For sub-second freshness, readers ask moonlink for the current table state over the unix socket:

1. **Postgres**: with the [`pg_mooncake`](https://github.com/Mooncake-Labs/pg_mooncake) extension.
2. **DuckDB**: with the [`duckdb_mooncake`](https://github.com/Mooncake-Labs/duckdb_mooncake) extension (also used by pg_mooncake).

## Embedding

pg_mooncake starts moonlink from its background worker:

```rust
moonlink_service::start_with_config(moonlink_service::ServiceConfig {
    base_path: "pg_mooncake".to_owned(),
})
.await
```

The service listens on `{base_path}/moonlink.sock` and stores tables, caches and its SQLite metadata under `base_path`. Logs go to stdout (the Postgres server log).

The RPC wire format (`src/moonlink_rpc`, and the `ScanTableBegin` payload in `src/moonlink_table_metadata`) is shared with prebuilt duckdb_mooncake binaries that pin `moonlink_rpc` by git, so it is append-only. `tests/wire_golden.rs` in both crates holds byte-level goldens.

## Development

```bash
cargo build
cargo clippy --all-targets --all-features -- -D warnings
cargo nextest run --lib --workspace              # unit tests

# Postgres-backed tests need a Postgres with wal_level=logical.
# They share one replication slot, so run them with cargo test (one process), not nextest.
export DATABASE_URL=postgresql://<user>:<password>@localhost:5432/postgres
cargo test -p moonlink_connectors --lib --features connector-pg
cargo test -p moonlink_backend --test '*'
cargo test -p moonlink_service --lib --features postgres-integration
```

## Contributing

We’re grateful for our contributors. If you'd like to help improve Moonlink, join our [community](https://join.slack.com/t/mooncake-devs/shared_invite/zt-2sepjh5hv-rb9jUtfYZ9bvbxTCUrsEEA).

🥮
