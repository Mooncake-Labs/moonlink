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

Mirroring a table sets `REPLICA IDENTITY FULL` on the source table, so updates carry full old rows, including unchanged TOASTed columns.

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

The RPC wire format (`src/moonlink_rpc`, and the `ScanTableBegin` payload in `src/moonlink_table_metadata`) is shared with prebuilt duckdb_mooncake binaries that pin `moonlink_rpc` by git, so it is append-only. `tests/wire_golden.rs` in both crates holds byte-level goldens; CI runs them in the `wire_format` job.

## Development

```bash
cargo build
cargo clippy --all-targets --all-features -- -D warnings
cargo nextest run --lib --workspace              # unit tests
# Wire-format goldens live under tests/ (not covered by --lib).
cargo test -p moonlink_rpc -p moonlink_table_metadata --tests
cargo test -p moonlink --test scan_payload_order

# Postgres-backed tests need a Postgres with wal_level=logical.
# They share one replication slot, so run them with cargo test (one process), not nextest.
export DATABASE_URL=postgresql://<user>:<password>@localhost:5432/postgres
cargo test -p moonlink_connectors --lib --features connector-pg
cargo test -p moonlink_backend --test '*'
cargo test -p moonlink_service --lib --features postgres-integration
```

## Durability and Recovery

Moonlink acknowledges the Postgres replication slot only up to what is persisted in Iceberg, and replays the rest from the slot after a restart. Expect Postgres to retain more `pg_wal`: changes stay in it until they reach Iceberg (at most about 5 minutes for idle tables, which get a forced snapshot every 300 s).

After upgrading, or after a crash, if a mirror looks wrong, drop it and re-create it (`CALL mooncake.create_table(...)` again). Old `_wal` directories from earlier versions are unused and can be deleted.

## What mirrors do not follow

A mirror that can no longer follow its source table enters the errored state: queries on it fail with a message that names the reason, every other mirror keeps replicating, and the remedy is always to drop the mirror and then create it again (`CALL mooncake.create_table(...)`); creating a new mirror of the same table while the errored one still exists is refused. An errored mirror stays errored across restarts.

- **Schema changes.** Any change to the source table's columns (add, drop, rename, change type, drop and re-add, add a generated column) or setting its replica identity to anything but `FULL` errors the mirror, also when made while moonlink is stopped.
- **Not detected:** `ALTER TABLE ... ALTER COLUMN ... TYPE <same type> USING <expression>` rewrites every row without replicating the change. Re-create the mirror after running one.
- **`TRUNCATE`** of the source table errors the mirror.
- **Partitioned tables** need a primary key. A new partition is followed if it is still empty when moonlink first sees it, so create partitions ahead of use. A partition that already holds rows when it is added (attached, or created and filled at once), and detaching (even to attach it again) or dropping any partition (including routine retention), error the mirror. An `UPDATE` or `DELETE` in a new partition within about 5 seconds of its creation can error the mirror too.
- **Refused at `create_table`:** tables with generated columns, inheritance parents, partitioned tables without a primary key, and column types moonlink cannot store exactly (for example `interval`, `timetz`, enums, multi-dimensional arrays, `numeric` with a precision above 38 or a scale outside 0..precision).
- **Values a mirror cannot store** (for example `numeric` `NaN` or `Infinity`, more fractional digits than an unconstrained `numeric` keeps, dates outside the supported range) error the mirror instead of being stored differently.
- **After upgrading to this version,** mirrors of tables without a primary key that have `json`, `jsonb`, `char(n)`, `"char"`, unconstrained `numeric`, array or composite columns are errored at the first start, because they were stored differently before. Re-create them.

A source that cannot be reached when moonlink starts (restarting, refusing connections) is not an error: its mirrors report that they are not available yet and are restored automatically once it is back. Postgres retains WAL for that database meanwhile.

## Contributing

We’re grateful for our contributors. If you'd like to help improve Moonlink, join our [community](https://join.slack.com/t/mooncake-devs/shared_invite/zt-2sepjh5hv-rb9jUtfYZ9bvbxTCUrsEEA).

🥮
