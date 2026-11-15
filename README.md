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

> **Before upgrading from pg_mooncake 0.2 (Moonlink with its own WAL), snapshot every mirror, or re-create every mirror after the upgrade.** That version acknowledged changes to Postgres once they were in its `_wal` directory. The new version does not read that directory, and Postgres does not send those changes again, so a mirror silently misses every change that had not reached Iceberg yet. Before upgrading:
> 1. Stop writes to the mirrored tables.
> 2. Still on the old version, run `CALL mooncake.create_snapshot('<mirror>');` for every mirror.
> 3. Upgrade.
>
> If you upgraded without these steps, drop and re-create every mirror. Until the old WAL directory, `$PGDATA/pg_mooncake/_wal`, is deleted, Moonlink logs a warning at startup naming the mirrors that have files in it. Delete it once every mirror is snapshotted or re-created; dropping a mirror no longer removes its part of it.

After upgrading, or after a crash, if a mirror looks wrong, drop it and re-create it (`CALL mooncake.create_table(...)` again). Old `_wal` directories from earlier versions are unused and can be deleted. Data files are made durable before the Iceberg commit that references them, but deletion vector and index puffin files are not fsynced into their directory, so a power loss right after a commit can leave a mirror that fails to load and must be re-created.

On the first start after upgrading, every existing mirror rebuilds its lookup indices from its data files (earlier versions hashed rows with a seed that changed on every restart, so updates and deletes of already-flushed rows could fail after a restart). This is a one-time read of each table's key columns, logged as a warning per table. If a table's indices cannot be rebuilt, that mirror is marked out of sync and must be dropped and re-created; other mirrors are not affected.

This version also deletes Iceberg files that neither the current version nor the one before it references (old snapshots, metadata files, manifests, deletion vectors, files replaced by compaction), which earlier versions kept forever. A replaced data or deletion-vector file is kept for an hour after it left the table, so running scans can finish. On the first start after upgrading, each table's leftover files are removed when the table loads. Set `MOONLINK_ICEBERG_GC=dry-run` to only log what would be deleted (each file once, as a count and size per commit; paths at debug level), or `MOONLINK_ICEBERG_GC=off` to keep every file. Dry-run is meant for a short look: it keeps listing and remembering every superseded file, so its cost grows the longer it runs; use `off` to keep files for good.

If the disk fills up, mirrors stop advancing instead of failing: flushes and Iceberg commits are retried (at most every minute) until space is available, and files of failed attempts are removed. Changes that wait for disk space are held in memory, so a mirror is marked out of sync once a flush cannot be written for 30 minutes, or sooner, once more than 512 MiB of changes are waiting, counted over all mirrors together (busy mirrors on a full disk would otherwise grow the Postgres background worker until it is killed for memory); then the mirror holding the most of them is marked out of sync, and the others keep waiting. A mirror that is marked out of sync or dropped releases its waiting changes at once. Postgres keeps the unconfirmed changes in `pg_wal` meanwhile.

## Contributing

We’re grateful for our contributors. If you'd like to help improve Moonlink, join our [community](https://join.slack.com/t/mooncake-devs/shared_invite/zt-2sepjh5hv-rb9jUtfYZ9bvbxTCUrsEEA).

🥮
