# Testing moonlink

Everything runs from the repo root with the standard Rust toolchain. `scripts/test.sh` is the entry point:

```sh
scripts/test.sh fast          # lint, unit tests, wire-format goldens; no services
scripts/test.sh pg            # Postgres-backed suites (needs a local Postgres, see below)
scripts/test.sh slow          # chaos tests, regression replays, bulk ingest (long)
scripts/test.sh               # all three tiers
scripts/test.sh --list all    # print the exact cargo commands without running them
scripts/test.sh pg --repeat 3 # rerun a tier to look for flaky tests
```

Per-step logs go to `target/test-logs/` (override with `TEST_OUT`). The script runs every step, prints a
summary table, and exits non-zero if any step failed. Use `--fail-fast` to stop at the first failure. Steps are
discovered from the tree (crates, features, chaos tests), so `--list` always shows what will actually run.

## Prerequisites

| What | Needed for | Notes |
|---|---|---|
| Rust toolchain from `rust-toolchain.toml` (via rustup) | all | Includes `clippy` and `rustfmt`. |
| `cargo-nextest` | `fast` (optional) | Runs unit tests as separate processes. Without it the script uses `cargo test --lib`. |
| `cargo-sort`, `cargo-deny` | `fast` (optional) | Shown as `skip` in the summary when not installed. CI runs both, so install them to match CI. |
| `mold` + `clang` | optional | Faster links: `RUSTFLAGS="-Dwarnings -C link-arg=-fuse-ld=mold"`. |
| Postgres 14+ with `wal_level=logical`, plus `psql` | `pg`, `slow` (bulk_ingest) | See below. |
| ~40 GB free disk | all | Each feature combination builds separately. |

### Local Postgres for the `pg` tier

The suites connect to `DATABASE_URL` (default `postgresql://postgres@localhost:5432/postgres`, no password).
They create publications and logical replication slots and terminate other replication backends, so use a
superuser on a throwaway server you don't need for anything else. **Never point `DATABASE_URL` at a database
that a real pg_mooncake mirrors:** the backend tests drop and re-create the publication `moonlink_pub` and
terminate and drop the slot `moonlink_slot_<database>`, which are the objects pg_mooncake uses. The examples
below start a throwaway server that only listens on localhost and uses `trust` authentication. If your server
needs a password, put it in `DATABASE_URL`, which takes a standard libpq connection URI.

With Docker:

```sh
docker run -d --name moonlink-pg -p 127.0.0.1:5432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust \
  postgres:17 -c wal_level=logical -c max_replication_slots=64 -c max_wal_senders=64
```

With a locally installed server, as your own user. On Debian/Ubuntu the server binaries are not on `PATH`,
so first run `export PATH=/usr/lib/postgresql/<version>/bin:$PATH`.

```sh
initdb -D /tmp/moonlink-pg -U postgres -A trust
cat >> /tmp/moonlink-pg/postgresql.conf <<EOF
listen_addresses = 'localhost'
unix_socket_directories = '/tmp'
wal_level = logical
max_replication_slots = 64
max_wal_senders = 64
max_connections = 300
EOF
pg_ctl -D /tmp/moonlink-pg -l /tmp/moonlink-pg.log start
```

Before the first Postgres-backed step, the script checks that `wal_level` is `logical`. It also looks for
leftovers of an aborted earlier run: the fixed-name test tables, publications, inactive replication slots and
composite types that the connectors suites create. The suites drop their own fixtures before they start, so
leftovers are only reported. `scripts/test.sh pg --clean-pg` drops exactly those objects, and only on a local
server. It never touches `moonlink_slot_*`, `moonlink_pub` or anything else.

Two rules for these suites:

- **One run per server at a time.** The tests use fixed names: the publication `moonlink_pub`, the slot
  `moonlink_slot_<database>`, and fixed table and type names. Two concurrent runs against one server break
  each other.
- **Use `cargo test`, not nextest.** The suites are serialized with `#[serial]`, which only works inside one
  test process. `scripts/test.sh` already does this.

## Tiers

### fast

| Step | What it checks |
|---|---|
| fmt, clippy, sort, deny | Formatting, lints across every feature combination (`--all-features -D warnings`), Cargo.toml ordering, dependency advisories and licenses |
| unit | Library unit tests of every crate: row and type conversion, mem and disk slices, indexes, deletion vectors, Iceberg snapshot persistence and recovery on the local filesystem, compaction and index merge, read-cache state, the table handler event loop, union read |
| wire_golden | Byte-level goldens for the RPC protocol (`moonlink_rpc`), the `ScanTableBegin` payload (`moonlink_table_metadata`, `scan_payload_order`) and the deletion-vector blob framing. pg_mooncake and duckdb_mooncake decode these bytes directly, so a failure here is a wire-format break. Change a golden only when the break is intentional and coordinated with both clients. |

### pg

| Step | What it covers |
|---|---|
| connectors | `pgoutput` decoding and CDC apply (insert, update, delete, NULLs, composite and array types), Postgres-to-Arrow type mapping |
| metadata_store | Table metadata persistence (sqlite and Postgres stores) |
| service | The RPC server on its unix socket against a real Postgres source: create, scan, list, drop |
| backend_integration | One test binary per `src/moonlink_backend/tests/*.rs`: create and drop, initial copy with concurrent DML, sharded copy, replication reconnect and resume, large streamed transactions, table and control-plane recovery after a restart, scans at an LSN, Iceberg snapshots. Runs with `--features test-utils`. Without it the restart-recovery tests that need test hooks compile to zero tests and "pass". |

### slow

| Step | What it covers |
|---|---|
| `*_chaos*` | Randomized workloads against the table handler: inserts, deletes, updates, streamed transactions, snapshots, index merge, compaction, restart and replay. `chaos_injection` adds I/O errors and latency. Output (`--nocapture`) prints the random seed, and a failure leaves a dump in `/tmp/chaos_test_*`. |
| regression | Replays of past chaos failures |
| accessor_chaos | Filesystem accessor with injected latency, errors and timeouts |
| bulk_ingest | Several bulk-insert iterations through the backend (needs Postgres) |

## Expected runtime

Measured on 8 vCPU / 64 GB with mold. Most of the cold time is compilation. In `slow` it is the tests themselves.

| Tier | Cold target dir | Warm (no code change) |
|---|---|---|
| fast | ~10 min | ~2 min |
| pg | ~20 min | ~3 min |
| slow | ~60 min | ~50 min (chaos ~35 min, bulk_ingest ~15 min) |

## Not covered by `scripts/test.sh`

Feature-gated suites that need external services, which CI starts as containers. Run them by hand where the
feature still exists in your checkout. Add the hostnames to `/etc/hosts` and start the containers as in
`.github/workflows/ci.yml`.

| Feature | Service | Commands |
|---|---|---|
| `storage-s3` | MinIO at `s3.local:9000` (`minioadmin`/`minioadmin`) | `cargo test --lib --features=storage-s3`; `cargo test test_s3_chaos --features=chaos-test,storage-s3 -p moonlink` |
| `storage-gcs` | fake-gcs-server at `gcs.local:4443` | `cargo test --lib --features=storage-gcs`; `cargo test test_gcs_chaos --features=chaos-test,storage-gcs -p moonlink` |
| `catalog-rest` | Iceberg REST fixture at `iceberg-rest.local:8181` | `cargo test --lib --features=catalog-rest` |
| `catalog-glue` | moto at `moto-glue.local:5000` plus MinIO | `cargo test -p moonlink --features=catalog-glue,storage-s3` |
| `test-tls` | Postgres with TLS (`sslmode=verify-full`) | not run by CI either |

## Known flaky tests

These tests fail intermittently. When one of them fails, rerun before investigating.

| Test | Symptom | Cause |
|---|---|---|
| backend `test_basic::test_create_iceberg_snapshot`, `test_table_recovery::test_recovery`, `test_replication_recovery::test_reconnect_resumes_replication_*` and other scans right after a restart or reconnect | panic in `union_read/read_state_manager.rs`: `assertion failed: (left <= right)` | A scan reads the table's commit LSN and the replication LSN without a consistent view, and they are published in the opposite order. This is a real race in the read path, not a test bug, and shows up more under CPU load. |
| service `test::postgres::test_create_table_from_postgres` | `assertion left == right failed` on `cardinality` (0 vs 2) | `list_tables` reports the rows of the latest in-memory snapshot. Right after `create_table` returns, the initial-copy rows may not be in a snapshot yet. |
| connectors `cdc_event_test::{test_null, test_composite_types}` | `replication slot ... is active for PID n` during cleanup | Fixed in the test helpers (slot drop terminates the walsender and retries). An aborted run of an older checkout leaves the test table, publication and types behind; `scripts/test.sh pg --clean-pg` removes them. |

To check a change for new flakes, run the affected tier several times: `scripts/test.sh pg --repeat 3`.

## Writing tests

- Put unit tests next to the code in `#[cfg(test)]` modules. Use `tempfile::TempDir` for every path, never a fixed `/tmp/...`.
- Postgres-backed tests go in `src/moonlink_backend/tests/` or behind the `connector-pg` feature, and are marked `#[serial]`.
- Make cleanup idempotent: `DROP ... IF EXISTS` at the start of a test, not only in `Drop`. A panic inside `Drop` during unwinding aborts the whole test binary.
- Wait for a condition with a timeout, not a fixed `sleep`.
- A change to anything sent over the RPC socket or in the `ScanTableBegin` payload must first fail the wire goldens. Update them in the same change and call out the break.
