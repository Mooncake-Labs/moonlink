# Moonlink Chaos Test README

This document explains the purpose, scope, and usage of the Moonlink **chaos test** suite that stress‑tests moonlink core, including persistence, and snapshot semantics across backends (FS/S3/GCS) and table modes (regular, append‑only, upsert).

---

## Why this exists

The chaos tests simulate realistic, adversarial event sequences (append/update/delete, streaming/non‑streaming txns, commits/flushes/aborts), interleaved with maintenance operations (index merge, data compaction, mooncake and iceberg snapshots). They validate end‑to‑end invariants:

* **Causality & ordering**: Begin transaction happens before End; End transaction only after Begin; LSN strictly increases.
* **State consistency**: Only committed changes affect snapshots; uncommitted buffers are isolated.
* **Row constraints**: Deletes/updates apply to valid candidates; no double‑delete or update‑after‑delete in a single txn.
* **Streaming rules**: Streaming allows flush/abort, repeated updates, and deleting rows inserted in the same txn.
* **Persistence fidelity**: A forced snapshot at LSN *N* reproduces exactly the in‑memory committed state at *N* when reloaded from storage.
* **Maintenance safety**: Foreground maintenance runs only when the system is quiescent and at a controlled cadence.

---

## Event taxonomy

* **Txn open/close**: `BeginStreamingTxn`, `BeginNonStreamingTxn`, `Commit`, `CommitFlush`, `StreamFlush`, `StreamAbort`.
* **Row ops**: `Append`, `Delete`, `Update` (modeled as delete+append of the same row content).
* **Maintenance**: `ForceRegularIndexMerge`, `ForceRegularDataCompaction`, `ForegroundForceSnapshot`.
* **Observation**: `ReadSnapshot` and `ReadIcebergSnapshot`.

> In **upsert** mode, pure `Append` is disabled. Updates may introduce new rows; deletes may target non‑existent rows (delete‑if‑exists).

---

## Transaction & state model

* **States**: `Empty`, `InNonStreaming`, `InStreaming`.
* **Buffers** (per txn): uncommitted inserts, updates, and deleted‑id sets (committed/uncommitted).
* **Commit**: moves uncommitted inserts to committed, applies deletions, clears buffers, updates `last_commit_lsn`, and (for streaming) increments `xact_id`.
* **Abort (streaming only)**: clears buffers, increments `xact_id`, no committed state change.

**Invariants encoded by assertions**:

* A txn can only begin when `txn_state == Empty` and all per‑txn buffers are empty.
* End operations are only legal inside an active txn.
* `cur_lsn` strictly increases for all LSN‑bearing events.
* Candidate sets for delete/update are non‑empty when chosen.

---

## Snapshot semantics

* `ReadSnapshot` targets the current `last_commit_lsn` and compares ids to the in‑memory model.
* `ForegroundForceSnapshot(lsn)` blocks until persisted, then a **fresh** table instance loads from storage and must match in‑memory ids at that LSN.

---

## Maintenance scheduling

Foreground maintenance (`ForceRegularIndexMerge`, `ForceRegularDataCompaction`, `ForegroundForceSnapshot`) is scheduled only when:

1. The previous txn **committed successfully**, and
2. There are **no** uncommitted changes.

All non‑update commands are **rate‑limited** by LSN distance:

```
NON_UPDATE_COMMAND_INTERVAL_LSN = 5
```

---

## Randomness & reproducibility

* The RNG is `StdRng::seed_from_u64(seed)`.
* Provide `--seed <u64>` to reproduce a sequence; otherwise a timestamp‑derived default is used.
* `--print-events-on-success` dumps replay events even on green runs.

---

## Modes & knobs

* **SpecialTableOption**

  * `None`: regular table
  * `AppendOnly`: only appends; no deletes/updates
  * `UpsertDeleteIfExists`: upsert semantics; delete‑if‑exists allowed

* **TableMaintenanceOption**

  * `NoTableMaintenance`: background maintenance disabled
  * `IndexMerge`: index merge enabled by default
  * `DataCompaction`: compaction enabled by default

* **Error/chaos injection**

  * `disk_slice_write_chaos_enabled` introduces disk‑write nondeterminism
  * `error_injection_enabled` toggles Iceberg‑layer errors/delays for resilience testing

* **Event volume**

  * `event_count`: total random steps per run (e.g., 2000–3500 for stress; 100 for chaos injection smoke).

---

## Output & replay

* Each run writes a metadata line and one JSON event per line to:

```
/tmp/chaos_test_<random>
```

* On failure, all handler replay events print to stdout for deterministic reproduction.
* On success, you can opt to print via `--print-events-on-success`.

---

## Debugging tips

* Use `--nocapture` to see printlns in test output.
* Pin a `--seed` to reproduce flakes; then instrument assertions around the failing branch.
* Inspect the replay file and pipe it into a small driver that feeds `TableHandler` to reproduce outside of the random harness.
* If `drop_table()` fails at the end, it often signals a latent error in the event loop or pending tasks; bump the post‑loop sleep or add explicit synchronization in `TableEventManager`.
