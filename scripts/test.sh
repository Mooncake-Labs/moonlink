#!/usr/bin/env bash
# Run the moonlink test matrix in tiers. See TESTING.md.
#
#   scripts/test.sh [fast|pg|slow|all ...] [--list] [--fail-fast] [--repeat N] [--clean-pg]
#
#   fast  fmt, clippy, cargo-sort/cargo-deny, lib unit tests, wire-format goldens                   (no services)
#   pg    Postgres-backed suites: connectors CDC, metadata store, service RPC, backend integration (needs Postgres)
#   slow  chaos tests, regression replays, storage accessor chaos, bulk ingest                     (long)
#   all   fast + pg + slow (default)
#
#   --clean-pg  before the Postgres steps, drop leftovers of an aborted earlier run: the fixed-name test tables,
#               publications, inactive replication slots and composite types the suites create (local server only)
#
# Environment:
#   DATABASE_URL   Postgres with wal_level=logical (default postgresql://postgres@localhost:5432/postgres)
#   TEST_OUT       directory for per-step logs (default target/test-logs)
set -uo pipefail

cd "$(dirname "$0")/.."
export DATABASE_URL=${DATABASE_URL:-postgresql://postgres@localhost:5432/postgres}
export RUST_BACKTRACE=${RUST_BACKTRACE:-1}
export RUSTFLAGS=${RUSTFLAGS:--Dwarnings}
OUT=${TEST_OUT:-target/test-logs}
FAIL_FAST=0 LIST=0 REPEAT=1 CLEAN_PG=0 TIERS=()
while [ $# -gt 0 ]; do
  case "$1" in
    fast|pg|slow) TIERS+=("$1") ;;
    all) TIERS+=(fast pg slow) ;;
    --fail-fast) FAIL_FAST=1 ;;
    --list) LIST=1 ;;
    --clean-pg) CLEAN_PG=1 ;;
    --repeat)
      [[ "${2:-}" =~ ^[1-9][0-9]*$ ]] || { echo "--repeat needs a positive integer" >&2; exit 2; }
      REPEAT=$2; shift ;;
    -h|--help) sed -n '2,16p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
  shift
done
[ ${#TIERS[@]} -eq 0 ] && TIERS=(fast pg slow)

# Steps are discovered from the tree, so the script keeps working as crates and features are removed.
# Anything expected but not found becomes a visible "skip" row instead of silently disappearing.
have_crate() { [ -f "src/$1/Cargo.toml" ]; }
have_feature() { have_crate "$1" && grep -Eq "^\s*\"?$2\"?\s*=" "src/$1/Cargo.toml"; }
have_tool() { command -v "cargo-$1" >/dev/null 2>&1; }
have_test() { grep -rqs "fn $2" "src/$1/src"; }   # prefix match: chaos tests are named <prefix>_with_<mode>

STEPS=()   # "tier|name|command" or "tier|name|SKIP:<reason>"
add() { STEPS+=("$1|$2|$3"); }
skip() { STEPS+=("$1|$2|SKIP:$3"); }

# ── fast ──
add fast fmt "cargo fmt --check"
add fast clippy "cargo clippy --locked --workspace --all-targets --all-features -- -D warnings"
if have_tool sort; then add fast sort "cargo sort --workspace --check"; else skip fast sort "cargo-sort not installed (CI runs it)"; fi
if have_tool deny; then add fast deny "cargo deny check"; else skip fast deny "cargo-deny not installed (CI runs it)"; fi
if have_tool nextest; then
  add fast unit "cargo nextest run --locked --lib --workspace --no-fail-fast"
else
  add fast unit "cargo test --locked --lib --workspace --no-fail-fast"
fi
# The wire-format goldens live in tests/, so --lib does not run them. They pin the RPC and scan payload
# layout that pg_mooncake and duckdb_mooncake decode.
WIRE=""
for c in moonlink_rpc moonlink_table_metadata; do [ -d "src/$c/tests" ] && WIRE="$WIRE -p $c"; done
if [ -n "$WIRE" ]; then
  W="cargo test --locked --no-fail-fast$WIRE --tests"
  [ -f src/moonlink/tests/scan_payload_order.rs ] && W="$W && cargo test --locked -p moonlink --test scan_payload_order"
  add fast wire_golden "$W"
else
  skip fast wire_golden "no wire-golden tests found"
fi

# ── pg ── cargo test, not nextest: the suites share one replication slot and fixed table/type names and are
# serialized with #[serial], which only works inside one test process.
if have_feature moonlink_connectors connector-pg; then
  add pg connectors "cargo test --locked --package moonlink_connectors --lib --features connector-pg"
else
  skip pg connectors "feature connector-pg not found"
fi
if have_feature moonlink_metadata_store metadata-postgres; then
  F="metadata-all test-utils"
  have_feature moonlink_metadata_store storage-s3 && F="$F storage-s3"
  have_feature moonlink_metadata_store storage-gcs && F="$F storage-gcs"
  add pg metadata_store "cargo test --locked --no-fail-fast --package moonlink_metadata_store --tests --features '$F'"
elif have_crate moonlink_metadata_store; then
  skip pg metadata_store "no Postgres metadata store (sqlite store tests run in unit)"
fi
if have_feature moonlink_service postgres-integration; then
  SF=postgres-integration
  have_feature moonlink_service standalone-test && SF="standalone-test,postgres-integration"
  add pg service "cargo test --locked --no-fail-fast --package moonlink_service --lib --features $SF"
else
  skip pg service "feature postgres-integration not found"
fi
# test-utils: the restart/slot-replay recovery tests are compiled only with it (they need test hooks).
BF=""; have_feature moonlink_backend test-utils && BF=" --features test-utils"
add pg backend_integration "cargo test --locked --no-fail-fast --package moonlink_backend$BF --test '*'"

# ── slow ──
for t in test_chaos_on_local_fs test_local_system_optimization_chaos test_chaos_injection test_disk_slice_chaos \
         test_append_only_chaos test_upsert_chaos test_replay_chaos; do
  if have_test moonlink "$t"; then
    add slow "${t#test_}" "cargo test --locked table_handler::chaos_test::$t --features=chaos-test -p moonlink -- --nocapture"
  else
    skip slow "${t#test_}" "chaos test $t not found"
  fi
done
add slow regression "cargo test --locked table_handler::regression --features=chaos-test -p moonlink"
add slow accessor_chaos "cargo test --locked storage::filesystem::accessor -p moonlink --features=chaos-test"
# bulk ingest replicates from Postgres
if have_feature moonlink_backend bulk-ingestion-test; then
  add slow bulk_ingest "cargo test --locked test_bulk_insert_multiple_iterations --features=bulk-ingestion-test -p moonlink_backend"
else
  skip slow bulk_ingest "feature bulk-ingestion-test not found"
fi

selected() { local t; for t in "${TIERS[@]}"; do [ "$t" = "$1" ] && return 0; done; return 1; }
needs_pg() { [ "$1" = pg ] || [ "$2" = bulk_ingest ]; }

if [ "$LIST" = 1 ]; then
  for s in "${STEPS[@]}"; do
    IFS='|' read -r tier name cmd <<<"$s"
    selected "$tier" || continue
    case "$cmd" in SKIP:*) cmd="(skip: ${cmd#SKIP:})" ;; esac
    printf '%-5s %-34s %s\n' "$tier" "$name" "$cmd"
  done
  exit 0
fi

# Objects with fixed names that the suites create and normally drop themselves (connectors
# pg_replicate/tests). An aborted run can leave them behind; --clean-pg removes exactly these.
# moonlink_slot_* / moonlink_pub are deliberately not touched: on a real setup they belong to pg_mooncake.
PG_TEST_TABLES="test_composite_cdc test_cdc_string_null test_basic_composite test_nested test_array_composite test_nested_array test_deep test_mixed"
PG_TEST_PUBLICATIONS="test_composite_pub test_cdc_string_null_pub"
PG_TEST_SLOTS="test_composite_slot test_cdc_string_null_slot"
PG_TEST_TYPES="test_person test_location test_point test_address test_item test_skill test_coords test_venue test_tag test_meta test_doc"

pgq() { PGOPTIONS='-c client_min_messages=warning' psql "$DATABASE_URL" -X -qtA "$@"; }
sql_list() { local out="" x; for x in $1; do out="$out,'$x'"; done; echo "${out#,}"; }
qualified() { local out="" x; for x in $1; do out="$out, public.$x"; done; echo "${out#, }"; }
PUBLIC_NS="(select oid from pg_namespace where nspname = 'public')"

pg_leftovers() {
  pgq -c "select 'table '||relname from pg_class where relkind in ('r','p') and relnamespace = $PUBLIC_NS and relname in ($(sql_list "$PG_TEST_TABLES"))
          union all select 'publication '||pubname from pg_publication where pubname in ($(sql_list "$PG_TEST_PUBLICATIONS"))
          union all select 'slot '||slot_name from pg_replication_slots where slot_type = 'logical' and not active
                      and database = current_database() and slot_name in ($(sql_list "$PG_TEST_SLOTS"))
          union all select 'type '||t.typname from pg_type t join pg_class c on c.oid = t.typrelid
                      where c.relkind = 'c' and t.typnamespace = $PUBLIC_NS and t.typname in ($(sql_list "$PG_TEST_TYPES"))"
}

PG_OK=""
pg_preflight() {
  command -v psql >/dev/null || { echo "psql not found (needed for Postgres-backed steps)" >&2; return 1; }
  local wl left
  wl=$(pgq -c "show wal_level" 2>&1) || { echo "cannot connect to DATABASE_URL=$DATABASE_URL: $wl" >&2; return 1; }
  [ "$wl" = logical ] || { echo "Postgres wal_level is '$wl'; these steps need wal_level=logical" >&2; return 1; }
  left=$(pg_leftovers) || return 1
  [ -z "$left" ] && return 0
  if [ "$CLEAN_PG" = 0 ]; then
    echo "note: leftovers of an earlier test run found (the suites usually drop these themselves; --clean-pg removes them):"
    echo "$left" | sed 's/^/  /'
    return 0
  fi
  local addr
  addr=$(pgq -c "select coalesce(host(inet_server_addr()), 'local')")
  case "$addr" in
    local|127.*|::1) ;;
    *) echo "--clean-pg only runs against a local server (server address $addr); drop the leftovers by hand" >&2; return 1 ;;
  esac
  echo "--clean-pg: dropping"; echo "$left" | sed 's/^/  /'
  pgq -c "DROP TABLE IF EXISTS $(qualified "$PG_TEST_TABLES")" || echo "warning: could not drop the test tables (something else depends on them?)" >&2
  pgq -c "DROP PUBLICATION IF EXISTS ${PG_TEST_PUBLICATIONS// /, }" >/dev/null
  local s
  for s in $(pgq -c "select slot_name from pg_replication_slots where slot_type = 'logical' and not active and database = current_database() and slot_name in ($(sql_list "$PG_TEST_SLOTS"))"); do
    pgq -c "select pg_drop_replication_slot('$s')" >/dev/null
  done
  # One statement, no CASCADE: the types may depend on each other, but nothing outside this list may depend on them.
  pgq -c "DROP TYPE IF EXISTS $(qualified "$PG_TEST_TYPES")" || echo "warning: could not drop the test composite types (something else depends on them?)" >&2
  return 0
}

mkdir -p "$OUT"
RESULTS=() FAILED=0 RAN=0
for iter in $(seq 1 "$REPEAT"); do
  for s in "${STEPS[@]}"; do
    IFS='|' read -r tier name cmd <<<"$s"
    selected "$tier" || continue
    case "$cmd" in SKIP:*) [ "$iter" = 1 ] && RESULTS+=("$tier|$name|skip|0|$iter|${cmd#SKIP:}"); continue ;; esac
    if needs_pg "$tier" "$name"; then
      [ -z "$PG_OK" ] && { if pg_preflight; then PG_OK=yes; else PG_OK=no; fi; }
      if [ "$PG_OK" = no ]; then RESULTS+=("$tier|$name|FAIL|0|$iter|Postgres not usable, see above"); FAILED=1; continue; fi
    fi
    log="$OUT/$tier-$name.log"; [ "$REPEAT" -gt 1 ] && log="$OUT/$tier-$name-$iter.log"
    printf '==> [%s] %s (%s)\n' "$tier" "$name" "$log"
    start=$(date +%s)
    bash -c "$cmd" >"$log" 2>&1 </dev/null
    rc=$?
    RAN=$((RAN + 1))
    secs=$(( $(date +%s) - start ))
    if [ $rc -eq 0 ]; then st=ok; else
      st=FAIL; FAILED=1
      grep -E '^test .* FAILED$|^\s+FAIL \[|panicked at|^error(\[|:)' "$log" | head -20
    fi
    RESULTS+=("$tier|$name|$st|$secs|$iter|")
    [ $rc -ne 0 ] && [ "$FAIL_FAST" = 1 ] && break 2
  done
  PG_OK=""   # re-check Postgres before each repetition
done

echo
printf '%-5s %-34s %-6s %6s %3s  %s\n' tier step result secs run note
for r in "${RESULTS[@]}"; do
  IFS='|' read -r tier name st secs iter note <<<"$r"
  printf '%-5s %-34s %-6s %6s %3s  %s\n' "$tier" "$name" "$st" "$secs" "$iter" "$note"
done
[ "$RAN" -eq 0 ] && { echo "no step ran" >&2; exit 1; }
exit $FAILED
