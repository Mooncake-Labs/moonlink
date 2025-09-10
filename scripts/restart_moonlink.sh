#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="/home/ubuntu/moonlink"
BASE_PATH="/data"
BIN_PATH="$PROJECT_DIR/target/debug/moonlink_service"
OUT_FILE="$PROJECT_DIR/moonlink_service.out"

echo "[1/5] Killing existing moonlink processes (if any)" >&2
pkill -f "moonlink_service" || true
pkill -f "moonlink-service" || true

# wait up to 10s for processes to exit
for i in {1..10}; do
  if pgrep -f "moonlink_service|moonlink-service" >/dev/null 2>&1; then
    sleep 1
  else
    break
  fi
done

echo "[2/5] Wiping $BASE_PATH" >&2
if [[ -d "$BASE_PATH" ]]; then
  # Remove contents without risking removing the mount itself
  find "$BASE_PATH" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
else
  mkdir -p "$BASE_PATH"
fi

echo "[3/5] Ensuring moonlink_service binary exists" >&2
if [[ ! -x "$BIN_PATH" ]]; then
  (cd "$PROJECT_DIR" && cargo build -p moonlink_service --features storage-s3)
fi

echo "[4/5] Starting moonlink_service with nohup (RUST_LOG=trace)" >&2
# Max verbosity with crate-specific filters and full backtraces
nohup env \
  RUST_BACKTRACE=full \
  RUST_LOG=trace,moonlink=trace,moonlink_backend=trace,moonlink_connectors=trace,moonlink_service=trace,opendal=trace,reqsign=trace,reqwest=trace,hyper=trace,sqlx=trace,axum=trace,tower_http=trace \
  "$BIN_PATH" "$BASE_PATH" > "$OUT_FILE" 2>&1 &

echo "[5/5] Waiting for readiness probe at http://127.0.0.1:5050/ready" >&2
# Wait up to 60 seconds for readiness
for i in {1..60}; do
  if curl -sf http://127.0.0.1:5050/ready | grep -q "ready"; then
    echo "moonlink is ready" >&2
    exit 0
  fi
  sleep 1
done

echo "ERROR: moonlink failed readiness within 60s. See $OUT_FILE" >&2
exit 1







