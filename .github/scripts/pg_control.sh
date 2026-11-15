#!/usr/bin/env bash
# Stop or start the CI Postgres service container, for backend tests that take the source down
# (see MOONLINK_TEST_PG_CTL).
set -euo pipefail
case "${1:-}" in
  stop)
    docker stop postgres > /dev/null
    ;;
  start)
    docker start postgres > /dev/null
    for _ in $(seq 1 30); do
      if pg_isready -h localhost -p 5432 -U postgres > /dev/null; then
        exit 0
      fi
      sleep 1
    done
    echo "Postgres did not come back" >&2
    exit 1
    ;;
  *)
    echo "usage: $0 stop|start" >&2
    exit 2
    ;;
esac
