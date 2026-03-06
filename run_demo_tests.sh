#!/usr/bin/env bash
# ── Demo mirror test runner ─────────────────────────────────────────
# Runs ONLY the test_demo_* files — the mirror tests for each demo.
#
# These tests require full platform infrastructure (Deephaven JVM,
# QuestDB, MinIO, PostgreSQL, Gemini API) and take longer to run.
#
# Usage:  ./run_demo_tests.sh [pytest args...]
# Examples:
#   ./run_demo_tests.sh                                    # all demo tests
#   ./run_demo_tests.sh tests/test_demo_trading.py         # single demo
#   ./run_demo_tests.sh -k "test_demo_bridge"              # by name
set -uo pipefail

cd "$(dirname "$0")"

# ── Kill stale services ─────────────────────────────────────────────
# All ports from conftest.py session fixtures:
#   10000  = Deephaven streaming        8765   = MarketDataServer
#   9200   = QuestDB HTTP               9209   = QuestDB ILP
#   8922   = QuestDB PG                 5490   = Lakehouse embedded PG
#   8183   = Lakekeeper (Iceberg REST)  9004   = Lakehouse MinIO API
#   9005   = Lakehouse MinIO console    9102   = MediaServer MinIO API
#   9103   = MediaServer MinIO console  8050   = Datacube UI (Tornado)
echo "Cleaning up stale services..."
for port in 10000 8765 9200 9209 8922 5490 8183 9004 9005 9102 9103 8050; do
    lsof -ti :"$port" 2>/dev/null | xargs kill -9 2>/dev/null || true
done
pkill -9 -f postgres 2>/dev/null || true
pkill -9 -f lakekeeper 2>/dev/null || true
pkill -9 -f minio 2>/dev/null || true

# Clean up stale System V shared memory segments left by SIGKILL'd postgres.
for seg in $(ipcs -m 2>/dev/null | awk '/^m / || /^0x/ {print $2}' | grep -E '^[0-9]+$'); do
    ipcrm -m "$seg" 2>/dev/null || true
done
sleep 1

# ── Run demo tests ─────────────────────────────────────────────────
LOG_DIR=".test_runs"
mkdir -p "$LOG_DIR"
LOGFILE="$LOG_DIR/$(date +%Y%m%d_%H%M%S)_demo.txt"

if [ $# -eq 0 ]; then
    python3 -m pytest tests/test_demo_*.py -v --durations=0 2>&1 | tee "$LOGFILE"
else
    python3 -m pytest "$@" --durations=0 2>&1 | tee "$LOGFILE"
fi
RC=${PIPESTATUS[0]}

# Keep only the 10 most recent runs
ls -t "$LOG_DIR"/*.txt 2>/dev/null | tail -n +11 | xargs rm -f 2>/dev/null || true

echo ""
echo "Log saved: $LOGFILE"
echo "History:   ls -lt $LOG_DIR/"
exit $RC
