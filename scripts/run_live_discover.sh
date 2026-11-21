#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/env.sh"
MODE="${1:-enable}"
BASE_ENDPOINTS="GET /api/flags,GET /api/telemetry/cohorts,GET /api/telemetry/cohorts/{flag_key}/orders,GET /api/telemetry/merchants/{merchant_id}/orders"
DISCOVER_ENDPOINT="POST /api/flags/{flag_key}/exclude-merchants/{merchant_ids}"

cd "$ROOT/services/flag-edge-gateway"

if [ "$MODE" = "enable" ]; then
  "$ROOT/scripts/toggle_discover_endpoint.sh" enable
  go build -o flag-edge-gateway .
  NO_COLOR=1 FORCE_COLOR=0 synapse build --discover --auto --endpoints "${BASE_ENDPOINTS},${DISCOVER_ENDPOINT}" </dev/null
elif [ "$MODE" = "disable" ]; then
  "$ROOT/scripts/toggle_discover_endpoint.sh" disable
  go build -o flag-edge-gateway .
  NO_COLOR=1 FORCE_COLOR=0 synapse build --discover --auto --endpoints "${BASE_ENDPOINTS}" </dev/null
else
  echo "Usage: $0 [enable|disable]"
  exit 1
fi

PID_FILE="$ROOT/flag-edge-gateway.pid"
if [ -f "$PID_FILE" ]; then
  OLD_PID="$(cat "$PID_FILE" || true)"
  if [ -n "$OLD_PID" ] && kill -0 "$OLD_PID" 2>/dev/null; then
    kill "$OLD_PID" || true
    for _ in $(seq 1 25); do
      kill -0 "$OLD_PID" 2>/dev/null || break
      sleep 0.1
    done
  fi
  NEW_PID="$(CANARYBRAKE_ROOT="$ROOT" python -c '
import os, subprocess
root = os.environ["CANARYBRAKE_ROOT"]
logf = open(os.path.join(root, "flag-edge-gateway.log"), "ab")
p = subprocess.Popen(
    [os.path.join(root, "services", "flag-edge-gateway", "flag-edge-gateway")],
    cwd=os.path.join(root, "services", "flag-edge-gateway"),
    stdin=subprocess.DEVNULL,
    stdout=logf,
    stderr=subprocess.STDOUT,
    close_fds=True,
    start_new_session=True,
)
print(p.pid)
')"
  echo "$NEW_PID" > "$PID_FILE"
  for _ in $(seq 1 40); do
    if curl -fsS http://127.0.0.1:8081/health >/dev/null 2>&1; then
      echo "✓ Hot-restarted flag-edge-gateway (PID $NEW_PID) and verified /health 200 OK"
      break
    fi
    sleep 0.1
  done
fi
