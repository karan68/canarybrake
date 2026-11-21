#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

for svc in copilot-console order-replayer flag-edge-gateway payment-sla-ledger; do
  pidfile="$ROOT/${svc}.pid"
  if [ -f "$pidfile" ]; then
    pid="$(cat "$pidfile" || true)"
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" || true
      echo "Stopped $svc (PID $pid)"
    fi
    rm -f "$pidfile"
  fi
done
