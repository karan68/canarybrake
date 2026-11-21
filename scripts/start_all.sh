#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/env.sh"

"$ROOT/scripts/stop_all.sh"
"$ROOT/scripts/run_live_discover.sh" disable

wait_ok() {
  local name="$1" url="$2"
  for _ in $(seq 1 60); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      echo "✓ $name healthy ($url)"
      return 0
    fi
    sleep 0.25
  done
  echo "✖ $name failed to become healthy ($url)" >&2
  exit 1
}

echo "Starting System 2: payment-sla-ledger (:8082)..."
(
  cd "$ROOT/services/payment-sla-ledger"
  nohup python -m uvicorn app:app --host 127.0.0.1 --port 8082 --log-level info \
    </dev/null > "$ROOT/payment-sla-ledger.log" 2>&1 &
  echo $! > "$ROOT/payment-sla-ledger.pid"
)
wait_ok "payment-sla-ledger" "http://127.0.0.1:8082/health"

echo "Starting System 1: flag-edge-gateway (:8081)..."
(
  cd "$ROOT/services/flag-edge-gateway"
  nohup ./flag-edge-gateway </dev/null > "$ROOT/flag-edge-gateway.log" 2>&1 &
  echo $! > "$ROOT/flag-edge-gateway.pid"
)
wait_ok "flag-edge-gateway" "http://127.0.0.1:8081/health"

echo "Starting Real Olist Order Replayer (:8083)..."
(
  cd "$ROOT/services/order-replayer"
  nohup python -m uvicorn replayer:app --host 127.0.0.1 --port 8083 --log-level info \
    </dev/null > "$ROOT/order-replayer.log" 2>&1 &
  echo $! > "$ROOT/order-replayer.pid"
)
wait_ok "order-replayer" "http://127.0.0.1:8083/health"

echo "Starting CanaryBrake Live Incident Console & MCP Copilot (:3000)..."
(
  cd "$ROOT/services/copilot-console"
  nohup python -m uvicorn app:app --host 127.0.0.1 --port 3000 --log-level info \
    </dev/null > "$ROOT/copilot-console.log" 2>&1 &
  echo $! > "$ROOT/copilot-console.pid"
)
wait_ok "copilot-console" "http://127.0.0.1:3000/health"

echo "Resetting databases and warming up 120 real Olist commercial orders..."
curl -fsS -X POST "http://127.0.0.1:3000/api/demo/reset-all" >/dev/null

echo ""
echo "================================================================="
echo " CanaryBrake Live Stack Running!"
echo " Console & MCP Copilot : http://127.0.0.1:3000"
echo " Classic UI Fallback   : http://127.0.0.1:3000/classic"
echo " Go Flag Gateway       : http://127.0.0.1:8081"
echo " Python Payment Ledger : http://127.0.0.1:8082"
echo " Olist Order Replayer  : http://127.0.0.1:8083"
echo "================================================================="
