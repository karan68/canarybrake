#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/env.sh"

echo "=== [1/4] Go flag-edge-gateway Race & Unit Tests ==="
(cd "$ROOT/services/flag-edge-gateway" && go test -v -race ./...)

echo "=== [2/4] Phase 1 Real Olist Ingestion & Replayer Tests ==="
(cd "$ROOT" && pytest services/order-replayer/tests -v)

echo "=== [3/4] Phase 3 Python Ledger & 200-Order Live Cross-Service Tests ==="
(cd "$ROOT" && pytest services/payment-sla-ledger/tests -v)

echo "=== [4/4] Phase 4 & 5 Live Synapse MCP + --discover + Gemini Copilot E2E Tests ==="
(cd "$ROOT" && pytest tests/test_phase4_mcp_e2e.py services/copilot-console/tests/test_phase5_copilot_e2e.py -v)
