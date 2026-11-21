#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/env.sh"

curl -fsS -X POST "http://127.0.0.1:3000/api/demo/reset-all" >/dev/null
echo "✓ Demo reset to clean 4-tool Go baseline (9 total MCP tools) with 120 fresh real Olist orders."
