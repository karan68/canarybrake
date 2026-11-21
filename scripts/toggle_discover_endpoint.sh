#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="${1:-status}"
TARGET="$ROOT/services/flag-edge-gateway/discover_routes.go"

case "$MODE" in
  enable)
    cat > "$TARGET" <<'GOEOF'
package main

import "net/http"

// Live --discover endpoint: excludes targeted merchant IDs from a canary flag cohort without disabling the flag globally.
func registerLiveDiscoverRoutes(mux *http.ServeMux, g *GatewayServer) {
	mux.HandleFunc("POST /api/flags/{flag_key}/exclude-merchants/{merchant_ids}", g.handleExcludeMerchants)
}
GOEOF
    echo "ENABLED live discover endpoint: POST /api/flags/{flag_key}/exclude-merchants/{merchant_ids}"
    ;;
  disable)
    cat > "$TARGET" <<'GOEOF'
package main

import "net/http"

// Baseline state: cohort-exclude route not yet exposed (4 operational GET endpoints).
// Run scripts/toggle_discover_endpoint.sh enable before `synapse build --discover`.
func registerLiveDiscoverRoutes(_ *http.ServeMux, _ *GatewayServer) {
}
GOEOF
    echo "DISABLED live discover endpoint (restored 4-endpoint baseline)"
    ;;
  *)
    grep -n "HandleFunc" "$TARGET" || echo "Currently in 4-endpoint baseline mode"
    ;;
esac
