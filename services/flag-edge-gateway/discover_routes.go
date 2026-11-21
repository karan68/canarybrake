package main

import "net/http"

// Baseline state: cohort-exclude route not yet exposed (4 operational GET endpoints).
// Run scripts/toggle_discover_endpoint.sh enable before `synapse build --discover`.
func registerLiveDiscoverRoutes(_ *http.ServeMux, _ *GatewayServer) {
}
