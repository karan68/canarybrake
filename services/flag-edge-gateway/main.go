package main

import (
	"log"
	"net/http"
	"os"
	"path/filepath"
)

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	root := envOr("CANARYBRAKE_ROOT", "../..")
	dbPath := envOr("EDGE_FLAGS_DB", filepath.Join(root, "data", "edge_flags.db"))
	ledgerURL := envOr("LEDGER_BASE_URL", "http://127.0.0.1:8082")
	addr := envOr("GATEWAY_ADDR", "127.0.0.1:8081")

	store, err := NewStore(dbPath)
	if err != nil {
		log.Fatalf("failed to open edge_flags.db: %v", err)
	}
	defer store.Close()

	srv := NewGatewayServer(store, ledgerURL)
	log.Printf("flag-edge-gateway listening on http://%s (ledger=%s, db=%s)", addr, ledgerURL, dbPath)
	if err := http.ListenAndServe(addr, srv.BuildMux()); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
