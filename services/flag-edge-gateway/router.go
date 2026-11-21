package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type GatewayServer struct {
	store         *Store
	ledgerBaseURL string
	httpClient    *http.Client
	retryDelay    time.Duration
}

func NewGatewayServer(store *Store, ledgerBaseURL string) *GatewayServer {
	return &GatewayServer{
		store:         store,
		ledgerBaseURL: strings.TrimRight(ledgerBaseURL, "/"),
		httpClient:    &http.Client{Timeout: 5 * time.Second},
		retryDelay:    35 * time.Millisecond,
	}
}

func (g *GatewayServer) BuildMux() *http.ServeMux {
	mux := http.NewServeMux()

	// Curated operational routes exposed via Synapse Auto MCP Server #1
	mux.HandleFunc("GET /api/flags", g.handleListFlags)
	mux.HandleFunc("GET /api/telemetry/cohorts", g.handleCohortSummaries)
	mux.HandleFunc("GET /api/telemetry/cohorts/{flag_key}/orders", g.handleOrdersByCohort)
	mux.HandleFunc("GET /api/telemetry/merchants/{merchant_id}/orders", g.handleOrdersByMerchant)

	// Live --discover extension hook (empty in baseline; adds cohort exclusion route when enabled)
	registerLiveDiscoverRoutes(mux, g)

	// Internal ingress & dashboard routes (intentionally excluded from MCP surface!)
	mux.HandleFunc("GET /health", g.handleHealth)
	mux.HandleFunc("POST /checkout", g.handleCheckout)
	mux.HandleFunc("POST /internal/reset", g.handleReset)
	mux.HandleFunc("GET /internal/dashboard-state", g.handleDashboardState)

	return mux
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func (g *GatewayServer) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "service": "flag-edge-gateway"})
}

func (g *GatewayServer) handleListFlags(w http.ResponseWriter, _ *http.Request) {
	flags, err := g.store.ListFlags()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"flags": flags, "count": len(flags)})
}

func (g *GatewayServer) handleCohortSummaries(w http.ResponseWriter, _ *http.Request) {
	cohorts, err := g.store.CohortSummaries()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"cohorts": cohorts, "count": len(cohorts)})
}

func (g *GatewayServer) handleOrdersByCohort(w http.ResponseWriter, r *http.Request) {
	flagKey := strings.TrimSpace(r.PathValue("flag_key"))
	orders, err := g.store.OrdersByCohort(flagKey, 80)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"flag_key": flagKey, "orders": orders, "count": len(orders)})
}

func (g *GatewayServer) handleOrdersByMerchant(w http.ResponseWriter, r *http.Request) {
	raw := strings.TrimSpace(r.PathValue("merchant_id"))
	mids := strings.Split(raw, ",")
	orders, err := g.store.OrdersByMerchants(mids, 120)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"merchant_ids": mids, "orders": orders, "count": len(orders)})
}

func (g *GatewayServer) handleExcludeMerchants(w http.ResponseWriter, r *http.Request) {
	flagKey := strings.TrimSpace(r.PathValue("flag_key"))
	pathMerchants := strings.TrimSpace(r.PathValue("merchant_ids"))

	var mids []string
	if pathMerchants != "" {
		mids = append(mids, strings.Split(pathMerchants, ",")...)
	}
	if r.Body != nil {
		var body struct {
			MerchantIDs []string `json:"merchant_ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err == nil {
			mids = append(mids, body.MerchantIDs...)
		}
	}
	if len(mids) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "merchant_ids required"})
		return
	}
	updated, err := g.store.ExcludeMerchants(flagKey, mids)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":                 true,
		"flag_key":           flagKey,
		"excluded_merchants": updated,
		"excluded_count":     len(updated),
		"routing_behavior":   "Excluded merchants immediately route to fallback_variant (v1_stable)",
	})
}

func (g *GatewayServer) handleReset(w http.ResponseWriter, _ *http.Request) {
	if err := g.store.ResetState(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "reset": true})
}

func (g *GatewayServer) handleDashboardState(w http.ResponseWriter, _ *http.Request) {
	flags, err := g.store.ListFlags()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	cohorts, err := g.store.CohortSummaries()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	recent, err := g.store.RecentTelemetry(30)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"flags":            flags,
		"cohorts":          cohorts,
		"recent_telemetry": recent,
	})
}

func (g *GatewayServer) postLedger(path string, payload any) (int, []byte, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return 0, nil, err
	}
	req, err := http.NewRequest(http.MethodPost, g.ledgerBaseURL+path, bytes.NewReader(raw))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := g.httpClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, body, nil
}

func (g *GatewayServer) handleCheckout(w http.ResponseWriter, r *http.Request) {
	var chk CheckoutRequest
	if err := json.NewDecoder(r.Body).Decode(&chk); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}
	if chk.OrderID == "" || chk.MerchantID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "order_id and merchant_id required"})
		return
	}

	start := time.Now()
	flagKey, variant, err := g.store.EvaluateCheckoutCohort(chk.MerchantID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	// Step 1: Authorize payment hold in System 2 (payment-sla-ledger)
	authStatus, authBody, err := g.postLedger("/internal/authorize", chk)
	if err != nil {
		_ = g.store.RecordTelemetry(chk.OrderID, chk.MerchantID, flagKey, variant, 502, 502, 0, time.Since(start))
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": fmt.Sprintf("ledger unreachable: %v", err)})
		return
	}
	if authStatus < 200 || authStatus >= 300 {
		_ = g.store.RecordTelemetry(chk.OrderID, chk.MerchantID, flagKey, variant, authStatus, authStatus, 0, time.Since(start))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(authStatus)
		_, _ = w.Write(authBody)
		return
	}

	// Step 2: Route capture call according to evaluated feature-flag cohort
	initialStatus := 200
	finalStatus := 200
	retries := 0
	var finalBody []byte

	switch variant {
	case "v2_canary":
		// Canary v2 rail: fails with 504 timeout on multi-installment / boleto orders after AUTHORIZED_HOLD
		st, b, callErr := g.postLedger("/internal/capture-v2", chk)
		if callErr != nil {
			initialStatus, finalStatus = 504, 504
			finalBody = []byte(`{"error":"v2 capture gateway timeout"}`)
		} else {
			initialStatus, finalStatus = st, st
			finalBody = b
		}
	case "v1_smart_retry":
		// Smart retry v1: deterministic queue contention on every 3rd order -> 503 on attempt 1, recovers 200 on retry!
		contendPath := "/internal/capture-v1"
		if chk.SeqID%3 == 0 {
			contendPath = "/internal/capture-v1?contend=1"
		}
		st, b, callErr := g.postLedger(contendPath, chk)
		if callErr != nil {
			initialStatus, finalStatus = 503, 503
		} else {
			initialStatus = st
			finalStatus = st
			finalBody = b
		}
		if initialStatus == http.StatusServiceUnavailable {
			retries = 1
			time.Sleep(g.retryDelay)
			st2, b2, callErr2 := g.postLedger("/internal/capture-v1", chk)
			if callErr2 == nil {
				finalStatus = st2
				finalBody = b2
			}
		}
	default:
		st, b, callErr := g.postLedger("/internal/capture-v1", chk)
		if callErr != nil {
			initialStatus, finalStatus = 502, 502
			finalBody = []byte(`{"error":"v1 capture error"}`)
		} else {
			initialStatus, finalStatus = st, st
			finalBody = b
		}
	}

	elapsed := time.Since(start)
	_ = g.store.RecordTelemetry(chk.OrderID, chk.MerchantID, flagKey, variant, initialStatus, finalStatus, retries, elapsed)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(finalStatus)
	if len(finalBody) > 0 {
		_, _ = w.Write(finalBody)
	} else {
		_, _ = w.Write([]byte(`{"status":"completed"}`))
	}
}
