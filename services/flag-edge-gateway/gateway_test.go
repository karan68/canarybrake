package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func findMerchantForFlag(t *testing.T, s *Store, targetFlag string) string {
	t.Helper()
	for i := 0; i < 5000; i++ {
		mid := fmt.Sprintf("merch_candidate_%04d", i)
		fk, _, err := s.EvaluateCheckoutCohort(mid)
		if err != nil {
			t.Fatalf("evaluate error: %v", err)
		}
		if fk == targetFlag {
			return mid
		}
	}
	t.Fatalf("failed to find merchant landing in %s", targetFlag)
	return ""
}

func TestGatewayCohortsRetryTimeoutsAndExclusion(t *testing.T) {
	dbFile := filepath.Join(t.TempDir(), "test_flags.db")
	store, err := NewStore(dbFile)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer store.Close()

	var mu sync.Mutex
	authorizedOrders := map[string]bool{}
	capturedOrders := map[string]bool{}
	strandedOrders := map[string]bool{}

	// Plain HandlerFunc switch so tree-sitter route scanner only indexes real gateway routes
	ledgerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var c CheckoutRequest
		_ = json.NewDecoder(r.Body).Decode(&c)
		mu.Lock()
		defer mu.Unlock()
		switch r.URL.Path {
		case "/internal/authorize":
			authorizedOrders[c.OrderID] = true
			writeJSON(w, http.StatusOK, map[string]any{"status": "AUTHORIZED_HOLD", "order_id": c.OrderID})
		case "/internal/capture-v1":
			if r.URL.Query().Get("contend") == "1" {
				writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "transient queue contention"})
				return
			}
			capturedOrders[c.OrderID] = true
			writeJSON(w, http.StatusOK, map[string]any{"status": "CAPTURED", "order_id": c.OrderID})
		case "/internal/capture-v2":
			if c.Installments > 1 || c.PaymentMethod == "boleto" {
				strandedOrders[c.OrderID] = true
				writeJSON(w, http.StatusGatewayTimeout, map[string]any{"status": "HOLD_STRANDED", "order_id": c.OrderID})
				return
			}
			capturedOrders[c.OrderID] = true
			writeJSON(w, http.StatusOK, map[string]any{"status": "CAPTURED", "order_id": c.OrderID})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ledgerSrv.Close()

	gw := NewGatewayServer(store, ledgerSrv.URL)
	gw.retryDelay = 2 * time.Millisecond
	gwSrv := httptest.NewServer(gw.BuildMux())
	defer gwSrv.Close()

	upiMerchant := findMerchantForFlag(t, store, "checkout_gateway_v2")
	retryMerchant := findMerchantForFlag(t, store, "smart_retry_v1")

	postCheckout := func(c CheckoutRequest) int {
		b, _ := json.Marshal(c)
		resp, err := http.Post(gwSrv.URL+"/checkout", "application/json", bytes.NewReader(b))
		if err != nil {
			t.Fatalf("post /checkout: %v", err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}

	st1 := postCheckout(CheckoutRequest{
		SeqID: 1, OrderID: "ord_v2_fail_1", MerchantID: upiMerchant,
		IdempotencyKey: "idem_1", PaymentMethod: "credit_card", Installments: 4, AmountCents: 25000,
	})
	if st1 != http.StatusGatewayTimeout {
		t.Fatalf("expected 504 on multi-installment v2 order, got %d", st1)
	}
	if !authorizedOrders["ord_v2_fail_1"] || !strandedOrders["ord_v2_fail_1"] {
		t.Fatalf("expected ord_v2_fail_1 authorized and stranded")
	}

	st2 := postCheckout(CheckoutRequest{
		SeqID: 2, OrderID: "ord_v2_ok_2", MerchantID: upiMerchant,
		IdempotencyKey: "idem_2", PaymentMethod: "credit_card", Installments: 1, AmountCents: 9900,
	})
	if st2 != http.StatusOK {
		t.Fatalf("expected 200 on 1-installment v2 order, got %d", st2)
	}

	st3 := postCheckout(CheckoutRequest{
		SeqID: 3, OrderID: "ord_retry_ok_3", MerchantID: retryMerchant,
		IdempotencyKey: "idem_3", PaymentMethod: "credit_card", Installments: 6, AmountCents: 42000,
	})
	if st3 != http.StatusOK || !capturedOrders["ord_retry_ok_3"] {
		t.Fatalf("expected smart_retry_v1 order to recover to 200 CAPTURED, got %d", st3)
	}

	respCohorts, err := http.Get(gwSrv.URL + "/api/telemetry/cohorts")
	if err != nil {
		t.Fatalf("get cohorts: %v", err)
	}
	defer respCohorts.Body.Close()
	var cohortPayload struct {
		Cohorts []CohortTelemetrySummary `json:"cohorts"`
	}
	_ = json.NewDecoder(respCohorts.Body).Decode(&cohortPayload)
	if len(cohortPayload.Cohorts) < 2 {
		t.Fatalf("expected >=2 cohorts, got %+v", cohortPayload.Cohorts)
	}

	// Invoke handleExcludeMerchants directly via SetPathValue so test file adds zero HandleFunc needles
	exReq := httptest.NewRequest(http.MethodPost, "/api/flags/checkout_gateway_v2/exclude-merchants/"+upiMerchant, nil)
	exReq.SetPathValue("flag_key", "checkout_gateway_v2")
	exReq.SetPathValue("merchant_ids", upiMerchant)
	exRec := httptest.NewRecorder()
	gw.handleExcludeMerchants(exRec, exReq)
	if exRec.Code != http.StatusOK {
		t.Fatalf("expected 200 on handleExcludeMerchants, got %d", exRec.Code)
	}

	st4 := postCheckout(CheckoutRequest{
		SeqID: 4, OrderID: "ord_rescued_4", MerchantID: upiMerchant,
		IdempotencyKey: "idem_4", PaymentMethod: "credit_card", Installments: 6, AmountCents: 31000,
	})
	if st4 != http.StatusOK || !capturedOrders["ord_rescued_4"] {
		t.Fatalf("expected excluded merchant multi-installment order to succeed 200 CAPTURED, got %d", st4)
	}
}
