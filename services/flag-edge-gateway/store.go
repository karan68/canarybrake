package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

type FeatureFlag struct {
	FlagKey           string   `json:"flag_key"`
	Description       string   `json:"description"`
	RolloutPct        int      `json:"rollout_pct"`
	ActiveVariant     string   `json:"active_variant"`
	FallbackVariant   string   `json:"fallback_variant"`
	Enabled           bool     `json:"enabled"`
	ExcludedMerchants []string `json:"excluded_merchants"`
}

type CheckoutRequest struct {
	SeqID             int64  `json:"seq_id"`
	OrderID           string `json:"order_id"`
	MerchantID        string `json:"merchant_id"`
	CustomerID        string `json:"customer_id"`
	IdempotencyKey    string `json:"idempotency_key"`
	PaymentMethod     string `json:"payment_method"`
	Installments      int    `json:"installments"`
	ItemCount         int    `json:"item_count"`
	AmountCents       int64  `json:"amount_cents"`
	PurchaseTimestamp string `json:"purchase_timestamp"`
}

type TelemetryRecord struct {
	ID            int64   `json:"id"`
	OrderID       string  `json:"order_id"`
	MerchantID    string  `json:"merchant_id"`
	FlagKey       string  `json:"flag_key"`
	Variant       string  `json:"variant"`
	InitialStatus int     `json:"initial_status"`
	FinalStatus   int     `json:"final_status"`
	RetryAttempts int     `json:"retry_attempts"`
	LatencyMs     float64 `json:"latency_ms"`
	RecordedAt    string  `json:"recorded_at"`
}

type CohortTelemetrySummary struct {
	FlagKey               string   `json:"flag_key"`
	Variant               string   `json:"variant"`
	TotalRequests         int      `json:"total_requests"`
	OK200Count            int      `json:"ok_200_count"`
	Initial503Count       int      `json:"initial_503_count"`
	RetryRecoveredCount   int      `json:"retry_recovered_count"`
	Timeout504Count       int      `json:"timeout_504_count"`
	OtherErrorCount       int      `json:"other_error_count"`
	P95LatencyMs          float64  `json:"p95_latency_ms"`
	ImpactedMerchantCount int      `json:"impacted_merchant_count"`
	ImpactedMerchantIDs   []string `json:"impacted_merchant_ids"`
	SampleFailedOrderIDs  []string `json:"sample_failed_order_ids"`
}

type Store struct {
	db *sql.DB
}

func NewStore(dbPath string) (*Store, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)", dbPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	if err := s.initSchema(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) initSchema() error {
	schema := `
	CREATE TABLE IF NOT EXISTS feature_flags (
		flag_key TEXT PRIMARY KEY,
		description TEXT NOT NULL,
		rollout_pct INTEGER NOT NULL,
		active_variant TEXT NOT NULL,
		fallback_variant TEXT NOT NULL,
		enabled INTEGER NOT NULL DEFAULT 1,
		eval_priority INTEGER NOT NULL
	);

	CREATE TABLE IF NOT EXISTS flag_exclusions (
		flag_key TEXT NOT NULL,
		merchant_id TEXT NOT NULL,
		excluded_at TEXT NOT NULL,
		PRIMARY KEY (flag_key, merchant_id)
	);

	CREATE TABLE IF NOT EXISTS gateway_telemetry (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		order_id TEXT NOT NULL,
		merchant_id TEXT NOT NULL,
		flag_key TEXT NOT NULL,
		variant TEXT NOT NULL,
		initial_status INTEGER NOT NULL,
		final_status INTEGER NOT NULL,
		retry_attempts INTEGER NOT NULL,
		latency_us INTEGER NOT NULL,
		recorded_at TEXT NOT NULL
	);

	CREATE INDEX IF NOT EXISTS idx_tel_flag ON gateway_telemetry(flag_key, id DESC);
	CREATE INDEX IF NOT EXISTS idx_tel_merch ON gateway_telemetry(merchant_id, id DESC);
	`
	if _, err := s.db.Exec(schema); err != nil {
		return err
	}
	return s.seedDefaultFlags()
}

func (s *Store) seedDefaultFlags() error {
	defaults := []struct {
		key, desc, active, fallback string
		pct, prio                   int
	}{
		{
			"checkout_gateway_v2",
			"Canary Acquirer Settlement Rail v2 for marketplace checkout capture",
			"v2_canary",
			"v1_stable",
			40,
			1,
		},
		{
			"smart_retry_v1",
			"Adaptive 35ms jittered retry on transient upstream 503 pool contention",
			"v1_smart_retry",
			"v1_stable",
			55,
			2,
		},
		{
			"fraud_guard_v3",
			"Inline pre-auth velocity and BIN risk scoring prior to v1 capture",
			"v3_guard",
			"v1_stable",
			30,
			3,
		},
	}
	for _, f := range defaults {
		_, err := s.db.Exec(
			`INSERT OR IGNORE INTO feature_flags
			 (flag_key, description, rollout_pct, active_variant, fallback_variant, enabled, eval_priority)
			 VALUES (?, ?, ?, ?, ?, 1, ?)`,
			f.key, f.desc, f.pct, f.active, f.fallback, f.prio,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func HashBucket(flagKey, merchantID string) int {
	sum := sha256.Sum256([]byte(flagKey + ":" + merchantID))
	val := binary.BigEndian.Uint32(sum[:4])
	return int(val % 100)
}

func (s *Store) EvaluateCheckoutCohort(merchantID string) (string, string, error) {
	flags, err := s.ListFlags()
	if err != nil {
		return "baseline_v1", "v1_stable", err
	}
	for _, f := range flags {
		if !f.Enabled {
			continue
		}
		excluded := false
		for _, ex := range f.ExcludedMerchants {
			if ex == merchantID {
				excluded = true
				break
			}
		}
		if excluded {
			continue
		}
		if HashBucket(f.FlagKey, merchantID) < f.RolloutPct {
			return f.FlagKey, f.ActiveVariant, nil
		}
	}
	return "baseline_v1", "v1_stable", nil
}

func (s *Store) ListFlags() ([]FeatureFlag, error) {
	rows, err := s.db.Query(
		`SELECT flag_key, description, rollout_pct, active_variant, fallback_variant, enabled
		 FROM feature_flags ORDER BY eval_priority ASC`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []FeatureFlag
	for rows.Next() {
		var f FeatureFlag
		var en int
		if err := rows.Scan(&f.FlagKey, &f.Description, &f.RolloutPct, &f.ActiveVariant, &f.FallbackVariant, &en); err != nil {
			return nil, err
		}
		f.Enabled = en != 0
		f.ExcludedMerchants = []string{}
		out = append(out, f)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	exRows, err := s.db.Query(`SELECT flag_key, merchant_id FROM flag_exclusions ORDER BY excluded_at ASC`)
	if err != nil {
		return nil, err
	}
	defer exRows.Close()
	exMap := map[string][]string{}
	for exRows.Next() {
		var fk, mid string
		if err := exRows.Scan(&fk, &mid); err != nil {
			return nil, err
		}
		exMap[fk] = append(exMap[fk], mid)
	}
	for i := range out {
		if list, ok := exMap[out[i].FlagKey]; ok {
			out[i].ExcludedMerchants = list
		}
	}
	return out, nil
}

func (s *Store) ExcludeMerchants(flagKey string, merchantIDs []string) ([]string, error) {
	var exists int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM feature_flags WHERE flag_key = ?`, flagKey).Scan(&exists); err != nil {
		return nil, err
	}
	if exists == 0 {
		return nil, fmt.Errorf("unknown flag_key: %s", flagKey)
	}
	now := time.Now().UTC().Format(time.RFC3339)
	for _, raw := range merchantIDs {
		mid := strings.TrimSpace(raw)
		if mid == "" {
			continue
		}
		if _, err := s.db.Exec(
			`INSERT OR IGNORE INTO flag_exclusions (flag_key, merchant_id, excluded_at) VALUES (?, ?, ?)`,
			flagKey, mid, now,
		); err != nil {
			return nil, err
		}
	}
	flags, err := s.ListFlags()
	if err != nil {
		return nil, err
	}
	for _, f := range flags {
		if f.FlagKey == flagKey {
			return f.ExcludedMerchants, nil
		}
	}
	return []string{}, nil
}

func (s *Store) RecordTelemetry(orderID, merchantID, flagKey, variant string, initialStatus, finalStatus, retries int, latency time.Duration) error {
	_, err := s.db.Exec(
		`INSERT INTO gateway_telemetry
		 (order_id, merchant_id, flag_key, variant, initial_status, final_status, retry_attempts, latency_us, recorded_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		orderID, merchantID, flagKey, variant, initialStatus, finalStatus, retries, latency.Microseconds(), time.Now().UTC().Format(time.RFC3339Nano),
	)
	return err
}

func (s *Store) ResetState() error {
	if _, err := s.db.Exec(`DELETE FROM gateway_telemetry`); err != nil {
		return err
	}
	if _, err := s.db.Exec(`DELETE FROM flag_exclusions`); err != nil {
		return err
	}
	return nil
}

func (s *Store) CohortSummaries() ([]CohortTelemetrySummary, error) {
	rows, err := s.db.Query(
		`SELECT order_id, merchant_id, flag_key, variant, initial_status, final_status, retry_attempts, latency_us
		 FROM gateway_telemetry ORDER BY id ASC`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type acc struct {
		sum       CohortTelemetrySummary
		latencies []float64
		merchSet  map[string]struct{}
	}
	byFlag := map[string]*acc{}
	order := []string{}

	for rows.Next() {
		var oid, mid, fk, variant string
		var initSt, finSt, retries int
		var latUs int64
		if err := rows.Scan(&oid, &mid, &fk, &variant, &initSt, &finSt, &retries, &latUs); err != nil {
			return nil, err
		}
		a, ok := byFlag[fk]
		if !ok {
			a = &acc{
				sum: CohortTelemetrySummary{
					FlagKey:              fk,
					Variant:              variant,
					ImpactedMerchantIDs:  []string{},
					SampleFailedOrderIDs: []string{},
				},
				merchSet: map[string]struct{}{},
			}
			byFlag[fk] = a
			order = append(order, fk)
		}
		a.sum.TotalRequests++
		if finSt >= 200 && finSt < 300 {
			a.sum.OK200Count++
		}
		if initSt == 503 {
			a.sum.Initial503Count++
			if finSt == 200 && retries > 0 {
				a.sum.RetryRecoveredCount++
			}
		}
		if finSt == 504 {
			a.sum.Timeout504Count++
			if _, seen := a.merchSet[mid]; !seen {
				a.merchSet[mid] = struct{}{}
				a.sum.ImpactedMerchantIDs = append(a.sum.ImpactedMerchantIDs, mid)
			}
			if len(a.sum.SampleFailedOrderIDs) < 20 {
				a.sum.SampleFailedOrderIDs = append(a.sum.SampleFailedOrderIDs, oid)
			}
		} else if finSt >= 400 {
			a.sum.OtherErrorCount++
		}
		a.latencies = append(a.latencies, float64(latUs)/1000.0)
	}

	sort.Strings(order)
	out := make([]CohortTelemetrySummary, 0, len(order))
	for _, fk := range order {
		a := byFlag[fk]
		a.sum.ImpactedMerchantCount = len(a.sum.ImpactedMerchantIDs)
		if len(a.latencies) > 0 {
			sort.Float64s(a.latencies)
			idx := int(math.Ceil(0.95*float64(len(a.latencies)))) - 1
			if idx < 0 {
				idx = 0
			}
			a.sum.P95LatencyMs = math.Round(a.latencies[idx]*100) / 100
		}
		out = append(out, a.sum)
	}
	return out, nil
}

func (s *Store) OrdersByCohort(flagKey string, limit int) ([]TelemetryRecord, error) {
	if limit <= 0 || limit > 200 {
		limit = 60
	}
	rows, err := s.db.Query(
		`SELECT id, order_id, merchant_id, flag_key, variant, initial_status, final_status, retry_attempts, latency_us, recorded_at
		 FROM gateway_telemetry WHERE flag_key = ? ORDER BY id DESC LIMIT ?`,
		flagKey, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTelemetryRows(rows)
}

func (s *Store) OrdersByMerchants(merchantIDs []string, limit int) ([]TelemetryRecord, error) {
	if limit <= 0 || limit > 250 {
		limit = 100
	}
	cleaned := []string{}
	args := []any{}
	for _, m := range merchantIDs {
		t := strings.TrimSpace(m)
		if t != "" {
			cleaned = append(cleaned, "?")
			args = append(args, t)
		}
	}
	if len(cleaned) == 0 {
		return []TelemetryRecord{}, nil
	}
	args = append(args, limit)
	q := fmt.Sprintf(
		`SELECT id, order_id, merchant_id, flag_key, variant, initial_status, final_status, retry_attempts, latency_us, recorded_at
		 FROM gateway_telemetry WHERE merchant_id IN (%s) ORDER BY id DESC LIMIT ?`,
		strings.Join(cleaned, ","),
	)
	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTelemetryRows(rows)
}

func (s *Store) RecentTelemetry(limit int) ([]TelemetryRecord, error) {
	if limit <= 0 || limit > 200 {
		limit = 40
	}
	rows, err := s.db.Query(
		`SELECT id, order_id, merchant_id, flag_key, variant, initial_status, final_status, retry_attempts, latency_us, recorded_at
		 FROM gateway_telemetry ORDER BY id DESC LIMIT ?`,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTelemetryRows(rows)
}

func scanTelemetryRows(rows *sql.Rows) ([]TelemetryRecord, error) {
	out := []TelemetryRecord{}
	for rows.Next() {
		var r TelemetryRecord
		var latUs int64
		if err := rows.Scan(
			&r.ID, &r.OrderID, &r.MerchantID, &r.FlagKey, &r.Variant,
			&r.InitialStatus, &r.FinalStatus, &r.RetryAttempts, &latUs, &r.RecordedAt,
		); err != nil {
			return nil, err
		}
		r.LatencyMs = math.Round(float64(latUs)/10.0) / 100.0
		out = append(out, r)
	}
	return out, rows.Err()
}
