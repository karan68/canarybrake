# CanaryBrake

**Cross-System Canary Checkout Incident & Settlement Control Plane**  
*2nd Brain Labs × Geek Room: Vibe-a-thon — Track 3 (`A copilot that works across two systems`)*

---

## Track 3 Deliverables & Judging Matrix

| Official Track 3 Requirement | How CanaryBrake Delivers It | Where to Verify |
| :--- | :--- | :--- |
| **Two separate codebases turned into two MCP servers with Synapse** | **System 1 (`Go 1.23`):** [`services/flag-edge-gateway`](./services/flag-edge-gateway) → Hosted Auto Server `e2d50a55-9ebc-4aee-8a17-b47ced0025aa`<br/>**System 2 (`Python FastAPI`):** [`services/payment-sla-ledger`](./services/payment-sla-ledger) → Hosted Auto Server `059607eb-feb9-4fa8-874e-907d522e6ebd` + Optional Custom Server [`replay_stranded_merchant_holds_server.py`](./services/payment-sla-ledger/replay_stranded_merchant_holds_server.py) | [`mcp-config/mcp_servers.template.json`](./mcp-config/mcp_servers.template.json), [`.synapse/config.json`](./services/flag-edge-gateway/.synapse/config.json) |
| **Expose only what the agent needs (Least-Privilege Curation)** | Go exposes **4 of 8** routes (withholding `/checkout` buyer ingress & `/internal/reset`). Python Auto exposes **4 of 10** routes (withholding raw `/internal/authorize`, `/internal/capture-*`, `/internal/reset`), while mutations go strictly through a `confirm: bool` Custom tool. | UI Tab 4 (`Live Discovery`) & [`scripts/run_live_discover.sh`](./scripts/run_live_discover.sh) |
| **2+ Cross-System Questions** | **Q1 (Root-Cause vs. False Alarm):** Joins Go `503`/`504` cohorts with Python `HOLD_STRANDED` balances & SLA tiers (`smart_retry_v1` = `$0.00` lost vs `checkout_gateway_v2` = `100%` of stranded holds).<br/>**Q2 (Double-Charge Lock Audit):** Joins cohort merchant orders with active `bank_hold_active` & `idempotency_key` locks. | UI Tab 1 (`Incident Copilot` Runbook Steps 1–3) & [`test_phase5_copilot_e2e.py`](./services/copilot-console/tests/test_phase5_copilot_e2e.py) |
| **Live `synapse build --discover` Update** | Enables `POST /api/flags/{flag_key}/exclude-merchants/{merchant_ids}` in [`discover_routes.go`](./services/flag-edge-gateway/discover_routes.go), runs `synapse build --discover` updating `e2d50a55-9ebc-4aee-8a17-b47ced0025aa` in place (`4 -> 5` tools, same `SYNAPSE_SERVER_ID`), reloads MCP `stdio`, and quarantines impacted Tier-1 merchants immediately. | UI Tab 4 (`Live Discovery`) & [`test_phase4_mcp_e2e.py`](./tests/test_phase4_mcp_e2e.py) |
| **Real Data & Real App Execution** | Streams **98,665 real commercial orders** & **3,095 real merchants** from the public Olist Brazilian E-Commerce dataset over live HTTP (`:8083` -> `:8081` -> `:8082`) backed by two isolated SQLite WAL databases. | [`scripts/ingest_olist.py`](./scripts/ingest_olist.py), [`services/order-replayer`](./services/order-replayer) |

---

## Architecture & Domain Isolation

```
 [Olist Real Order Stream · 98,665 Orders / 3,095 Merchants (:8083)]
                                │
                    HTTP POST /checkout
                                ▼
 ┌───────────────────────────────────────────────────────────────────┐
 │ SYSTEM 1: flag-edge-gateway (Go 1.23 · :8081 · edge_flags.db)     │
 │ • Deterministic SHA-256(flag_key:merchant_id) % 100 cohorts       │
 │ • Logs HTTP 200, 503->Retry 200, 504 timeouts & measured latency  │
 │ • Zero financial hold, currency, or SLA columns                   │
 └──────────────┬────────────────────────────────────▲───────────────┘
                │ Live HTTP Authorize + Capture      │ MCP Server #1 (Go Hosted Auto)
                ▼                                    │ @2ndbrainlabs-ai/synapse-mcp
 ┌──────────────────────────────────────────────┐    │
 │ SYSTEM 2: payment-sla-ledger (FastAPI :8082) │    │
 │ • AUTHORIZED_HOLD -> CAPTURED / HOLD_STRANDED│    │
 │ • 409 DOUBLE_CHARGE_PREVENTED idempotency    │    │
 │ • Contractual Tier-1 Merchant SLA compliance │    │
 │ • Zero feature-flag evaluation columns       │    │
 └──────────────▲───────────────────────────────┘    │
                │ MCP Server #2 (Python Hosted Auto) │
                │ + MCP Server #3 (Python Custom)    │
                └───────────────┬────────────────────┘
                                ▼
        ┌───────────────────────────────────────────────┐
        │ CanaryBrake Control Plane & MCP Copilot (:3000)│
        │  • Studio UI (Carbon & Paper) + /classic view  │
        └───────────────────────────────────────────────┘
```

---

## Quick Start

### Prerequisites
- Go `1.22+`, Node.js `20+`, Python `3.10+`, `@2ndbrainlabs-ai/synapse-cli`
- Environment variables: `SYNAPSE_API_KEY` (Synapse Console) and `GEMINI_API_KEY`

### 1. Ingest Real Olist CSVs (First Run Only)
Place `olist_orders_dataset.csv`, `olist_order_payments_dataset.csv`, `olist_order_items_dataset.csv`, and `olist_sellers_dataset.csv` in `data/olist/`, then run:
```bash
python scripts/ingest_olist.py
```

### 2. Launch All 4 Microservices & Seed 120 Real Orders
```bash
./scripts/start_all.sh
```
Open **`http://localhost:3000`** (`http://localhost:3000/classic` for the alternate classic view, or toggle **Paper / Carbon** in the header).

### 3. 2-Minute End-to-End Demo Flow
1. **Runbook Step 1 (Triage):** Copilot queries both Go & Python MCP servers and proves `smart_retry_v1` recovered 100% of `503`s (`$0.00` stranded), while `checkout_gateway_v2` multi-installment `504` timeouts caused 100% of stranded holds.
2. **Runbook Step 2 (Hold Lock Audit):** Copilot inspects active `bank_hold_active` and `idempotency_key` locks on breaching Enterprise Tier-1 merchants so naive retries don't double-charge buyers.
3. **Runbook Step 3 -> Step 4 (Live `synapse build --discover`):** Copilot hits the read-only Go MCP capability gap -> open **Live Discovery** tab -> click **Expose exclusion route & run discover** -> Synapse rescans Go and updates `e2d50a55-9ebc-4aee-8a17-b47ced0025aa` from **4 to 5 tools** without changing client config.
4. **Runbook Step 5 -> Step 6 (Quarantine + Guarded Replay):** Copilot calls the newly discovered 5th Go tool to quarantine those Tier-1 merchants onto `v1_stable`, runs `replay_stranded_merchant_holds(confirm=False)` for dry-run preview, and on **Step 6 (`confirm=True`)** settles all stranded holds to `$0.00` and issues a signed SHA-256 recovery receipt.

### 4. Run Full Automated Verification Suite
```bash
./scripts/run_all_tests.sh
```
