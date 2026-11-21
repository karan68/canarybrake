# CanaryBrake — System Architecture & Cross-System MCP Design

## 1. Operational Problem
During canary checkout rollouts, incident response splits across two isolated engineering silos:

1. **System 1 — `flag-edge-gateway` (Go 1.23 · `:8081` · `edge_flags.db`):**
   Evaluates deterministic `SHA-256(flag_key:merchant_id) % 100` percentage rollouts (`checkout_gateway_v2`, `smart_retry_v1`, `fraud_guard_v3`), applies upstream retry rules, and records HTTP status codes (`200`, `503`, `504`) and wall-clock latencies. It stores **no** bank-hold locks, currency values, or Merchant SLA contracts.
2. **System 2 — `payment-sla-ledger` (Python FastAPI · `:8082` · `payment_ledger.db`):**
   Enforces `AUTHORIZED_HOLD` -> `CAPTURED` / `HOLD_STRANDED` / `REPLAYED_SAFE` transitions, `409 DOUBLE_CHARGE_PREVENTED` idempotency locks, and contractual Merchant SLA breach windows. It stores **no** feature-flag evaluation metadata.

## 2. Synapse MCP Surface & Least-Privilege Curation

- **MCP Server #1 (`flag-edge-gateway` · Go Hosted Auto):**
  - Baseline: 4 diagnostic `GET` routes (`GET /api/flags`, `GET /api/telemetry/cohorts`, `GET /api/telemetry/cohorts/{flag_key}/orders`, `GET /api/telemetry/merchants/{merchant_id}/orders`).
  - Withheld: `POST /checkout` (live buyer ingress), `POST /internal/reset` (state wipe), `GET /internal/dashboard-state`, `GET /health`.
  - Live Evolution (`synapse build --discover`): Adds `POST /api/flags/{flag_key}/exclude-merchants/{merchant_ids}`, updating Hosted Server `e2d50a55-9ebc-4aee-8a17-b47ced0025aa` in place (`4 -> 5` tools) without modifying `SYNAPSE_SERVER_ID`.

- **MCP Server #2 (`payment-sla-ledger` · Python Hosted Auto):**
  - Exposed: 4 read routes (`GET /api/ledger/stranded-summary`, `GET /api/ledger/merchant-slas`, `GET /api/ledger/holds/by-merchants/{merchant_ids}`, `GET /api/ledger/holds/by-orders/{order_ids}`).
  - Withheld: `POST /internal/authorize`, `POST /internal/capture-v1`, `POST /internal/capture-v2`, `POST /internal/reset`.

- **MCP Server #3 (`replay-stranded-merchant-holds` · Python Custom Mode):**
  - Generated via `synapse build --local --custom` into `services/payment-sla-ledger/replay_stranded_merchant_holds_server.py`.
  - Composes `inspect_stranded_merchant_holds`, `execute_idempotent_v1_replay`, and `issue_sla_recovery_receipt` behind a mandatory `confirm: bool = False` dry-run preview gate.

## 3. Single-Task Actor MCP Hub & Studio Control Plane (`:3000`)
`services/copilot-console/mcp_hub.py` runs all three MCP `stdio` client sessions inside a dedicated owner `asyncio.Task` driven by an `asyncio.Queue`, guaranteeing `anyio` cancel-scope affinity across live `synapse build --discover` reloads with zero leaked child processes.
