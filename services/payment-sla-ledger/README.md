# System 2 — `payment-sla-ledger` (Python FastAPI · `:8082`)

Standalone payment authorization, settlement state machine, and contractual Merchant SLA ledger backed by SQLite WAL (`payment_ledger.db`).

- Enforces `AUTHORIZED_HOLD` -> `CAPTURED` / `HOLD_STRANDED` / `REPLAYED_SAFE` transitions and `409 DOUBLE_CHARGE_PREVENTED` `idempotency_key` locks.
- Tracks Enterprise Tier-1 (`>=95.0%` target) and Growth Tier-2 (`>=88.0%` target) Merchant SLA compliance across 3,095 real Olist sellers.
- **Silo boundary:** Stores zero feature-flag cohort or rollout evaluation columns.
- **Synapse Hosted Auto Server ID:** `059607eb-feb9-4fa8-874e-907d522e6ebd` (4 curated read endpoints exposed; raw `/internal/*` mutation endpoints withheld).
- **Synapse Custom Mode Server:** [`replay_stranded_merchant_holds_server.py`](./replay_stranded_merchant_holds_server.py) composing `inspect_stranded_merchant_holds`, `execute_idempotent_v1_replay`, and `issue_sla_recovery_receipt` behind a mandatory `confirm: bool` dry-run confirmation gate.
