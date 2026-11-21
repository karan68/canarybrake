# System 1 — `flag-edge-gateway` (Go 1.23 · `:8081`)

Standalone feature-flag rollout and checkout routing microservice backed by SQLite WAL (`edge_flags.db`).

- Evaluates deterministic `SHA-256(flag_key:merchant_id) % 100` cohorts across `checkout_gateway_v2` (40%), `smart_retry_v1` (55%), and `fraud_guard_v3` (30%).
- Records HTTP status transitions (`200`, `503 -> 200` retry recovery, `504` gateway timeout) and measured microsecond latencies.
- **Silo boundary:** Stores zero customer bank authorization holds, dollar values, or contractual Merchant SLA data.
- **Synapse Hosted Auto Server ID:** `e2d50a55-9ebc-4aee-8a17-b47ced0025aa` (4 diagnostic GET routes exposed initially; 5th route `POST /api/flags/{flag_key}/exclude-merchants/{merchant_ids}` added live via `synapse build --discover`).
