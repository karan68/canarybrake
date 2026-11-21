"""System 2: CanaryBrake Payment Authorization & Merchant SLA Ledger (FastAPI :8082)."""
from __future__ import annotations

import asyncio
import contextlib
from typing import Optional

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from ledger_ops.db import (
    authorize_intent,
    capture_v1_intent,
    capture_v2_intent,
    connect_ledger,
    get_holds_by_merchants_data,
    get_holds_by_orders_data,
    get_merchant_slas_data,
    get_stranded_summary_data,
    init_ledger_db,
    reset_ledger_state,
)


class InternalCheckoutPayload(BaseModel):
    seq_id: Optional[int] = 0
    order_id: str
    merchant_id: str
    customer_id: Optional[str] = "unknown"
    idempotency_key: str
    payment_method: str = "credit_card"
    installments: int = 1
    item_count: int = 1
    amount_cents: int
    purchase_timestamp: Optional[str] = None


@contextlib.asynccontextmanager
async def lifespan(_app: FastAPI):
    init_ledger_db()
    yield


app = FastAPI(
    title="CanaryBrake Payment Authorization & Merchant SLA Ledger",
    lifespan=lifespan,
)


# ─── Curated Operational Endpoints (Exposed via Synapse Auto MCP Server #2) ───

@app.get("/api/ledger/stranded-summary")
def get_stranded_holds_summary() -> dict:
    """Return total captured, stranded hold, and safely replayed payment counts and dollar amounts across the ledger."""
    return get_stranded_summary_data()


@app.get("/api/ledger/merchant-slas")
def get_merchant_sla_status() -> dict:
    """Return Enterprise Tier-1 and Growth Tier-2 merchants with stranded holds, SLA breach percentages, and minutes to SLA breach."""
    return get_merchant_slas_data()


@app.get("/api/ledger/holds/by-merchants/{merchant_ids}")
def get_holds_by_merchants(merchant_ids: str) -> dict:
    """Inspect payment intents, active bank authorization holds, idempotency keys, and double-charge risk for comma-separated merchant_ids."""
    return get_holds_by_merchants_data(merchant_ids.split(","))


@app.get("/api/ledger/holds/by-orders/{order_ids}")
def get_holds_by_orders(order_ids: str) -> dict:
    """Inspect ledger payment state, active bank hold lock, and idempotency_key for comma-separated order_ids."""
    return get_holds_by_orders_data(order_ids.split(","))


# ─── Internal Ingress & UI Endpoints (Intentionally Excluded from MCP!) ───────

@app.get("/health")
def health_check() -> dict:
    """Internal health endpoint."""
    return {"ok": True, "service": "payment-sla-ledger"}


@app.post("/internal/authorize")
def internal_authorize(payload: InternalCheckoutPayload):
    """Internal gateway authorization hook creating AUTHORIZED_HOLD with idempotency lock."""
    code, body = authorize_intent(payload.model_dump())
    return JSONResponse(body, status_code=code)


@app.post("/internal/capture-v1")
def internal_capture_v1(
    payload: InternalCheckoutPayload,
    contend: int = Query(default=0),
):
    """Internal stable v1 capture rail."""
    code, body = capture_v1_intent(payload.model_dump(), contend=(contend == 1))
    return JSONResponse(body, status_code=code)


@app.post("/internal/capture-v2")
async def internal_capture_v2(payload: InternalCheckoutPayload):
    """Internal canary v2 capture rail (hits 115ms acquirer schedule timeout -> 504 on multi-installment or boleto orders)."""
    if payload.installments > 1 or payload.payment_method == "boleto":
        await asyncio.sleep(0.115)
    code, body = capture_v2_intent(payload.model_dump())
    return JSONResponse(body, status_code=code)


@app.post("/internal/reset")
def internal_reset() -> dict:
    """Internal demo/test state reset."""
    return reset_ledger_state()


@app.get("/internal/dashboard-state")
def internal_dashboard_state() -> dict:
    """Live state snapshot for Center Pane of CanaryBrake Incident Console."""
    with connect_ledger() as conn:
        recent = [
            dict(r)
            for r in conn.execute(
                """
                SELECT p.order_id, p.merchant_id, COALESCE(m.display_name, p.merchant_id) AS merchant_name,
                       COALESCE(m.sla_tier, 'STANDARD_SMB') AS sla_tier,
                       p.payment_method, p.installments, p.amount_cents, p.status,
                       p.bank_hold_active, p.updated_at
                FROM payment_intents p
                LEFT JOIN merchants m ON m.merchant_id = p.merchant_id
                ORDER BY p.rowid DESC
                LIMIT 30
                """
            ).fetchall()
        ]
        audits = [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM replay_audit_log ORDER BY created_at DESC LIMIT 10"
            ).fetchall()
        ]
    return {
        "summary": get_stranded_summary_data(),
        "merchant_slas": get_merchant_slas_data()["merchants"][:12],
        "recent_intents": recent,
        "audit_receipts": audits,
    }
