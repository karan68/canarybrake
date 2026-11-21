"""Phase 1 end-to-end verification tests for real Olist ingestion and HTTP OrderStreamReplayer."""
from __future__ import annotations

import asyncio
import os
import sqlite3
import sys
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

ROOT = Path(os.environ.get("CANARYBRAKE_ROOT", str(Path(__file__).resolve().parents[3])))
sys.path.insert(0, str(ROOT / "services" / "order-replayer"))
import replayer as replayer_mod  # noqa: E402

DB_PATH = ROOT / "data" / "olist_source.db"


def test_real_olist_database_integrity():
    assert DB_PATH.exists(), "olist_source.db must exist"
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)

    merchants_cnt = conn.execute("SELECT COUNT(*) FROM merchants").fetchone()[0]
    events_cnt = conn.execute("SELECT COUNT(*) FROM checkout_events").fetchone()[0]
    tier1_cnt = conn.execute(
        "SELECT COUNT(*) FROM merchants WHERE sla_tier = 'ENTERPRISE_TIER_1'"
    ).fetchone()[0]
    zero_or_neg = conn.execute(
        "SELECT COUNT(*) FROM checkout_events WHERE amount_cents <= 0"
    ).fetchone()[0]
    multi_inst = conn.execute(
        "SELECT COUNT(*) FROM checkout_events WHERE installments > 1"
    ).fetchone()[0]

    assert merchants_cnt == 3095
    assert events_cnt > 95000
    assert tier1_cnt == 50
    assert zero_or_neg == 0
    assert multi_inst > 40000

    # Verify Black Friday peak window contains real Enterprise Tier-1 multi-installment orders
    bf_seq = int(
        conn.execute(
            "SELECT value FROM metadata WHERE key = 'default_peak_seq_id'"
        ).fetchone()[0]
    )
    tier1_multi_in_first_300 = conn.execute(
        """
        SELECT COUNT(*)
        FROM checkout_events e
        JOIN merchants m ON m.merchant_id = e.merchant_id
        WHERE e.seq_id BETWEEN ? AND ?
          AND m.sla_tier = 'ENTERPRISE_TIER_1'
          AND e.installments > 1
        """,
        (bf_seq, bf_seq + 300),
    ).fetchone()[0]
    assert tier1_multi_in_first_300 >= 15, (
        f"Expected dense Tier-1 multi-installment traffic in peak window, got {tier1_multi_in_first_300}"
    )
    conn.close()


def test_replayer_step_start_pause_reset_against_http_receiver():
    async def _run():
        received_orders: list[dict] = []
        mock_gateway = FastAPI()

        @mock_gateway.post("/checkout")
        async def checkout(req: Request):
            body = await req.json()
            received_orders.append(body)
            if body["installments"] > 3:
                return JSONResponse({"status": "timeout"}, status_code=504)
            return JSONResponse({"status": "captured"}, status_code=200)

        rep = replayer_mod.OrderStreamReplayer(
            db_path=DB_PATH,
            gateway_checkout_url="http://mock-gateway/checkout",
            default_rps=50.0,
        )
        rep._client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=mock_gateway),
            base_url="http://mock-gateway",
        )
        try:
            start_cursor = rep.cursor_seq_id
            res = await rep.step_many(25)
            assert res["stepped"] == 25
            assert len(received_orders) == 25
            assert rep.cursor_seq_id == start_cursor + 25
            assert res["ok_2xx_in_batch"] + res["err_5xx_in_batch"] == 25
            assert res["err_5xx_in_batch"] >= 1
            # Ensure real Olist 32-char hex order_ids & merchant_ids
            assert len(received_orders[0]["order_id"]) == 32
            assert len(received_orders[0]["merchant_id"]) == 32
            assert received_orders[0]["idempotency_key"].startswith("idem_")

            # Background start/pause test
            rep.start_background(rps=80.0)
            await asyncio.sleep(0.18)
            await rep.pause_background()
            assert rep.running is False
            assert rep.total_dispatched > 25

            # Reset test
            await rep.reset(seek_seq_id=100)
            st = rep.status()
            assert st["cursor_seq_id"] == 100
            assert st["total_dispatched"] == 0
        finally:
            await rep.stop_client()

    asyncio.run(_run())
