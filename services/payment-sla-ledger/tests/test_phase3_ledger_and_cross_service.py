"""Phase 3 rigorous unit + live Go<->Python real-Olist cross-service integration tests."""
from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

ROOT = Path(os.environ.get("CANARYBRAKE_ROOT", str(Path(__file__).resolve().parents[3])))
sys.path.insert(0, str(ROOT / "services" / "payment-sla-ledger"))
sys.path.insert(0, str(ROOT / "services" / "order-replayer"))

import app as ledger_app_mod  # noqa: E402
from ledger_ops import db as ldb  # noqa: E402
from ledger_ops import remediation as lrem  # noqa: E402
import replayer as replayer_mod  # noqa: E402


def test_ledger_idempotency_double_charge_guard_and_remediation(tmp_path, monkeypatch):
    test_db = tmp_path / "test_ledger.db"
    monkeypatch.setenv("LEDGER_DB_PATH", str(test_db))
    ldb.init_ledger_db(db_path=test_db)

    # Pick a real ENTERPRISE_TIER_1 merchant imported from olist_source.db
    with ldb.connect_ledger(test_db) as conn:
        t1 = conn.execute(
            "SELECT merchant_id FROM merchants WHERE sla_tier = 'ENTERPRISE_TIER_1' LIMIT 1"
        ).fetchone()
    assert t1 is not None
    mid = t1["merchant_id"]

    # 1. Authorize + v2 multi-installment failure -> HOLD_STRANDED
    c1, b1 = ldb.authorize_intent(
        {
            "order_id": "ord_t1_001",
            "merchant_id": mid,
            "idempotency_key": "idem_ord_t1_001_1",
            "payment_method": "credit_card",
            "installments": 6,
            "amount_cents": 45000,
        },
        db_path=test_db,
    )
    assert c1 == 200 and b1["status"] == "AUTHORIZED_HOLD"

    c2, b2 = ldb.capture_v2_intent(
        {
            "order_id": "ord_t1_001",
            "merchant_id": mid,
            "idempotency_key": "idem_ord_t1_001_1",
            "payment_method": "credit_card",
            "installments": 6,
            "amount_cents": 45000,
        },
        db_path=test_db,
    )
    assert c2 == 504 and b2["status"] == "HOLD_STRANDED"

    # 2. Naive retry with NEW idempotency_key MUST be blocked with 409 DOUBLE_CHARGE_PREVENTED!
    c_dup, b_dup = ldb.authorize_intent(
        {
            "order_id": "ord_t1_001",
            "merchant_id": mid,
            "idempotency_key": "idem_NAIVE_NEW_KEY",
            "payment_method": "credit_card",
            "installments": 6,
            "amount_cents": 45000,
        },
        db_path=test_db,
    )
    assert c_dup == 409 and b_dup["error"] == "DOUBLE_CHARGE_PREVENTED"

    # 3. Inspect + idempotent replay + SHA-256 receipt
    preview = lrem.inspect_stranded_merchant_holds(mid)
    assert preview["eligible"] is True
    assert preview["stranded_count"] == 1
    assert preview["stranded_amount_cents"] == 45000
    assert preview["all_have_active_bank_holds"] is True

    replayed = lrem.execute_idempotent_v1_replay(mid)
    assert replayed["replayed_count"] == 1
    assert replayed["replayed_amount_cents"] == 45000
    assert replayed["duplicate_charges_created"] == 0

    rcpt = lrem.issue_sla_recovery_receipt(mid, replayed["replayed_count"], replayed["replayed_amount_cents"])
    assert rcpt["receipt_id"].startswith("rcpt_")
    assert len(rcpt["sha256_signature"]) == 64

    # Second replay must be a safe idempotent no-op
    replayed_again = lrem.execute_idempotent_v1_replay(mid)
    assert replayed_again["replayed_count"] == 0


def _wait_http_ok(url: str, timeout_s: float = 8.0) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            r = httpx.get(url, timeout=1.0)
            if r.status_code == 200:
                return
        except Exception:
            pass
        time.sleep(0.1)
    raise RuntimeError(f"Service did not become healthy: {url}")


def test_live_200_real_olist_orders_across_go_gateway_and_python_ledger(tmp_path, monkeypatch):
    edge_db = tmp_path / "live_edge_flags.db"
    ledger_db = tmp_path / "live_payment_ledger.db"
    monkeypatch.setenv("LEDGER_DB_PATH", str(ledger_db))

    py_env = os.environ.copy()
    py_env["LEDGER_DB_PATH"] = str(ledger_db)
    py_proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "app:app",
            "--host",
            "127.0.0.1",
            "--port",
            "18082",
            "--log-level",
            "warning",
        ],
        cwd=str(ROOT / "services" / "payment-sla-ledger"),
        env=py_env,
    )

    go_env = os.environ.copy()
    go_env["EDGE_FLAGS_DB"] = str(edge_db)
    go_env["LEDGER_BASE_URL"] = "http://127.0.0.1:18082"
    go_env["GATEWAY_ADDR"] = "127.0.0.1:18081"
    go_proc = subprocess.Popen(
        [str(ROOT / "services" / "flag-edge-gateway" / "flag-edge-gateway")],
        cwd=str(ROOT / "services" / "flag-edge-gateway"),
        env=go_env,
    )

    try:
        _wait_http_ok("http://127.0.0.1:18082/health")
        _wait_http_ok("http://127.0.0.1:18081/health")

        # Stream 200 REAL Black Friday Olist commercial orders through Go -> Python!
        async def _stream_200():
            rep = replayer_mod.OrderStreamReplayer(
                db_path=ROOT / "data" / "olist_source.db",
                gateway_checkout_url="http://127.0.0.1:18081/checkout",
            )
            try:
                return await rep.step_many(200)
            finally:
                await rep.stop_client()

        batch_res = asyncio.run(_stream_200())
        assert batch_res["stepped"] == 200
        assert batch_res["err_5xx_in_batch"] >= 10
        assert batch_res["ok_2xx_in_batch"] >= 100

        cohorts = httpx.get("http://127.0.0.1:18081/api/telemetry/cohorts").json()["cohorts"]
        by_flag = {c["flag_key"]: c for c in cohorts}

        # Prove smart_retry_v1 had real 503 contention that 100% recovered to 200 OK ($0 stranded!)
        assert by_flag["smart_retry_v1"]["initial_503_count"] >= 5
        assert by_flag["smart_retry_v1"]["retry_recovered_count"] == by_flag["smart_retry_v1"]["initial_503_count"]
        assert by_flag["smart_retry_v1"]["timeout_504_count"] == 0

        # Prove checkout_gateway_v2 caused 100% of 504 timeouts & stranded holds!
        v2_504_count = by_flag["checkout_gateway_v2"]["timeout_504_count"]
        assert v2_504_count >= 10

        summary = httpx.get("http://127.0.0.1:18082/api/ledger/stranded-summary").json()
        assert summary["stranded_hold_count"] == v2_504_count
        assert summary["stranded_amount_cents"] > 0

        # Verify smart_retry_v1 orders have ZERO stranded holds in Python ledger
        retry_orders = httpx.get(
            "http://127.0.0.1:18081/api/telemetry/cohorts/smart_retry_v1/orders"
        ).json()["orders"]
        retry_oids = ",".join(o["order_id"] for o in retry_orders[:25])
        retry_ledger = httpx.get(
            f"http://127.0.0.1:18082/api/ledger/holds/by-orders/{retry_oids}"
        ).json()["orders"]
        assert all(o["status"] == "CAPTURED" for o in retry_ledger)

        # Verify impacted Enterprise Tier-1 merchants appear in SLA report & can be remediated cleanly
        slas = httpx.get("http://127.0.0.1:18082/api/ledger/merchant-slas").json()["merchants"]
        impacted_tier1 = [
            m["merchant_id"]
            for m in slas
            if m["sla_tier"] == "ENTERPRISE_TIER_1" and m["stranded_holds"] > 0
        ]
        assert len(impacted_tier1) >= 1

        csv_mids = ",".join(impacted_tier1)
        rep_out = lrem.execute_idempotent_v1_replay(csv_mids)
        assert rep_out["replayed_count"] >= 1
        after_holds = httpx.get(
            f"http://127.0.0.1:18082/api/ledger/holds/by-merchants/{csv_mids}"
        ).json()
        assert after_holds["active_bank_hold_stranded_count"] == 0
    finally:
        go_proc.terminate()
        py_proc.terminate()
        go_proc.wait(timeout=5)
        py_proc.wait(timeout=5)
