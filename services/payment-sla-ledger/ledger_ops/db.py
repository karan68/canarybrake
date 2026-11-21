"""SQLite WAL state machine and merchant SLA queries for payment-sla-ledger."""
from __future__ import annotations

import hashlib
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


def default_ledger_db_path() -> Path:
    root = Path(os.environ.get("CANARYBRAKE_ROOT", str(Path(__file__).resolve().parents[3])))
    return Path(os.environ.get("LEDGER_DB_PATH", str(root / "data" / "payment_ledger.db")))


def default_olist_db_path() -> Path:
    root = Path(os.environ.get("CANARYBRAKE_ROOT", str(Path(__file__).resolve().parents[3])))
    return Path(os.environ.get("OLIST_DB_PATH", str(root / "data" / "olist_source.db")))


def connect_ledger(db_path: Optional[Path] = None) -> sqlite3.Connection:
    target = Path(db_path) if db_path else default_ledger_db_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(target), timeout=10.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def init_ledger_db(db_path: Optional[Path] = None, olist_db_path: Optional[Path] = None) -> None:
    olist_path = Path(olist_db_path) if olist_db_path else default_olist_db_path()
    with connect_ledger(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS merchants (
                merchant_id TEXT PRIMARY KEY,
                display_name TEXT NOT NULL,
                city TEXT NOT NULL,
                state TEXT NOT NULL,
                gmv_rank INTEGER NOT NULL,
                sla_tier TEXT NOT NULL,
                max_stranded_pct REAL NOT NULL,
                sla_window_minutes INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS payment_intents (
                order_id TEXT PRIMARY KEY,
                merchant_id TEXT NOT NULL,
                customer_id TEXT NOT NULL,
                idempotency_key TEXT NOT NULL UNIQUE,
                payment_method TEXT NOT NULL,
                installments INTEGER NOT NULL,
                item_count INTEGER NOT NULL,
                amount_cents INTEGER NOT NULL,
                status TEXT NOT NULL,
                bank_hold_active INTEGER NOT NULL,
                authorized_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS replay_audit_log (
                receipt_id TEXT PRIMARY KEY,
                merchant_ids_csv TEXT NOT NULL,
                replayed_count INTEGER NOT NULL,
                replayed_amount_cents INTEGER NOT NULL,
                sha256_signature TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_pi_status ON payment_intents(status);
            CREATE INDEX IF NOT EXISTS idx_pi_merchant ON payment_intents(merchant_id);
            """
        )
        existing = conn.execute("SELECT COUNT(*) FROM merchants").fetchone()[0]
        if existing == 0 and olist_path.exists():
            src = sqlite3.connect(f"file:{olist_path}?mode=ro", uri=True)
            rows = src.execute(
                """
                SELECT merchant_id, display_name, city, state, gmv_rank,
                       sla_tier, max_stranded_pct, sla_window_minutes
                FROM merchants
                """
            ).fetchall()
            src.close()
            conn.executemany(
                "INSERT OR IGNORE INTO merchants VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                rows,
            )
        conn.commit()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def authorize_intent(payload: dict, db_path: Optional[Path] = None) -> tuple[int, dict]:
    order_id = payload["order_id"]
    idem_key = payload["idempotency_key"]
    ts = now_iso()
    with connect_ledger(db_path) as conn:
        row = conn.execute(
            "SELECT order_id, idempotency_key, status, bank_hold_active, amount_cents FROM payment_intents WHERE order_id = ?",
            (order_id,),
        ).fetchone()
        if row is not None:
            if row["idempotency_key"] != idem_key and row["bank_hold_active"] == 1:
                return 409, {
                    "error": "DOUBLE_CHARGE_PREVENTED",
                    "message": "Order already has an active bank authorization hold under a different idempotency_key",
                    "order_id": order_id,
                    "existing_idempotency_key": row["idempotency_key"],
                    "status": row["status"],
                }
            return 200, {
                "order_id": order_id,
                "status": row["status"],
                "idempotent_reused": True,
            }

        conn.execute(
            """
            INSERT INTO payment_intents (
                order_id, merchant_id, customer_id, idempotency_key,
                payment_method, installments, item_count, amount_cents,
                status, bank_hold_active, authorized_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'AUTHORIZED_HOLD', 1, ?, ?)
            """,
            (
                order_id,
                payload["merchant_id"],
                payload.get("customer_id", "unknown"),
                idem_key,
                payload.get("payment_method", "credit_card"),
                int(payload.get("installments", 1)),
                int(payload.get("item_count", 1)),
                int(payload["amount_cents"]),
                ts,
                ts,
            ),
        )
        conn.commit()
    return 200, {"order_id": order_id, "status": "AUTHORIZED_HOLD", "bank_hold_active": True}


def capture_v1_intent(payload: dict, contend: bool = False, db_path: Optional[Path] = None) -> tuple[int, dict]:
    if contend:
        return 503, {
            "error": "UPSTREAM_QUEUE_CONTENTION",
            "order_id": payload["order_id"],
            "retryable": True,
        }
    order_id = payload["order_id"]
    idem_key = payload["idempotency_key"]
    ts = now_iso()
    with connect_ledger(db_path) as conn:
        row = conn.execute(
            "SELECT idempotency_key, status FROM payment_intents WHERE order_id = ?",
            (order_id,),
        ).fetchone()
        if row is None:
            return 404, {"error": "INTENT_NOT_FOUND", "order_id": order_id}
        if row["idempotency_key"] != idem_key:
            return 409, {"error": "IDEMPOTENCY_KEY_MISMATCH", "order_id": order_id}
        if row["status"] in ("CAPTURED", "REPLAYED_SAFE"):
            return 200, {"order_id": order_id, "status": row["status"], "idempotent_reused": True}
        conn.execute(
            "UPDATE payment_intents SET status = 'CAPTURED', bank_hold_active = 0, updated_at = ? WHERE order_id = ?",
            (ts, order_id),
        )
        conn.commit()
    return 200, {"order_id": order_id, "status": "CAPTURED"}


def capture_v2_intent(payload: dict, db_path: Optional[Path] = None) -> tuple[int, dict]:
    order_id = payload["order_id"]
    installments = int(payload.get("installments", 1))
    pmethod = str(payload.get("payment_method", "credit_card"))
    ts = now_iso()
    with connect_ledger(db_path) as conn:
        row = conn.execute(
            "SELECT idempotency_key, status FROM payment_intents WHERE order_id = ?",
            (order_id,),
        ).fetchone()
        if row is None:
            return 404, {"error": "INTENT_NOT_FOUND", "order_id": order_id}

        # Real v2 contract defect: multi-installment (>1) and boleto settlement metadata times out,
        # leaving the customer's bank authorization hold stranded!
        if installments > 1 or pmethod == "boleto":
            conn.execute(
                "UPDATE payment_intents SET status = 'HOLD_STRANDED', bank_hold_active = 1, updated_at = ? WHERE order_id = ?",
                (ts, order_id),
            )
            conn.commit()
            return 504, {
                "error": "V2_SETTLEMENT_GATEWAY_TIMEOUT",
                "order_id": order_id,
                "status": "HOLD_STRANDED",
                "bank_hold_active": True,
            }

        conn.execute(
            "UPDATE payment_intents SET status = 'CAPTURED', bank_hold_active = 0, updated_at = ? WHERE order_id = ?",
            (ts, order_id),
        )
        conn.commit()
    return 200, {"order_id": order_id, "status": "CAPTURED"}


def get_stranded_summary_data(db_path: Optional[Path] = None) -> dict:
    with connect_ledger(db_path) as conn:
        totals = conn.execute(
            """
            SELECT
                SUM(CASE WHEN status = 'CAPTURED' THEN 1 ELSE 0 END) AS captured_count,
                SUM(CASE WHEN status = 'CAPTURED' THEN amount_cents ELSE 0 END) AS captured_cents,
                SUM(CASE WHEN status = 'HOLD_STRANDED' THEN 1 ELSE 0 END) AS stranded_count,
                SUM(CASE WHEN status = 'HOLD_STRANDED' THEN amount_cents ELSE 0 END) AS stranded_cents,
                SUM(CASE WHEN status = 'REPLAYED_SAFE' THEN 1 ELSE 0 END) AS replayed_count,
                SUM(CASE WHEN status = 'REPLAYED_SAFE' THEN amount_cents ELSE 0 END) AS replayed_cents,
                COUNT(*) AS total_intents
            FROM payment_intents
            """
        ).fetchone()

        sample_orders = [
            r["order_id"]
            for r in conn.execute(
                "SELECT order_id FROM payment_intents WHERE status = 'HOLD_STRANDED' ORDER BY updated_at DESC LIMIT 25"
            ).fetchall()
        ]
        by_inst = [
            {"installments": r["installments"], "stranded_count": r["cnt"], "stranded_cents": r["cents"]}
            for r in conn.execute(
                """
                SELECT installments, COUNT(*) AS cnt, SUM(amount_cents) AS cents
                FROM payment_intents
                WHERE status = 'HOLD_STRANDED'
                GROUP BY installments
                ORDER BY cnt DESC
                """
            ).fetchall()
        ]

        cap_cents = int(totals["captured_cents"] or 0)
        str_cents = int(totals["stranded_cents"] or 0)
        rep_cents = int(totals["replayed_cents"] or 0)
        return {
            "total_intents": int(totals["total_intents"] or 0),
            "captured_count": int(totals["captured_count"] or 0),
            "captured_amount_cents": cap_cents,
            "captured_amount_formatted": f"${cap_cents / 100:,.2f}",
            "stranded_hold_count": int(totals["stranded_count"] or 0),
            "stranded_amount_cents": str_cents,
            "stranded_amount_formatted": f"${str_cents / 100:,.2f}",
            "replayed_safe_count": int(totals["replayed_count"] or 0),
            "replayed_amount_cents": rep_cents,
            "replayed_amount_formatted": f"${rep_cents / 100:,.2f}",
            "stranded_by_installments": by_inst,
            "sample_stranded_order_ids": sample_orders,
        }


def get_merchant_slas_data(db_path: Optional[Path] = None) -> dict:
    with connect_ledger(db_path) as conn:
        rows = conn.execute(
            """
            SELECT
                p.merchant_id,
                COALESCE(m.display_name, p.merchant_id) AS display_name,
                COALESCE(m.sla_tier, 'STANDARD_SMB') AS sla_tier,
                COALESCE(m.max_stranded_pct, 25.0) AS max_stranded_pct,
                COALESCE(m.sla_window_minutes, 60) AS sla_window_minutes,
                COUNT(*) AS total_orders,
                SUM(CASE WHEN p.status IN ('CAPTURED', 'REPLAYED_SAFE') THEN 1 ELSE 0 END) AS settled_orders,
                SUM(CASE WHEN p.status = 'HOLD_STRANDED' THEN 1 ELSE 0 END) AS stranded_holds,
                SUM(CASE WHEN p.status = 'HOLD_STRANDED' THEN p.amount_cents ELSE 0 END) AS stranded_cents
            FROM payment_intents p
            LEFT JOIN merchants m ON m.merchant_id = p.merchant_id
            GROUP BY p.merchant_id
            HAVING stranded_holds > 0 OR sla_tier = 'ENTERPRISE_TIER_1'
            ORDER BY
                CASE WHEN COALESCE(m.sla_tier, 'STANDARD_SMB') = 'ENTERPRISE_TIER_1' THEN 0
                     WHEN COALESCE(m.sla_tier, 'STANDARD_SMB') = 'GROWTH_TIER_2' THEN 1
                     ELSE 2 END,
                stranded_cents DESC,
                total_orders DESC
            LIMIT 30
            """
        ).fetchall()

        merchants_out = []
        for r in rows:
            total = int(r["total_orders"])
            stranded = int(r["stranded_holds"] or 0)
            stranded_pct = round((stranded / total) * 100.0, 2) if total > 0 else 0.0
            max_pct = float(r["max_stranded_pct"])
            win_min = int(r["sla_window_minutes"])
            if stranded == 0:
                sla_status = "HEALTHY"
                mins_to_breach = win_min
            elif stranded_pct >= max_pct:
                sla_status = "AT_RISK_IMMINENT_BREACH" if stranded < 3 else "SLA_BREACHING"
                mins_to_breach = max(1, win_min - stranded * 3)
            else:
                sla_status = "DEGRADED"
                mins_to_breach = max(3, win_min - stranded * 2)

            settled = int(r["settled_orders"] or 0)
            capture_success_pct = round((settled / total) * 100.0, 1) if total > 0 else 100.0
            sla_target_pct = round(100.0 - max_pct, 1)
            sc = int(r["stranded_cents"] or 0)
            merchants_out.append(
                {
                    "merchant_id": r["merchant_id"],
                    "capture_success_pct": capture_success_pct,
                    "sla_target_pct": sla_target_pct,
                    "display_name": r["display_name"],
                    "sla_tier": r["sla_tier"],
                    "total_orders": total,
                    "settled_orders": int(r["settled_orders"] or 0),
                    "stranded_holds": stranded,
                    "stranded_pct": stranded_pct,
                    "max_allowed_stranded_pct": max_pct,
                    "sla_status": sla_status,
                    "minutes_to_sla_breach": mins_to_breach,
                    "stranded_amount_cents": sc,
                    "stranded_amount_formatted": f"${sc / 100:,.2f}",
                }
            )
        return {"merchants": merchants_out, "count": len(merchants_out)}


def get_holds_by_merchants_data(merchant_ids: list[str], db_path: Optional[Path] = None) -> dict:
    cleaned = [m.strip() for m in merchant_ids if m.strip()]
    if not cleaned:
        return {"orders": [], "count": 0}
    placeholders = ",".join("?" for _ in cleaned)
    with connect_ledger(db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT order_id, merchant_id, idempotency_key, payment_method,
                   installments, amount_cents, status, bank_hold_active, authorized_at, updated_at
            FROM payment_intents
            WHERE merchant_id IN ({placeholders})
            ORDER BY updated_at DESC
            LIMIT 150
            """,
            cleaned,
        ).fetchall()
    orders = [
        {
            **dict(r),
            "bank_hold_active": bool(r["bank_hold_active"]),
            "double_charge_risk_on_naive_retry": bool(r["bank_hold_active"] == 1 and r["status"] == "HOLD_STRANDED"),
        }
        for r in rows
    ]
    stranded_active = [o for o in orders if o["double_charge_risk_on_naive_retry"]]
    return {
        "merchant_ids": cleaned,
        "total_orders_returned": len(orders),
        "active_bank_hold_stranded_count": len(stranded_active),
        "stranded_amount_cents": sum(o["amount_cents"] for o in stranded_active),
        "orders": orders,
    }


def get_holds_by_orders_data(order_ids: list[str], db_path: Optional[Path] = None) -> dict:
    cleaned = [o.strip() for o in order_ids if o.strip()]
    if not cleaned:
        return {"orders": [], "count": 0}
    placeholders = ",".join("?" for _ in cleaned)
    with connect_ledger(db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT order_id, merchant_id, idempotency_key, payment_method,
                   installments, amount_cents, status, bank_hold_active, authorized_at, updated_at
            FROM payment_intents
            WHERE order_id IN ({placeholders})
            """,
            cleaned,
        ).fetchall()
    orders = [
        {
            **dict(r),
            "bank_hold_active": bool(r["bank_hold_active"]),
            "double_charge_risk_on_naive_retry": bool(r["bank_hold_active"] == 1 and r["status"] == "HOLD_STRANDED"),
        }
        for r in rows
    ]
    return {"orders": orders, "count": len(orders)}


def reset_ledger_state(db_path: Optional[Path] = None) -> dict:
    with connect_ledger(db_path) as conn:
        conn.execute("DELETE FROM payment_intents")
        conn.execute("DELETE FROM replay_audit_log")
        conn.commit()
    return {"ok": True, "reset": True}


def sign_audit_receipt(merchant_ids_csv: str, replayed_count: int, replayed_amount_cents: int, db_path: Optional[Path] = None) -> dict:
    ts = now_iso()
    payload = f"{merchant_ids_csv}|{replayed_count}|{replayed_amount_cents}|{ts}"
    sig = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    receipt_id = f"rcpt_{sig[:12]}"
    with connect_ledger(db_path) as conn:
        conn.execute(
            "INSERT INTO replay_audit_log VALUES (?, ?, ?, ?, ?, ?)",
            (receipt_id, merchant_ids_csv, replayed_count, replayed_amount_cents, sig, ts),
        )
        conn.commit()
    return {
        "receipt_id": receipt_id,
        "sha256_signature": sig,
        "replayed_count": replayed_count,
        "replayed_amount_cents": replayed_amount_cents,
        "replayed_amount_formatted": f"${replayed_amount_cents / 100:,.2f}",
        "recorded_at": ts,
    }
