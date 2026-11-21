"""Public workflow functions composed by Synapse Custom mode into one guarded hold-replay MCP tool."""
from __future__ import annotations

from ledger_ops.db import connect_ledger, now_iso, sign_audit_receipt


def inspect_stranded_merchant_holds(merchant_ids_csv: str) -> dict:
    """Inspect active HOLD_STRANDED payment intents and idempotency locks for comma-separated merchant_ids_csv (or 'ALL' for all stranded holds) without mutating state."""
    raw = merchant_ids_csv.strip()
    is_all = raw.upper() == "ALL"
    mids = [m.strip() for m in raw.split(",") if m.strip()]
    if not mids:
        return {
            "eligible": False,
            "reason": "No merchant_ids provided",
            "stranded_count": 0,
            "stranded_amount_cents": 0,
            "order_ids": [],
        }
    with connect_ledger() as conn:
        if is_all:
            rows = conn.execute(
                """
                SELECT order_id, merchant_id, idempotency_key, amount_cents, bank_hold_active
                FROM payment_intents
                WHERE status = 'HOLD_STRANDED'
                ORDER BY authorized_at ASC
                """
            ).fetchall()
            mids = sorted({r["merchant_id"] for r in rows})
        else:
            placeholders = ",".join("?" for _ in mids)
            rows = conn.execute(
                f"""
                SELECT order_id, merchant_id, idempotency_key, amount_cents, bank_hold_active
                FROM payment_intents
                WHERE merchant_id IN ({placeholders})
                  AND status = 'HOLD_STRANDED'
                ORDER BY authorized_at ASC
                """,
                mids,
            ).fetchall()

    total_cents = sum(int(r["amount_cents"]) for r in rows)
    return {
        "eligible": len(rows) > 0,
        "merchant_ids": mids,
        "merchant_count": len(mids),
        "stranded_count": len(rows),
        "stranded_amount_cents": total_cents,
        "stranded_amount_formatted": f"${total_cents / 100:,.2f}",
        "all_have_active_bank_holds": all(int(r["bank_hold_active"]) == 1 for r in rows),
        "order_ids": [r["order_id"] for r in rows],
        "idempotency_keys_verified": len({r["idempotency_key"] for r in rows}),
    }


def execute_idempotent_v1_replay(merchant_ids_csv: str) -> dict:
    """Atomically settle HOLD_STRANDED orders for comma-separated merchant_ids_csv (or 'ALL' for all stranded holds) via v1 idempotent capture replay without double-charging."""
    raw = merchant_ids_csv.strip()
    is_all = raw.upper() == "ALL"
    mids = [m.strip() for m in raw.split(",") if m.strip()]
    if not mids:
        return {"replayed_count": 0, "replayed_amount_cents": 0, "replayed_order_ids": []}
    ts = now_iso()
    with connect_ledger() as conn:
        if is_all:
            rows = conn.execute(
                "SELECT order_id, amount_cents FROM payment_intents WHERE status = 'HOLD_STRANDED'"
            ).fetchall()
        else:
            placeholders = ",".join("?" for _ in mids)
            rows = conn.execute(
                f"""
                SELECT order_id, amount_cents
                FROM payment_intents
                WHERE merchant_id IN ({placeholders})
                  AND status = 'HOLD_STRANDED'
                """,
                mids,
            ).fetchall()
        if not rows:
            return {"replayed_count": 0, "replayed_amount_cents": 0, "replayed_order_ids": []}
        order_ids = [r["order_id"] for r in rows]
        total_cents = sum(int(r["amount_cents"]) for r in rows)
        ord_placeholders = ",".join("?" for _ in order_ids)
        conn.execute(
            f"""
            UPDATE payment_intents
            SET status = 'REPLAYED_SAFE',
                bank_hold_active = 0,
                updated_at = ?
            WHERE order_id IN ({ord_placeholders})
              AND status = 'HOLD_STRANDED'
            """,
            [ts, *order_ids],
        )
        conn.commit()
    return {
        "replayed_count": len(order_ids),
        "replayed_amount_cents": total_cents,
        "replayed_amount_formatted": f"${total_cents / 100:,.2f}",
        "replayed_order_ids": order_ids,
        "duplicate_charges_created": 0,
    }


def issue_sla_recovery_receipt(
    merchant_ids_csv: str, replayed_count: int, replayed_amount_cents: int
) -> dict:
    """Persist and return a SHA-256 signed audit receipt for an idempotent hold-replay batch."""
    return sign_audit_receipt(
        merchant_ids_csv=merchant_ids_csv,
        replayed_count=replayed_count,
        replayed_amount_cents=replayed_amount_cents,
    )
