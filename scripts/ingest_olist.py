#!/usr/bin/env python3
"""Deterministic ingestion of real public Olist E-Commerce CSVs into olist_source.db."""
from __future__ import annotations

import csv
import os
import sqlite3
from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

ROOT = Path(os.environ.get("CANARYBRAKE_ROOT", str(Path(__file__).resolve().parents[1])))
OLIST_DIR = ROOT / "data" / "olist"
OUT_DB = ROOT / "data" / "olist_source.db"


def to_cents(raw: str) -> int:
    d = Decimal(raw.strip() or "0").quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return int(d * 100)


def ingest(olist_dir: Path = OLIST_DIR, out_db: Path = OUT_DB) -> dict:
    sellers_csv = olist_dir / "olist_sellers_dataset.csv"
    items_csv = olist_dir / "olist_order_items_dataset.csv"
    payments_csv = olist_dir / "olist_order_payments_dataset.csv"
    orders_csv = olist_dir / "olist_orders_dataset.csv"

    for p in (sellers_csv, items_csv, payments_csv, orders_csv):
        if not p.exists():
            raise FileNotFoundError(f"Missing real Olist file: {p}")

    # 1. Load real sellers (merchants)
    sellers: dict[str, tuple[str, str]] = {}
    with sellers_csv.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            sid = row["seller_id"].strip()
            sellers[sid] = (row["seller_city"].strip(), row["seller_state"].strip())

    # 2. Aggregate primary seller + item count + GMV per order from real order_items
    order_primary_seller: dict[str, tuple[str, int, int]] = {}
    order_seller_gmv: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    order_item_counts: dict[str, int] = defaultdict(int)
    seller_total_gmv_cents: dict[str, int] = defaultdict(int)
    seller_order_sets: dict[str, set[str]] = defaultdict(set)

    with items_csv.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            oid = row["order_id"].strip()
            sid = row["seller_id"].strip()
            item_total = to_cents(row["price"]) + to_cents(row["freight_value"])
            order_seller_gmv[oid][sid] += item_total
            order_item_counts[oid] += 1
            seller_total_gmv_cents[sid] += item_total
            seller_order_sets[sid].add(oid)

    for oid, s_map in order_seller_gmv.items():
        # Primary merchant for the checkout is the highest-value seller on the order
        primary_sid, _ = max(s_map.items(), key=lambda kv: (kv[1], kv[0]))
        order_primary_seller[oid] = (primary_sid, order_item_counts[oid], sum(s_map.values()))

    # 3. Rank real merchants by actual historical GMV to assign realistic contractual SLA tiers
    ranked_sellers = sorted(
        sellers.keys(),
        key=lambda s: (seller_total_gmv_cents.get(s, 0), len(seller_order_sets.get(s, ())), s),
        reverse=True,
    )

    merchant_rows = []
    for rank_idx, sid in enumerate(ranked_sellers, start=1):
        city, state = sellers[sid]
        gmv = seller_total_gmv_cents.get(sid, 0)
        order_cnt = len(seller_order_sets.get(sid, ()))
        short_code = sid[:6].upper()
        display_name = f"MERCH-{state}-{short_code} ({city.title()}, {state})"
        if rank_idx <= 50:
            tier = "ENTERPRISE_TIER_1"
            max_stranded_pct = 5.0
            sla_window_min = 15
        elif rank_idx <= 300:
            tier = "GROWTH_TIER_2"
            max_stranded_pct = 12.0
            sla_window_min = 30
        else:
            tier = "STANDARD_SMB"
            max_stranded_pct = 25.0
            sla_window_min = 60
        merchant_rows.append(
            (sid, display_name, city, state, rank_idx, tier, gmv, order_cnt, max_stranded_pct, sla_window_min)
        )

    # 4. Aggregate primary payment record per order from real order_payments
    #    Keep exact sum of payment_value, max installments, and primary payment_type
    order_payments: dict[str, list[tuple[int, str, int, int]]] = defaultdict(list)
    with payments_csv.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            oid = row["order_id"].strip()
            seq = int(row["payment_sequential"] or "1")
            ptype = row["payment_type"].strip() or "credit_card"
            inst = max(1, int(row["payment_installments"] or "1"))
            val_cents = to_cents(row["payment_value"])
            if val_cents > 0 and ptype != "not_defined":
                order_payments[oid].append((seq, ptype, inst, val_cents))

    # 5. Stream real orders, join with primary merchant & payment, sort chronologically
    events = []
    with orders_csv.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            oid = row["order_id"].strip()
            if oid not in order_primary_seller or oid not in order_payments:
                continue
            ts = row["order_purchase_timestamp"].strip()
            if not ts:
                continue
            cust_id = row["customer_id"].strip()
            ostatus = row["order_status"].strip()
            sid, item_cnt, _ = order_primary_seller[oid]
            pays = sorted(order_payments[oid], key=lambda x: (-x[3], x[0]))
            primary_seq, primary_ptype, _, _ = pays[0]
            max_installments = max(p[2] for p in pays)
            total_payment_cents = sum(p[3] for p in pays)
            if total_payment_cents <= 0:
                continue
            idem_key = f"idem_{oid}_{primary_seq}"
            events.append(
                (
                    ts,
                    oid,
                    sid,
                    cust_id,
                    idem_key,
                    primary_ptype,
                    max_installments,
                    item_cnt,
                    total_payment_cents,
                    ostatus,
                )
            )

    events.sort(key=lambda e: (e[0], e[1]))

    # Write clean SQLite DB atomically
    tmp_db = out_db.with_suffix(".db.tmp")
    if tmp_db.exists():
        tmp_db.unlink()
    conn = sqlite3.connect(str(tmp_db))
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.executescript(
        """
        CREATE TABLE merchants (
            merchant_id TEXT PRIMARY KEY,
            display_name TEXT NOT NULL,
            city TEXT NOT NULL,
            state TEXT NOT NULL,
            gmv_rank INTEGER NOT NULL,
            sla_tier TEXT NOT NULL,
            historical_gmv_cents INTEGER NOT NULL,
            historical_order_count INTEGER NOT NULL,
            max_stranded_pct REAL NOT NULL,
            sla_window_minutes INTEGER NOT NULL
        );

        CREATE TABLE checkout_events (
            seq_id INTEGER PRIMARY KEY AUTOINCREMENT,
            purchase_timestamp TEXT NOT NULL,
            order_id TEXT NOT NULL UNIQUE,
            merchant_id TEXT NOT NULL,
            customer_id TEXT NOT NULL,
            idempotency_key TEXT NOT NULL UNIQUE,
            payment_method TEXT NOT NULL,
            installments INTEGER NOT NULL,
            item_count INTEGER NOT NULL,
            amount_cents INTEGER NOT NULL,
            original_olist_status TEXT NOT NULL,
            FOREIGN KEY (merchant_id) REFERENCES merchants(merchant_id)
        );

        CREATE INDEX idx_events_ts ON checkout_events(purchase_timestamp);
        CREATE INDEX idx_events_merchant ON checkout_events(merchant_id);
        """
    )
    conn.executemany(
        "INSERT INTO merchants VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        merchant_rows,
    )
    conn.executemany(
        """
        INSERT INTO checkout_events (
            purchase_timestamp, order_id, merchant_id, customer_id,
            idempotency_key, payment_method, installments, item_count,
            amount_cents, original_olist_status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        events,
    )

    # Identify dense Black Friday peak offset (2017-11-24) so live replay hits dense real Enterprise traffic
    bf_row = conn.execute(
        "SELECT MIN(seq_id) FROM checkout_events WHERE purchase_timestamp >= ?",
        ("2017-11-24 00:00:00",),
    ).fetchone()
    default_peak_seq = int(bf_row[0] or 1)

    conn.execute(
        "CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
    )
    conn.executemany(
        "INSERT INTO metadata VALUES (?, ?)",
        [
            ("dataset", "Olist Brazilian E-Commerce Public Dataset"),
            ("total_merchants", str(len(merchant_rows))),
            ("total_checkout_events", str(len(events))),
            ("default_peak_seq_id", str(default_peak_seq)),
        ],
    )
    conn.commit()
    conn.close()
    tmp_db.replace(out_db)

    return {
        "db_path": str(out_db),
        "merchants": len(merchant_rows),
        "checkout_events": len(events),
        "default_peak_seq_id": default_peak_seq,
    }


if __name__ == "__main__":
    summary = ingest()
    print("Olist real dataset ingested successfully:", summary)
