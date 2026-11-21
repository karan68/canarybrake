"""Live HTTP Order-Stream Replayer over real public Olist e-commerce transactions."""
from __future__ import annotations

import asyncio
import contextlib
import os
import sqlite3
from pathlib import Path
from typing import Literal, Optional

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

DEFAULT_DB = Path(os.environ.get("CANARYBRAKE_ROOT", str(Path(__file__).resolve().parents[2]))) / "data" / "olist_source.db"


class ControlRequest(BaseModel):
    action: Literal["start", "pause", "reset", "step"]
    rps: Optional[float] = Field(default=None, gt=0.1, le=200.0)
    count: int = Field(default=10, ge=1, le=500)
    seek_seq_id: Optional[int] = Field(default=None, ge=1)


class OrderStreamReplayer:
    def __init__(self, db_path: Path, gateway_checkout_url: str, default_rps: float = 4.0) -> None:
        self.db_path = Path(db_path)
        self.gateway_checkout_url = gateway_checkout_url
        self.rps = default_rps
        self.running = False
        self._lock = asyncio.Lock()
        self._task: Optional[asyncio.Task[None]] = None
        self._client: Optional[httpx.AsyncClient] = None

        self.default_seq_id = self._read_default_seq_id()
        self.max_seq_id = self._read_max_seq_id()
        self.cursor_seq_id = self.default_seq_id

        self.total_dispatched = 0
        self.ok_2xx = 0
        self.err_5xx = 0
        self.err_other = 0
        self.last_order_id: Optional[str] = None
        self.last_status_code: Optional[int] = None

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        return conn

    def _read_default_seq_id(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT value FROM metadata WHERE key = ?", ("default_peak_seq_id",)).fetchone()
            return int(row["value"]) if row else 1

    def _read_max_seq_id(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT MAX(seq_id) AS m FROM checkout_events").fetchone()
            return int(row["m"] or 1)

    def _fetch_batch(self, start_seq: int, limit: int) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT seq_id, purchase_timestamp, order_id, merchant_id, customer_id,
                       idempotency_key, payment_method, installments, item_count, amount_cents
                FROM checkout_events
                WHERE seq_id >= ?
                ORDER BY seq_id ASC
                LIMIT ?
                """,
                (start_seq, limit),
            ).fetchall()
            return [dict(r) for r in rows]

    async def start_client(self) -> None:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=httpx.Timeout(12.0))

    async def stop_client(self) -> None:
        self.running = False
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _send_event(self, ev: dict) -> dict:
        assert self._client is not None
        try:
            resp = await self._client.post(self.gateway_checkout_url, json=ev)
            code = resp.status_code
            self.total_dispatched += 1
            self.last_order_id = ev["order_id"]
            self.last_status_code = code
            if 200 <= code < 300:
                self.ok_2xx += 1
            elif 500 <= code < 600:
                self.err_5xx += 1
            else:
                self.err_other += 1
            return {"order_id": ev["order_id"], "status_code": code, "amount_cents": ev["amount_cents"]}
        except httpx.HTTPError as exc:
            self.total_dispatched += 1
            self.err_other += 1
            self.last_order_id = ev["order_id"]
            self.last_status_code = 0
            return {"order_id": ev["order_id"], "status_code": 0, "error": str(exc), "amount_cents": ev["amount_cents"]}

    async def dispatch_one(self) -> dict:
        await self.start_client()
        async with self._lock:
            if self.cursor_seq_id > self.max_seq_id:
                self.cursor_seq_id = 1
            batch = self._fetch_batch(self.cursor_seq_id, 1)
            if not batch:
                self.cursor_seq_id = 1
                batch = self._fetch_batch(1, 1)
            ev = batch[0]
            self.cursor_seq_id = int(ev["seq_id"]) + 1
        return await self._send_event(ev)

    async def step_many(self, count: int) -> dict:
        await self.start_client()
        async with self._lock:
            if self.cursor_seq_id > self.max_seq_id:
                self.cursor_seq_id = 1
            events = self._fetch_batch(self.cursor_seq_id, count)
            if len(events) < count:
                rem = count - len(events)
                events.extend(self._fetch_batch(1, rem))
            if events:
                self.cursor_seq_id = int(events[-1]["seq_id"]) + 1

        sem = asyncio.Semaphore(8)

        async def _worker(ev: dict) -> dict:
            async with sem:
                return await self._send_event(ev)

        results = await asyncio.gather(*(_worker(ev) for ev in events))
        ok_cnt = sum(1 for r in results if 200 <= r["status_code"] < 300)
        err5_cnt = sum(1 for r in results if 500 <= r["status_code"] < 600)
        stranded_cents = sum(int(r["amount_cents"]) for r in results if r["status_code"] == 504)
        settled_cents = sum(int(r["amount_cents"]) for r in results if 200 <= r["status_code"] < 300)
        return {
            "stepped": len(results),
            "ok_2xx_in_batch": ok_cnt,
            "err_5xx_in_batch": err5_cnt,
            "err_other_in_batch": len(results) - ok_cnt - err5_cnt,
            "settled_amount_formatted": f"${settled_cents / 100:,.2f}",
            "stranded_amount_formatted": f"${stranded_cents / 100:,.2f}",
            "status": self.status(),
        }

    async def _loop(self) -> None:
        while self.running:
            await self.dispatch_one()
            await asyncio.sleep(max(0.005, 1.0 / self.rps))

    def start_background(self, rps: Optional[float] = None) -> None:
        if rps is not None:
            self.rps = rps
        if not self.running:
            self.running = True
            self._task = asyncio.create_task(self._loop())

    async def pause_background(self) -> None:
        self.running = False
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def reset(self, seek_seq_id: Optional[int] = None) -> None:
        await self.pause_background()
        async with self._lock:
            self.cursor_seq_id = seek_seq_id if seek_seq_id is not None else self.default_seq_id
            self.total_dispatched = 0
            self.ok_2xx = 0
            self.err_5xx = 0
            self.err_other = 0
            self.last_order_id = None
            self.last_status_code = None

    def status(self) -> dict:
        return {
            "running": self.running,
            "rps": self.rps,
            "cursor_seq_id": self.cursor_seq_id,
            "default_peak_seq_id": self.default_seq_id,
            "max_seq_id": self.max_seq_id,
            "total_dispatched": self.total_dispatched,
            "ok_2xx": self.ok_2xx,
            "err_5xx": self.err_5xx,
            "err_other": self.err_other,
            "last_order_id": self.last_order_id,
            "last_status_code": self.last_status_code,
            "gateway_checkout_url": self.gateway_checkout_url,
        }


def create_app(
    db_path: Path = DEFAULT_DB,
    gateway_checkout_url: Optional[str] = None,
) -> FastAPI:
    checkout_url = gateway_checkout_url or os.environ.get(
        "GATEWAY_CHECKOUT_URL", "http://127.0.0.1:8081/checkout"
    )
    replayer = OrderStreamReplayer(db_path=db_path, gateway_checkout_url=checkout_url)

    @contextlib.asynccontextmanager
    async def lifespan(_app: FastAPI):
        await replayer.start_client()
        if os.environ.get("REPLAYER_AUTOSTART", "0") == "1":
            replayer.start_background()
        try:
            yield
        finally:
            await replayer.stop_client()

    app = FastAPI(title="CanaryBrake Olist Order Stream Replayer", lifespan=lifespan)
    app.state.replayer = replayer

    @app.get("/health")
    async def health() -> dict:
        return {"ok": True, "service": "order-replayer"}

    @app.get("/status")
    async def get_status() -> dict:
        return replayer.status()

    @app.post("/control")
    async def control(req: ControlRequest) -> dict:
        if req.rps is not None:
            replayer.rps = req.rps
        if req.action == "start":
            replayer.start_background(req.rps)
            return {"ok": True, "action": "start", "status": replayer.status()}
        if req.action == "pause":
            await replayer.pause_background()
            return {"ok": True, "action": "pause", "status": replayer.status()}
        if req.action == "reset":
            await replayer.reset(seek_seq_id=req.seek_seq_id)
            return {"ok": True, "action": "reset", "status": replayer.status()}
        if req.action == "step":
            return await replayer.step_many(req.count)
        raise HTTPException(status_code=400, detail="Unsupported action")

    return app


app = create_app()
