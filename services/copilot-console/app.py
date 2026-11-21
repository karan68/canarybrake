"""CanaryBrake Unified Incident Console & Cross-System MCP Copilot Backend (:3000)."""
from __future__ import annotations

import asyncio
import contextlib
import os
import re
import subprocess
import time
from pathlib import Path
from typing import Any, Literal, Optional

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from agent import CanaryBrakeCopilot
from mcp_hub import MultiServerMcpHub

ROOT = Path(os.environ.get("CANARYBRAKE_ROOT", str(Path(__file__).resolve().parents[2])))
STATIC_DIR = Path(__file__).parent / "static"
ANSI_RE = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")


def clean_ansi(s: str) -> str:
    cleaned = ANSI_RE.sub("", s)
    # Remove carriage-return spinner overwrites
    lines = [ln.split("\r")[-1] for ln in cleaned.splitlines()]
    return "\n".join(lines).strip()


class ChatRequest(BaseModel):
    message: str


class ReplayerProxyRequest(BaseModel):
    action: Literal["start", "pause", "reset", "step"]
    rps: Optional[float] = None
    count: int = 60


class DiscoverRequest(BaseModel):
    mode: Literal["enable", "disable"]


class OperationTracker:
    """Tracks live multi-step backend operations to drive the UI Loading Carousel."""

    def __init__(self) -> None:
        self.active: bool = False
        self.op_type: str = ""
        self.title: str = ""
        self.steps: list[dict[str, str]] = []
        self.step_index: int = 0
        self.started_at: float = 0.0

    def begin(self, op_type: str, title: str, steps: list[tuple[str, str]]) -> None:
        self.active = True
        self.op_type = op_type
        self.title = title
        self.started_at = time.time()
        self.step_index = 0
        self.steps = [
            {"label": lbl, "detail": det, "state": "active" if idx == 0 else "pending"}
            for idx, (lbl, det) in enumerate(steps)
        ]

    def advance(self, idx: int) -> None:
        self.step_index = idx
        for i, st in enumerate(self.steps):
            if i < idx:
                st["state"] = "done"
            elif i == idx:
                st["state"] = "active"
            else:
                st["state"] = "pending"

    def finish(self) -> None:
        for st in self.steps:
            st["state"] = "done"
        self.active = False

    def snapshot(self) -> dict[str, Any]:
        return {
            "active": self.active,
            "op_type": self.op_type,
            "title": self.title,
            "step_index": self.step_index,
            "steps": self.steps,
            "elapsed_s": round(time.time() - self.started_at, 1) if self.active else 0.0,
        }


def create_console_app(
    gateway_url: Optional[str] = None,
    ledger_url: Optional[str] = None,
    replayer_url: Optional[str] = None,
    ledger_db_path: Optional[str] = None,
) -> FastAPI:
    gw = gateway_url or os.environ.get("GATEWAY_BASE_URL", "http://127.0.0.1:8081")
    lg = ledger_url or os.environ.get("LEDGER_BASE_URL", "http://127.0.0.1:8082")
    rp = replayer_url or os.environ.get("REPLAYER_BASE_URL", "http://127.0.0.1:8083")

    hub = MultiServerMcpHub(gateway_base_url=gw, ledger_base_url=lg, ledger_db_path=ledger_db_path)
    copilot = CanaryBrakeCopilot(hub=hub)
    http_client = httpx.AsyncClient(timeout=httpx.Timeout(30.0))
    op_tracker = OperationTracker()
    op_lock = asyncio.Lock()

    async def _run_discover_script(mode: str) -> str:
        res = await asyncio.to_thread(
            subprocess.run,
            [str(ROOT / "scripts" / "run_live_discover.sh"), mode],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            close_fds=True,
            timeout=60,
        )
        log_txt = clean_ansi(res.stdout.decode("utf-8", errors="replace"))
        if res.returncode != 0:
            raise HTTPException(status_code=500, detail=log_txt)
        return log_txt

    @contextlib.asynccontextmanager
    async def lifespan(_app: FastAPI):
        await hub.connect_all()
        try:
            yield
        finally:
            await hub.close()
            await http_client.aclose()

    app = FastAPI(title="CanaryBrake Live Incident Console", lifespan=lifespan)
    app.state.hub = hub
    app.state.copilot = copilot
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    @app.get("/")
    async def index():
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/classic")
    async def classic_ui():
        return FileResponse(STATIC_DIR / "classic.html")

    @app.get("/video")
    async def watch_video():
        return HTMLResponse("""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"/><title>CanaryBrake — Hackathon Submission Video</title>
<style>
  body { margin:0; background:#0d0e10; color:#f3f4f6; font-family:Inter,-apple-system,sans-serif; display:flex; flex-direction:column; align-items:center; justify-content:center; min-height:100vh; gap:18px; padding:24px; }
  .bar { width:min(1280px,95vw); display:flex; justify-content:space-between; align-items:center; }
  video { width:min(1280px,95vw); border-radius:10px; border:1px solid #252a32; box-shadow:0 24px 64px rgba(0,0,0,0.65); background:#000; }
  a.btn { padding:8px 14px; border-radius:6px; background:#f3f4f6; color:#0d0e10; text-decoration:none; font-size:13px; font-weight:600; }
  a.ghost { padding:8px 14px; border-radius:6px; border:1px solid #252a32; color:#cbd5e1; text-decoration:none; font-size:13px; }
</style></head><body>
  <div class="bar">
    <div><strong>CanaryBrake — Submission Film</strong> <span style="color:#8a929e;font-size:13px;margin-left:8px;">1920×1080 · 30fps · Studio Voiceover</span></div>
    <div style="display:flex;gap:8px;">
      <a class="ghost" href="/">← Live App</a>
      <a class="btn" href="/video/download" download="CanaryBrake_Hackathon_Submission.mp4">Download MP4 (10.1 MB)</a>
    </div>
  </div>
  <video controls autoplay playsinline src="/video/stream"></video>
</body></html>""")

    @app.get("/video/stream")
    async def stream_video():
        vid = ROOT.parent / "canarybrake_video" / "CanaryBrake_Hackathon_Submission.mp4"
        if not vid.exists():
            raise HTTPException(status_code=404, detail="Video file not found")
        return FileResponse(vid, media_type="video/mp4")

    @app.get("/video/download")
    async def download_video():
        vid = ROOT.parent / "canarybrake_video" / "CanaryBrake_Hackathon_Submission.mp4"
        if not vid.exists():
            raise HTTPException(status_code=404, detail="Video file not found")
        return FileResponse(vid, media_type="video/mp4", filename="CanaryBrake_Hackathon_Submission.mp4")


    @app.get("/health")
    async def health() -> dict:
        return {"ok": True, "service": "copilot-console", "mcp": hub.registry_summary()}

    @app.get("/api/ops/status")
    async def ops_status() -> dict:
        return op_tracker.snapshot()

    @app.get("/api/discover/source")
    async def discover_source() -> dict:
        p = ROOT / "services" / "flag-edge-gateway" / "discover_routes.go"
        code = p.read_text(encoding="utf-8") if p.exists() else ""
        enabled = "exclude-merchants" in code
        return {
            "enabled": enabled,
            "file_path": "services/flag-edge-gateway/discover_routes.go",
            "source_code": code,
        }

    @app.get("/api/live-state")
    async def live_state() -> dict:
        gw_task = http_client.get(f"{gw}/internal/dashboard-state")
        lg_task = http_client.get(f"{lg}/internal/dashboard-state")
        rp_task = http_client.get(f"{rp}/status")
        gw_res, lg_res, rp_res = await asyncio.gather(gw_task, lg_task, rp_task, return_exceptions=True)

        p = ROOT / "services" / "flag-edge-gateway" / "discover_routes.go"
        disc_enabled = "exclude-merchants" in (p.read_text(encoding="utf-8") if p.exists() else "")

        return {
            "gateway": gw_res.json() if not isinstance(gw_res, Exception) and gw_res.status_code == 200 else {"error": str(gw_res)},
            "ledger": lg_res.json() if not isinstance(lg_res, Exception) and lg_res.status_code == 200 else {"error": str(lg_res)},
            "replayer": rp_res.json() if not isinstance(rp_res, Exception) and rp_res.status_code == 200 else {"error": str(rp_res)},
            "mcp_registry": hub.registry_summary(),
            "discover_enabled": disc_enabled,
            "op_status": op_tracker.snapshot(),
        }

    @app.post("/api/mcp/reload")
    async def reload_mcp() -> dict:
        async with op_lock:
            op_tracker.begin(
                "reload_mcp",
                "Refreshing Multi-Server Synapse MCP Hub",
                [
                    ("Closing stale stdio transports", "Releasing existing MCP client pipes cleanly"),
                    ("Syncing Hosted Synapse configs", "Fetching live tool schemas for Go & Python servers"),
                    ("Initializing FastMCP custom server", "Verifying guarded hold-replay tool signature"),
                ],
            )
            try:
                await asyncio.sleep(0.18)
                op_tracker.advance(1)
                summary = await hub.connect_all()
                op_tracker.advance(2)
                await asyncio.sleep(0.15)
                return {"ok": True, "mcp_registry": summary}
            finally:
                op_tracker.finish()

    @app.post("/api/chat")
    async def chat(req: ChatRequest) -> dict:
        if not req.message.strip():
            raise HTTPException(status_code=400, detail="Empty message")
        return await copilot.ask(req.message.strip())

    @app.post("/api/chat/clear")
    async def clear_chat() -> dict:
        copilot.clear_history()
        return {"ok": True}

    @app.post("/api/replayer/control")
    async def control_replayer(req: ReplayerProxyRequest) -> dict:
        payload = req.model_dump(exclude_none=True)
        res = await http_client.post(f"{rp}/control", json=payload)
        if res.status_code >= 400:
            raise HTTPException(status_code=res.status_code, detail=res.text)
        return res.json()

    @app.post("/api/demo/reset-all")
    async def reset_all_demo_state() -> dict:
        async with op_lock:
            op_tracker.begin(
                "reset_demo",
                "Resetting CanaryBrake Stage Environment",
                [
                    ("Pausing Olist Order Stream", "Resetting cursor to Black Friday peak (seq #35,856)"),
                    ("Restoring 4-Tool Go Baseline", "Syncing Hosted MCP Server e2d50a55-… via synapse build --discover"),
                    ("Clearing Isolated SQLite WAL DBs", "Wiping edge_flags.db & payment_ledger.db and reloading MCP"),
                    ("Replaying 120 Real Olist Checkouts", "Seeding live traffic across v1_stable, smart_retry_v1 & v2_canary"),
                ],
            )
            try:
                await http_client.post(f"{rp}/control", json={"action": "reset"})

                op_tracker.advance(1)
                p = ROOT / "services" / "flag-edge-gateway" / "discover_routes.go"
                go_tools = next(
                    (s["tool_count"] for s in hub.registry_summary()["servers"] if s["server_key"] == "flag_edge_gateway"),
                    4,
                )
                if (p.exists() and "exclude-merchants" in p.read_text(encoding="utf-8")) or go_tools > 4:
                    await _run_discover_script("disable")

                op_tracker.advance(2)
                await http_client.post(f"{gw}/internal/reset")
                await http_client.post(f"{lg}/internal/reset")
                summary = await hub.connect_all()
                copilot.clear_history()

                op_tracker.advance(3)
                step_res = await http_client.post(f"{rp}/control", json={"action": "step", "count": 120})
                return {
                    "ok": True,
                    "mcp_registry": summary,
                    "warmup": step_res.json() if step_res.status_code == 200 else {},
                }
            finally:
                op_tracker.finish()

    @app.post("/api/demo/live-discover")
    async def trigger_live_discover(req: DiscoverRequest) -> dict:
        async with op_lock:
            verb = "Enabling 5th Endpoint" if req.mode == "enable" else "Restoring 4-Tool Baseline"
            op_tracker.begin(
                "live_discover",
                f"Live Evolution · {verb} via synapse build --discover",
                [
                    ("Updating Go Route Surface", "Patching services/flag-edge-gateway/discover_routes.go"),
                    ("Compiling Go 1.23 Binary & Rescanning", "Running tree-sitter delta scan + synapse build --discover"),
                    ("Hot-Restarting Gateway & Reloading MCP", "Verifying :8081/health and refreshing MCP stdio tool list"),
                ],
            )
            try:
                await asyncio.sleep(0.1)
                op_tracker.advance(1)
                cli_log = await _run_discover_script(req.mode)
                op_tracker.advance(2)
                summary = await hub.connect_all()
                return {
                    "ok": True,
                    "mode": req.mode,
                    "cli_output": cli_log,
                    "mcp_registry": summary,
                }
            finally:
                op_tracker.finish()

    return app


app = create_console_app()
