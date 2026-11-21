"""Phase 5 full live Gemini + Multi-Server Synapse MCP Copilot end-to-end scenario test."""
from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "services" / "copilot-console"))
sys.path.insert(0, str(ROOT / "services" / "order-replayer"))

from agent import CanaryBrakeCopilot  # noqa: E402
from mcp_hub import MultiServerMcpHub  # noqa: E402
import replayer as replayer_mod  # noqa: E402


def _wait_http(url: str, timeout_s: float = 10.0) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            if httpx.get(url, timeout=1.0).status_code == 200:
                return
        except Exception:
            pass
        time.sleep(0.1)
    raise RuntimeError(f"Service did not start: {url}")


def test_copilot_all_three_hackathon_scenarios_end_to_end(tmp_path):
    edge_db = tmp_path / "p5_edge.db"
    ledger_db = tmp_path / "p5_ledger.db"

    subprocess.run([str(ROOT / "scripts" / "run_live_discover.sh"), "disable"], check=True)

    py_env = os.environ.copy()
    py_env["LEDGER_DB_PATH"] = str(ledger_db)
    py_proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app:app", "--host", "127.0.0.1", "--port", "20082", "--log-level", "warning"],
        cwd=str(ROOT / "services" / "payment-sla-ledger"),
        env=py_env,
    )

    def start_go():
        go_env = os.environ.copy()
        go_env["EDGE_FLAGS_DB"] = str(edge_db)
        go_env["LEDGER_BASE_URL"] = "http://127.0.0.1:20082"
        go_env["GATEWAY_ADDR"] = "127.0.0.1:20081"
        p = subprocess.Popen(
            [str(ROOT / "services" / "flag-edge-gateway" / "flag-edge-gateway")],
            cwd=str(ROOT / "services" / "flag-edge-gateway"),
            env=go_env,
        )
        _wait_http("http://127.0.0.1:20081/health")
        return p

    _wait_http("http://127.0.0.1:20082/health")
    go_proc = start_go()

    try:
        async def _run():
            nonlocal go_proc
            rep = replayer_mod.OrderStreamReplayer(
                db_path=ROOT / "data" / "olist_source.db",
                gateway_checkout_url="http://127.0.0.1:20081/checkout",
            )
            try:
                await rep.step_many(180)
            finally:
                await rep.stop_client()

            hub = MultiServerMcpHub(
                gateway_base_url="http://127.0.0.1:20081",
                ledger_base_url="http://127.0.0.1:20082",
                ledger_db_path=str(ledger_db),
            )
            await hub.connect_all()
            try:
                assert hub.registry_summary()["total_tools"] == 9  # 4 Go + 4 Py Auto + 1 Py Custom
                copilot = CanaryBrakeCopilot(hub=hub)

                # QUESTION 1: Cross-System Root Cause & False-Alarm Triage
                r1 = await copilot.ask(
                    "Gateway error alerts are firing on both smart_retry_v1 and checkout_gateway_v2. "
                    "Cross-reference both systems to tell me which flag cohort is actually stranding merchant money "
                    "in uncaptured holds versus recovering on retry, and list the impacted Enterprise Tier-1 merchants."
                )
                servers_used_1 = {t["server_label"] for t in r1["tool_traces"]}
                assert "flag-edge-gateway" in servers_used_1, f"Must query Go MCP, got {servers_used_1}"
                assert "payment-sla-ledger" in servers_used_1, f"Must query Python MCP, got {servers_used_1}"
                assert "checkout_gateway_v2" in r1["answer"]

                # QUESTION 2: Double-Charge Risk & Active Bank Holds Audit
                r2 = await copilot.ask(
                    "For those impacted Enterprise Tier-1 merchants, check their orders across both systems: "
                    "do their stranded orders already have active customer bank holds that would double-charge buyers on naive retry?"
                )
                assert len(r2["tool_traces"]) >= 1
                assert "hold" in r2["answer"].lower() or "idempoten" in r2["answer"].lower()

                # STEP 3a: Pre-Discover Capability Gap Check
                r3a = await copilot.ask(
                    "Exclude those impacted Enterprise Tier-1 merchants from checkout_gateway_v2 right now without disabling the flag globally."
                )
                # Ensure no exclude tool existed yet
                assert all("exclude" not in t["tool_name"] for t in r3a["tool_traces"])

                # STEP 3b: Run live synapse build --discover + reload MCP + Exclude + Dry-Run Preview!
                subprocess.run([str(ROOT / "scripts" / "run_live_discover.sh"), "enable"], check=True)
                go_proc.terminate()
                go_proc.wait(timeout=5)
                go_proc = start_go()
                await hub.connect_all()
                assert hub.registry_summary()["total_tools"] == 10  # 5 Go + 4 Py Auto + 1 Py Custom!

                r3b = await copilot.ask(
                    "flag-edge-gateway was just updated via synapse build --discover! "
                    "Now exclude those impacted Enterprise Tier-1 merchants from checkout_gateway_v2, "
                    "and run replay_stranded_merchant_holds with confirm=False to show me the dry-run preview."
                )
                called_tools_3b = [t["tool_name"] for t in r3b["tool_traces"]]
                assert any("exclude" in name for name in called_tools_3b), f"Expected new exclude tool call, got {called_tools_3b}"
                assert "replay_stranded_merchant_holds" in called_tools_3b

                # Verify Go flags DB actually excluded those merchants AND ledger still awaits confirmation!
                flags_now = httpx.get("http://127.0.0.1:20081/api/flags").json()["flags"]
                upi_flag = [f for f in flags_now if f["flag_key"] == "checkout_gateway_v2"][0]
                assert len(upi_flag["excluded_merchants"]) >= 1

                # STEP 3c: Explicit User Confirmation -> Atomic Idempotent Replay + SHA-256 Receipt!
                r3c = await copilot.ask(
                    "Confirm: execute replay_stranded_merchant_holds with confirm=True for those excluded Enterprise Tier-1 merchants."
                )
                replay_traces = [
                    t for t in r3c["tool_traces"]
                    if t["tool_name"] == "replay_stranded_merchant_holds" and t["arguments"].get("confirm") is True
                ]
                assert len(replay_traces) >= 1, f"Expected confirm=True call, got {r3c['tool_traces']}"
                assert replay_traces[0]["output"]["status"] == "success"
                assert "rcpt_" in r3c["answer"] or replay_traces[0]["output"]["receipt"]["receipt_id"].startswith("rcpt_")
            finally:
                await hub.close()

        asyncio.run(_run())
    finally:
        go_proc.terminate()
        py_proc.terminate()
        go_proc.wait(timeout=5)
        py_proc.wait(timeout=5)
        subprocess.run([str(ROOT / "scripts" / "run_live_discover.sh"), "disable"], check=True)
