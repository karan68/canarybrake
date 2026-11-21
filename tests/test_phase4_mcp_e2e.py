"""Phase 4 end-to-end live MCP protocol test across Go Hosted Auto, Python Hosted Auto, Python Custom, and live --discover."""
from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import httpx
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "services" / "order-replayer"))
import replayer as replayer_mod  # noqa: E402

GO_SERVER_ID = "e2d50a55-9ebc-4aee-8a17-b47ced0025aa"
PY_SERVER_ID = "059607eb-feb9-4fa8-874e-907d522e6ebd"


def _wait_http(url: str, timeout_s: float = 8.0) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            if httpx.get(url, timeout=1.0).status_code == 200:
                return
        except Exception:
            pass
        time.sleep(0.1)
    raise RuntimeError(f"Service did not start: {url}")


async def _list_and_call(params: StdioServerParameters, call_name: str | None = None, call_args: dict | None = None):
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools_res = await session.list_tools()
            tool_names = [t.name for t in tools_res.tools]
            out = None
            if call_name is not None:
                res = await session.call_tool(call_name, call_args or {})
                txt = "".join(
                    getattr(c, "text", "") for c in res.content
                )
                try:
                    out = json.loads(txt)
                except Exception:
                    out = {"raw": txt}
            return tool_names, out


def test_full_mcp_stdio_and_live_discover_flow(tmp_path):
    synapse_key = os.environ["SYNAPSE_API_KEY"]
    edge_db = tmp_path / "mcp_edge.db"
    ledger_db = tmp_path / "mcp_ledger.db"

    # Ensure baseline 4-endpoint state before starting
    subprocess.run([str(ROOT / "scripts" / "run_live_discover.sh"), "disable"], check=True)

    py_env = os.environ.copy()
    py_env["LEDGER_DB_PATH"] = str(ledger_db)
    py_proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app:app", "--host", "127.0.0.1", "--port", "19082", "--log-level", "warning"],
        cwd=str(ROOT / "services" / "payment-sla-ledger"),
        env=py_env,
    )

    def start_go():
        go_env = os.environ.copy()
        go_env["EDGE_FLAGS_DB"] = str(edge_db)
        go_env["LEDGER_BASE_URL"] = "http://127.0.0.1:19082"
        go_env["GATEWAY_ADDR"] = "127.0.0.1:19081"
        p = subprocess.Popen(
            [str(ROOT / "services" / "flag-edge-gateway" / "flag-edge-gateway")],
            cwd=str(ROOT / "services" / "flag-edge-gateway"),
            env=go_env,
        )
        _wait_http("http://127.0.0.1:19081/health")
        return p

    _wait_http("http://127.0.0.1:19082/health")
    go_proc = start_go()

    try:
        async def _run():
            nonlocal go_proc
            rep = replayer_mod.OrderStreamReplayer(
                db_path=ROOT / "data" / "olist_source.db",
                gateway_checkout_url="http://127.0.0.1:19081/checkout",
            )
            try:
                await rep.step_many(150)
            finally:
                await rep.stop_client()

            go_mcp = StdioServerParameters(
                command=str(ROOT / "tools" / "node" / "bin" / "synapse-mcp") if (ROOT / "tools" / "node" / "bin" / "synapse-mcp").exists() else "npx",
                args=[] if (ROOT / "tools" / "node" / "bin" / "synapse-mcp").exists() else ["-y", "@2ndbrainlabs-ai/synapse-mcp"],
                env={
                    **os.environ,
                    "SYNAPSE_SERVER_ID": GO_SERVER_ID,
                    "SYNAPSE_API_KEY": synapse_key,
                    "SYNAPSE_API_URL": "https://api.synapse.2ndbrainlabs.ai",
                    "ENDPOINT_BASE_URL": "http://127.0.0.1:19081",
                },
            )
            py_mcp = StdioServerParameters(
                command=str(ROOT / "tools" / "node" / "bin" / "synapse-mcp") if (ROOT / "tools" / "node" / "bin" / "synapse-mcp").exists() else "npx",
                args=[] if (ROOT / "tools" / "node" / "bin" / "synapse-mcp").exists() else ["-y", "@2ndbrainlabs-ai/synapse-mcp"],
                env={
                    **os.environ,
                    "SYNAPSE_SERVER_ID": PY_SERVER_ID,
                    "SYNAPSE_API_KEY": synapse_key,
                    "SYNAPSE_API_URL": "https://api.synapse.2ndbrainlabs.ai",
                    "ENDPOINT_BASE_URL": "http://127.0.0.1:19082",
                },
            )
            custom_mcp = StdioServerParameters(
                command=sys.executable,
                args=[str(ROOT / "services" / "payment-sla-ledger" / "replay_stranded_merchant_holds_server.py")],
                env={
                    **os.environ,
                    "CANARYBRAKE_ROOT": str(ROOT),
                    "LEDGER_DB_PATH": str(ledger_db),
                    "PYTHONPATH": str(ROOT / "services" / "payment-sla-ledger"),
                },
            )

            # 1. Verify Go Hosted Auto MCP Server #1 has EXACTLY 4 tools & returns real cohort telemetry!
            go_tools_4, cohort_out = await _list_and_call(go_mcp, "get_api_telemetry_cohorts", {})
            assert len(go_tools_4) == 4, f"Expected 4 baseline Go MCP tools, got {go_tools_4}"
            assert any(c["flag_key"] == "checkout_gateway_v2" and c["timeout_504_count"] > 0 for c in cohort_out["cohorts"])

            # 2. Verify Python Hosted Auto MCP Server #2 returns real SLA status!
            py_tools_4, sla_out = await _list_and_call(py_mcp, "get_merchant_sla_status", {})
            assert len(py_tools_4) == 4, f"Expected 4 Python Auto MCP tools, got {py_tools_4}"
            tier1_impacted = [
                m["merchant_id"]
                for m in sla_out["merchants"]
                if m["sla_tier"] == "ENTERPRISE_TIER_1" and m["stranded_holds"] > 0
            ]
            assert len(tier1_impacted) >= 1
            csv_mids = ",".join(tier1_impacted)

            # 3. LIVE DISCOVER! Enable 5th endpoint + synapse build --discover (same GO_SERVER_ID!)
            subprocess.run([str(ROOT / "scripts" / "run_live_discover.sh"), "enable"], check=True)
            go_proc.terminate()
            go_proc.wait(timeout=5)
            go_proc = start_go()

            go_tools_5, _ = await _list_and_call(go_mcp)
            assert len(go_tools_5) == 5, f"Expected 5 Go MCP tools after --discover, got {go_tools_5}"
            exclude_tool = [t for t in go_tools_5 if t not in go_tools_4][0]

            _, ex_out = await _list_and_call(
                go_mcp,
                exclude_tool,
                {"flag_key": "checkout_gateway_v2", "merchant_ids": csv_mids},
            )
            assert ex_out["ok"] is True
            assert ex_out["excluded_count"] >= len(tier1_impacted)

            # 4. Verify Python Custom MCP Server dry-run confirmation gate (confirm=False -> confirm=True)!
            custom_tools, dry_out = await _list_and_call(
                custom_mcp,
                "replay_stranded_merchant_holds",
                {"merchant_ids_csv": csv_mids, "confirm": False},
            )
            assert custom_tools == ["replay_stranded_merchant_holds"]
            assert dry_out["confirmation_required"] is True

            _, exec_out = await _list_and_call(
                custom_mcp,
                "replay_stranded_merchant_holds",
                {"merchant_ids_csv": csv_mids, "confirm": True},
            )
            assert exec_out["status"] == "success"
            assert exec_out["replay_result"]["replayed_count"] >= 1
            assert exec_out["receipt"]["receipt_id"].startswith("rcpt_")

        asyncio.run(_run())
    finally:
        go_proc.terminate()
        py_proc.terminate()
        go_proc.wait(timeout=5)
        py_proc.wait(timeout=5)
        # Restore baseline 4-endpoint state cleanly
        subprocess.run([str(ROOT / "scripts" / "run_live_discover.sh"), "disable"], check=True)
