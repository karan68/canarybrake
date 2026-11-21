"""Single-task Actor MCP stdio hub — guarantees anyio cancel-scope affinity across reloads and tool calls."""
from __future__ import annotations

import asyncio
import contextlib
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

ROOT = Path(os.environ.get("CANARYBRAKE_ROOT", str(Path(__file__).resolve().parents[2])))
GO_SERVER_ID = os.environ.get("GO_SYNAPSE_SERVER_ID", "e2d50a55-9ebc-4aee-8a17-b47ced0025aa")
PY_SERVER_ID = os.environ.get("PY_SYNAPSE_SERVER_ID", "059607eb-feb9-4fa8-874e-907d522e6ebd")


@dataclass
class RegisteredTool:
    namespaced_name: str
    server_key: str
    server_label: str
    language_badge: str
    raw_tool_name: str
    description: str
    input_schema: dict[str, Any]


class MultiServerMcpHub:
    """Manages persistent MCP stdio sessions inside one dedicated owner asyncio.Task."""

    def __init__(
        self,
        gateway_base_url: str = "http://127.0.0.1:8081",
        ledger_base_url: str = "http://127.0.0.1:8082",
        ledger_db_path: Optional[str] = None,
    ) -> None:
        self.gateway_base_url = gateway_base_url
        self.ledger_base_url = ledger_base_url
        self.ledger_db_path = ledger_db_path or str(ROOT / "data" / "payment_ledger.db")

        self._tools: dict[str, RegisteredTool] = {}
        self._server_meta: list[dict[str, Any]] = []
        self.last_reloaded_at: Optional[float] = None
        self.reload_generation: int = 0

        self._cmd_q: Optional[asyncio.Queue[tuple[str, Any, asyncio.Future[Any]]]] = None
        self._actor_task: Optional[asyncio.Task[None]] = None

    def _server_specs(self) -> list[tuple[str, str, str, str, StdioServerParameters]]:
        synapse_key = os.environ.get("SYNAPSE_API_KEY", "")
        import shutil
        local_bin = ROOT / "tools" / "node" / "bin" / "synapse-mcp"
        synapse_mcp_bin = str(local_bin) if local_bin.exists() else (shutil.which("synapse-mcp") or "npx")
        synapse_mcp_args = [] if synapse_mcp_bin != "npx" else ["-y", "@2ndbrainlabs-ai/synapse-mcp"]
        base_env = os.environ.copy()

        return [
            (
                "flag_edge_gateway",
                "flag-edge-gateway",
                "Go 1.23 · Hosted Auto",
                GO_SERVER_ID,
                StdioServerParameters(
                    command=synapse_mcp_bin,
                    args=synapse_mcp_args,
                    env={
                        **base_env,
                        "SYNAPSE_SERVER_ID": GO_SERVER_ID,
                        "SYNAPSE_API_KEY": synapse_key,
                        "SYNAPSE_API_URL": "https://api.synapse.2ndbrainlabs.ai",
                        "ENDPOINT_BASE_URL": self.gateway_base_url,
                    },
                ),
            ),
            (
                "payment_sla_ledger",
                "payment-sla-ledger",
                "Python · Hosted Auto",
                PY_SERVER_ID,
                StdioServerParameters(
                    command=synapse_mcp_bin,
                    args=synapse_mcp_args,
                    env={
                        **base_env,
                        "SYNAPSE_SERVER_ID": PY_SERVER_ID,
                        "SYNAPSE_API_KEY": synapse_key,
                        "SYNAPSE_API_URL": "https://api.synapse.2ndbrainlabs.ai",
                        "ENDPOINT_BASE_URL": self.ledger_base_url,
                    },
                ),
            ),
            (
                "replay_holds_custom",
                "replay-stranded-merchant-holds",
                "Python · Custom Composed",
                "local:replay_stranded_merchant_holds_server.py",
                StdioServerParameters(
                    command=sys.executable,
                    args=[
                        str(
                            ROOT
                            / "services"
                            / "payment-sla-ledger"
                            / "replay_stranded_merchant_holds_server.py"
                        )
                    ],
                    env={
                        **base_env,
                        "CANARYBRAKE_ROOT": str(ROOT),
                        "LEDGER_DB_PATH": self.ledger_db_path,
                        "PYTHONPATH": str(ROOT / "services" / "payment-sla-ledger"),
                    },
                ),
            ),
        ]

    def _ensure_actor(self) -> None:
        if self._actor_task is None or self._actor_task.done():
            self._cmd_q = asyncio.Queue()
            self._actor_task = asyncio.create_task(self._actor_loop())

    async def _actor_loop(self) -> None:
        assert self._cmd_q is not None
        stack: Optional[contextlib.AsyncExitStack] = None
        sessions: dict[str, ClientSession] = {}

        async def _close_stack() -> None:
            nonlocal stack, sessions
            if stack is not None:
                with contextlib.suppress(Exception):
                    await stack.aclose()
                stack = None
            sessions = {}

        async def _open_stack() -> dict[str, Any]:
            nonlocal stack, sessions
            await _close_stack()
            new_stack = contextlib.AsyncExitStack()
            new_sessions: dict[str, ClientSession] = {}
            new_tools: dict[str, RegisteredTool] = {}
            new_meta: list[dict[str, Any]] = []

            try:
                for skey, slabel, badge, sid, params in self._server_specs():
                    read, write = await new_stack.enter_async_context(stdio_client(params))
                    sess = await new_stack.enter_async_context(ClientSession(read, write))
                    await sess.initialize()
                    listed = await sess.list_tools()
                    new_sessions[skey] = sess

                    server_tool_names = []
                    server_tool_details = []
                    for t in listed.tools:
                        ns_name = f"{skey}__{t.name}"
                        schema = (
                            t.inputSchema
                            if isinstance(t.inputSchema, dict)
                            else {"type": "object", "properties": {}}
                        )
                        new_tools[ns_name] = RegisteredTool(
                            namespaced_name=ns_name,
                            server_key=skey,
                            server_label=slabel,
                            language_badge=badge,
                            raw_tool_name=t.name,
                            description=t.description or f"{slabel}: {t.name}",
                            input_schema=schema,
                        )
                        server_tool_names.append(t.name)
                        server_tool_details.append(
                            {
                                "name": t.name,
                                "description": t.description or "",
                                "params": list((schema.get("properties") or {}).keys()),
                            }
                        )

                    new_meta.append(
                        {
                            "server_key": skey,
                            "server_label": slabel,
                            "badge": badge,
                            "server_id": sid,
                            "tool_count": len(server_tool_names),
                            "tools": server_tool_names,
                            "tool_details": server_tool_details,
                        }
                    )
            except Exception:
                await new_stack.aclose()
                raise

            stack = new_stack
            sessions = new_sessions
            self._tools = new_tools
            self._server_meta = new_meta
            self.last_reloaded_at = time.time()
            self.reload_generation += 1
            return self.registry_summary()

        try:
            while True:
                op, payload, fut = await self._cmd_q.get()
                if op == "stop":
                    await _close_stack()
                    if not fut.done():
                        fut.set_result(True)
                    break
                elif op == "reload":
                    try:
                        summary = await _open_stack()
                        if not fut.done():
                            fut.set_result(summary)
                    except Exception as exc:
                        if not fut.done():
                            fut.set_exception(exc)
                elif op == "call":
                    ns_name, arguments = payload
                    reg = self._tools.get(ns_name)
                    if reg is None or reg.server_key not in sessions:
                        if not fut.done():
                            fut.set_result(
                                {
                                    "is_error": True,
                                    "server_label": "unknown",
                                    "language_badge": "unknown",
                                    "tool_name": ns_name,
                                    "arguments": arguments,
                                    "latency_ms": 0.0,
                                    "output": {"error": f"Unknown tool: {ns_name}"},
                                }
                            )
                        continue
                    sess = sessions[reg.server_key]
                    t0 = time.perf_counter()
                    try:
                        res = await sess.call_tool(reg.raw_tool_name, arguments or {})
                        latency_ms = round((time.perf_counter() - t0) * 1000.0, 1)
                        raw_text = "".join(getattr(c, "text", "") for c in res.content)
                        try:
                            parsed = json.loads(raw_text)
                        except Exception:
                            parsed = {"raw_text": raw_text}
                        out = {
                            "is_error": bool(res.isError),
                            "server_label": reg.server_label,
                            "language_badge": reg.language_badge,
                            "tool_name": reg.raw_tool_name,
                            "arguments": arguments or {},
                            "latency_ms": latency_ms,
                            "output": parsed,
                        }
                    except Exception as exc:
                        latency_ms = round((time.perf_counter() - t0) * 1000.0, 1)
                        out = {
                            "is_error": True,
                            "server_label": reg.server_label,
                            "language_badge": reg.language_badge,
                            "tool_name": reg.raw_tool_name,
                            "arguments": arguments or {},
                            "latency_ms": latency_ms,
                            "output": {"error": str(exc)},
                        }
                    if not fut.done():
                        fut.set_result(out)
        finally:
            await _close_stack()

    async def connect_all(self) -> dict[str, Any]:
        self._ensure_actor()
        assert self._cmd_q is not None
        fut: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        await self._cmd_q.put(("reload", None, fut))
        return await fut

    async def close(self) -> None:
        if self._actor_task is not None and not self._actor_task.done() and self._cmd_q is not None:
            fut: asyncio.Future[bool] = asyncio.get_running_loop().create_future()
            await self._cmd_q.put(("stop", None, fut))
            await fut
            await self._actor_task
        self._actor_task = None
        self._cmd_q = None

    def registry_summary(self) -> dict[str, Any]:
        return {
            "servers": self._server_meta,
            "total_tools": len(self._tools),
            "last_reloaded_at": self.last_reloaded_at,
            "reload_generation": self.reload_generation,
        }

    def list_registered_tools(self) -> list[RegisteredTool]:
        return list(self._tools.values())

    async def call_namespaced_tool(self, namespaced_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        self._ensure_actor()
        assert self._cmd_q is not None
        fut: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        await self._cmd_q.put(("call", (namespaced_name, arguments), fut))
        return await fut
