"""CanaryBrake Cross-System Incident Copilot powered by Gemini + official MCP stdio servers."""
from __future__ import annotations

import os
from typing import Any

from google import genai
from google.genai import types

from mcp_hub import MultiServerMcpHub, RegisteredTool

SYSTEM_PROMPT = """You are CanaryBrake, a production checkout incident and revenue-rescue copilot operating across two strictly isolated microservices exposed via Synapse MCP servers:

1. `flag_edge_gateway` (Go 1.23 microservice): Tracks feature-flag rollouts (`checkout_gateway_v2`, `smart_retry_v1`, `fraud_guard_v3`), HTTP status codes (`200`, `503`, `504`), retry recovery counts, latencies, and cohort order/merchant mappings. It has ZERO visibility into bank authorization holds, dollar amounts, or merchant SLAs.
2. `payment_sla_ledger` (Python FastAPI microservice) & `replay_holds_custom` (Python Custom composed tool): Tracks payment intents (`CAPTURED`, `HOLD_STRANDED`, `REPLAYED_SAFE`), exact dollar amounts, active customer bank holds (`bank_hold_active`), `idempotency_key` locks, and Enterprise Tier-1 Merchant SLA breach countdowns. It has ZERO visibility into feature-flag cohorts.

STRICT OPERATING RULES:
- Never guess or invent numbers. Always call tools from BOTH systems when answering cross-system incident, root-cause, double-charge risk, or merchant-impact questions, and cross-reference `order_id` and `merchant_id` across both responses.
- When passing multiple merchant IDs or order IDs to path-parameter tools (`merchant_id`, `merchant_ids`, `order_ids`, `merchant_ids_csv`), join them as a single comma-separated string with no spaces (e.g. `id1,id2,id3`).
- CAPABILITY HONESTY: Before `synapse build --discover` runs, `flag_edge_gateway` only has 4 read (`get_*`) tools and NO tool to exclude merchants from a flag cohort. If asked to exclude/quarantine merchants from a flag when no exclude tool is present in your tool declarations, clearly state that `flag-edge-gateway` currently lacks a cohort-exclusion endpoint on its MCP surface (suggesting `synapse build --discover` once the endpoint is added). Once the exclude tool appears in your tool list, call it immediately!
- SAFETY CONFIRMATION GATE: For `replay_holds_custom__replay_stranded_merchant_holds`, ALWAYS call with `confirm=False` first to show the dry-run inspection preview (order count, exact dollar amount, active bank holds) and ask the operator to confirm. ONLY pass `confirm=True` when the user explicitly confirms execution (e.g. "Confirm", "Yes, execute replay", "Proceed with confirm=True").
- Format responses concisely using crisp Markdown bullet points, bold metrics, and merchant display names alongside IDs."""


def _sanitize_schema(raw: dict[str, Any]) -> dict[str, Any]:
    props = {}
    for k, v in (raw.get("properties") or {}).items():
        if not isinstance(v, dict):
            continue
        t = v.get("type", "string")
        if isinstance(t, list):
            t = next((x for x in t if x != "null"), "string")
        entry: dict[str, Any] = {"type": t}
        if "description" in v and isinstance(v["description"], str):
            entry["description"] = v["description"]
        if t == "array":
            entry["items"] = {"type": "string"}
        props[k] = entry

    out: dict[str, Any] = {"type": "object", "properties": props}
    req = [r for r in (raw.get("required") or []) if r in props]
    if req:
        out["required"] = req
    return out


def _build_declarations(reg_tools: list[RegisteredTool]) -> list[types.FunctionDeclaration]:
    decls = []
    for t in reg_tools:
        decls.append(
            types.FunctionDeclaration(
                name=t.namespaced_name,
                description=f"[{t.server_label} | {t.language_badge}] {t.description}",
                parameters_json_schema=_sanitize_schema(t.input_schema),
            )
        )
    return decls


class CanaryBrakeCopilot:
    def __init__(self, hub: MultiServerMcpHub, model_id: str = "gemini-3.5-flash") -> None:
        api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("SYNAPSE_LLM_API_KEY")
        if not api_key:
            raise RuntimeError("GEMINI_API_KEY or SYNAPSE_LLM_API_KEY is required")
        self.client = genai.Client(api_key=api_key)
        self.hub = hub
        self.model_id = model_id
        self.history: list[types.Content] = []

    def clear_history(self) -> None:
        self.history = []

    async def ask(self, user_message: str, max_rounds: int = 8) -> dict[str, Any]:
        reg_tools = self.hub.list_registered_tools()
        decls = _build_declarations(reg_tools)
        tool_cfg = [types.Tool(function_declarations=decls)] if decls else None

        working_contents: list[types.Content] = [
            *self.history,
            types.Content(role="user", parts=[types.Part.from_text(text=user_message)]),
        ]

        tool_traces: list[dict[str, Any]] = []
        final_text = ""

        for _ in range(max_rounds):
            resp = await self.client.aio.models.generate_content(
                model=self.model_id,
                contents=working_contents,
                config=types.GenerateContentConfig(
                    system_instruction=SYSTEM_PROMPT,
                    tools=tool_cfg,
                    thinking_config=types.ThinkingConfig(thinking_budget=256),
                ),
            )

            cand = resp.candidates[0]
            model_content = cand.content
            working_contents.append(model_content)

            fn_calls = [p.function_call for p in (model_content.parts or []) if p.function_call]
            if not fn_calls:
                final_text = "".join(
                    p.text for p in (model_content.parts or []) if p.text and not getattr(p, "thought", False)
                ).strip()
                break

            fn_response_parts: list[types.Part] = []
            for fc in fn_calls:
                args = dict(fc.args) if fc.args else {}
                trace = await self.hub.call_namespaced_tool(fc.name, args)
                tool_traces.append(trace)
                fn_response_parts.append(
                    types.Part.from_function_response(
                        name=fc.name,
                        response={"result": trace["output"]},
                    )
                )

            working_contents.append(types.Content(role="user", parts=fn_response_parts))

        if not final_text:
            final_text = "Completed tool execution; review trace cards above for details."

        # Slice history ONLY on clean user text turn boundaries so function_call/function_response pairs never split
        user_turn_indices = [
            idx
            for idx, item in enumerate(working_contents)
            if item.role == "user" and all(not getattr(pt, "function_response", None) for pt in (item.parts or []))
        ]
        keep_from = user_turn_indices[-6] if len(user_turn_indices) > 6 else (user_turn_indices[0] if user_turn_indices else 0)
        self.history = working_contents[keep_from:]

        return {
            "answer": final_text,
            "tool_traces": tool_traces,
            "active_tool_count": len(reg_tools),
            "registry": self.hub.registry_summary(),
        }
