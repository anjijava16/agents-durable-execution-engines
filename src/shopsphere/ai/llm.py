"""Model plumbing.

Two jobs:

1. `model_provider(name)` - handed to `DeepAgentsPlugin(model_provider=...)`.
   It runs on the worker, inside the model activity. The workflow only ever
   ships the model *name*; API keys live in the worker's environment and
   never touch workflow history.

2. `RetailSupportFakeModel` - a deterministic stand-in for an LLM so the whole
   project runs offline (`LLM_MODEL=fake:retail`, the default). It behaves
   like a real tool-calling chat model: it looks at the conversation, emits
   tool calls, reads tool results and writes a final answer. Swap
   `LLM_MODEL` to a real provider string and nothing else in the system
   changes - that's the point.
"""

from __future__ import annotations

import json
import re
from collections.abc import Sequence
from typing import Any

from langchain_core.callbacks import AsyncCallbackManagerForLLMRun, CallbackManagerForLLMRun
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult

ORDER_ID_RE = re.compile(r"\bORD-[A-Za-z0-9-]+\b", re.IGNORECASE)

POLICY_WORDS = ("policy", "how long", "how many days", "final sale", "can i return", "return window", "refund take")
RETURN_WORDS = ("return", "refund", "send it back", "money back", "broken", "damaged", "doesn't fit", "does not fit")
STATUS_WORDS = ("where", "status", "track", "when will", "arrive", "shipped")


def is_fake(model_name: str) -> bool:
    return model_name.startswith("fake")


def model_provider(model_name: str) -> Any:
    """Build a chat model on the worker. Retries are Temporal's job, so the SDK's are off."""
    if is_fake(model_name):
        return RetailSupportFakeModel()
    from langchain.chat_models import init_chat_model

    return init_chat_model(model_name, max_retries=0)


def _text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):  # content blocks from newer chat models
        return " ".join(b.get("text", "") if isinstance(b, dict) else str(b) for b in content)
    return str(content)


def _tool_name(schema: Any) -> str | None:
    if isinstance(schema, dict):
        if "function" in schema:
            return schema["function"].get("name")
        return schema.get("name") or schema.get("title")
    return getattr(schema, "name", None)


class RetailSupportFakeModel(BaseChatModel):
    """A rule-based 'LLM' that drives the real agent loop end to end."""

    tool_names: list[str] = []

    @property
    def _llm_type(self) -> str:
        return "shopsphere-fake-retail"

    def bind_tools(self, tools: Sequence[Any], **kwargs: Any) -> "RetailSupportFakeModel":
        names = [n for n in (_tool_name(t) for t in tools) if n]
        return RetailSupportFakeModel(tool_names=names)

    # -- the "reasoning" ---------------------------------------------------

    def _decide(self, messages: list[BaseMessage]) -> AIMessage:
        last = messages[-1]
        human = next((m for m in reversed(messages) if isinstance(m, HumanMessage)), None)
        ask = _text(human.content) if human else ""
        low = ask.lower()
        match = ORDER_ID_RE.search(ask)
        order_id = match.group(0).upper() if match else None
        n = len(messages)

        def call(name: str, args: dict[str, Any], text: str = "") -> AIMessage:
            return AIMessage(
                content=text,
                tool_calls=[{"name": name, "args": args, "id": f"call_{name}_{n}", "type": "tool_call"}],
            )

        if isinstance(last, ToolMessage):
            return self._after_tool(last, messages, ask, order_id, call)

        wants_policy = any(w in low for w in POLICY_WORDS) and not order_id
        wants_return = any(w in low for w in RETURN_WORDS)
        wants_status = any(w in low for w in STATUS_WORDS)

        if wants_policy:
            if "task" in self.tool_names:
                # Main agent: delegate to the policy sub-agent (Deep Agents `task` tool).
                return call("task", {"description": ask, "subagent_type": "policy-expert"})
            if "lookup_return_policy" in self.tool_names:
                topic = (
                    "final_sale" if "final sale" in low or "socks" in low
                    else "refund_timing" if "take" in low or "how long" in low
                    else "window"
                )
                return call("lookup_return_policy", {"topic": topic})

        if wants_return and order_id:
            if "write_todos" in self.tool_names:
                plan = [
                    {"content": f"Check return eligibility for {order_id}", "status": "in_progress"},
                    {"content": "Create a prepaid return label", "status": "pending"},
                    {"content": "Issue the refund (needs supervisor approval)", "status": "pending"},
                    {"content": "Confirm next steps with the customer", "status": "pending"},
                ]
                return call("write_todos", {"todos": plan})
            return call("check_return_eligibility", {"order_id": order_id})

        if wants_return:
            return AIMessage(content="I can help with that return. What's your order number? It looks like ORD-1234.")

        if (wants_status or order_id) and order_id:
            return call("get_order_details", {"order_id": order_id})

        return AIMessage(
            content=(
                "Hi! I'm the ShopSphere assistant. I can track an order, start a return "
                "or explain our return policy. Include your order number (ORD-...) if you have one."
            )
        )

    def _after_tool(self, last: ToolMessage, messages, ask, order_id, call) -> AIMessage:
        name = last.name or ""
        body = _text(last.content)

        if name == "write_todos":
            return call("check_return_eligibility", {"order_id": order_id})

        if name == "check_return_eligibility":
            data = json.loads(body)
            if not data.get("eligible"):
                return AIMessage(content=f"I'm sorry - {order_id} can't be returned: {data.get('reason')}.")
            return call("create_return_label", {"order_id": order_id})

        if name == "create_return_label":
            elig = next(
                (json.loads(_text(m.content)) for m in reversed(messages)
                 if isinstance(m, ToolMessage) and m.name == "check_return_eligibility"),
                {},
            )
            return call(
                "issue_refund",
                {"request": {"order_id": order_id, "reason": ask[:200], "amount": elig.get("refundable_amount")}},
            )

        if name == "issue_refund":
            if last.status == "error" or "reject" in body.lower() or "declin" in body.lower():
                return AIMessage(
                    content=(
                        f"A supervisor reviewed the refund for {order_id} and didn't approve it. "
                        "Your return label is still valid, and someone from the team will follow up by email."
                    )
                )
            data = json.loads(body)
            rma = next(
                (json.loads(_text(m.content)).get("rma_number") for m in reversed(messages)
                 if isinstance(m, ToolMessage) and m.name == "create_return_label"),
                None,
            )
            return AIMessage(
                content=(
                    f"All set. I've refunded ${data['amount']:.2f} for {data['order_id']} "
                    f"(refund {data['refund_id']}). Your return label {rma} is in your inbox - "
                    "drop the package at any carrier location. The money usually lands in 3-5 business days."
                )
            )

        if name == "get_order_details":
            data = json.loads(body)
            if not data.get("found"):
                return AIMessage(content=f"I couldn't find {order_id}. Could you double-check the number?")
            tracking = f" Tracking: {data['tracking_number']}." if data.get("tracking_number") else ""
            return AIMessage(content=f"{data['order_id']} is currently {data['status']} (total ${data['total']:.2f}).{tracking}")

        if name == "lookup_return_policy":
            return AIMessage(content=f"Per our policy: {body}")

        if name == "task":
            return AIMessage(content=body)

        return AIMessage(content=body or "Done.")

    # -- BaseChatModel plumbing ----------------------------------------------

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: CallbackManagerForLLMRun | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        return ChatResult(generations=[ChatGeneration(message=self._decide(messages))])

    async def _agenerate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: AsyncCallbackManagerForLLMRun | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        return ChatResult(generations=[ChatGeneration(message=self._decide(messages))])
