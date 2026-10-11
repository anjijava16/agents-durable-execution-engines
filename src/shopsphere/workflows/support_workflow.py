"""SupportAgentWorkflow - a durable, long-lived chat session with a Deep Agent.

This is the "entity workflow" pattern: one workflow per support session that
stays open, receives each customer message as an Update, and answers
synchronously. When the agent wants to issue a refund, the Deep Agent's
human-in-the-loop interrupt pauses the graph; the session parks in
"awaiting_approval" until a supervisor sends the `decide` Update - minutes
or days later, across worker restarts.

Long sessions call continue-as-new so history never grows without bound.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from langgraph.types import Command

    from shopsphere.ai.support_agent import build_support_agent
    from shopsphere.models import (
        ApprovalDecision,
        ChatReply,
        ChatTurn,
        PendingAction,
        SupportSessionInput,
        SupportView,
    )


def _text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return " ".join(b.get("text", "") if isinstance(b, dict) else str(b) for b in content)
    return str(content)


@workflow.defn
class SupportAgentWorkflow:
    @workflow.init
    def __init__(self, inp: SupportSessionInput) -> None:
        self._inp = inp
        self._history: list[dict[str, str]] = list(inp.history)
        self._turn = inp.turn
        self._pending: PendingAction | None = None
        self._closed = False
        self._activity = 0
        # Updates can arrive concurrently; the agent must handle one at a time.
        self._lock = asyncio.Lock()
        self._agent = None
        self._thread_config: dict[str, Any] | None = None

    @workflow.run
    async def run(self, inp: SupportSessionInput) -> SupportView:
        while not self._closed:
            seen = self._activity
            try:
                await workflow.wait_condition(
                    lambda: self._closed
                    or self._activity != seen
                    or workflow.info().is_continue_as_new_suggested(),
                    timeout=timedelta(seconds=inp.idle_timeout_seconds),
                )
            except TimeoutError:
                self._history.append({"role": "system", "content": "Session closed after inactivity"})
                self._closed = True
                break

            if (
                workflow.info().is_continue_as_new_suggested()
                and self._pending is None
                and not self._lock.locked()
            ):
                await workflow.wait_condition(workflow.all_handlers_finished)
                workflow.continue_as_new(
                    SupportSessionInput(
                        session_id=inp.session_id,
                        customer_id=inp.customer_id,
                        ai_model=inp.ai_model,
                        idle_timeout_seconds=inp.idle_timeout_seconds,
                        history=self._history,
                        turn=self._turn,
                    )
                )

        await workflow.wait_condition(workflow.all_handlers_finished)
        return self._view()

    # ------------------------------------------------------------ agent

    def _get_agent(self):
        # Built lazily inside workflow context (the plugin needs that), once per run.
        if self._agent is None:
            self._agent = build_support_agent(self._inp.ai_model)
        return self._agent

    def _handle(self, result: dict[str, Any]) -> ChatReply:
        interrupts = result.get("__interrupt__")
        if interrupts:
            value = interrupts[0].value
            req = value["action_requests"][0]
            self._pending = PendingAction(
                tool=req["name"], args=dict(req["args"]), description=req.get("description", "")
            )
            friendly = {"issue_refund": "the refund"}.get(req["name"], f"the {req['name']} action")
            msg = (
                f"I've prepared {friendly} and sent it to a supervisor for approval. "
                "I'll confirm as soon as it's reviewed."
            )
            self._history.append({"role": "assistant", "content": msg})
            return ChatReply("awaiting_approval", msg, self._pending)

        answer = _text(result["messages"][-1].content)
        self._history.append({"role": "assistant", "content": answer})
        return ChatReply("answered", answer)

    # ------------------------------------------------------------ handlers

    @workflow.update
    async def chat(self, turn: ChatTurn) -> ChatReply:
        async with self._lock:
            self._activity += 1
            self._turn += 1
            self._history.append({"role": "user", "content": turn.message})
            # Each turn is its own LangGraph thread; the conversation so far is
            # passed in explicitly so it also survives continue-as-new.
            self._thread_config = {
                "configurable": {"thread_id": f"{workflow.info().workflow_id}-turn-{self._turn}"}
            }
            messages = [m for m in self._history if m["role"] in ("user", "assistant")]
            result = await self._get_agent().ainvoke({"messages": messages}, config=self._thread_config)
            return self._handle(result)

    @chat.validator
    def _validate_chat(self, turn: ChatTurn) -> None:
        if self._closed:
            raise ValueError("Session is closed")
        if self._pending is not None:
            raise ValueError("A supervisor decision is pending; try again once it's resolved")
        if not turn.message.strip():
            raise ValueError("Empty message")

    @workflow.update
    async def decide(self, decision: ApprovalDecision) -> ChatReply:
        async with self._lock:
            self._activity += 1
            pending = self._pending
            if decision.approve:
                resume = {"type": "approve"}
                note = f"Supervisor {decision.reviewer} approved {pending.tool if pending else 'action'}"
            else:
                resume = {
                    "type": "reject",
                    "message": f"Rejected by supervisor {decision.reviewer}: {decision.note or 'no reason given'}",
                }
                note = f"Supervisor {decision.reviewer} rejected {pending.tool if pending else 'action'}"
            self._history.append({"role": "system", "content": note})
            self._pending = None
            result = await self._get_agent().ainvoke(
                Command(resume={"decisions": [resume]}), config=self._thread_config
            )
            return self._handle(result)

    @decide.validator
    def _validate_decide(self, decision: ApprovalDecision) -> None:
        if self._pending is None:
            raise ValueError("Nothing is waiting for approval")

    @workflow.signal
    def close(self) -> None:
        self._closed = True

    @workflow.query
    def get_session(self) -> SupportView:
        return self._view()

    def _view(self) -> SupportView:
        return SupportView(
            session_id=self._inp.session_id,
            customer_id=self._inp.customer_id,
            turns=self._turn,
            closed=self._closed,
            history=list(self._history),
            pending_action=self._pending,
        )
