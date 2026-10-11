r"""Fraud risk assessment as a LangGraph StateGraph, run durably by Temporal.

    START -> collect_signals -> score_rules --(grey zone)--> llm_review -> finalize -> END
                                           \--(clear cut)----------------^

Each node is tagged with `execute_in`:

* "activity" for nodes that do I/O or call an LLM (non-deterministic). The
  Temporal LangGraph plugin runs them as activities: timeouts, retries, and
  their results recorded in history so a replay never calls the LLM again.
* "workflow" for pure, deterministic nodes. They run inline in the workflow,
  cost nothing, and replay for free. They MUST be `async def`: LangGraph runs
  sync nodes (and sync routing functions) in a thread pool, and workflow code
  is not allowed to spawn threads, so a sync node fails with NotImplementedError.

The graph is registered once on the worker:
    LangGraphPlugin(graphs={FRAUD_GRAPH: build_fraud_graph()})
and the workflow fetches it by name with `graph(FRAUD_GRAPH).compile()`.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

from langgraph.graph import END, START, StateGraph
from pydantic import BaseModel, Field
from temporalio.common import RetryPolicy
from typing_extensions import TypedDict

from shopsphere.ai.llm import is_fake
from shopsphere.services.db import connect

FRAUD_GRAPH = "fraud-risk"

HIGH_RISK_STATES = {"NV", "FL"}  # pretend these have elevated reshipping-fraud rates


class FraudState(TypedDict, total=False):
    order: dict[str, Any]
    signals: dict[str, Any]
    rule_score: float
    rule_reasons: list[str]
    llm: dict[str, Any] | None
    decision: str
    risk_score: float
    reasons: list[str]


class RiskAssessment(BaseModel):
    """Structured output the LLM must return."""

    risk_score: float = Field(ge=0, le=1, description="0 = clearly legitimate, 1 = clearly fraudulent")
    decision: str = Field(description="one of: approve, review, reject")
    rationale: str = Field(description="one or two sentences a human analyst can read")


# --------------------------------------------------------------------------
# Nodes
# --------------------------------------------------------------------------


def _customer(customer_id: str) -> dict[str, Any] | None:
    with connect() as conn:
        row = conn.execute("SELECT * FROM customers WHERE customer_id = ?", (customer_id,)).fetchone()
        return dict(row) if row else None


async def collect_signals(state: FraudState) -> dict[str, Any]:
    """[activity] Pull customer history from the CRM. I/O -> must be an activity."""
    order = state["order"]
    cust = await asyncio.to_thread(_customer, order["customer_id"])
    if cust is None:
        signals = {"known_customer": False, "account_age_days": 0, "lifetime_orders": 0, "chargebacks": 0}
    else:
        created = datetime.fromisoformat(cust["created_at"])
        signals = {
            "known_customer": True,
            "account_age_days": (datetime.now(timezone.utc) - created).days,
            "lifetime_orders": cust["lifetime_orders"],
            "chargebacks": cust["chargebacks"],
            "email_matches_profile": cust["email"].lower() == order["email"].lower(),
        }
    return {"signals": signals}


async def score_rules(state: FraudState) -> dict[str, Any]:
    """[workflow] Deterministic rule engine - pure function of state, safe to replay."""
    o, s = state["order"], state["signals"]
    score, reasons = 0.0, []
    if not s.get("known_customer"):
        score += 0.35
        reasons.append("unknown customer")
    if s.get("account_age_days", 0) < 7:
        score += 0.2
        reasons.append("account younger than 7 days")
    if s.get("chargebacks", 0) > 0:
        score += 0.3 * s["chargebacks"]
        reasons.append(f"{s['chargebacks']} prior chargeback(s)")
    if o["order_total"] > 1000:
        score += 0.25
        reasons.append("order over $1,000")
    if o["shipping_state"] in HIGH_RISK_STATES:
        score += 0.1
        reasons.append(f"ships to {o['shipping_state']}")
    if s.get("known_customer") and not s.get("email_matches_profile", True):
        score += 0.15
        reasons.append("email differs from profile")
    if s.get("lifetime_orders", 0) >= 10 and s.get("chargebacks", 0) == 0:
        score -= 0.2
        reasons.append("loyal customer with clean history")
    return {"rule_score": round(min(max(score, 0.0), 1.0), 2), "rule_reasons": reasons}


async def route_after_rules(state: FraudState) -> str:
    """[workflow] Only the ambiguous middle gets the (slow, paid) LLM."""
    return "llm_review" if 0.3 <= state["rule_score"] < 0.8 else "finalize"


async def llm_review(state: FraudState) -> dict[str, Any]:
    """[activity] Ask an LLM to weigh the signals. Non-deterministic -> activity."""
    order, signals = state["order"], state["signals"]
    model_name = order.get("ai_model", "fake:retail")

    if is_fake(model_name):
        # Offline stand-in that behaves like a cautious analyst.
        score = state["rule_score"]
        if signals.get("chargebacks", 0) > 0:
            decision, rationale = "reject", "Prior chargebacks plus current risk factors."
        elif order["order_total"] > 1000 and signals.get("account_age_days", 0) < 7:
            decision, rationale = "review", "New account placing a high-value order; worth a human look."
        else:
            decision, rationale = "approve", "Risk factors are mild and typical of legitimate first orders."
        return {"llm": {"risk_score": score, "decision": decision, "rationale": rationale, "model": model_name}}

    from langchain.chat_models import init_chat_model

    llm = init_chat_model(model_name, max_retries=0).with_structured_output(RiskAssessment)
    prompt = (
        "You are a fraud analyst at an online retailer. Given the order and the signals, "
        "return a risk score and a decision. Prefer 'review' over 'reject' when unsure; "
        "rejecting a real customer costs us money too.\n\n"
        f"Order: {order}\nSignals: {signals}\n"
        f"Rule engine score: {state['rule_score']} because {state['rule_reasons']}"
    )
    result: RiskAssessment = await llm.ainvoke(prompt)
    out = result.model_dump()
    out["model"] = model_name
    return {"llm": out}


async def finalize(state: FraudState) -> dict[str, Any]:
    """[workflow] Combine rules and (optional) LLM opinion into a final decision."""
    reasons = list(state.get("rule_reasons", []))
    llm = state.get("llm")
    if llm:
        decision = llm["decision"] if llm["decision"] in ("approve", "review", "reject") else "review"
        score = round((state["rule_score"] + float(llm["risk_score"])) / 2, 2)
        reasons.append(f"LLM: {llm['rationale']}")
    else:
        score = state["rule_score"]
        decision = "reject" if score >= 0.8 else "approve"
    return {"decision": decision, "risk_score": score, "reasons": reasons}


# --------------------------------------------------------------------------
# Graph
# --------------------------------------------------------------------------


def build_fraud_graph() -> StateGraph:
    g = StateGraph(FraudState)
    g.add_node(
        "collect_signals",
        collect_signals,
        metadata={
            "execute_in": "activity",
            "start_to_close_timeout": timedelta(seconds=10),
            "retry_policy": RetryPolicy(maximum_attempts=5),
            "summary": "Fetch customer risk signals",
        },
    )
    g.add_node("score_rules", score_rules, metadata={"execute_in": "workflow"})
    g.add_node(
        "llm_review",
        llm_review,
        metadata={
            "execute_in": "activity",
            "start_to_close_timeout": timedelta(seconds=60),
            "retry_policy": RetryPolicy(
                initial_interval=timedelta(seconds=2),
                backoff_coefficient=2.0,
                maximum_attempts=4,
            ),
            "summary": "LLM fraud review",
        },
    )
    g.add_node("finalize", finalize, metadata={"execute_in": "workflow"})

    g.add_edge(START, "collect_signals")
    g.add_edge("collect_signals", "score_rules")
    g.add_conditional_edges("score_rules", route_after_rules, ["llm_review", "finalize"])
    g.add_edge("llm_review", "finalize")
    g.add_edge("finalize", END)
    return g
