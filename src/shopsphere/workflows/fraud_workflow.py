"""Runs the LangGraph fraud graph durably. Lives on the AI task queue."""

from __future__ import annotations

from dataclasses import asdict

from temporalio import workflow
from temporalio.contrib.langgraph import graph

with workflow.unsafe.imports_passed_through():
    from shopsphere.ai.fraud_graph import FRAUD_GRAPH
    from shopsphere.models import FraudInput, FraudResult


@workflow.defn
class FraudAssessmentWorkflow:
    @workflow.run
    async def run(self, inp: FraudInput) -> FraudResult:
        app = graph(FRAUD_GRAPH).compile()
        final = await app.ainvoke({"order": asdict(inp)})
        workflow.logger.info(
            "Fraud decision for %s: %s (%.2f)", inp.order_id, final["decision"], final["risk_score"]
        )
        return FraudResult(
            decision=final["decision"],
            risk_score=final["risk_score"],
            reasons=final["reasons"],
            llm_used=final.get("llm") is not None,
        )
