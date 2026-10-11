"""Worker processes.

    python -m shopsphere.worker orders   # order saga, shipments, replenishment
    python -m shopsphere.worker ai       # fraud LangGraph + support Deep Agent
    python -m shopsphere.worker all      # both, in one process (handy locally)

Why two pools? They fail and scale differently. The orders fleet is cheap,
fast and must always be up. The AI fleet is rate-limited by the LLM
provider, holds API keys, and is the part you'd most like to deploy often.
Temporal task queues let each pool pull only its own work.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from datetime import timedelta

from temporalio.client import Client
from temporalio.contrib.deepagents import DeepAgentsPlugin
from temporalio.contrib.langgraph import LangGraphPlugin
from temporalio.worker import Worker

from shopsphere.activities import ORDER_ACTIVITIES, SUPPORT_TOOL_ACTIVITIES
from shopsphere.ai.fraud_graph import FRAUD_GRAPH, build_fraud_graph
from shopsphere.ai.llm import model_provider
from shopsphere.config import Settings, get_settings
from shopsphere.services.db import init_db
from shopsphere.workflows.fraud_workflow import FraudAssessmentWorkflow
from shopsphere.workflows.order_workflow import OrderFulfillmentWorkflow
from shopsphere.workflows.replenishment_workflow import InventoryReplenishmentWorkflow
from shopsphere.workflows.shipment_workflow import ShipmentWorkflow
from shopsphere.workflows.support_workflow import SupportAgentWorkflow

log = logging.getLogger("shopsphere.worker")


def orders_worker(client: Client, settings: Settings) -> Worker:
    return Worker(
        client,
        task_queue=settings.orders_task_queue,
        workflows=[OrderFulfillmentWorkflow, ShipmentWorkflow, InventoryReplenishmentWorkflow],
        activities=ORDER_ACTIVITIES,
        max_concurrent_activities=50,
    )


def ai_plugins() -> list:
    return [
        LangGraphPlugin(
            graphs={FRAUD_GRAPH: build_fraud_graph()},
            default_activity_options={"start_to_close_timeout": timedelta(seconds=60)},
        ),
        DeepAgentsPlugin(
            model_provider=model_provider,
            model_activity_options={"start_to_close_timeout": timedelta(minutes=2)},
            tool_activity_options={"start_to_close_timeout": timedelta(seconds=30)},
        ),
    ]


def ai_worker(client: Client, settings: Settings, plugins: list | None = None) -> Worker:
    return Worker(
        client,
        task_queue=settings.ai_task_queue,
        workflows=[FraudAssessmentWorkflow, SupportAgentWorkflow],
        activities=SUPPORT_TOOL_ACTIVITIES,
        plugins=plugins if plugins is not None else ai_plugins(),
        # LLM providers rate-limit you; cap concurrency per worker so a burst
        # of orders queues up in Temporal instead of failing with 429s.
        max_concurrent_activities=8,
    )


async def main(role: str) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    settings = get_settings()
    init_db()
    client = await Client.connect(settings.temporal_address, namespace=settings.temporal_namespace)

    workers: list[Worker] = []
    if role in ("orders", "all"):
        workers.append(orders_worker(client, settings))
    if role in ("ai", "all"):
        workers.append(ai_worker(client, settings))
    if not workers:
        raise SystemExit(f"unknown role {role!r}; use orders | ai | all")

    log.info(
        "Starting %s worker(s) on %s (orders=%s, ai=%s, model=%s)",
        role, settings.temporal_address, settings.orders_task_queue, settings.ai_task_queue, settings.llm_model,
    )
    await asyncio.gather(*(w.run() for w in workers))


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else "all"))
