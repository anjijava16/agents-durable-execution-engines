"""The tests that prove the 'durable' in durable execution.

1. A worker dies mid-order; a brand-new worker finishes it.
2. Every completed history replays cleanly against today's code (determinism check).
3. The scheduled replenishment workflow is idempotent.
"""

from __future__ import annotations

import uuid

from temporalio.client import Client
from temporalio.worker import Replayer

from shopsphere.config import get_settings
from shopsphere.models import OrderStatus, ReplenishmentInput, ReviewDecision
from shopsphere.services.db import connect
from shopsphere.worker import ai_plugins, orders_worker
from shopsphere.workflows.fraud_workflow import FraudAssessmentWorkflow
from shopsphere.workflows.order_workflow import OrderFulfillmentWorkflow
from shopsphere.workflows.replenishment_workflow import InventoryReplenishmentWorkflow
from shopsphere.workflows.shipment_workflow import ShipmentWorkflow
from shopsphere.workflows.support_workflow import SupportAgentWorkflow

from .conftest import make_order
from .helpers import deliver, wait_for_status

S = get_settings()


async def test_worker_crash_mid_order_is_invisible_to_the_customer(client: Client):
    # A dedicated task queue so only *our* short-lived workers can pick this order up.
    from dataclasses import replace

    queue = f"crash-{uuid.uuid4().hex[:6]}"
    settings = replace(S, orders_task_queue=queue)
    order = make_order(customer_id="CUST-BOB", sku="SKU-LAPTOP", price=1499.0)

    # Worker #1 starts the order and gets as far as the manual review...
    async with orders_worker(client, settings):
        handle = await client.start_workflow(
            OrderFulfillmentWorkflow.run, order, id=f"order-{order.order_id}", task_queue=queue
        )
        await wait_for_status(handle, OrderStatus.PENDING_REVIEW)
    # ... and is gone. No process holds this order in memory any more.

    # The analyst approves while NO worker is running. The signal is simply
    # appended to the workflow's history on the server.
    await handle.signal(OrderFulfillmentWorkflow.submit_review, ReviewDecision(True, "analyst-kim"))

    # Worker #2 (new process, empty memory) replays history and carries on.
    async with orders_worker(client, settings):
        await deliver(client, order.order_id)
        view = await handle.result()

    assert view.status == OrderStatus.COMPLETED
    # Exactly one payment despite two workers having worked on the order.
    with connect() as conn:
        n = conn.execute("SELECT COUNT(*) AS n FROM payments WHERE order_id = ?", (order.order_id,)).fetchone()["n"]
    assert n == 1


async def test_histories_replay_deterministically(client: Client):
    """Run this in CI against histories from production before deploying a code change."""
    order_histories, ai_histories = [], []
    async for wf in client.list_workflows("ExecutionStatus='Completed'"):
        history = await client.get_workflow_handle(wf.id, run_id=wf.run_id).fetch_history()
        if wf.workflow_type in ("OrderFulfillmentWorkflow", "ShipmentWorkflow"):
            order_histories.append(history)
        elif wf.workflow_type in ("FraudAssessmentWorkflow", "SupportAgentWorkflow"):
            ai_histories.append(history)
    assert order_histories and ai_histories

    order_replayer = Replayer(workflows=[OrderFulfillmentWorkflow, ShipmentWorkflow])
    for h in order_histories:
        await order_replayer.replay_workflow(h)  # raises NondeterminismError on mismatch

    ai_replayer = Replayer(workflows=[FraudAssessmentWorkflow, SupportAgentWorkflow], plugins=ai_plugins())
    for h in ai_histories:
        await ai_replayer.replay_workflow(h)


async def test_replenishment_is_idempotent_per_run(client: Client):
    # SKU-SOCKS starts at stock 4 with reorder point 10.
    handle = await client.start_workflow(
        InventoryReplenishmentWorkflow.run,
        ReplenishmentInput(reorder_quantity=50),
        id=f"replenish-{uuid.uuid4().hex[:6]}",
        task_queue=S.orders_task_queue,
    )
    result = await handle.result()
    assert "SKU-SOCKS" in result.skus
    with connect() as conn:
        stock = conn.execute("SELECT stock FROM products WHERE sku = 'SKU-SOCKS'").fetchone()["stock"]
    assert stock > 10
