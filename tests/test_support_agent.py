"""The Deep Agent support session: tools as activities, sub-agent, human approval."""

from __future__ import annotations

import uuid

import pytest
from temporalio.client import Client, WorkflowUpdateFailedError

from shopsphere.config import get_settings
from shopsphere.models import ApprovalDecision, ChatTurn, SupportSessionInput
from shopsphere.services.db import connect
from shopsphere.workflows.order_workflow import OrderFulfillmentWorkflow
from shopsphere.workflows.support_workflow import SupportAgentWorkflow

from .conftest import make_order
from .helpers import deliver

S = get_settings()


async def delivered_order(client: Client, **kw) -> str:
    order = make_order(**kw)
    handle = await client.start_workflow(
        OrderFulfillmentWorkflow.run, order, id=f"order-{order.order_id}", task_queue=S.orders_task_queue
    )
    await deliver(client, order.order_id)
    await handle.result()
    return order.order_id


async def new_session(client: Client):
    sid = f"support-{uuid.uuid4().hex[:8]}"
    return await client.start_workflow(
        SupportAgentWorkflow.run,
        SupportSessionInput(session_id=sid, customer_id="CUST-ALICE", ai_model="fake:retail"),
        id=sid,
        task_queue=S.ai_task_queue,
    )


async def test_order_status_question(client: Client):
    order_id = await delivered_order(client)
    session = await new_session(client)
    reply = await session.execute_update(SupportAgentWorkflow.chat, ChatTurn(f"Where is my order {order_id}?"))
    assert reply.status == "answered"
    assert order_id in reply.reply and "COMPLETED" in reply.reply
    await session.signal(SupportAgentWorkflow.close)
    view = await session.result()
    assert view.closed and view.turns == 1


async def test_policy_question_goes_to_subagent(client: Client):
    session = await new_session(client)
    reply = await session.execute_update(
        SupportAgentWorkflow.chat, ChatTurn("What's your return policy on final sale items?")
    )
    assert reply.status == "answered"
    assert "final sale" in reply.reply.lower()

    # The sub-agent's tool call shows up in history as a normal activity.
    activity_types = set()
    async for event in session.fetch_history_events():
        if event.HasField("activity_task_scheduled_event_attributes"):
            activity_types.add(event.activity_task_scheduled_event_attributes.activity_type.name)
    assert "lookup_return_policy" in activity_types
    assert "deepagents.invoke_model" in activity_types or any("model" in a for a in activity_types)
    await session.signal(SupportAgentWorkflow.close)


async def test_refund_requires_supervisor_approval(client: Client):
    order_id = await delivered_order(client, sku="SKU-COFFEE", price=24.5, qty=2)
    session = await new_session(client)

    reply = await session.execute_update(
        SupportAgentWorkflow.chat, ChatTurn(f"I'd like to return {order_id}, the beans arrived damaged")
    )
    assert reply.status == "awaiting_approval"
    assert reply.pending_action.tool == "issue_refund"
    assert reply.pending_action.args["request"]["amount"] == 49.0

    # While parked, no money has moved and new chat messages are refused by the validator.
    with connect() as conn:
        assert conn.execute("SELECT 1 FROM refunds WHERE order_id = ?", (order_id,)).fetchone() is None
    with pytest.raises(WorkflowUpdateFailedError):
        await session.execute_update(SupportAgentWorkflow.chat, ChatTurn("hello?"))

    final = await session.execute_update(
        SupportAgentWorkflow.decide, ApprovalDecision(True, "supervisor-dana")
    )
    assert final.status == "answered"
    assert "$49.00" in final.reply and f"RMA-{order_id}" in final.reply
    with connect() as conn:
        row = conn.execute("SELECT amount FROM refunds WHERE order_id = ?", (order_id,)).fetchone()
    assert row["amount"] == 49.0
    await session.signal(SupportAgentWorkflow.close)


async def test_refund_rejected_by_supervisor(client: Client):
    order_id = await delivered_order(client, sku="SKU-COFFEE", price=24.5)
    session = await new_session(client)
    reply = await session.execute_update(SupportAgentWorkflow.chat, ChatTurn(f"refund {order_id} please"))
    assert reply.status == "awaiting_approval"

    final = await session.execute_update(
        SupportAgentWorkflow.decide, ApprovalDecision(False, "supervisor-dana", "outside policy")
    )
    assert "didn't approve" in final.reply
    with connect() as conn:
        assert conn.execute("SELECT 1 FROM refunds WHERE order_id = ?", (order_id,)).fetchone() is None
    await session.signal(SupportAgentWorkflow.close)


async def test_final_sale_items_are_not_returnable(client: Client):
    order_id = await delivered_order(client, sku="SKU-SOCKS", price=18.0)
    session = await new_session(client)
    reply = await session.execute_update(SupportAgentWorkflow.chat, ChatTurn(f"I want to return {order_id}"))
    assert reply.status == "answered"
    assert "final sale" in reply.reply.lower()
    await session.signal(SupportAgentWorkflow.close)
