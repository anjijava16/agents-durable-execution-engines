"""End-to-end tests of the order saga against a real Temporal server."""

from __future__ import annotations

import pytest
from temporalio.client import Client, WorkflowUpdateFailedError

from shopsphere.config import get_settings
from shopsphere.models import Address, CancelRequest, OrderStatus, ReviewDecision
from shopsphere.services.db import connect
from shopsphere.workflows.order_workflow import OrderFulfillmentWorkflow

from .conftest import make_order
from .helpers import deliver, wait_for_status

TQ = get_settings().orders_task_queue


def stock(sku: str) -> int:
    with connect() as conn:
        return conn.execute("SELECT stock FROM products WHERE sku = ?", (sku,)).fetchone()["stock"]


async def start(client: Client, order):
    return await client.start_workflow(
        OrderFulfillmentWorkflow.run, order, id=f"order-{order.order_id}", task_queue=TQ
    )


async def test_happy_path_completes_and_awards_points(client: Client):
    before = stock("SKU-HEADPHONES")
    order = make_order()
    handle = await start(client, order)

    await deliver(client, order.order_id)
    view = await handle.result()

    assert view.status == OrderStatus.COMPLETED
    assert view.fraud.decision == "approve"
    assert view.tracking_number.startswith("1Z")
    assert stock("SKU-HEADPHONES") == before - 1
    details = " | ".join(e.detail for e in view.timeline)
    assert "loyalty points" in details
    assert "Carrier: OUT_FOR_DELIVERY" in details
    with connect() as conn:
        templates = {r["template"] for r in conn.execute(
            "SELECT template FROM notifications WHERE order_id = ?", (order.order_id,))}
    assert {"order_confirmed", "order_delivered", "review_request"} <= templates


async def test_flaky_gateway_is_retried_transparently(client: Client):
    order = make_order(token="tok_flaky")
    handle = await start(client, order)
    await deliver(client, order.order_id)
    view = await handle.result()
    assert view.status == OrderStatus.COMPLETED
    assert any("captured on attempt 3" in e.detail for e in view.timeline)


async def test_declined_card_runs_compensation(client: Client):
    before = stock("SKU-SNEAKERS")
    order = make_order(sku="SKU-SNEAKERS", price=129.0, qty=2, token="tok_decline")
    view = await (await start(client, order)).result()

    assert view.status == OrderStatus.FAILED
    assert "PaymentDeclined" in view.failure_reason
    assert stock("SKU-SNEAKERS") == before  # reservation was released
    assert any("Compensated: release_inventory" in e.detail for e in view.timeline)


async def test_out_of_stock_fails_fast(client: Client):
    order = make_order(sku="SKU-TV", qty=50, price=2299.0)
    view = await (await start(client, order)).result()
    assert view.status == OrderStatus.FAILED
    assert "OutOfStock" in view.failure_reason


async def test_fraud_reject_never_touches_money(client: Client):
    order = make_order(customer_id="CUST-MALLORY")
    view = await (await start(client, order)).result()
    assert view.status == OrderStatus.REJECTED
    assert view.fraud.llm_used
    with connect() as conn:
        assert conn.execute("SELECT 1 FROM payments WHERE order_id = ?", (order.order_id,)).fetchone() is None


async def test_manual_review_approve_then_address_update(client: Client):
    order = make_order(customer_id="CUST-BOB", sku="SKU-LAPTOP", price=1499.0)
    handle = await start(client, order)
    view = await wait_for_status(handle, OrderStatus.PENDING_REVIEW)
    assert view.fraud.decision == "review"

    # Update with validator: allowed while not shipped, and returns the new view synchronously.
    new_addr = Address("Bob Chen", "500 Pine St", "Seattle", "WA", "98101")
    updated = await handle.execute_update(OrderFulfillmentWorkflow.update_shipping_address, new_addr)
    assert updated.shipping_address.city == "Seattle"

    await handle.signal(OrderFulfillmentWorkflow.submit_review, ReviewDecision(True, "analyst-kim", "verified by phone"))

    # Once the parcel has a label the validator rejects address changes.
    # The rejection is returned to the caller and nothing is written to history.
    await wait_for_status(handle, OrderStatus.SHIPPED)
    with pytest.raises(WorkflowUpdateFailedError):
        await handle.execute_update(OrderFulfillmentWorkflow.update_shipping_address, new_addr)

    await deliver(client, order.order_id)
    view = await handle.result()
    assert view.status == OrderStatus.COMPLETED
    assert view.carrier == "UPS"  # carrier picked from the *updated* WA address


async def test_review_deadline_auto_rejects(client: Client):
    order = make_order(customer_id="CUST-BOB", sku="SKU-LAPTOP", price=1499.0, review_timeout_seconds=1)
    view = await (await start(client, order)).result()
    assert view.status == OrderStatus.REJECTED
    assert "deadline" in view.timeline[-1].detail


async def test_customer_cancel_while_in_review(client: Client):
    order = make_order(customer_id="CUST-BOB", sku="SKU-LAPTOP", price=1499.0)
    handle = await start(client, order)
    await wait_for_status(handle, OrderStatus.PENDING_REVIEW)
    await handle.signal(OrderFulfillmentWorkflow.cancel, CancelRequest("found it cheaper elsewhere"))
    view = await handle.result()
    assert view.status == OrderStatus.CANCELLED
    assert view.cancel_reason == "found it cheaper elsewhere"


async def test_visibility_search_by_status(client: Client):
    order = make_order()
    handle = await start(client, order)
    await deliver(client, order.order_id)
    await handle.result()
    # Visibility is eventually consistent; poll briefly.
    import asyncio

    for _ in range(50):
        found = [w.id async for w in client.list_workflows(
            f"WorkflowType='OrderFulfillmentWorkflow' AND OrderStatus='COMPLETED' AND WorkflowId='order-{order.order_id}'"
        )]
        if found:
            break
        await asyncio.sleep(0.2)
    assert found == [f"order-{order.order_id}"]


async def test_lost_parcel_refunds_but_keeps_stock_decremented(client: Client):
    """After the pivot point only the refund remains as a compensation."""
    before = stock("SKU-COFFEE")
    order = make_order(sku="SKU-COFFEE", price=24.5, delivery_sla_seconds=1)
    view = await (await start(client, order)).result()  # no carrier scans ever arrive

    assert view.status == OrderStatus.FAILED
    assert "ShipmentLost" in view.failure_reason
    assert stock("SKU-COFFEE") == before - 1  # the box left the warehouse
    with connect() as conn:
        assert conn.execute("SELECT 1 FROM refunds WHERE order_id = ?", (order.order_id,)).fetchone()
    details = [e.detail for e in view.timeline]
    assert "Compensated: refund_payment" in details
    assert "Compensated: release_inventory" not in details
