from __future__ import annotations

import asyncio

from temporalio.client import Client, WorkflowHandle
from temporalio.service import RPCError

from shopsphere.models import CarrierEvent, OrderStatus, OrderView
from shopsphere.workflows.order_workflow import OrderFulfillmentWorkflow
from shopsphere.workflows.shipment_workflow import ShipmentWorkflow


async def wait_for_status(
    handle: WorkflowHandle, *statuses: OrderStatus, timeout: float = 30
) -> OrderView:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        view = await handle.query(OrderFulfillmentWorkflow.get_status)
        if view.status in statuses:
            return view
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(f"status stuck at {view.status}, wanted {statuses}")
        await asyncio.sleep(0.2)


async def deliver(client: Client, order_id: str, timeout: float = 30) -> None:
    """Play the carrier: send scans to the shipment child workflow."""
    handle = client.get_workflow_handle(f"shipment-{order_id}")
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        try:
            await handle.describe()
            break
        except RPCError:
            if asyncio.get_running_loop().time() > deadline:
                raise
            await asyncio.sleep(0.2)
    for status, where in [("IN_TRANSIT", "Newark, NJ"), ("OUT_FOR_DELIVERY", "Philadelphia, PA"), ("DELIVERED", "Front door")]:
        await handle.signal(ShipmentWorkflow.carrier_event, CarrierEvent(order_id=order_id, status=status, location=where))
