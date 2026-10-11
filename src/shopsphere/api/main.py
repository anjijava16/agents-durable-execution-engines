"""ShopSphere HTTP API.

The API is a thin Temporal *client*. It never does business logic itself; it
starts workflows, sends signals and updates, and runs queries. That keeps the
API stateless and instantly restartable - all state lives in Temporal.

    uvicorn shopsphere.api.main:app --reload
"""

from __future__ import annotations

import asyncio
import uuid
from contextlib import asynccontextmanager
from dataclasses import asdict

from fastapi import FastAPI, Header, HTTPException
from temporalio.client import Client, WorkflowUpdateFailedError
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError, RPCStatusCode

from shopsphere.api.schemas import (
    AddressIn,
    CancelIn,
    CarrierEventIn,
    ChatIn,
    CreateOrderIn,
    CreateOrderOut,
    DecisionIn,
    ReviewIn,
    StartSessionIn,
)
from shopsphere.config import get_settings
from shopsphere.models import (
    Address,
    ApprovalDecision,
    CancelRequest,
    CarrierEvent,
    ChatTurn,
    LineItem,
    OrderInput,
    ReviewDecision,
    SupportSessionInput,
)
from shopsphere.services.db import connect, init_db
from shopsphere.workflows.order_workflow import OrderFulfillmentWorkflow
from shopsphere.workflows.shipment_workflow import ShipmentWorkflow

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    # One client per process; it's thread-safe and multiplexes over one gRPC connection.
    app.state.temporal = await Client.connect(settings.temporal_address, namespace=settings.temporal_namespace)
    yield


app = FastAPI(
    title="ShopSphere",
    description="Durable retail order fulfillment and AI support on Temporal",
    version="1.0.0",
    lifespan=lifespan,
)


def temporal() -> Client:
    return app.state.temporal


def _not_found(err: RPCError, what: str) -> HTTPException:
    if err.status == RPCStatusCode.NOT_FOUND:
        return HTTPException(404, f"{what} not found")
    return HTTPException(502, f"Temporal error: {err.message}")


# ---------------------------------------------------------------- catalog


def _products() -> list[dict]:
    with connect() as conn:
        return [dict(r) for r in conn.execute("SELECT * FROM products ORDER BY sku")]


@app.get("/health")
async def health():
    return {"ok": True, "temporal": settings.temporal_address}


@app.get("/products")
async def products():
    return await asyncio.to_thread(_products)


# ---------------------------------------------------------------- orders


@app.post("/orders", response_model=CreateOrderOut, status_code=201)
async def create_order(body: CreateOrderIn, idempotency_key: str | None = Header(default=None)):
    """Place an order. Send the same Idempotency-Key twice and you get the same order back."""
    catalog = {p["sku"]: p for p in await asyncio.to_thread(_products)}
    unknown = [i.sku for i in body.items if i.sku not in catalog]
    if unknown:
        raise HTTPException(422, f"Unknown SKUs: {unknown}")

    # Deterministic id from the idempotency key -> the workflow id IS the dedupe key.
    order_id = f"ORD-{(idempotency_key or uuid.uuid4().hex)[:12].upper()}"
    workflow_id = f"order-{order_id}"
    order = OrderInput(
        order_id=order_id,
        customer_id=body.customer_id,
        email=body.email,
        items=[LineItem(i.sku, i.quantity, catalog[i.sku]["price"]) for i in body.items],
        shipping_address=Address(**body.shipping_address.model_dump()),
        payment_token=body.payment_token,
        ai_model=settings.llm_model,
        ai_task_queue=settings.ai_task_queue,
        review_timeout_seconds=settings.review_timeout_seconds,
        review_request_delay_seconds=settings.review_request_delay_seconds,
        delivery_sla_seconds=settings.delivery_sla_seconds,
        use_search_attributes=settings.use_search_attributes,
    )
    try:
        handle = await temporal().start_workflow(
            OrderFulfillmentWorkflow.run,
            order,
            id=workflow_id,
            task_queue=settings.orders_task_queue,
            # A retried POST attaches to the running order instead of creating a second one...
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
            # ...and an order id can never be reused once it has run.
            id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
        )
    except WorkflowAlreadyStartedError:
        return CreateOrderOut(
            order_id=order_id, workflow_id=workflow_id, run_id=None,
            status_url=f"/orders/{order_id}", already_existed=True,
        )
    return CreateOrderOut(
        order_id=order_id,
        workflow_id=workflow_id,
        run_id=handle.result_run_id,
        status_url=f"/orders/{order_id}",
    )


@app.get("/orders")
async def list_orders(status: str | None = None, customer_id: str | None = None, limit: int = 20):
    """Search orders through Temporal Visibility (needs the custom search attributes)."""
    query = "WorkflowType='OrderFulfillmentWorkflow'"
    if status:
        query += f" AND OrderStatus='{status.upper()}'"
    if customer_id:
        query += f" AND CustomerId='{customer_id}'"
    out = []
    async for wf in temporal().list_workflows(query, limit=limit):
        sa = wf.typed_search_attributes
        out.append(
            {
                "workflow_id": wf.id,
                "execution_status": wf.status.name if wf.status else None,
                "order_status": next((p.value for p in sa if p.key.name == "OrderStatus"), None),
                "customer_id": next((p.value for p in sa if p.key.name == "CustomerId"), None),
                "started": wf.start_time.isoformat(),
            }
        )
    return out


@app.get("/orders/{order_id}")
async def get_order(order_id: str):
    """Live order state via a Query - answered from workflow memory, no DB involved."""
    try:
        view = await temporal().get_workflow_handle(f"order-{order_id}").query(OrderFulfillmentWorkflow.get_status)
    except RPCError as err:
        raise _not_found(err, "Order")
    return asdict(view)


@app.post("/orders/{order_id}/cancel", status_code=202)
async def cancel_order(order_id: str, body: CancelIn):
    """Fire-and-forget Signal. 202 because the workflow decides at its next checkpoint."""
    try:
        await temporal().get_workflow_handle(f"order-{order_id}").signal(
            OrderFulfillmentWorkflow.cancel, CancelRequest(body.reason)
        )
    except RPCError as err:
        raise _not_found(err, "Order")
    return {"accepted": True, "note": "Cancellation is honoured if the order has not shipped yet."}


@app.post("/orders/{order_id}/review", status_code=202)
async def review_order(order_id: str, body: ReviewIn):
    """Fraud analyst decision for orders in PENDING_REVIEW."""
    try:
        await temporal().get_workflow_handle(f"order-{order_id}").signal(
            OrderFulfillmentWorkflow.submit_review, ReviewDecision(body.approved, body.reviewer, body.note)
        )
    except RPCError as err:
        raise _not_found(err, "Order")
    return {"accepted": True}


@app.put("/orders/{order_id}/shipping-address")
async def change_address(order_id: str, body: AddressIn):
    """Synchronous Update: either the new state comes back, or a 409 explains why not."""
    try:
        view = await temporal().get_workflow_handle(f"order-{order_id}").execute_update(
            OrderFulfillmentWorkflow.update_shipping_address, Address(**body.model_dump())
        )
    except WorkflowUpdateFailedError as err:
        raise HTTPException(409, str(err.cause.message if err.cause else err))
    except RPCError as err:
        raise _not_found(err, "Order")
    return asdict(view)


@app.post("/carrier/webhook", status_code=202)
async def carrier_webhook(event: CarrierEventIn):
    """What UPS/FedEx would call. We turn the webhook into a Signal on the shipment workflow."""
    try:
        await temporal().get_workflow_handle(f"shipment-{event.order_id}").signal(
            ShipmentWorkflow.carrier_event,
            CarrierEvent(order_id=event.order_id, status=event.status, location=event.location),
        )
    except RPCError as err:
        raise _not_found(err, "Shipment")
    return {"accepted": True}


# ---------------------------------------------------------------- AI support


@app.post("/support/sessions", status_code=201)
async def start_session(body: StartSessionIn):
    session_id = f"support-{uuid.uuid4().hex[:10]}"
    await temporal().start_workflow(
        "SupportAgentWorkflow",
        SupportSessionInput(session_id=session_id, customer_id=body.customer_id, ai_model=settings.llm_model),
        id=session_id,
        task_queue=settings.ai_task_queue,
    )
    return {"session_id": session_id}


@app.post("/support/sessions/{session_id}/messages")
async def chat(session_id: str, body: ChatIn):
    """Send a message and wait for the agent's answer (an Update round-trip)."""
    try:
        reply = await temporal().get_workflow_handle(session_id).execute_update("chat", ChatTurn(body.message), result_type=dict)
    except WorkflowUpdateFailedError as err:
        raise HTTPException(409, str(err.cause.message if err.cause else err))
    except RPCError as err:
        raise _not_found(err, "Session")
    return reply


@app.post("/support/sessions/{session_id}/decision")
async def decide(session_id: str, body: DecisionIn):
    """Supervisor approves or rejects the action the agent is waiting on."""
    try:
        reply = await temporal().get_workflow_handle(session_id).execute_update(
            "decide", ApprovalDecision(body.approve, body.reviewer, body.note), result_type=dict
        )
    except WorkflowUpdateFailedError as err:
        raise HTTPException(409, str(err.cause.message if err.cause else err))
    except RPCError as err:
        raise _not_found(err, "Session")
    return reply


@app.get("/support/sessions/{session_id}")
async def get_session(session_id: str):
    try:
        return await temporal().get_workflow_handle(session_id).query("get_session", result_type=dict)
    except RPCError as err:
        raise _not_found(err, "Session")


@app.post("/support/sessions/{session_id}/close", status_code=202)
async def close_session(session_id: str):
    try:
        await temporal().get_workflow_handle(session_id).signal("close")
    except RPCError as err:
        raise _not_found(err, "Session")
    return {"accepted": True}
