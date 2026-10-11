"""Activities that the AI support agent can call as tools.

Each one is a normal Temporal activity. `activity_as_tool(...)` (see
ai/support_agent.py) turns it into a LangChain tool, so when the LLM decides
to call `issue_refund`, what actually happens is `workflow.execute_activity`
with timeouts, retries and a durable record in history.

They return JSON strings because that is what ends up in the model's context.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone

from temporalio import activity

from shopsphere.activities.payments import _refund
from shopsphere.models import RefundRequest, RefundToolRequest
from shopsphere.services.db import connect, now_iso

RETURN_WINDOW_DAYS = 30
FINAL_SALE_SKUS = {"SKU-SOCKS"}  # hygiene items

RETURN_POLICY = {
    "window": f"Most items can be returned within {RETURN_WINDOW_DAYS} days of delivery for a full refund.",
    "final_sale": "Socks, underwear and opened personal-care items are final sale and cannot be returned.",
    "electronics": "Electronics must be returned with all accessories. Opened TVs and laptops are still refundable inside the window.",
    "refund_timing": "Refunds go back to the original payment method and usually show up in 3-5 business days.",
    "approval": "Refunds are reviewed by a support supervisor before money moves.",
    "shipping": "Return shipping is free with the prepaid label we email you.",
}


def _order_row(order_id: str):
    with connect() as conn:
        return conn.execute("SELECT * FROM orders WHERE order_id = ?", (order_id,)).fetchone()


@activity.defn
async def get_order_details(order_id: str) -> str:
    """Look up an order's status, items, total and tracking number by order id (e.g. ORD-1001)."""
    row = await asyncio.to_thread(_order_row, order_id)
    if row is None:
        return json.dumps({"found": False, "order_id": order_id})
    return json.dumps(
        {
            "found": True,
            "order_id": row["order_id"],
            "status": row["status"],
            "total": row["total"],
            "items": json.loads(row["items"]),
            "tracking_number": row["tracking_number"],
            "delivered_at": row["delivered_at"],
        }
    )


def _eligibility(order_id: str) -> dict:
    row = _order_row(order_id)
    if row is None:
        return {"eligible": False, "order_id": order_id, "reason": "Order not found"}
    with connect() as conn:
        refunded = conn.execute("SELECT 1 FROM refunds WHERE order_id = ?", (order_id,)).fetchone()
    if refunded:
        return {"eligible": False, "order_id": order_id, "reason": "Order was already refunded"}
    if row["status"] not in ("DELIVERED", "COMPLETED"):
        return {
            "eligible": False,
            "order_id": order_id,
            "reason": f"Order is {row['status']}; returns open once it is delivered",
        }
    items = json.loads(row["items"])
    returnable = [i for i in items if i["sku"] not in FINAL_SALE_SKUS]
    if not returnable:
        return {"eligible": False, "order_id": order_id, "reason": "All items are final sale"}
    delivered_at = datetime.fromisoformat(row["delivered_at"]) if row["delivered_at"] else None
    if delivered_at and datetime.now(timezone.utc) - delivered_at > timedelta(days=RETURN_WINDOW_DAYS):
        return {"eligible": False, "order_id": order_id, "reason": "Outside the 30-day return window"}
    amount = round(sum(i["quantity"] * i["unit_price"] for i in returnable), 2)
    return {
        "eligible": True,
        "order_id": order_id,
        "refundable_amount": amount,
        "returnable_skus": [i["sku"] for i in returnable],
        "excluded_skus": [i["sku"] for i in items if i["sku"] in FINAL_SALE_SKUS],
    }


@activity.defn
async def check_return_eligibility(order_id: str) -> str:
    """Check whether an order can be returned and how much can be refunded."""
    return json.dumps(await asyncio.to_thread(_eligibility, order_id))


def _rma(order_id: str) -> str:
    with connect() as conn:
        row = conn.execute("SELECT rma_number FROM return_labels WHERE order_id = ?", (order_id,)).fetchone()
        if row:
            return row["rma_number"]
        rma = f"RMA-{order_id}"
        conn.execute(
            "INSERT INTO return_labels (order_id, rma_number, created_at) VALUES (?,?,?)",
            (order_id, rma, now_iso()),
        )
        return rma


@activity.defn
async def create_return_label(order_id: str) -> str:
    """Create a prepaid return shipping label (RMA) for an order and email it to the customer."""
    rma = await asyncio.to_thread(_rma, order_id)
    return json.dumps({"order_id": order_id, "rma_number": rma, "label_emailed": True})


def _issue(req: RefundToolRequest) -> dict:
    receipt = _refund(RefundRequest(order_id=req.order_id, reason=req.reason, amount=req.amount))
    with connect() as conn:
        conn.execute(
            "UPDATE orders SET status = 'REFUNDED', updated_at = ? WHERE order_id = ?",
            (now_iso(), req.order_id),
        )
    return {
        "refund_id": receipt.refund_id,
        "order_id": receipt.order_id,
        "amount": receipt.amount,
        "already_refunded": receipt.already_refunded,
    }


@activity.defn
async def issue_refund(request: RefundToolRequest) -> str:
    """Refund an order to the original payment method. Requires supervisor approval."""
    return json.dumps(await asyncio.to_thread(_issue, request))


@activity.defn
async def lookup_return_policy(topic: str) -> str:
    """Look up the store's return policy. Topics: window, final_sale, electronics, refund_timing, approval, shipping, all."""
    if topic in RETURN_POLICY:
        return RETURN_POLICY[topic]
    return json.dumps(RETURN_POLICY)
