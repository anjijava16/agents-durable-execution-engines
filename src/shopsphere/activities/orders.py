"""Order system-of-record and loyalty activities."""

from __future__ import annotations

import asyncio
import json

from temporalio import activity

from shopsphere.models import LoyaltyRequest, OrderSnapshot
from shopsphere.services.db import connect, now_iso


def _save(s: OrderSnapshot) -> None:
    with connect() as conn:
        conn.execute(
            """
            INSERT INTO orders (order_id, customer_id, email, status, total, items,
                                tracking_number, delivered_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?)
            ON CONFLICT(order_id) DO UPDATE SET
                status = excluded.status,
                tracking_number = COALESCE(excluded.tracking_number, orders.tracking_number),
                delivered_at = COALESCE(excluded.delivered_at, orders.delivered_at),
                updated_at = excluded.updated_at
            """,
            (
                s.order_id,
                s.customer_id,
                s.email,
                s.status,
                s.total,
                json.dumps([i.__dict__ for i in s.items]),
                s.tracking_number,
                s.delivered_at,
                now_iso(),
            ),
        )


@activity.defn
async def save_order_snapshot(snapshot: OrderSnapshot) -> None:
    """Write the order's current state to the orders DB (an upsert, so retries are harmless).

    The workflow is the source of truth while the order is in flight; this
    table is a read model other systems (and the support agent) can query
    without talking to Temporal.
    """
    await asyncio.to_thread(_save, snapshot)


def _award(req: LoyaltyRequest) -> int:
    points = int(req.amount)  # 1 point per dollar
    with connect() as conn:
        cur = conn.execute(
            "INSERT OR IGNORE INTO loyalty_ledger (order_id, customer_id, points, created_at) VALUES (?,?,?,?)",
            (req.order_id, req.customer_id, points, now_iso()),
        )
        if cur.rowcount:
            conn.execute(
                "UPDATE customers SET loyalty_points = loyalty_points + ?, lifetime_orders = lifetime_orders + 1 "
                "WHERE customer_id = ?",
                (points, req.customer_id),
            )
    return points


@activity.defn
async def award_loyalty_points(req: LoyaltyRequest) -> int:
    """Added in v2 of the order workflow - see workflow.patched() in order_workflow.py."""
    return await asyncio.to_thread(_award, req)
