"""Inventory activities: reserve, release, and replenishment helpers."""

from __future__ import annotations

import asyncio
import json
import uuid

from temporalio import activity
from temporalio.exceptions import ApplicationError

from shopsphere.models import LowStockItem, PurchaseOrderRequest, ReserveRequest
from shopsphere.services.db import connect, now_iso


def _reserve(req: ReserveRequest) -> str:
    with connect() as conn:
        existing = conn.execute(
            "SELECT status FROM reservations WHERE order_id = ?", (req.order_id,)
        ).fetchone()
        if existing and existing["status"] == "RESERVED":
            # Idempotent: a retry after a lost response must not double-reserve.
            activity.logger.info("Reservation for %s already exists, reusing it", req.order_id)
            return f"RES-{req.order_id}"

        for item in req.items:
            row = conn.execute("SELECT stock FROM products WHERE sku = ?", (item.sku,)).fetchone()
            if row is None:
                raise ApplicationError(
                    f"Unknown SKU {item.sku}", type="UnknownSku", non_retryable=True
                )
            if row["stock"] < item.quantity:
                # Business error: retrying will not create stock. Fail fast.
                raise ApplicationError(
                    f"Only {row['stock']} left of {item.sku}, wanted {item.quantity}",
                    {"sku": item.sku, "available": row["stock"]},
                    type="OutOfStock",
                    non_retryable=True,
                )
        for item in req.items:
            conn.execute(
                "UPDATE products SET stock = stock - ? WHERE sku = ?", (item.quantity, item.sku)
            )
        conn.execute(
            "INSERT OR REPLACE INTO reservations (order_id, items, status, created_at) VALUES (?,?,?,?)",
            (req.order_id, json.dumps([i.__dict__ for i in req.items]), "RESERVED", now_iso()),
        )
    return f"RES-{req.order_id}"


@activity.defn
async def reserve_inventory(req: ReserveRequest) -> str:
    """Decrement stock for every line item, all-or-nothing."""
    return await asyncio.to_thread(_reserve, req)


def _release(order_id: str) -> bool:
    with connect() as conn:
        row = conn.execute(
            "SELECT items, status FROM reservations WHERE order_id = ?", (order_id,)
        ).fetchone()
        if row is None or row["status"] != "RESERVED":
            return False  # nothing to undo - compensation must be safe to run twice
        for item in json.loads(row["items"]):
            conn.execute(
                "UPDATE products SET stock = stock + ? WHERE sku = ?", (item["quantity"], item["sku"])
            )
        conn.execute("UPDATE reservations SET status = 'RELEASED' WHERE order_id = ?", (order_id,))
    return True


@activity.defn
async def release_inventory(order_id: str) -> bool:
    """Compensation for reserve_inventory. Safe to call any number of times."""
    released = await asyncio.to_thread(_release, order_id)
    activity.logger.info("Release inventory for %s -> %s", order_id, released)
    return released


def _low_stock() -> list[LowStockItem]:
    with connect() as conn:
        rows = conn.execute(
            "SELECT sku, stock, reorder_point FROM products WHERE stock <= reorder_point ORDER BY sku"
        ).fetchall()
    return [LowStockItem(sku=r["sku"], stock=r["stock"], reorder_point=r["reorder_point"]) for r in rows]


@activity.defn
async def get_low_stock_skus() -> list[LowStockItem]:
    return await asyncio.to_thread(_low_stock)


def _create_pos(req: PurchaseOrderRequest) -> int:
    created = 0
    with connect() as conn:
        for item in req.items:
            # Deterministic PO id per (schedule run, sku) -> idempotent on retry.
            po_id = f"PO-{req.run_id[:8]}-{item.sku}"
            cur = conn.execute(
                "INSERT OR IGNORE INTO purchase_orders (po_id, sku, quantity, created_at) VALUES (?,?,?,?)",
                (po_id, item.sku, req.reorder_quantity, now_iso()),
            )
            if cur.rowcount:
                conn.execute(
                    "UPDATE products SET stock = stock + ? WHERE sku = ?",
                    (req.reorder_quantity, item.sku),
                )
                created += 1
    return created


@activity.defn
async def create_purchase_orders(req: PurchaseOrderRequest) -> int:
    """Raise POs with suppliers. For the demo, stock arrives instantly."""
    return await asyncio.to_thread(_create_pos, req)


def new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10].upper()}"
