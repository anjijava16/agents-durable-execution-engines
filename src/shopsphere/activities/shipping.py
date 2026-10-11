"""Carrier integration activities."""

from __future__ import annotations

import asyncio
import hashlib

from temporalio import activity

from shopsphere.models import ShipmentInput, ShippingLabel
from shopsphere.services.db import connect, now_iso


def _pick_carrier(state: str) -> str:
    return "UPS" if state.upper() in {"CA", "WA", "OR", "NV", "AZ"} else "FedEx"


def _label(inp: ShipmentInput, carrier: str) -> ShippingLabel:
    with connect() as conn:
        row = conn.execute(
            "SELECT tracking_number, carrier FROM shipments WHERE order_id = ?", (inp.order_id,)
        ).fetchone()
        if row:
            return ShippingLabel(row["tracking_number"], row["carrier"])
        digest = hashlib.sha1(inp.order_id.encode()).hexdigest()[:12].upper()
        tracking = f"1Z{digest}"
        conn.execute(
            "INSERT INTO shipments (order_id, tracking_number, carrier, created_at) VALUES (?,?,?,?)",
            (inp.order_id, tracking, carrier, now_iso()),
        )
    return ShippingLabel(tracking, carrier)


@activity.defn
async def create_shipping_label(inp: ShipmentInput) -> ShippingLabel:
    """Buy a label from the carrier.

    This one is deliberately slow-ish and heartbeats while it works. If the
    worker dies mid-way, Temporal notices the missing heartbeat within
    heartbeat_timeout (instead of waiting for the full start_to_close_timeout)
    and retries on another worker, passing the last heartbeat details along
    so we can resume instead of starting over.
    """
    carrier = _pick_carrier(inp.address.state)
    steps = ["rate-shop", "validate-address", "purchase-label", "schedule-pickup"]

    done = 0
    details = activity.info().heartbeat_details
    if details:
        done = int(details[0])
        activity.logger.info("Resuming label creation at step %d after a retry", done)

    for idx in range(done, len(steps)):
        await asyncio.sleep(0.3)  # pretend to call the carrier API
        activity.heartbeat(idx + 1)

    return await asyncio.to_thread(_label, inp, carrier)
