"""Customer notifications (email/SMS) - printed and recorded, never sent twice."""

from __future__ import annotations

import asyncio
import json

from temporalio import activity

from shopsphere.models import Notification
from shopsphere.services.db import connect, now_iso


def _record(n: Notification, key: str) -> bool:
    with connect() as conn:
        cur = conn.execute(
            "INSERT OR IGNORE INTO notifications (idempotency_key, order_id, email, template, data, sent_at) "
            "VALUES (?,?,?,?,?,?)",
            (key, n.order_id, n.email, n.template, json.dumps(n.data), now_iso()),
        )
        return cur.rowcount == 1


@activity.defn
async def send_notification(n: Notification) -> bool:
    """Send a templated message to the customer.

    The idempotency key combines the workflow run and the activity id, which
    are stable across retries of the same activity execution. A retry after a
    timeout therefore can't spam the customer with duplicates.
    """
    info = activity.info()
    key = f"{info.workflow_id}:{info.workflow_run_id}:{info.activity_id}"
    first_time = await asyncio.to_thread(_record, n, key)
    if first_time:
        activity.logger.info("[email -> %s] %s %s", n.email, n.template, n.data)
    else:
        activity.logger.info("Duplicate notification suppressed (%s)", key)
    return first_time
