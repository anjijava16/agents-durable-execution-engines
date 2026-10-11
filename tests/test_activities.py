"""Activity unit tests: no Temporal server needed."""

from __future__ import annotations

import dataclasses
import os
import tempfile

os.environ.setdefault("RETAIL_DB_PATH", os.path.join(tempfile.mkdtemp(), "unit.db"))

from temporalio.testing import ActivityEnvironment  # noqa: E402

from shopsphere.activities.payments import charge_payment, refund_payment  # noqa: E402
from shopsphere.activities.shipping import create_shipping_label  # noqa: E402
from shopsphere.models import Address, ChargeRequest, LineItem, RefundRequest, ShipmentInput  # noqa: E402
from shopsphere.services.db import connect, init_db  # noqa: E402


async def test_label_resumes_from_last_heartbeat():
    init_db()
    env = ActivityEnvironment()
    # Pretend attempt 1 got through two of the four steps before its worker died.
    env.info = dataclasses.replace(env.info, heartbeat_details=[2], attempt=2)
    beats: list[int] = []
    env.on_heartbeat = lambda *details: beats.append(details[0])

    label = await env.run(
        create_shipping_label,
        ShipmentInput("ORD-UNIT-1", "a@example.com", Address("A", "1 St", "Austin", "TX", "78701"), [LineItem("SKU-COFFEE", 1, 24.5)]),
    )
    assert beats == [3, 4]  # only the remaining steps ran
    assert label.carrier == "FedEx" and label.tracking_number.startswith("1Z")


async def test_charge_and_refund_are_idempotent():
    init_db()
    env = ActivityEnvironment()
    req = ChargeRequest("ORD-UNIT-2", "CUST-ALICE", 49.0, "tok_visa")

    first = await env.run(charge_payment, req)
    second = await env.run(charge_payment, req)  # e.g. a retry after a lost response
    assert first.payment_id == second.payment_id

    r1 = await env.run(refund_payment, RefundRequest("ORD-UNIT-2", "test"))
    r2 = await env.run(refund_payment, RefundRequest("ORD-UNIT-2", "test"))
    assert r1.amount == 49.0 and not r1.already_refunded
    assert r2.already_refunded

    with connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM payments WHERE order_id='ORD-UNIT-2'").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM refunds WHERE order_id='ORD-UNIT-2'").fetchone()[0] == 1
