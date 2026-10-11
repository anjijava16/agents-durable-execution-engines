"""Payment gateway activities.

Test tokens you can use from the API / demo:

    tok_visa     -> succeeds
    tok_flaky    -> gateway returns 503 on attempts 1 and 2, succeeds on 3
                    (watch the retries in the Temporal UI)
    tok_decline  -> card declined: non-retryable business failure, triggers
                    the saga compensation (inventory gets released)

CHAOS_PAYMENT_FAILURE_RATE=0.3 makes 30% of all charge attempts fail with a
transient error, which is a nice way to see that orders still complete.
"""

from __future__ import annotations

import asyncio
import random

from temporalio import activity
from temporalio.exceptions import ApplicationError

from shopsphere.config import get_settings
from shopsphere.models import ChargeRequest, PaymentReceipt, RefundReceipt, RefundRequest
from shopsphere.services.db import connect, now_iso


class GatewayUnavailable(Exception):
    """A transient error. Plain exceptions are retryable by default."""


def _charge(req: ChargeRequest, attempt: int) -> PaymentReceipt:
    with connect() as conn:
        # Idempotency key = order id. If attempt N charged the card but the
        # worker crashed before reporting success, attempt N+1 finds the
        # existing payment and returns it instead of charging twice.
        existing = conn.execute(
            "SELECT payment_id, amount FROM payments WHERE order_id = ?", (req.order_id,)
        ).fetchone()
        if existing:
            return PaymentReceipt(existing["payment_id"], existing["amount"], attempt)

        payment_id = f"PAY-{req.order_id}"
        conn.execute(
            "INSERT INTO payments (payment_id, order_id, amount, status, created_at) VALUES (?,?,?,?,?)",
            (payment_id, req.order_id, req.amount, "CAPTURED", now_iso()),
        )
    return PaymentReceipt(payment_id, req.amount, attempt)


@activity.defn
async def charge_payment(req: ChargeRequest) -> PaymentReceipt:
    """Capture payment for an order."""
    info = activity.info()
    activity.logger.info("Charging %s for %s (attempt %d)", req.amount, req.order_id, info.attempt)

    if req.payment_token == "tok_decline":
        raise ApplicationError(
            "Card was declined by the issuer",
            {"code": "card_declined"},
            type="PaymentDeclined",
            non_retryable=True,
        )
    if req.payment_token == "tok_flaky" and info.attempt < 3:
        raise GatewayUnavailable(f"503 from payment gateway (attempt {info.attempt})")

    rate = get_settings().payment_failure_rate
    if rate > 0 and random.random() < rate:
        raise GatewayUnavailable("Injected chaos: payment gateway timed out")

    # Simulate network latency to the gateway.
    await asyncio.sleep(0.2)
    return await asyncio.to_thread(_charge, req, info.attempt)


def _refund(req: RefundRequest) -> RefundReceipt:
    with connect() as conn:
        done = conn.execute(
            "SELECT refund_id, amount FROM refunds WHERE order_id = ?", (req.order_id,)
        ).fetchone()
        if done:
            return RefundReceipt(done["refund_id"], req.order_id, done["amount"], already_refunded=True)

        payment = conn.execute(
            "SELECT payment_id, amount FROM payments WHERE order_id = ?", (req.order_id,)
        ).fetchone()
        if payment is None:
            # Nothing was ever charged - refund is a no-op, not an error.
            return RefundReceipt("NONE", req.order_id, 0.0, already_refunded=True)

        amount = payment["amount"] if req.amount is None else min(req.amount, payment["amount"])
        refund_id = f"RFD-{req.order_id}"
        conn.execute(
            "INSERT INTO refunds (refund_id, order_id, amount, reason, created_at) VALUES (?,?,?,?,?)",
            (refund_id, req.order_id, amount, req.reason, now_iso()),
        )
        conn.execute(
            "UPDATE payments SET refunded_amount = ?, status = 'REFUNDED' WHERE order_id = ?",
            (amount, req.order_id),
        )
    return RefundReceipt(refund_id, req.order_id, amount)


@activity.defn
async def refund_payment(req: RefundRequest) -> RefundReceipt:
    """Compensation for charge_payment. Idempotent per order."""
    receipt = await asyncio.to_thread(_refund, req)
    activity.logger.info("Refund for %s: %s", req.order_id, receipt)
    return receipt
