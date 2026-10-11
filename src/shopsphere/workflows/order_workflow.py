"""OrderFulfillmentWorkflow - the heart of the project.

One workflow execution == one order, from "Place order" to "How did we do?"
email days later. Read this file top to bottom alongside docs/06-order-workflow-walkthrough.md.

    RECEIVED
      -> FRAUD_CHECK            child workflow on the AI task queue (LangGraph)
      -> [PENDING_REVIEW]       only if fraud says "review": wait for a human, with a deadline
      -> INVENTORY_RESERVED     activity, compensation = release_inventory
      -> PAID                   activity, compensation = refund_payment
      -> FULFILLING / SHIPPED / OUT_FOR_DELIVERY / DELIVERED   child workflow, progress via signals
      -> COMPLETED              after a durable timer sends the review-request email

Any failure after money or stock was touched runs the compensations in
reverse order (the saga pattern). A customer cancel (signal) before shipping
does the same. Address changes go through an Update with a validator.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta
from typing import Awaitable, Callable

from temporalio import workflow
from temporalio.common import RetryPolicy, SearchAttributeKey
from temporalio.exceptions import ActivityError, ApplicationError, ChildWorkflowError
from temporalio.workflow import ParentClosePolicy

with workflow.unsafe.imports_passed_through():
    from shopsphere.activities.inventory import release_inventory, reserve_inventory
    from shopsphere.activities.notifications import send_notification
    from shopsphere.activities.orders import award_loyalty_points, save_order_snapshot
    from shopsphere.activities.payments import charge_payment, refund_payment
    from shopsphere.models import (
        TERMINAL_STATUSES,
        Address,
        CancelRequest,
        CarrierEvent,
        ChargeRequest,
        FraudInput,
        FraudResult,
        LoyaltyRequest,
        Notification,
        OrderInput,
        OrderSnapshot,
        OrderStatus,
        OrderView,
        RefundRequest,
        ReserveRequest,
        ReviewDecision,
        ShipmentInput,
        ShipmentResult,
        TimelineEvent,
    )
    from shopsphere.workflows.shipment_workflow import ShipmentWorkflow

ORDER_STATUS_KEY = SearchAttributeKey.for_keyword("OrderStatus")
CUSTOMER_ID_KEY = SearchAttributeKey.for_keyword("CustomerId")

# Retry policies are part of your business logic, so they live next to it.
DB_RETRY = RetryPolicy(initial_interval=timedelta(milliseconds=500), maximum_attempts=10)
INVENTORY_RETRY = RetryPolicy(
    maximum_attempts=5, non_retryable_error_types=["OutOfStock", "UnknownSku"]
)
PAYMENT_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=8,
    non_retryable_error_types=["PaymentDeclined"],
)
# Compensations must eventually succeed, so they retry (practically) forever.
COMPENSATION_RETRY = RetryPolicy(initial_interval=timedelta(seconds=1), maximum_interval=timedelta(minutes=1))


class OrderCancelled(Exception):
    """Internal control-flow exception: the customer cancelled."""


class OrderRejected(Exception):
    """Internal control-flow exception: fraud/review said no."""


@dataclass
class _Compensation:
    name: str
    fn: Callable[[], Awaitable[object]]


@workflow.defn
class OrderFulfillmentWorkflow:
    @workflow.init
    def __init__(self, order: OrderInput) -> None:
        # @workflow.init runs before any signal/update/query handler, so state
        # is guaranteed to exist even if a signal arrives in the same task as start.
        self._order = order
        self._status = OrderStatus.RECEIVED
        self._address = order.shipping_address
        self._timeline: list[TimelineEvent] = []
        self._fraud: FraudResult | None = None
        self._review: ReviewDecision | None = None
        self._cancel: CancelRequest | None = None
        self._tracking: str | None = None
        self._carrier: str | None = None
        self._delivered_at: str | None = None
        self._failure: str | None = None
        self._shipment_started = False
        self._compensations: list[_Compensation] = []

    # ------------------------------------------------------------------ run

    @workflow.run
    async def run(self, order: OrderInput) -> OrderView:
        if order.use_search_attributes:
            workflow.upsert_search_attributes([CUSTOMER_ID_KEY.value_set(order.customer_id)])
        await self._set_status(OrderStatus.RECEIVED, f"Order for ${order.total():.2f} received")

        try:
            await self._fraud_check()
            self._ensure_not_cancelled()

            await self._reserve_inventory()
            self._ensure_not_cancelled()

            await self._charge_payment()
            await self._notify("order_confirmed", {"total": order.total()})

            # Versioning example: loyalty points were added after v1 shipped.
            # Old executions that already passed this point replay without it;
            # new ones take the new branch. See docs/09-versioning.md.
            if workflow.patched("loyalty-points-v2"):
                points = await workflow.execute_activity(
                    award_loyalty_points,
                    LoyaltyRequest(order.customer_id, order.order_id, order.total()),
                    start_to_close_timeout=timedelta(seconds=10),
                    retry_policy=DB_RETRY,
                )
                self._event(self._status, f"Awarded {points} loyalty points")

            self._ensure_not_cancelled()  # last chance to cancel
            await self._ship()

            # A durable timer. This can be 3 days; the worker can restart 50
            # times in between and the email still goes out once, on time.
            await workflow.sleep(timedelta(seconds=order.review_request_delay_seconds))
            await self._notify("review_request", {"tracking": self._tracking})
            await self._set_status(OrderStatus.COMPLETED, "Review request sent, order closed")

        except OrderCancelled:
            await self._compensate()
            reason = self._cancel.reason if self._cancel else "cancelled"
            await self._set_status(OrderStatus.CANCELLED, f"Cancelled: {reason}")
            await self._notify("order_cancelled", {"reason": reason})

        except OrderRejected as rej:
            await self._compensate()
            await self._set_status(OrderStatus.REJECTED, str(rej))
            await self._notify("order_rejected", {"reason": str(rej)})

        except (ActivityError, ChildWorkflowError) as err:
            # A business failure (declined card, out of stock, lost parcel ...)
            # is not a *workflow* failure. We undo what we did and finish
            # cleanly with a FAILED order that explains why.
            self._failure = _root_cause(err)
            workflow.logger.warning("Order %s failed: %s", order.order_id, self._failure)
            await self._compensate()
            await self._set_status(OrderStatus.FAILED, self._failure)
            await self._notify("order_failed", {"reason": self._failure})

        except asyncio.CancelledError:
            # Someone cancelled the *workflow* itself (temporal workflow cancel).
            # Clean up, then let the cancellation propagate.
            self._event(self._status, "Workflow cancellation requested by operator")
            await self._compensate()
            await self._set_status(OrderStatus.CANCELLED, "Cancelled by operator")
            raise

        # Don't return while a signal/update handler is still mid-flight.
        await workflow.wait_condition(workflow.all_handlers_finished)
        return self._view()

    # ------------------------------------------------------------ the steps

    async def _fraud_check(self) -> None:
        order = self._order
        await self._set_status(OrderStatus.FRAUD_CHECK, "Running fraud assessment")
        try:
            # Call by *name* + task queue: the orders worker doesn't need to
            # import any AI code, and the AI fleet scales independently.
            self._fraud = await workflow.execute_child_workflow(
                "FraudAssessmentWorkflow",
                FraudInput(
                    order_id=order.order_id,
                    customer_id=order.customer_id,
                    email=order.email,
                    order_total=order.total(),
                    item_count=sum(i.quantity for i in order.items),
                    shipping_state=self._address.state,
                    shipping_country=self._address.country,
                    ai_model=order.ai_model,
                ),
                id=f"fraud-{order.order_id}",
                task_queue=order.ai_task_queue,
                result_type=FraudResult,
                execution_timeout=timedelta(minutes=10),
            )
        except ChildWorkflowError as err:
            # Graceful degradation: if the AI fleet is down for 10 minutes we
            # don't lose the sale, we route the order to a human instead.
            self._fraud = FraudResult("review", 0.5, [f"fraud service unavailable: {_root_cause(err)}"])

        fraud = self._fraud
        self._event(OrderStatus.FRAUD_CHECK, f"Fraud decision: {fraud.decision} (score {fraud.risk_score})")

        if fraud.decision == "reject":
            raise OrderRejected(f"Rejected by fraud screening: {', '.join(fraud.reasons)}")

        if fraud.decision == "review":
            await self._set_status(OrderStatus.PENDING_REVIEW, "Waiting for a fraud analyst")
            await self._notify("ops_review_needed", {"risk": fraud.risk_score, "reasons": fraud.reasons})
            try:
                await workflow.wait_condition(
                    lambda: self._review is not None or self._cancel is not None,
                    timeout=timedelta(seconds=order.review_timeout_seconds),
                )
            except TimeoutError:
                raise OrderRejected("No analyst decision before the review deadline")
            self._ensure_not_cancelled()
            assert self._review is not None
            if not self._review.approved:
                raise OrderRejected(f"Rejected by analyst {self._review.reviewer}: {self._review.note}")
            self._event(OrderStatus.PENDING_REVIEW, f"Approved by {self._review.reviewer}")

    async def _reserve_inventory(self) -> None:
        order = self._order
        await workflow.execute_activity(
            reserve_inventory,
            ReserveRequest(order.order_id, order.items),
            start_to_close_timeout=timedelta(seconds=15),
            retry_policy=INVENTORY_RETRY,
        )
        # Register the undo *after* the do succeeded. (Registering before is
        # also valid when the undo is idempotent - see docs/07-saga.md.)
        self._compensations.append(
            _Compensation(
                "release_inventory",
                lambda: workflow.execute_activity(
                    release_inventory,
                    order.order_id,
                    start_to_close_timeout=timedelta(seconds=15),
                    retry_policy=COMPENSATION_RETRY,
                ),
            )
        )
        await self._set_status(OrderStatus.INVENTORY_RESERVED, "Stock reserved")

    async def _charge_payment(self) -> None:
        order = self._order
        # Register first: if the charge times out we don't know whether the
        # card was charged, and refund_payment is a safe no-op when it wasn't.
        self._compensations.append(
            _Compensation(
                "refund_payment",
                lambda: workflow.execute_activity(
                    refund_payment,
                    RefundRequest(order.order_id, reason="order not fulfilled"),
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=COMPENSATION_RETRY,
                ),
            )
        )
        receipt = await workflow.execute_activity(
            charge_payment,
            ChargeRequest(order.order_id, order.customer_id, order.total(), order.payment_token),
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=PAYMENT_RETRY,
        )
        await self._set_status(
            OrderStatus.PAID, f"Payment {receipt.payment_id} captured on attempt {receipt.attempts}"
        )

    async def _ship(self) -> None:
        order = self._order
        self._shipment_started = True
        # Stock has physically left the building: releasing it is no longer a valid undo.
        self._compensations = [c for c in self._compensations if c.name != "release_inventory"]
        await self._set_status(OrderStatus.FULFILLING, "Handed to the warehouse")

        result: ShipmentResult = await workflow.execute_child_workflow(
            ShipmentWorkflow.run,
            ShipmentInput(
                order_id=order.order_id,
                email=order.email,
                address=self._address,
                items=order.items,
                delivery_sla_seconds=order.delivery_sla_seconds,
            ),
            id=f"shipment-{order.order_id}",
            task_queue=workflow.info().task_queue,
            # If the order workflow is terminated, the shipment keeps going -
            # the box is already on a truck.
            parent_close_policy=ParentClosePolicy.ABANDON,
        )
        self._delivered_at = result.delivered_at
        self._compensations.clear()  # delivered: nothing left to undo
        await self._set_status(OrderStatus.DELIVERED, f"Delivered at {result.delivered_at}")
        await self._notify("order_delivered", {"tracking": result.tracking_number})

    async def _compensate(self) -> None:
        """Run undo steps in reverse order. Each one is idempotent and retried."""
        for comp in reversed(self._compensations):
            try:
                await comp.fn()
                self._event(self._status, f"Compensated: {comp.name}")
            except ActivityError as err:
                # With COMPENSATION_RETRY this only happens if the activity is
                # non-retryably broken. Record it loudly and keep unwinding.
                workflow.logger.error("Compensation %s failed: %s", comp.name, err)
                self._event(self._status, f"COMPENSATION FAILED: {comp.name} - needs manual action")
        self._compensations.clear()

    # ------------------------------------------------------------ handlers

    @workflow.signal
    def cancel(self, req: CancelRequest) -> None:
        """Customer asked to cancel. Honoured at the next checkpoint, if not yet shipped."""
        if self._shipment_started or self._status in TERMINAL_STATUSES:
            self._event(self._status, f"Cancel ignored (already {self._status.value}): {req.reason}")
            return
        self._cancel = req

    @workflow.signal
    def submit_review(self, decision: ReviewDecision) -> None:
        """A fraud analyst approved or rejected the order."""
        if self._status == OrderStatus.PENDING_REVIEW and self._review is None:
            self._review = decision

    @workflow.signal
    def shipment_progress(self, event: CarrierEvent) -> None:
        """Sent by the ShipmentWorkflow child as carrier scans come in."""
        self._tracking = event.tracking_number or self._tracking
        self._carrier = event.carrier or self._carrier
        mapping = {
            "LABEL_CREATED": OrderStatus.SHIPPED,
            "IN_TRANSIT": OrderStatus.SHIPPED,
            "OUT_FOR_DELIVERY": OrderStatus.OUT_FOR_DELIVERY,
        }
        new_status = mapping.get(event.status)
        if new_status:
            self._status = new_status
            self._upsert_status()
        self._event(self._status, f"Carrier: {event.status} {event.location}".strip())

    @workflow.update
    async def update_shipping_address(self, address: Address) -> OrderView:
        """Change where the parcel goes. Synchronous: the caller gets the new view back."""
        old = self._address
        self._address = address
        self._event(self._status, f"Address changed from {old.city} to {address.city}")
        await self._snapshot()  # handlers may run activities too
        return self._view()

    @update_shipping_address.validator
    def _validate_address(self, address: Address) -> None:
        # Validators run before the update is accepted and must not block or
        # mutate state. Raising here rejects the update and leaves NO trace in history.
        if self._shipment_started or self._status in TERMINAL_STATUSES:
            raise ValueError(f"Too late to change the address: order is {self._status.value}")
        if not address.postal_code.strip():
            raise ValueError("postal_code is required")

    @workflow.query
    def get_status(self) -> OrderView:
        return self._view()

    # ------------------------------------------------------------ helpers

    def _ensure_not_cancelled(self) -> None:
        if self._cancel is not None:
            raise OrderCancelled()

    def _event(self, status: OrderStatus, detail: str) -> None:
        # workflow.now() is deterministic (it's the time of the current
        # workflow task), unlike datetime.now().
        self._timeline.append(TimelineEvent(workflow.now().isoformat(), status.value, detail))

    def _upsert_status(self) -> None:
        if self._order.use_search_attributes:
            workflow.upsert_search_attributes([ORDER_STATUS_KEY.value_set(self._status.value)])

    async def _set_status(self, status: OrderStatus, detail: str) -> None:
        self._status = status
        self._event(status, detail)
        self._upsert_status()
        await self._snapshot()

    async def _snapshot(self) -> None:
        o = self._order
        await workflow.execute_activity(
            save_order_snapshot,
            OrderSnapshot(
                order_id=o.order_id,
                customer_id=o.customer_id,
                email=o.email,
                status=self._status.value,
                total=o.total(),
                items=o.items,
                tracking_number=self._tracking,
                delivered_at=self._delivered_at,
            ),
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=DB_RETRY,
        )

    async def _notify(self, template: str, data: dict) -> None:
        await workflow.execute_activity(
            send_notification,
            Notification(self._order.order_id, self._order.email, template, data),
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=DB_RETRY,
        )

    def _view(self) -> OrderView:
        o = self._order
        return OrderView(
            order_id=o.order_id,
            customer_id=o.customer_id,
            status=self._status.value,
            total=o.total(),
            shipping_address=self._address,
            fraud=self._fraud,
            tracking_number=self._tracking,
            carrier=self._carrier,
            cancel_reason=self._cancel.reason if self._cancel else None,
            failure_reason=self._failure,
            timeline=list(self._timeline),
        )


def _root_cause(err: BaseException) -> str:
    """Walk ActivityError/ChildWorkflowError -> ApplicationError to the real message."""
    cause: BaseException | None = err
    while cause is not None and isinstance(cause, (ActivityError, ChildWorkflowError)):
        cause = cause.cause
    if isinstance(cause, ApplicationError):
        return f"{cause.type}: {cause.message}" if cause.type else cause.message
    return str(cause or err)
