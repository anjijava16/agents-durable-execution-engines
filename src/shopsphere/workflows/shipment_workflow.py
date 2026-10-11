"""Child workflow: one shipment, from label purchase to doorstep.

Patterns on display:
* heartbeating activity (create_shipping_label)
* signals from the outside world (carrier webhooks -> carrier_event)
* waiting on a condition *with* a timeout (delivery SLA)
* child -> parent signalling through an external workflow handle
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from shopsphere.activities.notifications import send_notification
    from shopsphere.activities.shipping import create_shipping_label
    from shopsphere.models import CarrierEvent, Notification, ShipmentInput, ShipmentResult


@workflow.defn
class ShipmentWorkflow:
    def __init__(self) -> None:
        self._inbox: list[CarrierEvent] = []
        self._events: list[CarrierEvent] = []
        self._delivered_at: str | None = None

    @workflow.run
    async def run(self, inp: ShipmentInput) -> ShipmentResult:
        label = await workflow.execute_activity(
            create_shipping_label,
            inp,
            start_to_close_timeout=timedelta(minutes=2),
            heartbeat_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(maximum_attempts=10),
        )
        await self._report(
            CarrierEvent(
                order_id=inp.order_id,
                status="LABEL_CREATED",
                tracking_number=label.tracking_number,
                carrier=label.carrier,
                occurred_at=workflow.now().isoformat(),
            )
        )

        sla = timedelta(seconds=inp.delivery_sla_seconds)
        delay_notified = False
        while self._delivered_at is None:
            try:
                # Wakes up on every new carrier event, or when the SLA expires.
                await workflow.wait_condition(lambda: len(self._inbox) > 0, timeout=sla)
            except TimeoutError:
                if not delay_notified:
                    delay_notified = True
                    await workflow.execute_activity(
                        send_notification,
                        Notification(inp.order_id, inp.email, "shipment_delayed", {"tracking": label.tracking_number}),
                        start_to_close_timeout=timedelta(seconds=30),
                    )
                    continue  # give the carrier one more SLA window
                raise ApplicationError(
                    f"No delivery scan for {inp.order_id} after two SLA windows",
                    type="ShipmentLost",
                    non_retryable=True,
                )

            while self._inbox:
                event = self._inbox.pop(0)
                event.tracking_number = event.tracking_number or label.tracking_number
                event.carrier = event.carrier or label.carrier
                self._events.append(event)
                await self._report(event)
                if event.status == "DELIVERED":
                    self._delivered_at = event.occurred_at or workflow.now().isoformat()

        return ShipmentResult(
            tracking_number=label.tracking_number,
            carrier=label.carrier,
            delivered_at=self._delivered_at,
            events=self._events,
        )

    async def _report(self, event: CarrierEvent) -> None:
        parent = workflow.info().parent
        if parent is None:  # started standalone (e.g. in a test)
            return
        handle = workflow.get_external_workflow_handle(parent.workflow_id, run_id=parent.run_id)
        await handle.signal("shipment_progress", event)

    @workflow.signal
    def carrier_event(self, event: CarrierEvent) -> None:
        # Signal handlers should be quick: just record and let run() react.
        self._inbox.append(event)

    @workflow.query
    def events(self) -> list[CarrierEvent]:
        return self._events
