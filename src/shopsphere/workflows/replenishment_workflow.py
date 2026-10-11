"""Hourly stock replenishment, triggered by a Temporal Schedule (scripts/create_schedule.py)."""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from shopsphere.activities.inventory import create_purchase_orders, get_low_stock_skus
    from shopsphere.models import PurchaseOrderRequest, ReplenishmentInput, ReplenishmentResult


@workflow.defn
class InventoryReplenishmentWorkflow:
    @workflow.run
    async def run(self, inp: ReplenishmentInput) -> ReplenishmentResult:
        low = await workflow.execute_activity(
            get_low_stock_skus,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=5),
        )
        if not low:
            return ReplenishmentResult(0, [])
        created = await workflow.execute_activity(
            create_purchase_orders,
            # run_id is unique per scheduled run and stable across retries -> idempotent POs.
            PurchaseOrderRequest(workflow.info().run_id, low, inp.reorder_quantity),
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=5),
        )
        return ReplenishmentResult(created, [i.sku for i in low])
