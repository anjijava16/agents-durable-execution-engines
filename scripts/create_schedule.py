"""Create (or update) the hourly inventory replenishment Schedule.

    python scripts/create_schedule.py            # every hour
    python scripts/create_schedule.py --every 2m # handy for a demo
    temporal schedule list                       # see it
    temporal schedule trigger --schedule-id inventory-replenishment   # run now
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import timedelta

from temporalio.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleAlreadyRunningError,
    ScheduleIntervalSpec,
    ScheduleOverlapPolicy,
    SchedulePolicy,
    ScheduleSpec,
    ScheduleUpdate,
)

from shopsphere.config import get_settings
from shopsphere.models import ReplenishmentInput
from shopsphere.workflows.replenishment_workflow import InventoryReplenishmentWorkflow

SCHEDULE_ID = "inventory-replenishment"


def parse_every(text: str) -> timedelta:
    unit = text[-1]
    n = int(text[:-1])
    return {"s": timedelta(seconds=n), "m": timedelta(minutes=n), "h": timedelta(hours=n)}[unit]


async def main(every: str) -> None:
    s = get_settings()
    client = await Client.connect(s.temporal_address, namespace=s.temporal_namespace)
    schedule = Schedule(
        action=ScheduleActionStartWorkflow(
            InventoryReplenishmentWorkflow.run,
            ReplenishmentInput(reorder_quantity=50),
            id="replenishment",  # Temporal appends the scheduled time to keep ids unique
            task_queue=s.orders_task_queue,
        ),
        spec=ScheduleSpec(intervals=[ScheduleIntervalSpec(every=parse_every(every))]),
        # If a run is still going when the next one is due, skip the new one.
        policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
    )
    try:
        await client.create_schedule(SCHEDULE_ID, schedule)
        print(f"Created schedule {SCHEDULE_ID} (every {every})")
    except ScheduleAlreadyRunningError:
        handle = client.get_schedule_handle(SCHEDULE_ID)
        await handle.update(lambda _: ScheduleUpdate(schedule=schedule))
        print(f"Updated schedule {SCHEDULE_ID} (every {every})")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--every", default="1h")
    asyncio.run(main(p.parse_args().every))
