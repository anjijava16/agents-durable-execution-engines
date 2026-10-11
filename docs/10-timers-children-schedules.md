# 10 — Timers, child workflows, continue-as-new, schedules

These four features are what let a workflow live for days or years, and stay
organised while it does.

## Durable timers

```python
await workflow.sleep(timedelta(days=3))
```

That line produces a `StartTimer` command. The server stores the timer in the
shard's timer queue and, when it fires, appends `TimerFired` and schedules a
workflow task. While it's waiting:

* no worker holds any resources for it (once evicted from cache)
* deploys, restarts and scaling events don't matter
* a million sleeping orders cost a million small rows, nothing more

`asyncio.sleep` inside a workflow maps to the same timer, so either works.

### Timeouts on waits

The most common timer isn't a sleep, it's a deadline:

```python
try:
    await workflow.wait_condition(lambda: self._review is not None, timeout=timedelta(hours=24))
except TimeoutError:
    raise OrderRejected("No analyst decision before the review deadline")
```

Under the hood that's a timer racing the condition. If the condition wins,
the timer is cancelled (`TIMER_CANCELED` in history). You saw this in the
operator-cancelled order and in the support session, which restarts its idle
timer every time a message arrives:

```
   9  TIMER_STARTED      timer 1800s     <- idle timeout armed
  ...
  51  TIMER_CANCELED                     <- activity in the session
  52  TIMER_STARTED      timer 1800s     <- re-armed
```

### Racing a timer against an activity

```python
charge = workflow.start_activity(charge_payment, ..., start_to_close_timeout=timedelta(seconds=30))
nudge = asyncio.ensure_future(workflow.sleep(timedelta(seconds=10)))
done, _ = await workflow.wait([charge, nudge], return_when=asyncio.FIRST_COMPLETED)
if nudge in done:
    await self._notify("payment_taking_longer", {})
receipt = await charge
```

Deterministic, because which one "wins" is decided by the order of events in
history. Use `workflow.wait` (the SDK's deterministic version of
`asyncio.wait`) for this.

## Child workflows

The order workflow starts two children:

| Child | Why a child and not an activity |
|---|---|
| `FraudAssessmentWorkflow` on `shopsphere-ai` | It's itself a multi-step graph with its own retries per node; it runs on a different worker fleet; it has its own history you can inspect in isolation |
| `ShipmentWorkflow` on the same queue | It lives for days, receives its own signals (carrier webhooks address `shipment-{order_id}` directly), and has its own SLA logic |

Reasons to reach for a child workflow:

* **Separation of concerns and ownership.** Another team can own the child
  and deploy it independently, as long as the input/output contract holds.
* **Its own history.** Each child gets a fresh event history and its own
  limits. A parent fanning out 1,000 shipments would otherwise put all their
  events in one history.
* **Its own identity.** You can signal, query, cancel or reset the child by
  ID without touching the parent.
* **Different task queue.** Route to specialised workers.

Reasons not to: a child costs more events than an activity, and for "call one
API" it's overkill. Start with activities; promote to a child when one of the
reasons above appears.

### Parent close policy

What happens to a running child when the parent closes?

| Policy | Effect | Used here |
|---|---|---|
| `TERMINATE` (default) | child is terminated | fraud child — no point checking fraud for a dead order |
| `REQUEST_CANCEL` | child gets a cancellation request and can clean up | good for children that hold resources |
| `ABANDON` | child keeps running on its own | shipment child — the parcel exists regardless |

### Fan-out

```python
handles = [
    await workflow.start_child_workflow(ShipmentWorkflow.run, part, id=f"shipment-{order_id}-{i}")
    for i, part in enumerate(split_order(order))
]
results = await asyncio.gather(*handles)
```

For very large fan-outs (thousands), batch children into groups or use a
tree of children so no single history grows too big.

## Continue-as-new

Histories have limits: by default Temporal warns at about 10,000 events and
terminates a workflow whose history exceeds 51,200 events or 50 MB. A support
session that stays open for weeks, a subscription workflow that runs for
years, a loop polling something every hour — all of these will hit the limit
eventually.

**Continue-as-new** ends the current run and atomically starts a new run of
the same workflow ID with fresh input and an empty history. The workflow ID
doesn't change, so signals and queries addressed to it keep working.

```python
@workflow.run
async def run(self, inp: SupportSessionInput) -> SupportView:
    while not self._closed:
        ...
        if workflow.info().is_continue_as_new_suggested() and self._pending is None and not self._lock.locked():
            await workflow.wait_condition(workflow.all_handlers_finished)
            workflow.continue_as_new(
                SupportSessionInput(
                    session_id=inp.session_id,
                    customer_id=inp.customer_id,
                    ai_model=inp.ai_model,
                    idle_timeout_seconds=inp.idle_timeout_seconds,
                    history=self._history,      # carry the conversation forward
                    turn=self._turn,
                )
            )
```

Points to get right:

* **Carry the state you need in the input.** Here: the transcript and the
  turn counter. Everything else is rebuilt.
* **Use `is_continue_as_new_suggested()`.** The server sets it when history is
  getting long *or* large, so you don't need to count events yourself.
* **Don't continue-as-new in the middle of something.** The session waits
  until no approval is pending and no handler is running. Otherwise an
  in-flight update would be lost.
* **Signals can arrive during the switch.** Temporal moves unhandled signals
  to the new run, but it's another reason to drain handlers first.
* **Pick your carry-over format carefully.** The new run's input is a
  payload, so it's subject to payload size limits. A transcript of 10,000
  messages would itself be too big; real systems summarise or store the
  transcript externally and pass a reference.

## Schedules

Temporal Schedules replace cron. `scripts/create_schedule.py`:

```python
schedule = Schedule(
    action=ScheduleActionStartWorkflow(
        InventoryReplenishmentWorkflow.run,
        ReplenishmentInput(reorder_quantity=50),
        id="replenishment",
        task_queue=s.orders_task_queue,
    ),
    spec=ScheduleSpec(intervals=[ScheduleIntervalSpec(every=timedelta(hours=1))]),
    policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
)
await client.create_schedule("inventory-replenishment", schedule)
```

What you get over a cron job:

* **Overlap policies** — `SKIP`, `BUFFER_ONE`, `BUFFER_ALL`, `CANCEL_OTHER`,
  `TERMINATE_OTHER`, `ALLOW_ALL`. Decide explicitly what happens when the
  previous run is still going.
* **Catch-up** — if the cluster was down, missed runs within a catch-up window
  are run when it comes back.
* **Pause/unpause with a note** — "paused during inventory audit, ask Dana".
* **Backfill** — run the schedule for a past time range ("re-run last week's
  nightly jobs").
* **Every run is a real workflow** — retries, history, visibility and the UI
  all apply. Each run gets a workflow ID made of the schedule's ID plus the
  scheduled time, so runs are naturally deduplicated.
* **Calendar specs** — `ScheduleCalendarSpec(hour=[2], day_of_week=[...])`
  and time zones, besides simple intervals and cron strings.

The replenishment workflow is idempotent per run: purchase order IDs are
derived from `workflow.info().run_id` and the SKU, so a retried activity can't
create duplicate POs.

There's also an older `cron_schedule=` option on `start_workflow`. Prefer
Schedules for anything new.

## Timers vs schedules vs a loop

| Need | Use |
|---|---|
| "Do X three days after Y, for this order" | `workflow.sleep` inside the order workflow |
| "Every hour, do X" for the whole system | a Schedule |
| "Every hour, check on *this* subscription, forever" | a loop with `workflow.sleep` in an entity workflow, with continue-as-new |
| "At 9am in the customer's time zone" | compute the delta from `workflow.now()` and sleep, or a per-entity schedule |
