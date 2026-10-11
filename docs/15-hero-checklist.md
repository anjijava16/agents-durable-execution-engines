# 15 — Hero checklist

You've read the theory and run the code. This chapter is for making it stick:
exercises against this codebase, the mistakes everyone makes once, interview
questions with answers, and a one-page cheat sheet.

## Exercises

Each one changes the project. Write a test for each before you call it done.

### Level 1 — get comfortable

1. **Add a gift-wrap step.** If `OrderInput.gift_wrap` is true, call a new
   `wrap_gift` activity after payment. Make the new field default to `False`
   so old histories still deserialize. Does this need `workflow.patched()`?
   (Yes — think about orders that are mid-flight when you deploy.)
2. **Add a query** `get_timeline_since(at: str)` that returns only timeline
   events after a timestamp.
3. **Make `tok_slow` a token** whose charge sleeps 40 s. Watch it time out
   against the 30 s `start_to_close_timeout` and retry. Then make it heartbeat
   and add a `heartbeat_timeout`.
4. **Use the CLI** to find every order for `CUST-BOB` that is currently
   `PENDING_REVIEW`, approve one with `temporal workflow signal`, and inspect
   its history.

### Level 2 — patterns

5. **Cancel after payment.** Allow `cancel` until the shipment starts. Add a
   test that cancels while the order is `PAID` and asserts that both the
   refund and the stock release happened, in that order.
6. **Partial shipments.** Split an order with several SKUs into one
   `ShipmentWorkflow` child per warehouse, run them in parallel with
   `asyncio.gather`, and complete the order when all are delivered.
7. **Payment taking long.** Race the charge activity against a 10-second
   timer with `workflow.wait(..., return_when=FIRST_COMPLETED)`; if the timer
   wins, send a "still processing" email, then keep waiting for the charge.
8. **Shopping cart entity.** Write a `CartWorkflow` (one per customer) with
   `add_item` / `remove_item` updates, a `get_cart` query, an abandoned-cart
   email after 24 h of inactivity, and a `checkout` update that starts an
   `OrderFulfillmentWorkflow` and returns its order ID. Use
   update-with-start from the API.

### Level 3 — production thinking

9. **Encrypt payloads** with a codec (AES-GCM with a key from env), run the
   whole demo with it, and confirm the UI shows encrypted payloads.
10. **Replay gate.** Export five real histories with
    `temporal workflow show -o json`, commit them, write a parametrised replay
    test, then make an incompatible change to the order workflow and watch
    the test catch it. Fix it with `patched()`.
11. **Worker Versioning.** Run two builds of the orders worker with
    `WorkerDeploymentConfig` and `PINNED`; start orders on v1, promote v2,
    and confirm in-flight orders finish on v1 while new ones start on v2.
12. **Real LLM.** Switch `LLM_MODEL` to a real model. Write three tricky
    support conversations (wrong order number, two orders in one message,
    a customer trying to talk the agent into skipping approval) and see how
    the guardrails hold up. Where do prompt instructions fail and code
    guardrails save you?
13. **Agent budget.** Count model calls per support turn in workflow state
    and stop with an escalation message after 8.

## Mistakes everyone makes once

| Mistake | Symptom | Fix |
|---|---|---|
| `datetime.now()` / `random` / `uuid4()` in workflow code | `RestrictedWorkflowAccessError`, or NDE on replay | `workflow.now()`, `workflow.random()`, `workflow.uuid4()` |
| Reading env or config inside a workflow | different behaviour on different workers | pass it in the input |
| Calling an API or DB from workflow code | sandbox error, or effects repeated on replay | move it to an activity |
| Sync LangGraph node marked `execute_in="workflow"` | `NotImplementedError` from `run_in_executor` | make it `async def` |
| Only `schedule_to_close_timeout` set | dead workers noticed an hour late | always set `start_to_close_timeout` |
| Retrying business errors | "card declined" retried 50 times | `ApplicationError(non_retryable=True)` / `non_retryable_error_types` |
| `uuid4()` as an idempotency key inside an activity | duplicates on retry | business ID or `workflow_id + activity_id` |
| LLM client retries left on | 30 calls for one logical attempt | `max_retries=0`; let Temporal retry |
| Reordering activities in a deployed workflow | NDE, workflows stuck on "Workflow Task Failed" | `workflow.patched()` or Worker Versioning |
| Returning big documents from activities | payload size errors, slow histories | return IDs; store blobs elsewhere |
| Unbounded entity workflow | history limit termination after weeks | continue-as-new |
| Expecting a query to work with no workers | query timeout during an outage | read model or search attributes |
| Using signals when the caller needs the outcome | API returns 202 and the client polls forever | an update with a validator |
| `str`-mixin Enum fields in payloads | value comes back as a list of characters | keep enums internal; put `.value` on the wire |
| Workflow returns while an update handler is mid-flight | caller gets an error, SDK warns | `wait_condition(all_handlers_finished)` |
| Terminating instead of cancelling | no compensation ran | `cancel` and handle `CancelledError` |
| Business failure raised as workflow failure | alerts on every declined card | complete normally with a FAILED status |

## Interview questions (with short answers)

**What problem does Temporal solve?**
Reliable execution of long-running, multi-step processes across unreliable
systems. You write the process as ordinary code; Temporal persists every step
so the code survives crashes and resumes exactly where it was.

**How does a workflow survive a worker crash?**
Its event history is on the server. Another worker replays the code against
that history: completed activities return their recorded results instead of
running, timers and signals are re-delivered in order, and the code ends up in
the same state, then continues live.

**Why must workflow code be deterministic?**
Replay re-runs the code and matches its commands against recorded events. If
the code makes different decisions given the same history, the recorded past
and the code disagree and the SDK can't continue (non-determinism error).

**Are activities exactly-once?**
No. At-least-once. A successful result is recorded once, but an attempt can
run and have side effects without its success being recorded (e.g. worker
crash). Make activities idempotent with deterministic keys.

**Signal vs query vs update?**
Signal: async message, recorded, no result. Query: read-only, not recorded,
returns data. Update: recorded, can be validated and rejected before
recording, returns a result to the caller.

**What is continue-as-new and when do you use it?**
Atomically complete the current run and start a new run of the same workflow
ID with new input and empty history. Use it for long-lived or looping
workflows to stay under history limits.

**How do you change workflow code safely?**
Patching (`workflow.patched` → `deprecate_patch` → remove), Worker Versioning
(pin executions to the build they started on), or new workflow
types/queues. Verify with replay tests in CI.

**What's the difference between cancel and terminate?**
Cancel delivers a cancellation into the workflow, which can clean up
(compensate) and then end. Terminate stops it on the server without running
any more code.

**How would you implement a saga?**
Record a compensation for each step that changes external state (register
before steps with unknown outcomes), and on failure run them in reverse with
retries that never give up. Identify the pivot step after which rollback is
no longer possible.

**How do timeouts differ?**
Start-to-close: one attempt. Schedule-to-close: all attempts. Schedule-to-
start: time in queue. Heartbeat: max time between heartbeats. Always set
start-to-close (or schedule-to-close).

**Why would you put an LLM agent on Temporal?**
Each model call and tool call becomes a durable, retried step whose result is
recorded, so crashes don't cost tokens or duplicate side effects, human
approvals can pause the agent for days, and the agent's history is a full
audit log.

**What runs where in the Temporal LangGraph plugin?**
Nodes marked `execute_in="activity"` run as activities (I/O, LLM calls);
nodes marked `"workflow"` and routing functions run inside the workflow and
must be deterministic and async.

**How does the Temporal server scale?**
History is sharded by workflow ID across history hosts (shard count fixed at
cluster creation); matching partitions task queues; frontends are stateless;
the database is the ultimate limit. Workers scale independently, by adding
processes.

**What happens if no worker is running?**
Nothing breaks. Starts, signals and timers are recorded on the server; tasks
wait in task queues; queries fail. When workers return, work resumes.

## Cheat sheet

```python
# ---------- workflow ----------
@workflow.defn
class MyWorkflow:
    @workflow.init
    def __init__(self, inp: In) -> None: ...            # runs before any handler

    @workflow.run
    async def run(self, inp: In) -> Out:
        r = await workflow.execute_activity(act, arg,
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(maximum_attempts=5, non_retryable_error_types=["Bad"]))
        c = await workflow.execute_child_workflow(Child.run, arg, id="child-1", task_queue="q")
        await workflow.sleep(timedelta(days=1))
        await workflow.wait_condition(lambda: self.done, timeout=timedelta(hours=1))
        if workflow.patched("my-change"): ...
        if workflow.info().is_continue_as_new_suggested():
            workflow.continue_as_new(next_input)
        await workflow.wait_condition(workflow.all_handlers_finished)
        return Out(...)

    @workflow.signal
    def poke(self, x: X) -> None: ...
    @workflow.query
    def state(self) -> S: ...
    @workflow.update
    async def change(self, x: X) -> S: ...
    @change.validator
    def _check(self, x: X) -> None: ...                  # raise to reject

# deterministic helpers: workflow.now(), workflow.random(), workflow.uuid4(), workflow.logger

# ---------- activity ----------
@activity.defn
async def act(arg: In) -> Out:
    info = activity.info()                               # attempt, workflow_id, activity_id, heartbeat_details
    activity.heartbeat(progress)
    raise ApplicationError("msg", type="Bad", non_retryable=True)

# ---------- worker ----------
await Worker(client, task_queue="q", workflows=[MyWorkflow], activities=[act],
             max_concurrent_activities=50, plugins=[...]).run()

# ---------- client ----------
h = await client.start_workflow(MyWorkflow.run, inp, id="biz-key", task_queue="q",
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING)
await h.signal(MyWorkflow.poke, x)
s = await h.query(MyWorkflow.state)
s = await h.execute_update(MyWorkflow.change, x)
r = await h.result()
async for wf in client.list_workflows("WorkflowType='MyWorkflow' AND ExecutionStatus='Running'"): ...
```

```bash
temporal server start-dev --search-attribute OrderStatus=Keyword
temporal workflow list|describe|show|query|signal|cancel|terminate|reset
temporal workflow update execute -w ID --name NAME --input '{...}'
temporal schedule create|list|trigger|toggle|backfill
temporal task-queue describe --task-queue q
```

## Where to go from here

* The official docs at docs.temporal.io, especially the Python SDK developer
  guide and the "Workflow message passing" and "Versioning" pages.
* The `temporalio/samples-python` repository: dozens of small, focused
  examples.
* The plugin READMEs shipped inside the SDK:
  `temporalio/contrib/langgraph/README.md` and
  `temporalio/contrib/deepagents/README.md`.
* Temporal's free courses (Temporal 101/102 and the Python-specific ones).
* Nexus, which connects workflows across namespaces and teams with typed
  service contracts — the natural next step once several teams run Temporal.
