# 13 — Testing

Temporal code is unusually testable, because the hard parts — retries,
crashes, waiting — are things the test environment can simulate on demand.
This project's suite runs 18 end-to-end tests against a real Temporal server,
plus 2 activity unit tests, in under a minute.

## The three test environments

| Environment | What it is | Good for |
|---|---|---|
| `WorkflowEnvironment.start_local()` | starts the real dev server (the `temporal` CLI binary) on a random port | integration tests with real behaviour — search attributes, visibility, real timers. **This project uses it.** |
| `WorkflowEnvironment.start_time_skipping()` | a lightweight test server that fast-forwards time whenever all workflows are waiting on timers | testing "3 days later" logic in milliseconds |
| `ActivityEnvironment()` | runs a single activity function in-process with a fake `activity.info()`, heartbeat capture and cancellation | unit tests for activities |

Both server environments download a binary on first use. If your network
blocks that (corporate proxies often do), download the CLI from the GitHub
releases page and pass its path:

```python
WorkflowEnvironment.start_local(
    dev_server_existing_path=os.environ.get("TEMPORAL_CLI_PATH"),
    search_attributes=[ORDER_STATUS_KEY, CUSTOMER_ID_KEY],   # registered for you
)
```

## The fixtures

```python
# tests/conftest.py
@pytest.fixture(scope="session")
async def env():
    init_db(reset=True)
    environment = await WorkflowEnvironment.start_local(...)
    yield environment
    await environment.shutdown()

@pytest.fixture(scope="session")
async def client(env):
    async with orders_worker(env.client, settings), ai_worker(env.client, settings):
        yield env.client
```

One server and one pair of workers for the whole session, created with the
same factory functions production uses (`orders_worker`, `ai_worker`). The
tests are therefore testing the real wiring: plugins, task queues, registered
activities.

Timings are shortened through workflow *inputs*, not by patching code:

```python
OrderInput(..., review_timeout_seconds=30, review_request_delay_seconds=1, delivery_sla_seconds=30)
```

That's one more benefit of passing configuration in inputs (chapter 05).

## What's covered

```
tests/test_order_workflow.py
  test_happy_path_completes_and_awards_points
  test_flaky_gateway_is_retried_transparently        retries
  test_declined_card_runs_compensation               saga
  test_out_of_stock_fails_fast                       non-retryable errors
  test_fraud_reject_never_touches_money              LangGraph child workflow
  test_manual_review_approve_then_address_update     signals + update + validator
  test_review_deadline_auto_rejects                  wait_condition timeout
  test_customer_cancel_while_in_review               cancel signal
  test_visibility_search_by_status                   search attributes
  test_lost_parcel_refunds_but_keeps_stock_decremented   pivot point

tests/test_support_agent.py
  test_order_status_question                         Deep Agent + tool activity
  test_policy_question_goes_to_subagent              sub-agent via `task`
  test_refund_requires_supervisor_approval           interrupt_on + resume
  test_refund_rejected_by_supervisor                 reject decision
  test_final_sale_items_are_not_returnable           tool result drives answer

tests/test_durability.py
  test_worker_crash_mid_order_is_invisible_to_the_customer
  test_histories_replay_deterministically
  test_replenishment_is_idempotent_per_run

tests/test_activities.py                             no server needed
  test_label_resumes_from_last_heartbeat
  test_charge_and_refund_are_idempotent
```

## Testing a crash

The most convincing Temporal test you can write:

```python
async def test_worker_crash_mid_order_is_invisible_to_the_customer(client):
    queue = f"crash-{uuid.uuid4().hex[:6]}"
    settings = replace(S, orders_task_queue=queue)

    async with orders_worker(client, settings):              # worker #1
        handle = await client.start_workflow(OrderFulfillmentWorkflow.run, order, id=..., task_queue=queue)
        await wait_for_status(handle, OrderStatus.PENDING_REVIEW)
    # worker #1 is gone

    await handle.signal(OrderFulfillmentWorkflow.submit_review, ReviewDecision(True, "analyst-kim"))  # nobody running

    async with orders_worker(client, settings):              # worker #2, empty memory
        await deliver(client, order.order_id)
        view = await handle.result()

    assert view.status == OrderStatus.COMPLETED
    assert exactly_one_payment_row(order.order_id)
```

A dedicated task queue guarantees that only the short-lived workers can pick
the order up, so the second worker really has to replay from history.

## Testing replay

```python
async def test_histories_replay_deterministically(client):
    ... collect histories of completed workflows ...
    await Replayer(workflows=[OrderFulfillmentWorkflow, ShipmentWorkflow]).replay_workflow(h)
    await Replayer(workflows=[FraudAssessmentWorkflow, SupportAgentWorkflow], plugins=ai_plugins()).replay_workflow(h)
```

Note the `plugins=ai_plugins()` on the second replayer: AI workflows depend
on the plugins' sandbox configuration and data converter, so the replayer
needs them too.

Add saved production histories to this test (chapter 11) and it becomes your
deploy gate.

## Mocking activities

To test workflow logic without the real activity implementations, register
fakes with the *same activity name*:

```python
@activity.defn(name="charge_payment")
async def charge_payment_always_declines(req: ChargeRequest) -> PaymentReceipt:
    raise ApplicationError("declined", type="PaymentDeclined", non_retryable=True)

async with Worker(env.client, task_queue=tq, workflows=[OrderFulfillmentWorkflow],
                  activities=[charge_payment_always_declines, fake_reserve, fake_release, ...]):
    ...
```

The workflow calls activities by name, so it can't tell the difference.

## Mocking the LLM

Two options, both used in this project:

* a **rule-based fake chat model** (`RetailSupportFakeModel`) selected by the
  model name `fake:retail`. It inspects the conversation and emits real tool
  calls, so the actual agent loop runs. Best for end-to-end tests and demos.
* the plugin's **scripted helpers** in `temporalio.contrib.deepagents.testing`:
  `FakeModel([...responses...])`, `mock_model_provider([...])` and `MockTool`.
  Best for unit-testing one specific agent behaviour with exact replies.

For the LangGraph fraud graph, `llm_review` checks `is_fake(model_name)` and
uses a heuristic. In a bigger project you'd inject the model through the
graph's runtime context instead.

## Time skipping

For logic that waits days, use the time-skipping environment:

```python
async with await WorkflowEnvironment.start_time_skipping() as env:
    async with Worker(env.client, ...):
        handle = await env.client.start_workflow(..., OrderInput(review_request_delay_seconds=3 * 24 * 3600, ...))
        await deliver(...)
        view = await handle.result()   # returns almost instantly: the 3-day timer is skipped
```

Time skips automatically whenever the test is waiting on a workflow result
and every workflow is blocked on a timer. You can also skip manually with
`await env.sleep(timedelta(days=3))`. Activities still run in real time.

## Unit-testing activities

```python
from temporalio.testing import ActivityEnvironment

async def test_label_resumes_from_last_heartbeat():
    env = ActivityEnvironment()
    # Pretend attempt 1 got through two of the four steps before its worker died.
    env.info = dataclasses.replace(env.info, heartbeat_details=[2], attempt=2)
    beats = []
    env.on_heartbeat = lambda *details: beats.append(details[0])
    label = await env.run(create_shipping_label, shipment_input)
    assert beats == [3, 4]          # only the remaining steps ran

async def test_charge_and_refund_are_idempotent():
    first = await env.run(charge_payment, req)
    second = await env.run(charge_payment, req)     # a retry after a lost response
    assert first.payment_id == second.payment_id    # and exactly one payments row
```

## Known noise

* LangChain emits beta and pending-deprecation warnings from the Deep Agents
  plugin's serializer. Filtered in `pyproject.toml`.
* The experimental LangGraph plugin's interceptor runs cleanup code in a
  `finally` block that calls `workflow.info()`. When a worker shuts down at
  the end of the session, evicted workflow coroutines can be garbage-collected
  outside the workflow event loop, and that call raises inside the `finally`.
  Python reports it as an "unraisable exception". It's intermittent, happens
  only at teardown, and doesn't affect any result. The warning filter in
  `pyproject.toml` is scoped to exactly that message.
* The sandbox prints "Module X was imported after initial workflow load" for
  some lazy imports in LangChain. Harmless; filtered.

## A testing checklist for a new workflow

* [ ] happy path end to end
* [ ] each non-retryable failure → correct final status and compensations
* [ ] each signal/update in each relevant state, including validator rejections
* [ ] every timeout path (`wait_condition` timeouts, SLAs) with short inputs or time skipping
* [ ] worker crash at an interesting point
* [ ] replay of the histories the tests produced
* [ ] activity unit tests for idempotency (call twice, assert one effect)
