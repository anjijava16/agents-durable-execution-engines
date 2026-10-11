# 04 — Running it

## What you need

* Python 3.11 or newer (the LangGraph and Deep Agents plugins rely on
  asyncio context propagation that older versions lack)
* the Temporal CLI, which includes a single-binary dev server with the Web UI
* optionally Docker, if you prefer `docker compose up`
* optionally an OpenAI or Anthropic API key. Everything works offline without one.

## Install the Temporal CLI

```bash
# macOS
brew install temporal

# Linux / Windows: download from the GitHub releases page
# https://github.com/temporalio/cli/releases  -> temporal_cli_<version>_<os>_<arch>.tar.gz
tar xzf temporal_cli_*.tar.gz && sudo mv temporal /usr/local/bin/

temporal --version
```

## Option A: four terminals (best for learning)

```bash
unzip shopsphere-temporal.zip && cd shopsphere-temporal
python -m venv .venv && source .venv/bin/activate
make install            # pip install -e ".[dev,openai,anthropic]" and copies .env.example -> .env
```

Then:

```bash
# terminal 1 - Temporal dev server + UI on http://localhost:8233
make server

# terminal 2 - orders worker
make worker-orders

# terminal 3 - AI worker (fraud graph + support agent)
make worker-ai

# terminal 4 - API on http://localhost:8000/docs
make api
```

and in a fifth:

```bash
make demo
```

The demo plays every scenario: happy path with an idempotent retried POST,
flaky payment gateway, declined card with compensation, fraud rejection, human
review with an address change, a customer cancel, and a support conversation
that ends in a supervisor-approved refund.

Running each worker in its own terminal is deliberate. Try these while the
demo runs:

* `Ctrl-C` the orders worker in the middle of the demo, wait ten seconds,
  start it again. Watch the demo carry on.
* Stop the AI worker, place an order with `curl`, and look at it in the UI: it
  sits in `FRAUD_CHECK` with a pending child workflow. Start the worker and it
  continues.

## Option B: Docker Compose

```bash
cp .env.example .env
docker compose up --build
python scripts/demo.py     # from your host, needs: pip install httpx
```

Compose runs the dev server with both search attributes registered, two
replicas of the orders worker, one AI worker and the API. Kill a worker with
`docker compose kill worker-orders` and see that nothing is lost.

## Option C: just the tests

```bash
make test
```

The tests start their own Temporal dev server (the SDK downloads the CLI the
first time; set `TEMPORAL_CLI_PATH=/path/to/temporal` if your network blocks
that) and run 18 end-to-end scenarios against it (plus 2 activity unit tests), including a worker crash
mid-order and a replay check of every history they produce.

## Using a real LLM

```bash
pip install -e ".[openai]"          # or [anthropic]
export LLM_MODEL=openai:gpt-4.1-mini
export OPENAI_API_KEY=sk-...
make worker-ai                      # restart the AI worker (and the API, which passes the model name in)
```

Any model string LangChain's `init_chat_model` understands works. Only the AI
worker needs the key. The model *name* is passed to workflows in their input;
the *key* never leaves the worker's environment and never appears in history.

## Talking to the API by hand

```bash
# place an order
curl -s localhost:8000/orders -H 'content-type: application/json' \
  -H 'Idempotency-Key: my-first-order' -d '{
    "customer_id": "CUST-BOB", "email": "bob@example.com",
    "items": [{"sku": "SKU-LAPTOP", "quantity": 1}],
    "shipping_address": {"name":"Bob","line1":"1 Main St","city":"Austin","state":"TX","postal_code":"78701"},
    "payment_token": "tok_visa"}'

# it's a new customer buying a laptop -> PENDING_REVIEW
curl -s localhost:8000/orders/ORD-MY-FIRST-ORD | jq .status

# approve it as an analyst
curl -s localhost:8000/orders/ORD-MY-FIRST-ORD/review -H 'content-type: application/json' \
  -d '{"approved": true, "reviewer": "me"}'

# play the carrier
for s in IN_TRANSIT OUT_FOR_DELIVERY DELIVERED; do
  curl -s localhost:8000/carrier/webhook -H 'content-type: application/json' \
    -d "{\"order_id\":\"ORD-MY-FIRST-ORD\",\"status\":\"$s\"}"; done
```

Seeded test data:

| Customer | Profile | What happens |
|---|---|---|
| `CUST-ALICE` | 900-day-old account, 42 orders, no chargebacks | Approved instantly |
| `CUST-BOB` | 1-day-old account | Small orders pass; orders over $1,000 go to human review |
| `CUST-MALLORY` | 2 prior chargebacks | Rejected by the fraud graph |

| Payment token | Behaviour |
|---|---|
| `tok_visa` | Succeeds |
| `tok_flaky` | Fails with a 503 twice, succeeds on attempt 3 |
| `tok_decline` | Declined; non-retryable; triggers compensation |

| SKU | Notes |
|---|---|
| `SKU-TV` | Only 3 in stock — order 5 to see `OutOfStock` |
| `SKU-SOCKS` | Final sale (not returnable); below its reorder point, so replenishment picks it up |

## A tour of the Web UI (http://localhost:8233)

Open the UI and click any `order-ORD-...` workflow.

* **Summary** — status, start/close time, task queue, input and result.
  Click the input to see exactly what the API sent.
* **Event History** — the timeline view shows activities as bars; the
  compact/JSON views show every event. Click `ACTIVITY_TASK_SCHEDULED` for
  `charge_payment` on a `tok_flaky` order: the started event shows
  `attempt: 3`, and the "last failure" shows the 503 from attempt 2.
* **Relationships** — parent/child links. An order has two children: the
  fraud workflow (on the AI queue) and the shipment workflow.
* **Queries** — the UI can run `get_status` for you and show the live view.
* **Pending activities** — while something is retrying, this shows the
  attempt count, the last error, and when the next attempt is due. This panel
  is the first place to look when a workflow seems stuck.
* **Workers** — which worker identities are polling the task queue right now.

In the workflow list, try the query box:

```
WorkflowType = 'OrderFulfillmentWorkflow' AND OrderStatus = 'PENDING_REVIEW'
CustomerId = 'CUST-BOB'
ExecutionStatus = 'Running' AND StartTime > '2026-10-01T00:00:00Z'
```

## The CLI you'll actually use

```bash
# list and inspect
temporal workflow list --query "OrderStatus='PENDING_REVIEW'"
temporal workflow describe -w order-ORD-XXXX
temporal workflow show -w order-ORD-XXXX            # event history
temporal workflow show -w order-ORD-XXXX -o json > history.json   # for replay tests

# talk to a running workflow
temporal workflow query  -w order-ORD-XXXX --type get_status
temporal workflow signal -w order-ORD-XXXX --name cancel --input '{"reason":"cli test"}'
temporal workflow update execute -w order-ORD-XXXX --name update_shipping_address \
  --input '{"name":"A","line1":"2 St","city":"Boston","state":"MA","postal_code":"02108"}'

# start one directly, bypassing the API
temporal workflow start --task-queue shopsphere-orders --type InventoryReplenishmentWorkflow \
  -w replenish-manual --input '{"reorder_quantity": 25}'

# stop things
temporal workflow cancel    -w order-ORD-XXXX   # graceful: your code's CancelledError handler runs
temporal workflow terminate -w order-ORD-XXXX   # hard stop: no code runs, no compensation

# rewind time (see chapter 11)
temporal workflow reset -w order-ORD-XXXX --type LastWorkflowTask

# schedules
python scripts/create_schedule.py --every 2m
temporal schedule list
temporal schedule trigger --schedule-id inventory-replenishment
temporal schedule toggle  --schedule-id inventory-replenishment --pause --reason "inventory audit"

# task queue health
temporal task-queue describe --task-queue shopsphere-ai
```

Cancel versus terminate is worth remembering. **Cancel** is a request: it
raises `asyncio.CancelledError` inside the workflow at its current `await`, and
the order workflow catches it, runs compensations and records `CANCELLED`.
**Terminate** kills the execution on the server without running any of your
code. Use terminate only when the code itself is broken beyond repair.

## Project layout

```
shopsphere-temporal/
├── src/shopsphere/
│   ├── config.py                 env-driven settings (never read inside workflows)
│   ├── models.py                 dataclass payloads for every workflow/activity
│   ├── worker.py                 orders + AI worker pools, plugins wired here
│   ├── api/                      FastAPI: thin Temporal client
│   ├── workflows/
│   │   ├── order_workflow.py     the saga — start here
│   │   ├── shipment_workflow.py  child workflow, signals, SLA timers
│   │   ├── fraud_workflow.py     runs the LangGraph graph durably
│   │   ├── support_workflow.py   long-lived Deep Agent session, updates, HITL
│   │   └── replenishment_workflow.py   scheduled job
│   ├── activities/               every side effect, all idempotent
│   ├── ai/
│   │   ├── fraud_graph.py        LangGraph StateGraph with execute_in per node
│   │   ├── support_agent.py      Deep Agent: tools-as-activities, sub-agent, interrupt_on
│   │   └── llm.py                model provider + offline fake model
│   └── services/db.py            SQLite stand-in for downstream systems
├── tests/                        17 end-to-end tests on a real Temporal server
├── scripts/                      demo.py, create_schedule.py
├── docs/                         you are here
├── docker-compose.yml, Dockerfile, Makefile, .env.example
```
