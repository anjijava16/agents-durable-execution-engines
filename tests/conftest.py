"""Test harness: a real Temporal dev server + both workers, in-process.

The SDK downloads the `temporal` CLI the first time. If your network blocks
that, download it yourself from https://github.com/temporalio/cli/releases
and point TEMPORAL_CLI_PATH at the binary.
"""

from __future__ import annotations

import os
import tempfile
import uuid
from collections.abc import AsyncIterator

import pytest

# Isolated DB per test session - must be set before shopsphere modules read settings.
os.environ.setdefault("RETAIL_DB_PATH", os.path.join(tempfile.mkdtemp(), "test.db"))
os.environ.setdefault("LLM_MODEL", "fake:retail")

from temporalio.client import Client  # noqa: E402
from temporalio.testing import WorkflowEnvironment  # noqa: E402

from shopsphere.config import get_settings  # noqa: E402
from shopsphere.models import Address, LineItem, OrderInput  # noqa: E402
from shopsphere.services.db import init_db  # noqa: E402
from shopsphere.worker import ai_worker, orders_worker  # noqa: E402
from shopsphere.workflows.order_workflow import CUSTOMER_ID_KEY, ORDER_STATUS_KEY  # noqa: E402


@pytest.fixture(scope="session")
async def env() -> AsyncIterator[WorkflowEnvironment]:
    init_db(reset=True)
    environment = await WorkflowEnvironment.start_local(
        dev_server_existing_path=os.environ.get("TEMPORAL_CLI_PATH"),
        search_attributes=[ORDER_STATUS_KEY, CUSTOMER_ID_KEY],
    )
    yield environment
    await environment.shutdown()


@pytest.fixture(scope="session")
async def client(env: WorkflowEnvironment) -> AsyncIterator[Client]:
    settings = get_settings()
    o_worker = orders_worker(env.client, settings)
    a_worker = ai_worker(env.client, settings)
    async with o_worker, a_worker:
        yield env.client


def make_order(
    customer_id: str = "CUST-ALICE",
    sku: str = "SKU-HEADPHONES",
    qty: int = 1,
    price: float = 199.99,
    token: str = "tok_visa",
    **overrides,
) -> OrderInput:
    settings = get_settings()
    order = OrderInput(
        order_id=f"ORD-{uuid.uuid4().hex[:8].upper()}",
        customer_id=customer_id,
        email=f"{customer_id.lower()}@example.com".replace("cust-", ""),
        items=[LineItem(sku, qty, price)],
        shipping_address=Address("Alice Rivera", "1 Market St", "Philadelphia", "PA", "19103"),
        payment_token=token,
        ai_model="fake:retail",
        ai_task_queue=settings.ai_task_queue,
        review_timeout_seconds=30,
        review_request_delay_seconds=1,
        delivery_sla_seconds=30,
    )
    for k, v in overrides.items():
        setattr(order, k, v)
    return order
