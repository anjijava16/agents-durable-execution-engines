"""Runtime settings.

Everything is read from environment variables so the same image can run as the
API, the orders worker or the AI worker. Workflows never read this module
directly: anything a workflow needs (model name, task queue names, timeouts)
is passed in through its input, because reading env vars inside workflow code
would make replay depend on the machine it runs on.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class Settings:
    temporal_address: str = field(default_factory=lambda: _env("TEMPORAL_ADDRESS", "localhost:7233"))
    temporal_namespace: str = field(default_factory=lambda: _env("TEMPORAL_NAMESPACE", "default"))

    # Two task queues = two independently scalable worker pools.
    orders_task_queue: str = field(default_factory=lambda: _env("ORDERS_TASK_QUEUE", "shopsphere-orders"))
    ai_task_queue: str = field(default_factory=lambda: _env("AI_TASK_QUEUE", "shopsphere-ai"))

    # SQLite file that plays the role of our "downstream systems"
    # (inventory service, payment gateway, carrier API, CRM ...).
    db_path: str = field(default_factory=lambda: _env("RETAIL_DB_PATH", "./data/shopsphere.db"))

    # "fake:retail" runs fully offline with a deterministic rule-based model.
    # Set to e.g. "openai:gpt-4.1-mini" or "anthropic:claude-sonnet-4-5" for a real LLM.
    llm_model: str = field(default_factory=lambda: _env("LLM_MODEL", "fake:retail"))

    # Chaos knob: probability that the payment gateway throws a transient 503.
    payment_failure_rate: float = field(
        default_factory=lambda: float(_env("CHAOS_PAYMENT_FAILURE_RATE", "0.0"))
    )

    # Demo-friendly timings (seconds). Production values would be hours/days.
    review_timeout_seconds: int = field(default_factory=lambda: int(_env("REVIEW_TIMEOUT_SECONDS", "300")))
    review_request_delay_seconds: int = field(
        default_factory=lambda: int(_env("REVIEW_REQUEST_DELAY_SECONDS", "20"))
    )
    delivery_sla_seconds: int = field(default_factory=lambda: int(_env("DELIVERY_SLA_SECONDS", "600")))

    # Set to "false" if your Temporal server does not have the custom
    # search attributes registered (see docs/04-running-it.md).
    use_search_attributes: bool = field(
        default_factory=lambda: _env("USE_SEARCH_ATTRIBUTES", "true").lower() == "true"
    )


def get_settings() -> Settings:
    return Settings()
