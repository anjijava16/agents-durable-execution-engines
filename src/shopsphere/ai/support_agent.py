"""The customer-support Deep Agent.

`build_support_agent()` is called *inside* SupportAgentWorkflow. With the
DeepAgentsPlugin on the worker:

* every LLM call becomes a `deepagents.invoke_model` activity,
* every tool below becomes an activity (we wrap them with `activity_as_tool`),
* the agent loop itself (planning, todo list, routing to sub-agents,
  human-in-the-loop interrupts) runs as deterministic workflow code.

So if the worker is killed halfway through a refund conversation, another
worker replays history, skips every LLM call and tool call that already
finished (their results are in history), and carries on from the exact
point it stopped.
"""

from __future__ import annotations

from datetime import timedelta

from langchain.agents.middleware import TodoListMiddleware
from langgraph.checkpoint.memory import InMemorySaver
from temporalio.common import RetryPolicy
from temporalio.contrib.deepagents import activity_as_tool, create_temporal_deep_agent

from shopsphere.activities.support_tools import (
    check_return_eligibility,
    create_return_label,
    get_order_details,
    issue_refund,
    lookup_return_policy,
)

SYSTEM_PROMPT = """You are the ShopSphere customer support agent.

You help customers track orders, start returns and understand the return policy.

How to work:
- For a return or refund, first write a short todo plan, then check eligibility,
  then create the return label, then issue the refund. Never skip the eligibility check.
- issue_refund moves real money. A supervisor must approve it; the system will pause
  for that automatically, you don't need to ask the customer.
- For general policy questions, delegate to the policy-expert sub-agent.
- Never invent order details. If a tool says an order wasn't found, say so.
- Be brief and friendly. Mention amounts and reference numbers explicitly.
"""

POLICY_EXPERT_PROMPT = """You answer questions about ShopSphere's return policy.
Use the lookup_return_policy tool and quote the relevant rule in plain words.
Answer in two sentences or fewer."""

_TOOL_TIMEOUT = timedelta(seconds=30)
_TOOL_RETRY = RetryPolicy(maximum_attempts=3)


def build_support_agent(model_name: str):
    order_tool = activity_as_tool(
        get_order_details, start_to_close_timeout=_TOOL_TIMEOUT, retry_policy=_TOOL_RETRY
    )
    eligibility_tool = activity_as_tool(
        check_return_eligibility, start_to_close_timeout=_TOOL_TIMEOUT, retry_policy=_TOOL_RETRY
    )
    label_tool = activity_as_tool(
        create_return_label, start_to_close_timeout=_TOOL_TIMEOUT, retry_policy=_TOOL_RETRY
    )
    refund_tool = activity_as_tool(
        issue_refund, start_to_close_timeout=_TOOL_TIMEOUT, retry_policy=_TOOL_RETRY
    )
    policy_tool = activity_as_tool(
        lookup_return_policy, start_to_close_timeout=_TOOL_TIMEOUT, retry_policy=_TOOL_RETRY
    )

    return create_temporal_deep_agent(
        model=model_name,
        system_prompt=SYSTEM_PROMPT,
        tools=[order_tool, eligibility_tool, label_tool, refund_tool],
        subagents=[
            {
                "name": "policy-expert",
                "description": "Answers questions about the return and refund policy.",
                "system_prompt": POLICY_EXPERT_PROMPT,
                "tools": [policy_tool],
            }
        ],
        # Planning: gives the agent a `write_todos` tool and keeps the plan in
        # agent state. It is pure state, so it runs inside the workflow - no
        # activity, no history event, replayed for free. (Recent deepagents
        # releases ship filesystem + `task` tools by default; planning is opt-in.)
        middleware=[TodoListMiddleware()],
        # The money-moving tool pauses the graph for a human decision.
        interrupt_on={"issue_refund": True},
        # Required for interrupts. Temporal provides the durability, so the
        # in-memory saver is enough: it is rebuilt by replay after a crash.
        checkpointer=InMemorySaver(),
        activity_options={
            "start_to_close_timeout": timedelta(minutes=2),
            "retry_policy": RetryPolicy(
                initial_interval=timedelta(seconds=2),
                backoff_coefficient=2.0,
                maximum_attempts=5,
            ),
        },
    )
