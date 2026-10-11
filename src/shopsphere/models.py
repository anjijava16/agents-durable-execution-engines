"""Workflow and activity payloads.

Rules of thumb that this file follows (and that the docs explain in depth):

* Every workflow/activity takes ONE dataclass argument. Adding a field with a
  default later is backwards compatible; adding a positional parameter is not.
* Only plain data crosses the wire: str, int, float, bool, lists, dicts and
  nested dataclasses. Enums stay inside the workflow; payloads carry their
  .value (see OrderView.status for why). No datetimes (we use ISO strings), no
  live objects, no secrets.
* Payloads are stored in workflow history forever (well, for the retention
  period), so keep them small. Pass IDs, not documents.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class OrderStatus(str, Enum):
    RECEIVED = "RECEIVED"
    FRAUD_CHECK = "FRAUD_CHECK"
    PENDING_REVIEW = "PENDING_REVIEW"
    INVENTORY_RESERVED = "INVENTORY_RESERVED"
    PAID = "PAID"
    FULFILLING = "FULFILLING"
    SHIPPED = "SHIPPED"
    OUT_FOR_DELIVERY = "OUT_FOR_DELIVERY"
    DELIVERED = "DELIVERED"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    FAILED = "FAILED"


TERMINAL_STATUSES = {
    OrderStatus.COMPLETED,
    OrderStatus.CANCELLED,
    OrderStatus.REJECTED,
    OrderStatus.FAILED,
}


# --------------------------------------------------------------------------
# Order domain
# --------------------------------------------------------------------------


@dataclass
class LineItem:
    sku: str
    quantity: int
    unit_price: float


@dataclass
class Address:
    name: str
    line1: str
    city: str
    state: str
    postal_code: str
    country: str = "US"


@dataclass
class OrderInput:
    order_id: str
    customer_id: str
    email: str
    items: list[LineItem]
    shipping_address: Address
    payment_token: str
    # Passed in (not read from env inside the workflow) to keep replay deterministic.
    ai_model: str = "fake:retail"
    ai_task_queue: str = "shopsphere-ai"
    review_timeout_seconds: int = 24 * 3600
    review_request_delay_seconds: int = 3 * 24 * 3600
    delivery_sla_seconds: int = 7 * 24 * 3600
    use_search_attributes: bool = True

    def total(self) -> float:
        return round(sum(i.quantity * i.unit_price for i in self.items), 2)


@dataclass
class TimelineEvent:
    at: str
    status: str
    detail: str


@dataclass
class FraudInput:
    order_id: str
    customer_id: str
    email: str
    order_total: float
    item_count: int
    shipping_state: str
    shipping_country: str
    ai_model: str = "fake:retail"


@dataclass
class FraudResult:
    decision: str  # "approve" | "review" | "reject"
    risk_score: float
    reasons: list[str] = field(default_factory=list)
    llm_used: bool = False


@dataclass
class ReviewDecision:
    approved: bool
    reviewer: str
    note: str = ""


@dataclass
class CancelRequest:
    reason: str
    requested_by: str = "customer"


@dataclass
class OrderView:
    """What the Query returns. Built from workflow state only - no I/O."""

    order_id: str
    customer_id: str
    # A plain str on the wire. Temporal's default converter turns a (str, Enum)
    # field back into a list of characters - keep enums internal.
    status: str
    total: float
    shipping_address: Address
    fraud: FraudResult | None = None
    tracking_number: str | None = None
    carrier: str | None = None
    cancel_reason: str | None = None
    failure_reason: str | None = None
    timeline: list[TimelineEvent] = field(default_factory=list)


# --------------------------------------------------------------------------
# Activity payloads
# --------------------------------------------------------------------------


@dataclass
class OrderSnapshot:
    order_id: str
    customer_id: str
    email: str
    status: str
    total: float
    items: list[LineItem]
    tracking_number: str | None = None
    delivered_at: str | None = None


@dataclass
class ReserveRequest:
    order_id: str
    items: list[LineItem]


@dataclass
class ChargeRequest:
    order_id: str
    customer_id: str
    amount: float
    payment_token: str


@dataclass
class PaymentReceipt:
    payment_id: str
    amount: float
    attempts: int = 1


@dataclass
class RefundRequest:
    order_id: str
    reason: str
    amount: float | None = None  # None = full refund


@dataclass
class RefundReceipt:
    refund_id: str
    order_id: str
    amount: float
    already_refunded: bool = False


@dataclass
class Notification:
    order_id: str
    email: str
    template: str
    data: dict[str, Any] = field(default_factory=dict)


@dataclass
class LoyaltyRequest:
    customer_id: str
    order_id: str
    amount: float


# --------------------------------------------------------------------------
# Shipping
# --------------------------------------------------------------------------


@dataclass
class ShipmentInput:
    order_id: str
    email: str
    address: Address
    items: list[LineItem]
    delivery_sla_seconds: int = 7 * 24 * 3600


@dataclass
class ShippingLabel:
    tracking_number: str
    carrier: str


@dataclass
class CarrierEvent:
    order_id: str
    status: str  # LABEL_CREATED | IN_TRANSIT | OUT_FOR_DELIVERY | DELIVERED
    location: str = ""
    tracking_number: str | None = None
    carrier: str | None = None
    occurred_at: str | None = None


@dataclass
class ShipmentResult:
    tracking_number: str
    carrier: str
    delivered_at: str
    events: list[CarrierEvent] = field(default_factory=list)


# --------------------------------------------------------------------------
# Replenishment (scheduled)
# --------------------------------------------------------------------------


@dataclass
class ReplenishmentInput:
    reorder_quantity: int = 50


@dataclass
class LowStockItem:
    sku: str
    stock: int
    reorder_point: int


@dataclass
class PurchaseOrderRequest:
    run_id: str
    items: list[LowStockItem]
    reorder_quantity: int


@dataclass
class ReplenishmentResult:
    purchase_orders_created: int
    skus: list[str]


# --------------------------------------------------------------------------
# AI customer support (Deep Agent)
# --------------------------------------------------------------------------


@dataclass
class SupportSessionInput:
    session_id: str
    customer_id: str
    ai_model: str = "fake:retail"
    idle_timeout_seconds: int = 1800
    # Carried across continue-as-new so the conversation survives a history reset.
    history: list[dict[str, str]] = field(default_factory=list)
    turn: int = 0


@dataclass
class ChatTurn:
    message: str


@dataclass
class PendingAction:
    tool: str
    args: dict[str, Any]
    description: str = ""


@dataclass
class ChatReply:
    status: str  # "answered" | "awaiting_approval"
    reply: str
    pending_action: PendingAction | None = None


@dataclass
class ApprovalDecision:
    approve: bool
    reviewer: str
    note: str = ""


@dataclass
class SupportView:
    session_id: str
    customer_id: str
    turns: int
    closed: bool
    history: list[dict[str, str]]
    pending_action: PendingAction | None = None


@dataclass
class RefundToolRequest:
    order_id: str
    reason: str
    amount: float | None = None
