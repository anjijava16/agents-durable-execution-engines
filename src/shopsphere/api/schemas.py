"""HTTP request/response models (Pydantic). Kept separate from the workflow
dataclasses on purpose: the public API and the workflow contract evolve at
different speeds, and this is the seam where you translate between them."""

from __future__ import annotations

from pydantic import BaseModel, Field


class AddressIn(BaseModel):
    name: str
    line1: str
    city: str
    state: str = Field(min_length=2, max_length=2)
    postal_code: str
    country: str = "US"


class LineItemIn(BaseModel):
    sku: str
    quantity: int = Field(gt=0, le=20)


class CreateOrderIn(BaseModel):
    customer_id: str
    email: str
    items: list[LineItemIn] = Field(min_length=1)
    shipping_address: AddressIn
    payment_token: str = "tok_visa"


class CreateOrderOut(BaseModel):
    order_id: str
    workflow_id: str
    run_id: str | None
    status_url: str
    # True only when the id belongs to an order that already finished.
    # A retry while the order is still running simply attaches to it (USE_EXISTING).
    already_existed: bool = False


class CancelIn(BaseModel):
    reason: str = "changed my mind"


class ReviewIn(BaseModel):
    approved: bool
    reviewer: str
    note: str = ""


class CarrierEventIn(BaseModel):
    order_id: str
    status: str = Field(pattern="^(IN_TRANSIT|OUT_FOR_DELIVERY|DELIVERED)$")
    location: str = ""


class StartSessionIn(BaseModel):
    customer_id: str


class ChatIn(BaseModel):
    message: str = Field(min_length=1, max_length=2000)


class DecisionIn(BaseModel):
    approve: bool
    reviewer: str
    note: str = ""
