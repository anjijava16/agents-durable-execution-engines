"""Walk through every scenario over HTTP, like a real client would.

Prereqs: Temporal dev server, both workers and the API are running
(`make dev` prints the commands). Then:

    python scripts/demo.py

Open http://localhost:8233 while it runs and watch the workflows appear.
"""

from __future__ import annotations

import os
import sys
import time
import uuid

import httpx

API = os.environ.get("API_URL", "http://localhost:8000")
http = httpx.Client(base_url=API, timeout=60)

ADDRESS = {"name": "Alice Rivera", "line1": "1 Market St", "city": "Philadelphia", "state": "PA", "postal_code": "19103"}


def banner(text: str) -> None:
    print(f"\n{'=' * 72}\n{text}\n{'=' * 72}")


def place(customer: str, email: str, sku: str, qty: int = 1, token: str = "tok_visa", key: str | None = None) -> str:
    headers = {"Idempotency-Key": key} if key else {}
    r = http.post(
        "/orders",
        headers=headers,
        json={
            "customer_id": customer,
            "email": email,
            "items": [{"sku": sku, "quantity": qty}],
            "shipping_address": ADDRESS,
            "payment_token": token,
        },
    )
    r.raise_for_status()
    data = r.json()
    print(f"  placed {data['order_id']}")
    return data["order_id"]


def status(order_id: str) -> dict:
    return http.get(f"/orders/{order_id}").json()


def wait_for(order_id: str, *wanted: str, timeout: float = 60) -> dict:
    end = time.time() + timeout
    while time.time() < end:
        view = status(order_id)
        if view["status"] in wanted:
            return view
        time.sleep(0.5)
    sys.exit(f"  {order_id} stuck in {status(order_id)['status']}, expected {wanted}")


def carrier(order_id: str) -> None:
    wait_for(order_id, "SHIPPED")
    for s, loc in [("IN_TRANSIT", "Newark, NJ"), ("OUT_FOR_DELIVERY", "Philadelphia, PA"), ("DELIVERED", "Front porch")]:
        http.post("/carrier/webhook", json={"order_id": order_id, "status": s, "location": loc}).raise_for_status()
        print(f"  carrier -> {s}")
        time.sleep(0.3)


def timeline(view: dict) -> None:
    for e in view["timeline"]:
        print(f"    {e['at'][11:19]}  {e['status']:<18} {e['detail']}")


def main() -> None:
    banner("1. Happy path: Alice buys headphones (retry-safe POST with an Idempotency-Key)")
    key = uuid.uuid4().hex
    oid = place("CUST-ALICE", "alice@example.com", "SKU-HEADPHONES", key=key)
    again = place("CUST-ALICE", "alice@example.com", "SKU-HEADPHONES", key=key)  # client retry
    print(f"  duplicate POST attached to the same workflow: {again == oid}")
    carrier(oid)
    print("  waiting for the durable review-request timer ...")
    timeline(wait_for(oid, "COMPLETED", timeout=120))
    delivered_order = oid

    banner("2. Flaky payment gateway: two 503s, then success. Nobody notices.")
    oid = place("CUST-ALICE", "alice@example.com", "SKU-COFFEE", qty=2, token="tok_flaky")
    view = wait_for(oid, "SHIPPED", "PAID", "FULFILLING")
    print("  " + next(e["detail"] for e in view["timeline"] if e["status"] == "PAID"))
    carrier(oid)

    banner("3. Declined card -> saga compensation releases the stock")
    oid = place("CUST-ALICE", "alice@example.com", "SKU-SNEAKERS", token="tok_decline")
    timeline(wait_for(oid, "FAILED"))

    banner("4. High-risk customer -> LangGraph fraud graph rejects")
    oid = place("CUST-MALLORY", "mallory@example.net", "SKU-HEADPHONES")
    view = wait_for(oid, "REJECTED")
    print(f"  fraud: {view['fraud']}")

    banner("5. New customer, big order -> human review, address change, approval")
    oid = place("CUST-BOB", "bob@example.com", "SKU-LAPTOP")
    view = wait_for(oid, "PENDING_REVIEW")
    print(f"  fraud says: {view['fraud']['decision']} because {view['fraud']['reasons']}")
    r = http.put(f"/orders/{oid}/shipping-address", json={**ADDRESS, "name": "Bob Chen", "city": "Seattle", "state": "WA", "postal_code": "98101"})
    print(f"  address update -> {r.status_code}, now ships to {r.json()['shipping_address']['city']}")
    http.post(f"/orders/{oid}/review", json={"approved": True, "reviewer": "analyst-kim", "note": "called the customer"})
    carrier(oid)
    r = http.put(f"/orders/{oid}/shipping-address", json=ADDRESS)
    print(f"  late address update -> {r.status_code}: {r.json()['detail']}")

    banner("6. Customer cancels while the order waits for review")
    oid = place("CUST-BOB", "bob@example.com", "SKU-LAPTOP")
    wait_for(oid, "PENDING_REVIEW")
    http.post(f"/orders/{oid}/cancel", json={"reason": "found it cheaper"})
    timeline(wait_for(oid, "CANCELLED"))

    banner("7. AI support (Deep Agent): status, policy (sub-agent), refund with approval")
    sid = http.post("/support/sessions", json={"customer_id": "CUST-ALICE"}).json()["session_id"]
    for msg in [
        f"Where is my order {delivered_order}?",
        "What's your policy on final sale items?",
        f"I want to return {delivered_order}, the left earcup rattles",
    ]:
        print(f"\n  customer: {msg}")
        reply = http.post(f"/support/sessions/{sid}/messages", json={"message": msg}).json()
        print(f"  agent   : {reply['reply']}")
        if reply["status"] == "awaiting_approval":
            print(f"  >> parked, waiting on a supervisor for: {reply['pending_action']}")

    reply = http.post(f"/support/sessions/{sid}/decision", json={"approve": True, "reviewer": "supervisor-dana"}).json()
    print(f"\n  supervisor approved -> agent: {reply['reply']}")
    http.post(f"/support/sessions/{sid}/close")

    banner("8. Search orders through Temporal Visibility")
    for row in http.get("/orders", params={"status": "COMPLETED"}).json()[:5]:
        print(f"  {row}")
    print("\nDone. Open http://localhost:8233 to inspect every workflow's event history.")


if __name__ == "__main__":
    main()
