"""A tiny SQLite "backend" that stands in for real retail systems.

In production these would be separate services (inventory service, Stripe/
Adyen, a carrier API, the CRM, an email provider). Here they live in one
SQLite file so that the API, the orders worker and the AI worker - three
separate processes - see the same data without any extra infrastructure.

The important part is not the storage. It's that every write is idempotent,
keyed on something deterministic (order_id, workflow id + activity id).
Temporal guarantees an activity is *retried* until it succeeds; it does not
guarantee it runs *exactly once*. Idempotency is how you get effectively-once
side effects on top of at-least-once execution.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator

from shopsphere.config import get_settings

_lock = threading.Lock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS products (
    sku TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    price REAL NOT NULL,
    stock INTEGER NOT NULL,
    reorder_point INTEGER NOT NULL DEFAULT 5
);
CREATE TABLE IF NOT EXISTS customers (
    customer_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    created_at TEXT NOT NULL,
    lifetime_orders INTEGER NOT NULL DEFAULT 0,
    chargebacks INTEGER NOT NULL DEFAULT 0,
    loyalty_points INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    email TEXT NOT NULL,
    status TEXT NOT NULL,
    total REAL NOT NULL,
    items TEXT NOT NULL,
    tracking_number TEXT,
    delivered_at TEXT,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS reservations (
    order_id TEXT PRIMARY KEY,
    items TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS payments (
    payment_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL UNIQUE,
    amount REAL NOT NULL,
    refunded_amount REAL NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS refunds (
    refund_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL UNIQUE,
    amount REAL NOT NULL,
    reason TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS shipments (
    order_id TEXT PRIMARY KEY,
    tracking_number TEXT NOT NULL,
    carrier TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS return_labels (
    order_id TEXT PRIMARY KEY,
    rma_number TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS notifications (
    idempotency_key TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    email TEXT NOT NULL,
    template TEXT NOT NULL,
    data TEXT NOT NULL,
    sent_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS loyalty_ledger (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    points INTEGER NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS purchase_orders (
    po_id TEXT PRIMARY KEY,
    sku TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    created_at TEXT NOT NULL
);
"""

SEED_PRODUCTS = [
    ("SKU-HEADPHONES", "Noise-cancelling headphones", 199.99, 40, 5),
    ("SKU-SNEAKERS", "Trail running sneakers", 129.00, 25, 5),
    ("SKU-COFFEE", "Single-origin coffee beans 1kg", 24.50, 200, 20),
    ("SKU-LAPTOP", "14-inch ultrabook", 1499.00, 8, 3),
    ("SKU-TV", "65-inch OLED TV", 2299.00, 3, 2),
    ("SKU-SOCKS", "Merino wool socks (3-pack)", 18.00, 4, 10),
]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _seed_customers() -> list[tuple]:
    now = datetime.now(timezone.utc)
    return [
        # Long-time, clean history: should sail through fraud checks.
        ("CUST-ALICE", "Alice Rivera", "alice@example.com", (now - timedelta(days=900)).isoformat(), 42, 0, 0),
        # Brand-new account: pushes risk into the grey zone on big orders.
        ("CUST-BOB", "Bob Chen", "bob@example.com", (now - timedelta(days=1)).isoformat(), 0, 0, 0),
        # Prior chargebacks: high risk.
        ("CUST-MALLORY", "Mallory Quinn", "mallory@example.net", (now - timedelta(days=20)).isoformat(), 3, 2, 0),
    ]


def db_path() -> str:
    path = get_settings().db_path
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    return path


@contextmanager
def connect() -> Iterator[sqlite3.Connection]:
    # One process-wide lock keeps SQLite happy when many activities run in
    # parallel threads. A real service would have its own concurrency control.
    with _lock:
        conn = sqlite3.connect(db_path(), timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def init_db(reset: bool = False) -> None:
    if reset:
        Path(db_path()).unlink(missing_ok=True)
    with connect() as conn:
        conn.executescript(SCHEMA)
        conn.executemany(
            "INSERT OR IGNORE INTO products (sku, name, price, stock, reorder_point) VALUES (?,?,?,?,?)",
            SEED_PRODUCTS,
        )
        conn.executemany(
            "INSERT OR IGNORE INTO customers "
            "(customer_id, name, email, created_at, lifetime_orders, chargebacks, loyalty_points) "
            "VALUES (?,?,?,?,?,?,?)",
            _seed_customers(),
        )


def dumps(obj) -> str:
    return json.dumps(obj, default=lambda o: o.__dict__)


now_iso = _now
