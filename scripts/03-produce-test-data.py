#!/usr/bin/env python3
"""
03-produce-test-data.py
=======================
Produces sample plain-JSON messages to multiple Kafka topics to test the
Flink Dynamic Iceberg Sink end-to-end.

Topics produced to:
  - orders    (schema evolves at message #evolve_at — adds a `discount_pct` field)
  - payments  (static schema)
  - customers (static schema)

Prerequisites:
    pip install confluent-kafka

Usage:
    python scripts/03-produce-test-data.py [--count 100] [--evolve-at 50]
"""

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "localhost:9092"

# ---------------------------------------------------------------------------
# Data generators
# ---------------------------------------------------------------------------

STATUSES   = ["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"]
CURRENCIES = ["IDR", "USD", "SGD"]
METHODS    = ["BANK_TRANSFER", "CREDIT_CARD", "GOPAY", "OVO", "QRIS"]
SEGMENTS   = ["RETAIL", "CORPORATE", "PREMIUM", "SME"]

def now_iso():
    return datetime.now(tz=timezone.utc).isoformat()

def make_order_v1():
    return {
        "order_id":    str(uuid.uuid4()),
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "product_id":  f"PROD-{random.randint(100, 999)}",
        "quantity":    random.randint(1, 50),
        "unit_price":  round(random.uniform(10_000, 5_000_000), 2),
        "status":      random.choice(STATUSES),
        "created_at":  now_iso(),
    }

def make_order_v2():
    """Schema V2: adds `discount_pct` — demonstrates dynamic schema evolution."""
    rec = make_order_v1()
    rec["discount_pct"] = round(random.uniform(0, 0.30), 4) if random.random() > 0.5 else None
    return rec

def make_payment():
    return {
        "payment_id": str(uuid.uuid4()),
        "order_id":   str(uuid.uuid4()),
        "amount":     round(random.uniform(10_000, 10_000_000), 2),
        "currency":   random.choice(CURRENCIES),
        "method":     random.choice(METHODS),
        "paid_at":    now_iso(),
    }

def make_customer():
    return {
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "name":        f"Customer {random.randint(1, 9999)}",
        "email":       f"user{random.randint(1, 9999)}@example.com",
        "phone":       f"+62{random.randint(800_000_0000, 999_999_9999)}" if random.random() > 0.3 else None,
        "segment":     random.choice(SEGMENTS),
        "created_at":  now_iso(),
    }

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def delivery_report(err, msg):
    if err:
        print(f"[ERROR] {err}")
    else:
        print(f"  ✓ topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}")


def main():
    parser = argparse.ArgumentParser(description="Produce plain JSON data to Kafka")
    parser.add_argument("--count",    type=int,   default=100,  help="Records per topic (default: 100)")
    parser.add_argument("--delay",    type=float, default=0.1,  help="Seconds between messages (default: 0.1)")
    parser.add_argument("--evolve-at",type=int,   default=50,
                        help="Message # at which orders schema evolves to V2 (default: 50)")
    args = parser.parse_args()

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    print(f"\nProducing {args.count} plain-JSON records to each topic")
    print(f"  Bootstrap : {BOOTSTRAP_SERVERS}")
    print(f"  Schema V2 for 'orders' starts at message #{args.evolve_at}\n")

    for i in range(1, args.count + 1):
        # ---- orders ----
        order = make_order_v1() if i <= args.evolve_at else make_order_v2()
        print(f"[{i}/{args.count}] orders → {order['order_id']}"
              + (" (V2 schema)" if i > args.evolve_at else ""))
        producer.produce(
            topic="orders",
            key=order["order_id"].encode(),
            value=json.dumps(order).encode("utf-8"),
            on_delivery=delivery_report,
        )

        # ---- payments ----
        payment = make_payment()
        producer.produce(
            topic="payments",
            key=payment["payment_id"].encode(),
            value=json.dumps(payment).encode("utf-8"),
            on_delivery=delivery_report,
        )

        # ---- customers ----
        customer = make_customer()
        producer.produce(
            topic="customers",
            key=customer["customer_id"].encode(),
            value=json.dumps(customer).encode("utf-8"),
            on_delivery=delivery_report,
        )

        producer.poll(0)
        time.sleep(args.delay)

    producer.flush()
    print("\nDone. All messages delivered.")


if __name__ == "__main__":
    main()
