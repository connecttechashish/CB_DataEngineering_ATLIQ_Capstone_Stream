"""
AtliQ Commerce — Phase 2
Live Order Event Producer — stream simulated order events to Kafka.

Run:  python order_event_producer.py --rate 2 --duration 300
Test: python order_event_producer.py --rate 1 --duration 10
first time run: python order_event_producer.py --rate 5 --duration 1500
"""
import os
import json
import time
import uuid
import random
import argparse
from datetime import datetime, timezone

from dotenv import load_dotenv
from confluent_kafka import Producer

load_dotenv()

CITIES = ["Bengaluru", "Mumbai", "Delhi", "Hyderabad", "Chennai",
          "Pune", "Kolkata", "Ahmedabad", "Jaipur", "Surat"]
METHODS = ["UPI", "Credit Card", "Debit Card", "Net Banking", "Wallet", "COD"]
PRODUCT_PRICES = {1: 2499, 2: 3299, 3: 1799, 4: 1499, 5: 4999, 6: 899, 7: 1299,
                  8: 549, 9: 749, 10: 999, 11: 599, 12: 2199, 13: 1099, 14: 1599,
                  15: 899, 16: 299, 17: 449, 18: 549, 19: 799, 20: 1899,
                  21: 249, 22: 699, 23: 799, 24: 599, 25: 499}


def make_producer() -> Producer:
    # TODO 1: producer configured for Confluent Cloud
    return Producer({
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": os.environ["KAFKA_API_KEY"],
        "sasl.password": os.environ["KAFKA_API_SECRET"],
        "client.id": "atliq-producer",
    })


def on_delivery(err, msg):
    # TODO 2 (callback): print success/failure for every message
    if err is not None:
        print(f"FAILED: {err}")
    else:
        print(f"ok {msg.topic()}[{msg.partition()}]@{msg.offset()}")


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def base_event(event_type: str, order: dict) -> dict:
    """One event. event_id is your de-duplication key in Silver — keep it."""
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,      # order_placed | payment_received | order_shipped | order_cancelled
        "event_ts": now_iso(),
        "order_id": order["order_id"],
        "customer_id": order["customer_id"],
        "city": order["city"],
        "product_id": order["product_id"],
        "quantity": order["quantity"],
        "order_amount": order["order_amount"],
        "payment_method": order["payment_method"] if event_type == "payment_received" else None,
    }


def new_order(order_id: int) -> dict:
    product_id = random.choice(list(PRODUCT_PRICES))
    qty = random.randint(1, 3)
    return {
        "order_id": order_id,
        "customer_id": random.randint(1, 40),
        "city": random.choice(CITIES),
        "product_id": product_id,
        "quantity": qty,
        "order_amount": PRODUCT_PRICES[product_id] * qty,
        "payment_method": random.choice(METHODS),
    }


def run(rate: float, duration: int):
    producer = make_producer()
    topic = os.environ.get("KAFKA_TOPIC", "atliq.orders.events")
    open_orders, next_order_id, sent = [], 100_000, 0
    deadline = time.time() + duration

    print(f"Producing to '{topic}' at ~{rate} events/sec for {duration}s ...")
    try:
        while time.time() < deadline:
            roll = random.random()
            if roll < 0.55 or not open_orders:
                order = new_order(next_order_id); next_order_id += 1
                events = [base_event("order_placed", order)]
                if order["payment_method"] != "COD":
                    events.append(base_event("payment_received", order))
                open_orders.append(order)
            elif roll < 0.90:
                events = [base_event("order_shipped", open_orders.pop(random.randrange(len(open_orders))))]
            else:
                events = [base_event("order_cancelled", open_orders.pop(random.randrange(len(open_orders))))]

            for ev in events:
                # TODO 2: publish; key = order_id keeps one order's events in one partition
                producer.produce(
                    topic,
                    key=str(ev["order_id"]),
                    value=json.dumps(ev),
                    callback=on_delivery,
                )
                sent += 1

            producer.poll(0)
            time.sleep(1.0 / rate)
    except KeyboardInterrupt:
        print("\nStopping ...")
    finally:
        # TODO 3: deliver every buffered message before exiting
        producer.flush()
        print(f"Done. {sent} events sent.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=float, default=2.0)
    ap.add_argument("--duration", type=int, default=300)
    args = ap.parse_args()
    run(args.rate, args.duration)