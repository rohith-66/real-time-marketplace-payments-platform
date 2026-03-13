"""
producer/main.py
Kafka producer for synthetic marketplace payment events.

Run:
    python -m producer.main
"""

import json
import random
import time
import uuid
import signal
import sys
from datetime import datetime, timezone
from kafka import KafkaProducer

# --- Event constants (keep in sync with schemas/payment_event_schema.json) ---
CITIES = ["Phoenix", "New York", "Chicago", "Austin", "Seattle", "San Francisco"]
ORDER_CATEGORIES = ["food_delivery", "groceries", "pharmacy", "retail"]
PAYMENT_METHODS = ["card", "wallet", "upi"]
EVENT_TYPES = ["payment_authorized", "payment_captured", "refund_issued", "payout_sent", "payment_failed"]

# --- Kafka settings ---
KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]
KAFKA_TOPIC = "marketplace-payments"

# --- graceful shutdown flag ---
running = True


def generate_payment_event():
    order_amount = round(random.uniform(8.0, 250.0), 2)
    platform_fee = round(order_amount * random.uniform(0.08, 0.18), 2)
    seller_payout = round(order_amount - platform_fee, 2)

    return {
        "event_id": str(uuid.uuid4()),
        "order_id": f"ord_{random.randint(100000,999999)}",
        "customer_id": f"cust_{random.randint(1000,9999)}",
        "seller_id": f"seller_{random.randint(100,999)}",
        "city": random.choice(CITIES),
        "order_category": random.choice(ORDER_CATEGORIES),
        "payment_method": random.choice(PAYMENT_METHODS),
        "event_type": random.choice(EVENT_TYPES),
        "order_amount": order_amount,
        "platform_fee": platform_fee,
        "seller_payout": seller_payout,
        "currency": "USD",
        "event_timestamp": datetime.now(timezone.utc).isoformat()
    }


def shutdown(signum, frame):
    global running
    print("Shutting down producer...")
    running = False


def main():
    # set up graceful shutdown handlers
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Configure Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",          # wait for all replicas (safe for dev; single replica here)
        retries=5,
        linger_ms=10
    )

    print("Starting marketplace payment event producer -> Kafka topic:", KAFKA_TOPIC)

    try:
        while running:
            event = generate_payment_event()
            # The KafkaProducer send is async; get() blocks until ack (useful for local dev)
            future = producer.send(KAFKA_TOPIC, value=event)
            try:
                record_metadata = future.get(timeout=10)
                print(
                    f"produced event_id={event['event_id']} to {record_metadata.topic} partition={record_metadata.partition} offset={record_metadata.offset}"
                )
            except Exception as e:
                print("failed to produce event:", e)

            # control event throughput (2s is fine for dev); reduce to 0.5s for heavier load
            time.sleep(2)

    finally:
        try:
            producer.flush(timeout=10)
            producer.close()
        except Exception:
            pass
        print("producer stopped.")


if __name__ == "__main__":
    main()