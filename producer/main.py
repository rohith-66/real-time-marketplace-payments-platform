import json
import random
import time
import uuid
from datetime import datetime, timezone

CITIES = [
    "Phoenix",
    "New York",
    "Chicago",
    "Austin",
    "Seattle",
    "San Francisco"
]

ORDER_CATEGORIES = [
    "food_delivery",
    "groceries",
    "pharmacy",
    "retail"
]

PAYMENT_METHODS = [
    "card",
    "wallet",
    "upi"
]

EVENT_TYPES = [
    "payment_authorized",
    "payment_captured",
    "refund_issued",
    "payout_sent",
    "payment_failed"
]


def generate_payment_event():

    order_amount = round(random.uniform(8.0, 250.0), 2)

    platform_fee = round(order_amount * random.uniform(0.08, 0.18), 2)

    seller_payout = round(order_amount - platform_fee, 2)

    event = {
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

    return event


def main():

    print("Starting marketplace payment event stream...\n")

    while True:

        event = generate_payment_event()

        print(json.dumps(event))

        time.sleep(2)


if __name__ == "__main__":
    main()