import random
from datetime import datetime, timezone
from uuid import uuid4

from retail_stream.config import WAREHOUSE_ID

CATALOG = (("SKU-001", 1990), ("SKU-002", 3500), ("SKU-003", 799))


def create_order(rng: random.Random) -> dict:
    items = [
        {"sku": sku, "quantity": rng.randint(1, 3), "unit_price_cents": price}
        for sku, price in rng.sample(CATALOG, k=rng.randint(1, len(CATALOG)))
    ]
    return {
        "event_id": str(uuid4()),
        "event_type": "order_created",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "producer": "retail-simulator",
        "version": 2,
        "payload": {
            "order_id": str(uuid4()),
            "customer_id": f"CUST-{rng.randint(1, 100):03d}",
            "warehouse_id": WAREHOUSE_ID,
            "currency": "EUR",
            "items": items,
            "total_amount_cents": sum(i["quantity"] * i["unit_price_cents"] for i in items),
        },
    }
