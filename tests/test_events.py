import random
from datetime import datetime
from uuid import UUID

from retail_stream.config import WAREHOUSE_ID
from retail_stream.events import create_order


def test_order_has_valid_identifiers_and_utc_time():
    event = create_order(random.Random(42))
    UUID(event["event_id"])
    UUID(event["payload"]["order_id"])
    assert datetime.fromisoformat(event["event_time"]).utcoffset().total_seconds() == 0
    assert event["event_type"] == "order_created"
    assert event["version"] == 2


def test_amount_matches_items():
    rng = random.Random(42)
    for _ in range(100):
        payload = create_order(rng)["payload"]
        assert payload["warehouse_id"] == WAREHOUSE_ID
        assert payload["currency"] == "EUR"
        assert all(item["quantity"] > 0 for item in payload["items"])
        assert payload["total_amount_cents"] == sum(
            item["quantity"] * item["unit_price_cents"] for item in payload["items"]
        )


def test_seed_reproduces_business_data_but_not_identifiers():
    first = create_order(random.Random(42))
    second = create_order(random.Random(42))
    assert first["event_id"] != second["event_id"]
    assert first["payload"].pop("order_id") != second["payload"].pop("order_id")
    assert first["payload"] == second["payload"]
