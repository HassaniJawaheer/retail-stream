import json
import random
from datetime import datetime

import pytest

from retail_stream.config import WAREHOUSE_ID
from retail_stream.scenarios import encode_scenario, fixed_event
from retail_stream.streaming import create_spark, parse_orders
from retail_stream.validation import route_orders, validate_orders


@pytest.mark.integration
def test_validation_rules():
    spark = create_spark()
    cases = []
    try:
        for scenario, expected in [
            ("normal", None),
            ("invalid-json", "INVALID_JSON_OR_TYPE"),
            ("missing-field", "MISSING_ORDER_ID"),
            ("negative-quantity", "INVALID_ITEM"),
            ("wrong-total", "TOTAL_MISMATCH"),
            ("unknown-version", "UNSUPPORTED_VERSION"),
        ]:
            event = fixed_event(random.Random(42), scenario)
            key, value = encode_scenario(event, scenario)
            cases.append((key, value, expected))
        for field, value, expected in [
            ("items", [], "EMPTY_ITEMS"),
            ("items", [None], "INVALID_ITEM"),
            ("warehouse_id", "UNKNOWN", "UNKNOWN_WAREHOUSE"),
            ("currency", "USD", "UNSUPPORTED_CURRENCY"),
            ("customer_id", " ", "MISSING_CUSTOMER_ID"),
            ("total_amount_cents", None, "TOTAL_MISMATCH"),
        ]:
            event = fixed_event(random.Random(42), expected)
            event["payload"][field] = value
            key, encoded = encode_scenario(event, "normal")
            cases.append((key, encoded, expected))
        for version, warehouse, expected in [
            (1, "WH-PARIS", None),
            (1, "WH-LYON", None),
            (1, WAREHOUSE_ID, "UNKNOWN_WAREHOUSE"),
            (2, "WH-PARIS", "UNKNOWN_WAREHOUSE"),
            (2, "WH-LYON", "UNKNOWN_WAREHOUSE"),
            (2, WAREHOUSE_ID, None),
        ]:
            event = fixed_event(random.Random(42), f"warehouse-{version}-{warehouse}")
            event["version"] = version
            event["payload"]["warehouse_id"] = warehouse
            key, encoded = encode_scenario(event, "normal")
            cases.append((key, encoded, expected))
        for sku in ["SKU-INCONNU", "", None]:
            event = fixed_event(random.Random(42), f"unknown-product-{sku}")
            event["payload"]["items"][-1]["sku"] = sku
            key, encoded = encode_scenario(event, "normal")
            cases.append((key, encoded, "UNKNOWN_PRODUCT"))
        event = fixed_event(random.Random(42), "bad-date")
        event["event_time"] = "not-a-date"
        cases.append(
            (
                event["payload"]["order_id"].encode(),
                json.dumps(event).encode(),
                "INVALID_EVENT_TIME",
            )
        )
        event = fixed_event(random.Random(42), "bad-key")
        cases.append((b"wrong-key", json.dumps(event).encode(), "KEY_MISMATCH"))
        raw = spark.createDataFrame(
            [
                (key, value, "test", 0, i, datetime(2026, 1, 1))
                for i, (key, value, _) in enumerate(cases)
            ],
            "key binary, value binary, topic string, "
            "partition int, offset long, timestamp timestamp",
        )
        validated = validate_orders(parse_orders(raw))
        rows = validated.orderBy("offset").collect()
        routed = route_orders(validated, "valid", "invalid").collect()
        rejections = [json.loads(row.value) for row in routed if row.topic == "invalid"]
        assert sum("UNKNOWN_PRODUCT" in row["errors"] for row in rejections) >= 3
        for row, (_, _, expected) in zip(rows, cases, strict=True):
            if expected is None:
                assert row.errors == []
            else:
                assert expected in row.errors
    finally:
        spark.stop()
