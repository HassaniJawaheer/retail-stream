import json
import random
from datetime import datetime

import pytest

from retail_stream.scenarios import encode_scenario, fixed_event
from retail_stream.streaming import create_spark, parse_orders
from retail_stream.validation import validate_orders


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
        rows = validate_orders(parse_orders(raw)).orderBy("offset").collect()
        for row, (_, _, expected) in zip(rows, cases, strict=True):
            if expected is None:
                assert row.errors == []
            else:
                assert expected in row.errors
    finally:
        spark.stop()
