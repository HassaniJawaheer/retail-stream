import json
from copy import deepcopy
from datetime import datetime, timezone
from uuid import NAMESPACE_URL, uuid5

from retail_stream.events import create_order

SCENARIOS = (
    "normal",
    "invalid-json",
    "missing-field",
    "negative-quantity",
    "wrong-total",
    "unknown-version",
)


def make_message(rng, scenario="normal", index=0):
    event = create_order(rng)
    if scenario == "mixed":
        scenario = SCENARIOS[index % len(SCENARIOS)]
    return encode_scenario(event, scenario)


def encode_scenario(event, scenario):
    event = deepcopy(event)
    key = event["payload"]["order_id"].encode()
    if scenario == "invalid-json":
        return key, b'{"event_id":'
    if scenario == "missing-field":
        del event["payload"]["order_id"]
    elif scenario == "negative-quantity":
        event["payload"]["items"][0]["quantity"] = -1
    elif scenario == "wrong-total":
        event["payload"]["total_amount_cents"] += 1
    elif scenario == "unknown-version":
        event["version"] = 999
    elif scenario != "normal":
        raise ValueError(f"Scénario inconnu : {scenario}")
    return key, json.dumps(event).encode()


def fixed_event(rng, name):
    event = create_order(rng)
    event["event_id"] = str(uuid5(NAMESPACE_URL, f"event:{name}"))
    event["payload"]["order_id"] = str(uuid5(NAMESPACE_URL, f"order:{name}"))
    event["event_time"] = datetime(2026, 1, 1, tzinfo=timezone.utc).isoformat()
    return event
