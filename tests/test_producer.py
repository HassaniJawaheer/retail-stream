import random
from unittest.mock import Mock

import pytest

from retail_stream.producer import publish_orders
from retail_stream.scenarios import SCENARIOS, encode_scenario, fixed_event


def test_scenarios_are_reproducible_and_do_not_change_the_source():
    event = fixed_event(random.Random(42), "test")
    assert event == fixed_event(random.Random(42), "test")
    for scenario in SCENARIOS:
        assert encode_scenario(event, scenario) == encode_scenario(event, scenario)
    assert event["payload"]["items"][0]["quantity"] > 0
    assert event["payload"]["order_id"]
    assert event["version"] == 2


@pytest.mark.parametrize("remaining,delivery_error", [(1, None), (0, "delivery failed")])
def test_producer_reports_unfinished_or_failed_deliveries(monkeypatch, remaining, delivery_error):
    producer = Mock()
    producer.flush.return_value = remaining

    def produce(*args, **kwargs):
        if delivery_error:
            kwargs["on_delivery"](delivery_error, None)

    producer.produce.side_effect = produce
    monkeypatch.setattr("retail_stream.producer.Producer", lambda config: producer)
    monkeypatch.setattr("retail_stream.producer.time.sleep", lambda seconds: None)
    with pytest.raises(RuntimeError, match="Échec de livraison"):
        publish_orders("unused", "test", 1, 100, 42)
    producer.flush.assert_called_once_with(20)


def test_full_queue_retries_the_same_message(monkeypatch):
    producer = Mock()
    producer.produce.side_effect = [BufferError(), None]
    producer.flush.return_value = 0
    monkeypatch.setattr("retail_stream.producer.Producer", lambda config: producer)
    monkeypatch.setattr("retail_stream.producer.time.sleep", lambda seconds: None)
    publish_orders("unused", "test", 1, 100, 42)
    first, second = producer.produce.call_args_list
    assert first == second
    producer.poll.assert_any_call(0.1)


def test_duplicate_scenario_resends_identical_messages(monkeypatch):
    producer = Mock()
    producer.flush.return_value = 0
    monkeypatch.setattr("retail_stream.producer.Producer", lambda config: producer)
    monkeypatch.setattr("retail_stream.producer.time.sleep", lambda seconds: None)
    publish_orders("unused", "test", 5, 100, 42, "duplicates")
    calls = producer.produce.call_args_list
    assert len(calls) == 5
    for first, second in [(0, 1), (2, 3)]:
        assert calls[first].kwargs["key"] == calls[second].kwargs["key"]
        assert calls[first].kwargs["value"] == calls[second].kwargs["value"]
    assert len({call.kwargs["value"] for call in calls}) == 3
