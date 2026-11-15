import json
import os
import random
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from retail_stream.config import BOOTSTRAP_SERVERS
from retail_stream.deduplication import start_deduplication
from retail_stream.scenarios import encode_scenario, fixed_event
from retail_stream.streaming import create_spark


def snapshot(topic):
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": f"test-{uuid4().hex}",
            "enable.auto.commit": False,
        }
    )
    try:
        partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
        starts = []
        count = 0
        for partition in partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
            starts.append(TopicPartition(topic, partition, low))
            count += high - low
        consumer.assign(starts)
        rows = []
        deadline = time.monotonic() + 20
        while len(rows) < count and time.monotonic() < deadline:
            message = consumer.poll(0.2)
            if message is not None:
                assert not message.error(), message.error()
                rows.append((message.key(), message.value()))
        assert len(rows) == count
        return rows
    finally:
        consumer.close()


@pytest.mark.integration
def test_deduplication_across_batches_restart_and_watermark(tmp_path):
    prefix = f"retail.test.{uuid4().hex}"
    source, output = f"{prefix}.valid", f"{prefix}.unique"
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    for future in admin.create_topics([NewTopic(t, 3, 1) for t in (source, output)]).values():
        future.result(20)
    checkpoint = str(tmp_path / "checkpoint")
    spark = None
    query = None
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    failures = []
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def event(name, seconds=0):
        value = fixed_event(random.Random(42), prefix + name)
        value["event_time"] = (base + timedelta(seconds=seconds)).isoformat()
        return value

    def publish(*events):
        for value in events:
            key, encoded = encode_scenario(value, "normal")
            producer.produce(
                source,
                key=key,
                value=encoded,
                on_delivery=lambda error, message: failures.append(error) if error else None,
            )
        assert producer.flush(20) == 0
        assert not failures

    def ids():
        return Counter(json.loads(value)["event_id"] for _, value in snapshot(output))

    try:
        first = event("a")
        second = event("b", 10)
        second["payload"]["order_id"] = first["payload"]["order_id"]
        publish(first, first, second)
        spark = create_spark()
        query = start_deduplication(spark, BOOTSTRAP_SERVERS, source, output, checkpoint)
        query.processAllAvailable()
        assert ids() == Counter({first["event_id"]: 1, second["event_id"]: 1})
        publish(first)
        query.processAllAvailable()
        assert len(snapshot(output)) == 2
        assert any(
            state["customMetrics"].get("numDroppedDuplicateRows", 0) > 0
            for progress in query.recentProgress
            for state in progress["stateOperators"]
        )
        query.stop()
        query = None
        spark.stop()
        spark = None

        third = event("c", 20)
        publish(first, third)
        env = dict(
            os.environ,
            KAFKA_BOOTSTRAP_SERVERS=BOOTSTRAP_SERVERS,
            KAFKA_VALID_TOPIC=source,
            KAFKA_UNIQUE_TOPIC=output,
            SPARK_DEDUP_CHECKPOINT=checkpoint,
            SPARK_DEDUP_WATERMARK="2 minutes",
        )
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "from retail_stream.deduplication import main; main()",
                "--available-now",
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=90,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert ids() == Counter({e["event_id"]: 1 for e in (first, second, third)})

        spark = create_spark()
        query = start_deduplication(spark, BOOTSTRAP_SERVERS, source, output, checkpoint)
        future = event("future", 600)
        publish(future)
        query.processAllAvailable()
        assert any(
            state["numRowsRemoved"] > 0
            for progress in query.recentProgress
            for state in progress["stateOperators"]
        )
        late = event("late", 0)
        accepted_late = event("accepted-late", 540)
        publish(first, late, accepted_late)
        query.processAllAvailable()
        assert ids() == Counter(
            {e["event_id"]: 1 for e in (first, second, third, future, accepted_late)}
        )
        assert any(
            state["numRowsDroppedByWatermark"] >= 2
            for progress in query.recentProgress
            for state in progress["stateOperators"]
        )
        for key, value in snapshot(output):
            decoded = json.loads(value)
            assert key.decode() == decoded["payload"]["order_id"]
    finally:
        if query is not None and query.isActive:
            query.stop()
        if spark is not None:
            spark.stop()
        for future in admin.delete_topics([source, output]).values():
            future.result(20)
