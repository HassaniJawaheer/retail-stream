import json
import os
import random
import signal
import subprocess
import sys
import time
from uuid import uuid4

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from retail_stream.config import BOOTSTRAP_SERVERS
from retail_stream.scenarios import SCENARIOS, encode_scenario, fixed_event
from retail_stream.streaming import create_spark, start_validation


@pytest.mark.integration
def test_routing_and_checkpoint_restart(tmp_path):
    prefix = f"retail.test.{uuid4().hex}"
    raw, valid, invalid = [f"{prefix}.{suffix}" for suffix in ("raw", "valid", "invalid")]
    topics = [raw, valid, invalid]
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    for future in admin.create_topics([NewTopic(t, 3, 1) for t in topics]).values():
        future.result(20)
    spark = None
    query = None
    process = None
    checkpoint = str(tmp_path / "checkpoint")
    try:
        spark = create_spark()
        query = start_validation(spark, BOOTSTRAP_SERVERS, raw, valid, invalid, checkpoint)
        producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
        messages = []
        failures = []
        for i, scenario in enumerate(SCENARIOS):
            event = fixed_event(random.Random(42), f"{prefix}:{i}")
            key, value = encode_scenario(event, scenario)
            messages.append(value.decode())
            producer.produce(
                raw,
                key=key,
                value=value,
                on_delivery=lambda error, message: failures.append(error) if error else None,
            )
        assert producer.flush(20) == 0
        assert not failures
        query.processAllAvailable()
        query.stop()
        query = None

        def read(topic):
            return (
                spark.read.format("kafka")
                .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
                .option("subscribe", topic)
                .load()
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                .collect()
            )

        good = read(valid)
        bad = read(invalid)
        assert len(good) == 1
        assert len(bad) == 5
        assert good[0].value == messages[0]
        assert good[0].key == json.loads(messages[0])["payload"]["order_id"]
        rejected = [json.loads(row.value) for row in bad]
        assert {r["raw_value"] for r in rejected} == set(messages[1:])
        assert all(r["errors"] and r["source"]["topic"] == raw for r in rejected)
        assert len({row.key for row in bad}) == 5

        env = dict(
            os.environ,
            KAFKA_BOOTSTRAP_SERVERS=BOOTSTRAP_SERVERS,
            KAFKA_ORDERS_TOPIC=raw,
            KAFKA_VALID_TOPIC=valid,
            KAFKA_INVALID_TOPIC=invalid,
            SPARK_CHECKPOINT=checkpoint,
        )
        with (tmp_path / "spark.log").open("w+") as log:
            process = subprocess.Popen(
                [sys.executable, "-c", "from retail_stream.streaming import main; main()"],
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                log.seek(0)
                if "Validation active" in log.read():
                    break
                assert process.poll() is None
                time.sleep(0.2)
            else:
                pytest.fail("Spark n'a pas démarré")
            process.send_signal(signal.SIGINT)
            assert process.wait(timeout=30) == 0
            log.seek(0)
            assert "Traceback" not in log.read()

        key, value = encode_scenario(fixed_event(random.Random(43), prefix + "new"), "normal")
        producer.produce(
            raw,
            key=key,
            value=value,
            on_delivery=lambda error, message: failures.append(error) if error else None,
        )
        assert producer.flush(20) == 0
        assert not failures
        query = start_validation(spark, BOOTSTRAP_SERVERS, raw, valid, invalid, checkpoint, True)
        assert query.awaitTermination(60)
        assert len(read(valid)) == 2
        assert len(read(invalid)) == 5
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait()
        if query is not None and query.isActive:
            query.stop()
        if spark is not None:
            spark.stop()
        for future in admin.delete_topics(topics).values():
            future.result(20)
