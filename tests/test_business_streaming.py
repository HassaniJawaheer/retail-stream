import json
import os
import subprocess
import sys
from uuid import uuid4

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from retail_stream.business_streaming import start_business, write_business_batch
from retail_stream.catalog import connect
from retail_stream.config import BOOTSTRAP_SERVERS, INVENTORY_TOPIC, ORDERS_TOPIC
from retail_stream.inventory import finish_order, place_order
from retail_stream.outbox import publish_pending
from retail_stream.streaming import create_spark, start_validation

pytestmark = pytest.mark.integration


def snapshot(dsn):
    with connect(dsn) as conn:
        return [
            conn.execute(f"SELECT * FROM {table} ORDER BY 1,2").fetchall()
            for table in [
                "inventory_projection",
                "order_projection",
                "business_metrics",
                "movement_metrics",
            ]
        ]


def test_business_pipeline_replay_disorder_and_failure(operations_database, tmp_path, monkeypatch):
    prefix = f"retail.test.{uuid4().hex}"
    raw, valid, invalid, inv, inv_bad = [
        f"{prefix}.{s}" for s in ["raw", "valid", "invalid", "inventory", "bad"]
    ]
    topics = [raw, valid, invalid, inv, inv_bad]
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    for future in admin.create_topics([NewTopic(t, 3, 1) for t in topics]).values():
        future.result(20)
    spark = None
    queries = []

    def publish():
        with connect(operations_database) as conn:
            conn.execute("UPDATE outbox SET topic=%s WHERE topic=%s", (raw, ORDERS_TOPIC))
            conn.execute("UPDATE outbox SET topic=%s WHERE topic=%s", (inv, INVENTORY_TOPIC))
        return publish_pending(operations_database)

    def run():
        query = start_validation(
            spark,
            BOOTSTRAP_SERVERS,
            raw,
            valid,
            invalid,
            str(tmp_path / "validation"),
            True,
            operations_database,
        )
        queries.append(query)
        assert query.awaitTermination(90)
        query = start_business(
            spark,
            BOOTSTRAP_SERVERS,
            operations_database,
            str(tmp_path / "business"),
            valid,
            inv,
            inv_bad,
            True,
        )
        queries.append(query)
        assert query.awaitTermination(90)

    try:
        order = place_order("good", "c", [("SKU-001", 3)], operations_database)
        place_order("reject", "c", [("SKU-001", 100)], operations_database)
        finish_order(order["order_id"], "ship", operations_database)
        assert publish() == 23
        spark = create_spark()
        run()
        with connect(operations_database) as conn:
            stock = conn.execute(
                "SELECT * FROM inventory_projection WHERE sku='SKU-001'"
            ).fetchone()
            assert (stock["physical"], stock["reserved"]) == (17, 0)
            assert (
                conn.execute("SELECT sum(event_count) AS n FROM business_metrics").fetchone()["n"]
                == 5
            )
            assert conn.execute("SELECT count(*) AS n FROM analytics_events").fetchone()["n"] == 23
            conn.execute("UPDATE outbox SET published_at=NULL")
        before = snapshot(operations_database)
        assert publish() == 23
        run()
        assert snapshot(operations_database) == before
        original = write_business_batch

        def failure(batch, batch_id, dsn):
            original(batch, batch_id, dsn)
            raise RuntimeError("Failure after business commit")

        place_order("next", "c", [("SKU-002", 2)], operations_database)
        publish()
        monkeypatch.setattr("retail_stream.business_streaming.write_business_batch", failure)
        with pytest.raises(Exception, match="Failure after business commit"):
            run()
        after = snapshot(operations_database)
        spark.stop()
        spark = None
        monkeypatch.setattr("retail_stream.business_streaming.write_business_batch", original)
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "from retail_stream.business_streaming import main; main()",
                "--available-now",
            ],
            env=dict(
                os.environ,
                DATABASE_URL=operations_database,
                KAFKA_VALID_TOPIC=valid,
                KAFKA_INVENTORY_TOPIC=inv,
                KAFKA_INVENTORY_INVALID_TOPIC=inv_bad,
                SPARK_BUSINESS_CHECKPOINT=str(tmp_path / "business"),
            ),
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert snapshot(operations_database) == after
        with connect(operations_database) as conn:
            old = conn.execute(
                "SELECT event FROM outbox WHERE event->>'event_type'='inventory_changed' "
                "AND event->'payload'->>'sku'='SKU-001' ORDER BY sequence LIMIT 1"
            ).fetchone()["event"]
        old["event_id"] = str(uuid4())
        old["event_time"] = "2000-01-01T00:00:00+00:00"
        producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
        key = f"{old['payload']['warehouse_id']}:SKU-001"
        producer.produce(inv, key=key, value=json.dumps(old))
        bad = dict(old, event_id=str(uuid4()), payload=dict(old["payload"], physical=-1))
        producer.produce(inv, key=key, value=json.dumps(bad))
        assert producer.flush(20) == 0
        spark = create_spark()
        run()
        with connect(operations_database) as conn:
            row = conn.execute(
                "SELECT physical FROM inventory_projection WHERE sku='SKU-001'"
            ).fetchone()
            assert row["physical"] == 17
        rejections = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
            .option("subscribe", inv_bad)
            .load()
            .count()
        )
        assert rejections == 1
    finally:
        for query in queries:
            try:
                if query.isActive:
                    query.stop()
            except Exception:
                pass
        if spark is not None:
            spark.stop()
        for future in admin.delete_topics(topics).values():
            future.result(20)
