import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

import psycopg
import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from psycopg.conninfo import make_conninfo
from psycopg.sql import SQL, Identifier

from retail_stream.aggregation import aggregate_orders, start_analytics
from retail_stream.cleanup import clean_parquet
from retail_stream.config import BOOTSTRAP_SERVERS, DATABASE_URL, WAREHOUSE_ID
from retail_stream.storage import initialize_database, write_metrics
from retail_stream.streaming import create_spark


@pytest.fixture
def database():
    name = f"test_{uuid4().hex}"
    with psycopg.connect(DATABASE_URL) as connection:
        connection.execute(SQL("CREATE SCHEMA {}").format(Identifier(name)))
    dsn = make_conninfo(DATABASE_URL, options=f"-c search_path={name}")
    try:
        initialize_database(dsn)
        yield dsn
    finally:
        with psycopg.connect(DATABASE_URL) as connection:
            connection.execute(SQL("DROP SCHEMA {} CASCADE").format(Identifier(name)))


def metrics(dsn):
    with psycopg.connect(dsn) as connection:
        return connection.execute(
            "SELECT window_start, warehouse_id, order_count, total_amount_cents, "
            "item_count, average_order_amount_cents FROM order_metrics "
            "ORDER BY window_start, warehouse_id"
        ).fetchall()


@pytest.mark.integration
def test_metrics_upsert_and_transaction_rollback(database):
    spark = create_spark()
    try:
        facts = spark.createDataFrame(
            [
                (datetime(2026, 1, 1, 0, 0, 10), "WH-PARIS", 1000, 1),
                (datetime(2026, 1, 1, 0, 0, 50), "WH-PARIS", 3000, 2),
                (datetime(2026, 1, 1, 0, 1, 0), "WH-PARIS", 500, 1),
                (datetime(2026, 1, 1, 0, 0, 30), WAREHOUSE_ID, 2000, 1),
            ],
            "event_time timestamp, warehouse_id string, total_amount_cents long, item_count long",
        )
        frame = aggregate_orders(facts, "2 minutes")
        write_metrics(frame, 0, database)
        before = metrics(database)
        write_metrics(frame, 0, database)
        assert metrics(database) == before
        assert len(before) == 3
        assert before[1][1:] == ("WH-PARIS", 2, Decimal(4000), Decimal(3), Decimal("2000.00"))
        rows = frame.orderBy("window_start", "warehouse_id").collect()
        broken = [row.asDict() for row in rows]
        broken[0]["order_count"] = 99
        broken[-1]["order_count"] = -1
        with pytest.raises(psycopg.errors.CheckViolation):
            write_metrics(spark.createDataFrame(broken, frame.schema), 1, database)
        assert metrics(database) == before
    finally:
        spark.stop()


@pytest.mark.integration
def test_streaming_metrics_duplicates_lateness_and_replay(database, tmp_path, monkeypatch):
    topic = f"retail.test.{uuid4().hex}.unique"
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    admin.create_topics([NewTopic(topic, 3, 1)])[topic].result(20)
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    failures = []
    spark = None
    queries = ()
    root = str(tmp_path / "analytics")

    def event(name, minute=0, second=10, amount=1000, qty=1, warehouse="WH-PARIS"):
        return {
            "event_id": name,
            "event_type": "order_created",
            "version": 2 if warehouse == WAREHOUSE_ID else 1,
            "event_time": datetime(2026, 1, 1, 0, minute, second, tzinfo=timezone.utc).isoformat(),
            "producer": "test",
            "payload": {
                "order_id": name,
                "customer_id": "CUST-001",
                "warehouse_id": warehouse,
                "currency": "EUR",
                "total_amount_cents": amount,
                "items": [{"sku": "SKU-001", "quantity": qty, "unit_price_cents": amount // qty}],
            },
        }

    def publish(*events):
        for value in events:
            producer.produce(
                topic,
                key=value["payload"]["order_id"],
                value=json.dumps(value),
                on_delivery=lambda error, msg: failures.append(error) if error else None,
            )
        assert producer.flush(20) == 0
        assert not failures

    def run():
        nonlocal queries
        queries = start_analytics(
            spark, BOOTSTRAP_SERVERS, topic, database, root, available_now=True
        )
        assert queries[1].awaitTermination(60)

    try:
        spark = create_spark()
        a = event("a")
        b = event("b", second=50, amount=3000, qty=2)
        c = event("c", warehouse=WAREHOUSE_ID, amount=2000)
        publish(a, a, b, c)
        run()
        assert metrics(database)[1][1:] == ("WH-PARIS", 2, Decimal(4000), Decimal(3), Decimal(2000))
        publish(a, event("late-admissible", second=15, amount=6000, qty=3))
        run()
        assert metrics(database)[1][1:] == (
            "WH-PARIS",
            3,
            Decimal(10000),
            Decimal(6),
            Decimal("3333.33"),
        )
        before_cleanup = metrics(database)
        removed, _ = clean_parquet(root, retention_hours=0, apply=True)
        assert removed > 0
        run()
        assert metrics(database) == before_cleanup
        original = write_metrics

        def fail_after_commit(batch, batch_id, database_url):
            original(batch, batch_id, database_url)
            raise RuntimeError("Simulated failure after PostgreSQL commit")

        publish(event("e", minute=1, amount=500))
        monkeypatch.setattr("retail_stream.aggregation.write_metrics", fail_after_commit)
        with pytest.raises(Exception, match="Simulated failure after PostgreSQL commit"):
            run()
        after_commit = metrics(database)
        assert len(after_commit) == 3
        spark.stop()
        spark = None
        monkeypatch.setattr("retail_stream.aggregation.write_metrics", original)
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "from retail_stream.aggregation import main; main()",
                "--available-now",
            ],
            env=dict(
                os.environ,
                DATABASE_URL=database,
                SPARK_ANALYTICS_PATH=root,
                KAFKA_UNIQUE_TOPIC=topic,
                KAFKA_BOOTSTRAP_SERVERS=BOOTSTRAP_SERVERS,
            ),
            capture_output=True,
            text=True,
            timeout=90,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert metrics(database) == after_commit
        spark = create_spark()
        publish(a, event("future", minute=10))
        run()
        before_late = metrics(database)
        publish(event("too-late", second=20, amount=9999))
        run()
        assert metrics(database) == before_late
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        if spark is not None:
            spark.stop()
        admin.delete_topics([topic])[topic].result(20)
