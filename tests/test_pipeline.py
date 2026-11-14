from uuid import uuid4

import pytest
from confluent_kafka.admin import AdminClient, NewTopic

from retail_stream.config import BOOTSTRAP_SERVERS
from retail_stream.producer import publish_orders
from retail_stream.streaming import create_spark, read_orders


@pytest.mark.integration
def test_orders_travel_through_kafka_and_spark(tmp_path):
    topic = f"retail.test.{uuid4().hex}"
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)])[topic].result(20)
    spark = None
    try:
        assert publish_orders(BOOTSTRAP_SERVERS, topic, count=6, rate=100, seed=42) == 6
        spark = create_spark()
        orders = read_orders(spark, BOOTSTRAP_SERVERS, topic)
        query = (
            orders.writeStream.format("parquet")
            .option("path", str(tmp_path / "output"))
            .option("checkpointLocation", str(tmp_path / "checkpoint"))
            .trigger(availableNow=True)
            .start()
        )
        if not query.awaitTermination(90):
            query.stop()
            pytest.fail("Spark n'a pas terminé en 90 secondes")
        rows = spark.read.parquet(str(tmp_path / "output")).collect()
        assert len(rows) == 6
        assert len({row.event_id for row in rows}) == 6
        for row in rows:
            assert row.kafka_key == row.payload.order_id
            assert row.event_type == "order_created"
            assert row.event_time is not None
            assert row.partition in range(3)
            assert row.offset >= 0
            assert row.payload.total_amount_cents == sum(
                item.quantity * item.unit_price_cents for item in row.payload.items
            )
    finally:
        if spark is not None:
            spark.stop()
        admin.delete_topics([topic])[topic].result(20)
