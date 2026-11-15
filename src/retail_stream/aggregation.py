import argparse
import signal
from pathlib import Path
from threading import Event

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from retail_stream.config import (
    AGG_WATERMARK,
    ANALYTICS_PATH,
    BOOTSTRAP_SERVERS,
    DATABASE_URL,
    UNIQUE_TOPIC,
)
from retail_stream.deduplication import deduplicate_orders
from retail_stream.storage import initialize_database, write_metrics
from retail_stream.streaming import create_spark, read_orders

ORDER_FACT_SCHEMA = StructType(
    [
        StructField("event_time", TimestampType()),
        StructField("warehouse_id", StringType()),
        StructField("total_amount_cents", LongType()),
        StructField("item_count", LongType()),
    ]
)


def order_facts(orders: DataFrame) -> DataFrame:
    return orders.select(
        "event_time",
        "payload.warehouse_id",
        "payload.total_amount_cents",
        F.aggregate(
            "payload.items", F.lit(0).cast("long"), lambda total, item: total + item.quantity
        ).alias("item_count"),
    )


def aggregate_orders(facts: DataFrame, watermark: str) -> DataFrame:
    return (
        facts.withWatermark("event_time", watermark)
        .groupBy(F.window("event_time", "1 minute"), "warehouse_id")
        .agg(
            F.count("*").alias("order_count"),
            F.sum(F.col("total_amount_cents").cast("decimal(38,0)")).alias("total_amount_cents"),
            F.sum(F.col("item_count").cast("decimal(38,0)")).alias("item_count"),
            F.avg(F.col("total_amount_cents").cast("decimal(38,2)"))
            .cast("decimal(38,2)")
            .alias("average_order_amount_cents"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "warehouse_id",
            "order_count",
            "total_amount_cents",
            "item_count",
            "average_order_amount_cents",
        )
    )


def start_analytics(
    spark, bootstrap_servers, topic, database_url, path, watermark="2 minutes", available_now=False
):
    root = Path(path).resolve()
    staging = root / "orders"
    staging.mkdir(parents=True, exist_ok=True)
    initialize_database(database_url)
    orders = read_orders(spark, bootstrap_servers, topic)
    dedup_writer = (
        order_facts(deduplicate_orders(orders, watermark))
        .writeStream.format("parquet")
        .option("path", str(staging))
        .option("checkpointLocation", str(root / "dedup-checkpoint"))
        .queryName("analytics-deduplication")
        .outputMode("append")
    )
    facts = spark.readStream.schema(ORDER_FACT_SCHEMA).format("parquet").load(str(staging))
    metrics_writer = (
        aggregate_orders(facts, watermark)
        .writeStream.foreachBatch(
            lambda batch, batch_id: write_metrics(batch, batch_id, database_url)
        )
        .option("checkpointLocation", str(root / "metrics-checkpoint"))
        .queryName("order-metrics")
        .outputMode("update")
    )
    dedup = None
    try:
        if available_now:
            dedup = dedup_writer.trigger(availableNow=True).start()
            dedup.awaitTermination()
            metrics = metrics_writer.trigger(availableNow=True).start()
        else:
            dedup = dedup_writer.trigger(processingTime="5 seconds").start()
            metrics = metrics_writer.trigger(processingTime="5 seconds").start()
        return dedup, metrics
    except BaseException:
        if dedup is not None and dedup.isActive:
            dedup.stop()
        raise


def main():
    parser = argparse.ArgumentParser(description="Agréger les commandes et écrire dans PostgreSQL.")
    parser.add_argument("--available-now", action="store_true")
    args = parser.parse_args()
    spark = create_spark()
    stopping = Event()
    previous = {}
    queries = ()
    try:
        queries = start_analytics(
            spark,
            BOOTSTRAP_SERVERS,
            UNIQUE_TOPIC,
            DATABASE_URL,
            ANALYTICS_PATH,
            AGG_WATERMARK,
            args.available_now,
        )
        for sig in (signal.SIGINT, signal.SIGTERM):
            previous[sig] = signal.signal(sig, lambda signum, frame: stopping.set())
        print("Agrégations actives : commandes par minute et entrepôt → PostgreSQL", flush=True)
        while not stopping.is_set():
            for query in queries:
                query.awaitTermination(0.5)
            if not any(query.isActive for query in queries):
                break
    finally:
        try:
            for query in queries:
                if query.isActive:
                    query.stop()
        finally:
            spark.stop()
            for sig, handler in previous.items():
                signal.signal(sig, handler)
