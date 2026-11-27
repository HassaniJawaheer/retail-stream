import argparse
import os
import signal
import sys
from threading import Event

from pyspark import __version__
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from retail_stream.config import (
    BOOTSTRAP_SERVERS,
    BUSINESS_VALIDATION_CHECKPOINT,
    DATABASE_URL,
    INVALID_TOPIC,
    ORDERS_TOPIC,
    VALID_TOPIC,
)
from retail_stream.validation import route_orders, validate_orders

ITEM_SCHEMA = StructType(
    [
        StructField("sku", StringType()),
        StructField("quantity", IntegerType()),
        StructField("unit_price_cents", LongType()),
        StructField("product_version", LongType()),
    ]
)
EVENT_SCHEMA = StructType(
    [
        StructField("_corrupt_record", StringType()),
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("event_time", TimestampType()),
        StructField("producer", StringType()),
        StructField("version", IntegerType()),
        StructField("entity_version", LongType()),
        StructField(
            "payload",
            StructType(
                [
                    StructField("order_id", StringType()),
                    StructField("customer_id", StringType()),
                    StructField("warehouse_id", StringType()),
                    StructField("currency", StringType()),
                    StructField("items", ArrayType(ITEM_SCHEMA)),
                    StructField("total_amount_cents", LongType()),
                    StructField("status", StringType()),
                    StructField("reason", StringType()),
                    StructField("sku", StringType()),
                    StructField("operation_id", StringType()),
                    StructField("replenishment_id", StringType()),
                    StructField("quantity", LongType()),
                    StructField("physical", LongType()),
                    StructField("reserved", LongType()),
                    StructField("available", LongType()),
                    StructField("physical_delta", LongType()),
                    StructField("reserved_delta", LongType()),
                    StructField("stock_version", LongType()),
                ]
            ),
        ),
    ]
)


def create_spark() -> SparkSession:
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("RetailStream")
        .config("spark.jars.packages", f"org.apache.spark:spark-sql-kafka-0-10_2.13:{__version__}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "3")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def parse_orders(raw: DataFrame) -> DataFrame:
    return raw.select(
        col("key").cast("string").alias("kafka_key"),
        "topic",
        "partition",
        "offset",
        col("timestamp").alias("kafka_timestamp"),
        col("value").cast("string").alias("raw_value"),
        from_json(col("value").cast("string"), EVENT_SCHEMA).alias("event"),
    ).select("kafka_key", "topic", "partition", "offset", "kafka_timestamp", "raw_value", "event.*")


def read_orders(spark: SparkSession, bootstrap_servers: str, topic: str) -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 1000)
        .load()
    )
    return parse_orders(raw)


def start_validation(
    spark,
    bootstrap_servers,
    topic,
    valid_topic,
    invalid_topic,
    checkpoint,
    available_now=False,
    database_url=DATABASE_URL,
):
    if len({topic, valid_topic, invalid_topic}) != 3:
        raise ValueError("Les topics source, valide et invalide doivent être distincts")
    from retail_stream.catalog import list_products

    def process(batch, batch_id):
        known = None
        if not batch.filter(col("version") == 3).isEmpty():
            known = [row["sku"] for row in list_products(database_url)]
        routed = route_orders(validate_orders(batch, known), valid_topic, invalid_topic)
        routed.write.format("kafka").option("kafka.bootstrap.servers", bootstrap_servers).save()

    writer = (
        read_orders(spark, bootstrap_servers, topic)
        .writeStream.foreachBatch(process)
        .option("checkpointLocation", checkpoint)
        .queryName("orders-validation")
        .outputMode("append")
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime="5 seconds")
    return writer.start()


def main():
    parser = argparse.ArgumentParser(description="Valider les commandes et les router dans Kafka.")
    parser.add_argument(
        "--available-now",
        action="store_true",
        help="Traiter les messages disponibles puis quitter.",
    )
    args = parser.parse_args()
    spark = create_spark()
    stopping = Event()
    previous = {}
    query = None
    last_batch = None
    for sig in (signal.SIGINT, signal.SIGTERM):
        previous[sig] = signal.signal(sig, lambda signum, frame: stopping.set())
    try:
        query = start_validation(
            spark,
            BOOTSTRAP_SERVERS,
            ORDERS_TOPIC,
            VALID_TOPIC,
            INVALID_TOPIC,
            BUSINESS_VALIDATION_CHECKPOINT,
            args.available_now,
        )
        print(f"Validation active : {ORDERS_TOPIC} → {VALID_TOPIC} / {INVALID_TOPIC}", flush=True)
        while not stopping.is_set():
            finished = query.awaitTermination(1)
            progress = query.lastProgress
            if progress and progress["batchId"] != last_batch:
                last_batch = progress["batchId"]
                print(
                    f"Lot {last_batch} terminé : {progress['numInputRows']} messages lus",
                    flush=True,
                )
            if finished:
                break
    finally:
        try:
            if query is not None and query.isActive:
                query.stop()
        finally:
            spark.stop()
            for sig, handler in previous.items():
                signal.signal(sig, handler)
