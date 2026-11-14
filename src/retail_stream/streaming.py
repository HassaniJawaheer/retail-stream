import argparse
import os
import sys

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

from retail_stream.config import BOOTSTRAP_SERVERS, CHECKPOINT, ORDERS_TOPIC

ITEM_SCHEMA = StructType(
    [
        StructField("sku", StringType()),
        StructField("quantity", IntegerType()),
        StructField("unit_price_cents", LongType()),
    ]
)
EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("event_time", TimestampType()),
        StructField("producer", StringType()),
        StructField("version", IntegerType()),
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


def main():
    parser = argparse.ArgumentParser(description="Lire les commandes Kafka avec Spark.")
    parser.add_argument(
        "--available-now", action="store_true", help="Lire le stock actuel puis finir."
    )
    args = parser.parse_args()
    spark = create_spark()
    try:
        orders = read_orders(spark, BOOTSTRAP_SERVERS, ORDERS_TOPIC)
        writer = (
            orders.select(
                "event_id",
                "event_type",
                "event_time",
                "payload.order_id",
                "payload.warehouse_id",
                "payload.total_amount_cents",
                "partition",
                "offset",
            )
            .writeStream.format("console")
            .option("truncate", "false")
            .option("numRows", 20)
            .option("checkpointLocation", CHECKPOINT)
            .outputMode("append")
        )
        if args.available_now:
            writer = writer.trigger(availableNow=True)
        else:
            writer = writer.trigger(processingTime="5 seconds")
        writer.start().awaitTermination()
    except KeyboardInterrupt:
        pass
    finally:
        spark.stop()
