import argparse
import signal
from threading import Event

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from retail_stream.config import (
    BOOTSTRAP_SERVERS,
    DEDUP_CHECKPOINT,
    DEDUP_WATERMARK,
    INVALID_TOPIC,
    ORDERS_TOPIC,
    UNIQUE_TOPIC,
    VALID_TOPIC,
)
from retail_stream.streaming import create_spark, read_orders


def deduplicate_orders(orders: DataFrame, watermark: str) -> DataFrame:
    return orders.withWatermark("event_time", watermark).dropDuplicatesWithinWatermark(["event_id"])


def start_deduplication(
    spark,
    bootstrap_servers,
    source_topic,
    output_topic,
    checkpoint,
    watermark="2 minutes",
    available_now=False,
):
    if source_topic == output_topic:
        raise ValueError("Les topics source et sortie doivent être distincts")
    orders = read_orders(spark, bootstrap_servers, source_topic)
    writer = (
        deduplicate_orders(orders, watermark)
        .select(col("kafka_key").alias("key"), col("raw_value").alias("value"))
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("topic", output_topic)
        .option("checkpointLocation", checkpoint)
        .queryName("orders-deduplication")
        .outputMode("append")
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime="5 seconds")
    return writer.start()


def main():
    parser = argparse.ArgumentParser(description="Dédupliquer les événements valides par event_id.")
    parser.add_argument("--available-now", action="store_true")
    args = parser.parse_args()
    if UNIQUE_TOPIC in {ORDERS_TOPIC, VALID_TOPIC, INVALID_TOPIC}:
        parser.error("Le topic de sortie doit être distinct des topics raw, valid et invalid.")
    spark = create_spark()
    stopping = Event()
    previous = {}
    query = None
    last_batch = None
    for sig in (signal.SIGINT, signal.SIGTERM):
        previous[sig] = signal.signal(sig, lambda signum, frame: stopping.set())
    try:
        query = start_deduplication(
            spark,
            BOOTSTRAP_SERVERS,
            VALID_TOPIC,
            UNIQUE_TOPIC,
            DEDUP_CHECKPOINT,
            DEDUP_WATERMARK,
            args.available_now,
        )
        print(f"Déduplication active : {VALID_TOPIC} → {UNIQUE_TOPIC}", flush=True)
        while not stopping.is_set():
            finished = query.awaitTermination(1)
            progress = query.lastProgress
            if progress and progress["batchId"] != last_batch:
                last_batch = progress["batchId"]
                state = progress["stateOperators"][0]
                duplicates = state["customMetrics"].get("numDroppedDuplicateRows", 0)
                print(
                    f"Lot {last_batch} : {progress['numInputRows']} lus, "
                    f"{duplicates} doublons écartés, "
                    f"{state['numRowsDroppedByWatermark']} trop anciens, "
                    f"watermark={progress['eventTime'].get('watermark')}",
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
