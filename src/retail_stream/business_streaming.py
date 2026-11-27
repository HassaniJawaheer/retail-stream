import argparse
import json
import signal
from threading import Event

from psycopg.types.json import Jsonb
from pyspark.sql import functions as F

from retail_stream.catalog import connect, list_products, migrate
from retail_stream.config import (
    BOOTSTRAP_SERVERS,
    BUSINESS_CHECKPOINT,
    DATABASE_URL,
    INVENTORY_INVALID_TOPIC,
    INVENTORY_TOPIC,
    VALID_TOPIC,
    WAREHOUSE_ID,
)
from retail_stream.inventory import ORDER_STATES
from retail_stream.streaming import create_spark, read_orders
from retail_stream.validation import route_orders, validate_orders


def validate_inventory(frame, skus):
    p = F.col("payload")
    change = F.col("event_type") == "inventory_changed"
    uuid_pattern = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
    rules = {
        "INVALID_JSON": F.col("_corrupt_record").isNull() & F.col("raw_value").isNotNull(),
        "INVALID_EVENT_ID": F.col("event_id").rlike(uuid_pattern),
        "INVALID_TIME": F.col("event_time").isNotNull(),
        "INVALID_PRODUCER": F.length(F.trim(F.col("producer"))) > 0,
        "UNSUPPORTED_VERSION": F.col("version") == 1,
        "UNSUPPORTED_EVENT_TYPE": F.col("event_type").isin(
            "inventory_changed", "replenishment_requested", "replenishment_received"
        ),
        "UNKNOWN_PRODUCT": p.sku.isin(*skus),
        "UNKNOWN_WAREHOUSE": p.warehouse_id == WAREHOUSE_ID,
        "KEY_MISMATCH": F.col("kafka_key") == F.concat_ws(":", p.warehouse_id, p.sku),
        "INVALID_STOCK": ~change
        | (
            (p.physical >= 0)
            & (p.reserved >= 0)
            & (p.reserved <= p.physical)
            & (p.available == p.physical - p.reserved)
            & (p.stock_version > 0)
            & (F.col("entity_version") == p.stock_version)
            & p.physical_delta.isNotNull()
            & p.reserved_delta.isNotNull()
            & (F.length(p.operation_id) > 0)
            & p.reason.isin("initial", "reservation", "ship", "cancel", "expire", "receipt")
            & F.when(
                p.reason.isin("initial", "receipt"),
                (p.physical_delta >= 0) & (p.reserved_delta == 0),
            )
            .when(p.reason == "reservation", (p.physical_delta == 0) & (p.reserved_delta > 0))
            .when(
                p.reason == "ship", (p.physical_delta < 0) & (p.physical_delta == p.reserved_delta)
            )
            .otherwise((p.physical_delta == 0) & (p.reserved_delta < 0))
        ),
        "INVALID_REPLENISHMENT": change
        | (
            (p.quantity > 0)
            & p.replenishment_id.rlike(uuid_pattern)
            & (
                F.col("entity_version")
                == F.when(F.col("event_type") == "replenishment_requested", 1).otherwise(2)
            )
        ),
    }
    return frame.withColumn(
        "errors",
        F.filter(
            F.array(
                *[
                    F.when(~F.coalesce(condition, F.lit(False)), F.lit(code))
                    for code, condition in rules.items()
                ]
            ),
            lambda value: value.isNotNull(),
        ),
    )


def write_business_batch(batch, batch_id, dsn=DATABASE_URL):
    with connect(dsn) as conn:
        conn.execute("SELECT pg_advisory_xact_lock(hashtext(current_schema()),7086)")
        inserted = []
        for row in batch.select("event_id", "raw_value").toLocalIterator():
            event = json.loads(row.raw_value)
            fresh = conn.execute(
                "INSERT INTO analytics_events (event_id,event) VALUES (%s,%s) "
                "ON CONFLICT DO NOTHING RETURNING event_id",
                (row.event_id, Jsonb(event)),
            ).fetchone()
            if not fresh:
                previous = conn.execute(
                    "SELECT event FROM analytics_events WHERE event_id=%s", (row.event_id,)
                ).fetchone()["event"]
                if previous != event:
                    raise ValueError(f"Identifiant événement contradictoire : {row.event_id}")
                continue
            inserted.append(row.event_id)
            p = event["payload"]
            kind = event["event_type"]
            if kind == "inventory_changed":
                conn.execute(
                    """INSERT INTO inventory_projection VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (warehouse_id,sku) DO UPDATE SET physical=EXCLUDED.physical,
                    reserved=EXCLUDED.reserved,available=EXCLUDED.available,
                    stock_version=EXCLUDED.stock_version,event_time=EXCLUDED.event_time
                    WHERE inventory_projection.stock_version<EXCLUDED.stock_version""",
                    (
                        p["warehouse_id"],
                        p["sku"],
                        p["physical"],
                        p["reserved"],
                        p["available"],
                        p["stock_version"],
                        event["event_time"],
                    ),
                )
            elif kind in ORDER_STATES:
                conn.execute(
                    """INSERT INTO order_projection VALUES (%s,%s,%s,%s,%s)
                    ON CONFLICT (order_id) DO UPDATE SET
                    status=EXCLUDED.status,reason=EXCLUDED.reason,
                    entity_version=EXCLUDED.entity_version,event_time=EXCLUDED.event_time
                    WHERE order_projection.entity_version<EXCLUDED.entity_version""",
                    (
                        p["order_id"],
                        p["status"],
                        p.get("reason"),
                        event["entity_version"],
                        event["event_time"],
                    ),
                )
            elif kind.startswith("replenishment_"):
                conn.execute(
                    """INSERT INTO replenishment_projection VALUES (%s,%s,%s,%s,%s)
                    ON CONFLICT (replenishment_id) DO UPDATE SET
                    requested_at=COALESCE(replenishment_projection.requested_at,EXCLUDED.requested_at),
                    received_at=COALESCE(replenishment_projection.received_at,EXCLUDED.received_at)""",
                    (
                        p["replenishment_id"],
                        p["sku"],
                        p["quantity"],
                        event["event_time"] if kind.endswith("requested") else None,
                        event["event_time"] if kind.endswith("received") else None,
                    ),
                )
        if not inserted:
            return
        fresh = batch.filter(F.col("event_id").isin(*inserted)).dropDuplicates(["event_id"])
        orders = fresh.filter(F.col("event_type").isin(*ORDER_STATES))
        metrics = orders.groupBy(
            F.window("event_time", "1 minute").alias("minute"),
            "event_type",
            F.coalesce("payload.reason", F.lit("")).alias("reason"),
        ).agg(
            F.count("*").alias("n"),
            F.sum(F.col("payload.total_amount_cents").cast("decimal(38,0)")).alias("amount"),
        )
        for row in metrics.select(
            F.col("minute.start").cast("long").alias("epoch"), "event_type", "reason", "n", "amount"
        ).toLocalIterator():
            conn.execute(
                """INSERT INTO business_metrics VALUES (to_timestamp(%s),%s,%s,%s,%s)
                ON CONFLICT (window_start,event_type,reason) DO UPDATE SET
                event_count=business_metrics.event_count+EXCLUDED.event_count,
                amount_cents=business_metrics.amount_cents+EXCLUDED.amount_cents""",
                (row.epoch, row.event_type, row.reason, row.n, row.amount),
            )
        movements = (
            fresh.filter(F.col("event_type") == "inventory_changed")
            .groupBy(
                F.window("event_time", "1 minute").alias("minute"), "payload.sku", "payload.reason"
            )
            .agg(
                F.count("*").alias("n"),
                F.sum("payload.physical_delta").alias("physical"),
                F.sum("payload.reserved_delta").alias("reserved"),
            )
        )
        for row in movements.select(
            F.col("minute.start").cast("long").alias("epoch"),
            "sku",
            "reason",
            "n",
            "physical",
            "reserved",
        ).toLocalIterator():
            conn.execute(
                """INSERT INTO movement_metrics VALUES (to_timestamp(%s),%s,%s,%s,%s,%s)
                ON CONFLICT (window_start,sku,reason) DO UPDATE SET
                movement_count=movement_metrics.movement_count+EXCLUDED.movement_count,
                physical_delta=movement_metrics.physical_delta+EXCLUDED.physical_delta,
                reserved_delta=movement_metrics.reserved_delta+EXCLUDED.reserved_delta""",
                (row.epoch, row.sku, row.reason, row.n, row.physical, row.reserved),
            )


def start_business(
    spark,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    dsn=DATABASE_URL,
    checkpoint=BUSINESS_CHECKPOINT,
    orders_topic=VALID_TOPIC,
    inventory_topic=INVENTORY_TOPIC,
    invalid_topic=INVENTORY_INVALID_TOPIC,
    available_now=False,
):
    if len({orders_topic, inventory_topic, invalid_topic}) != 3:
        raise ValueError("Les topics doivent être distincts")
    migrate(dsn)
    source = read_orders(spark, bootstrap_servers, f"{orders_topic},{inventory_topic}")

    def process(batch, batch_id):
        skus = [p["sku"] for p in list_products(dsn)]
        inventory = validate_inventory(batch.filter(F.col("topic") == inventory_topic), skus)
        orders = validate_orders(
            batch.filter((F.col("topic") == orders_topic) & (F.col("version") == 3)), skus
        )
        checked = inventory.unionByName(orders)
        checked.persist()
        try:
            bad = checked.filter(F.size("errors") > 0)
            if not bad.isEmpty():
                route_orders(bad, invalid_topic, invalid_topic).write.format("kafka").option(
                    "kafka.bootstrap.servers", bootstrap_servers
                ).save()
            write_business_batch(checked.filter(F.size("errors") == 0), batch_id, dsn)
        finally:
            checked.unpersist()

    writer = source.writeStream.foreachBatch(process).option("checkpointLocation", checkpoint)
    if available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime="5 seconds")
    return writer.queryName("retail-business").start()


def main():
    parser = argparse.ArgumentParser(description="Analyser les commandes et les stocks métier.")
    parser.add_argument("--available-now", action="store_true")
    args = parser.parse_args()
    spark = create_spark()
    query = None
    stopping = Event()
    previous = {
        sig: signal.signal(sig, lambda signum, frame: stopping.set())
        for sig in (signal.SIGINT, signal.SIGTERM)
    }
    try:
        query = start_business(spark, available_now=args.available_now)
        print("Analyse métier active : commandes et stocks → PostgreSQL", flush=True)
        while not stopping.is_set():
            if query.awaitTermination(1):
                break
    finally:
        if query is not None and query.isActive:
            query.stop()
        spark.stop()
        for sig, handler in previous.items():
            signal.signal(sig, handler)
