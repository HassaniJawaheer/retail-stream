from datetime import datetime, timezone
from importlib.resources import files

import psycopg
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

UPSERT = """
INSERT INTO order_metrics (
    window_start, window_end, warehouse_id, order_count,
    total_amount_cents, item_count, average_order_amount_cents
) VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (window_start, window_end, warehouse_id) DO UPDATE SET
    order_count = EXCLUDED.order_count,
    total_amount_cents = EXCLUDED.total_amount_cents,
    item_count = EXCLUDED.item_count,
    average_order_amount_cents = EXCLUDED.average_order_amount_cents
"""


def initialize_database(database_url: str):
    with psycopg.connect(database_url, connect_timeout=10) as connection:
        connection.execute(files("retail_stream").joinpath("schema.sql").read_text())


def write_metrics(batch: DataFrame, batch_id: int, database_url: str):
    rows = batch.select(
        F.col("window_start").cast("long").alias("start_epoch"),
        F.col("window_end").cast("long").alias("end_epoch"),
        "warehouse_id",
        "order_count",
        "total_amount_cents",
        "item_count",
        "average_order_amount_cents",
    ).toLocalIterator()
    with psycopg.connect(database_url, connect_timeout=10) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                UPSERT,
                (
                    (
                        datetime.fromtimestamp(row.start_epoch, timezone.utc),
                        datetime.fromtimestamp(row.end_epoch, timezone.utc),
                        row.warehouse_id,
                        row.order_count,
                        row.total_amount_cents,
                        row.item_count,
                        row.average_order_amount_cents,
                    )
                    for row in rows
                ),
            )
