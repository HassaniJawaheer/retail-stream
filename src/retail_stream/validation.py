from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from retail_stream.config import WAREHOUSE_ID
from retail_stream.events import CATALOG


def validate_orders(orders: DataFrame, known_skus=None) -> DataFrame:
    def present(name):
        return F.length(F.trim(F.col(name))) > 0

    lifecycle = {
        "order_created": "created",
        "order_reserved": "reserved",
        "order_rejected": "rejected",
        "order_cancelled": "cancelled",
        "reservation_expired": "expired",
        "order_shipped": "shipped",
    }
    modern = F.col("version") == 3
    legacy = F.col("version").isin(1, 2)
    identifiers = [sku for sku, _ in CATALOG]
    known_skus = identifiers if known_skus is None else known_skus
    expected_status = F.create_map(*[F.lit(v) for pair in lifecycle.items() for v in pair])
    uuid_pattern = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
    items = F.col("payload.items")
    total = F.aggregate(
        items,
        F.lit(0).cast("decimal(38,0)"),
        lambda acc, item: (acc + item.quantity.cast("decimal(38,0)") * item.unit_price_cents).cast(
            "decimal(38,0)"
        ),
    )
    rules = {
        "INVALID_JSON_OR_TYPE": F.col("_corrupt_record").isNull() & F.col("raw_value").isNotNull(),
        "MISSING_EVENT_ID": present("event_id"),
        "UNSUPPORTED_EVENT_TYPE": (legacy & (F.col("event_type") == "order_created"))
        | (modern & F.col("event_type").isin(*lifecycle)),
        "UNSUPPORTED_VERSION": F.col("version").isin(1, 2, 3),
        "INVALID_EVENT_TIME": F.col("event_time").isNotNull(),
        "MISSING_PRODUCER": present("producer"),
        "MISSING_ORDER_ID": present("payload.order_id"),
        "MISSING_CUSTOMER_ID": present("payload.customer_id"),
        "UNKNOWN_WAREHOUSE": (
            (F.col("version") == 1) & F.col("payload.warehouse_id").isin("WH-PARIS", "WH-LYON")
        )
        | ((F.col("version") == 2) & (F.col("payload.warehouse_id") == WAREHOUSE_ID))
        | (
            modern
            & F.when(
                F.col("event_type").isin("order_created", "order_rejected"),
                F.col("payload.warehouse_id").isNull(),
            ).otherwise(F.col("payload.warehouse_id") == WAREHOUSE_ID)
        ),
        "INVALID_LIFECYCLE": ~modern
        | (
            (F.col("payload.status") == expected_status[F.col("event_type")])
            & (F.col("entity_version") > 0)
            & F.col("event_id").rlike(uuid_pattern)
            & F.col("payload.order_id").rlike(uuid_pattern)
            & F.when(F.col("event_type") == "order_rejected", present("payload.reason")).otherwise(
                F.lit(True)
            )
        ),
        "UNSUPPORTED_CURRENCY": F.col("payload.currency") == "EUR",
        "KEY_MISMATCH": F.col("kafka_key") == F.col("payload.order_id"),
        "EMPTY_ITEMS": F.size(items) > 0,
        "INVALID_ITEM": F.forall(
            items,
            lambda item: item.isNotNull()
            & (F.length(F.trim(item.sku)) > 0)
            & (item.quantity > 0)
            & (item.unit_price_cents >= 0),
        ),
        "INVALID_PRODUCT_VERSION": ~modern
        | F.forall(items, lambda item: (item.product_version > 0) & (item.unit_price_cents > 0)),
        "DUPLICATE_PRODUCT": ~modern
        | (F.size(F.array_distinct(F.transform(items, lambda item: item.sku))) == F.size(items)),
        "UNKNOWN_PRODUCT": F.forall(
            items,
            lambda item: F.when(modern, item.sku.isin(*known_skus)).otherwise(
                item.sku.isin(*identifiers)
            ),
        ),
        "TOTAL_MISMATCH": (F.col("payload.total_amount_cents") >= 0)
        & (F.col("payload.total_amount_cents") == total),
    }
    errors = F.array(
        *[
            F.when(~F.coalesce(condition, F.lit(False)), F.lit(code))
            for code, condition in rules.items()
        ]
    )
    return orders.withColumn("errors", F.filter(errors, lambda error: error.isNotNull()))


def route_orders(orders: DataFrame, valid_topic: str, invalid_topic: str) -> DataFrame:
    valid = F.size("errors") == 0
    source = F.struct("topic", "partition", "offset", "kafka_timestamp")
    rejection = F.to_json(
        F.struct(
            "raw_value", "errors", source.alias("source"), F.lit(1).alias("validation_version")
        )
    )
    return orders.select(
        F.when(valid, F.lit(valid_topic)).otherwise(F.lit(invalid_topic)).alias("topic"),
        F.when(valid, F.col("kafka_key"))
        .otherwise(
            F.concat_ws(
                ":", "topic", F.col("partition").cast("string"), F.col("offset").cast("string")
            )
        )
        .alias("key"),
        F.when(valid, F.col("raw_value")).otherwise(rejection).alias("value"),
    )
