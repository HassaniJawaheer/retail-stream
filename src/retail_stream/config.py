import os

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
ORDERS_TOPIC = os.getenv("KAFKA_ORDERS_TOPIC", "retail.orders.raw")
CHECKPOINT = os.getenv("SPARK_CHECKPOINT", ".state/checkpoints/orders-validation-v1")

VALID_TOPIC = os.getenv("KAFKA_VALID_TOPIC", "retail.orders.valid")
INVALID_TOPIC = os.getenv("KAFKA_INVALID_TOPIC", "retail.orders.invalid")

UNIQUE_TOPIC = os.getenv("KAFKA_UNIQUE_TOPIC", "retail.orders.unique")
DEDUP_CHECKPOINT = os.getenv("SPARK_DEDUP_CHECKPOINT", ".state/checkpoints/orders-dedup-v1")
DEDUP_WATERMARK = os.getenv("SPARK_DEDUP_WATERMARK", "2 minutes")

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://retail:retail_local@127.0.0.1:5433/retail")
ANALYTICS_PATH = os.getenv("SPARK_ANALYTICS_PATH", ".state/analytics")
AGG_WATERMARK = os.getenv("SPARK_AGG_WATERMARK", "2 minutes")

WAREHOUSE_ID = "WH-CORBEIL-01"

INVENTORY_TOPIC = os.getenv("KAFKA_INVENTORY_TOPIC", "retail.inventory.events")
INVENTORY_INVALID_TOPIC = os.getenv("KAFKA_INVENTORY_INVALID_TOPIC", "retail.inventory.invalid")
BUSINESS_CHECKPOINT = os.getenv("SPARK_BUSINESS_CHECKPOINT", ".state/checkpoints/business-v1")
BUSINESS_VALIDATION_CHECKPOINT = os.getenv(
    "SPARK_BUSINESS_VALIDATION_CHECKPOINT",
    os.getenv("SPARK_CHECKPOINT", ".state/checkpoints/orders-validation-v3"),
)
