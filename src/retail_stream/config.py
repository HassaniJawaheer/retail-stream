import os

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
ORDERS_TOPIC = os.getenv("KAFKA_ORDERS_TOPIC", "retail.orders.raw")
CHECKPOINT = os.getenv("SPARK_CHECKPOINT", ".state/checkpoints/orders-console")
