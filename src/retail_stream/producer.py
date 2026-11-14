import argparse
import json
import math
import random
import time

from confluent_kafka import Producer

from retail_stream.config import BOOTSTRAP_SERVERS, ORDERS_TOPIC
from retail_stream.events import create_order


def publish_orders(bootstrap_servers: str, topic: str, count: int, rate: float, seed: int) -> int:
    producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": True,
            "delivery.timeout.ms": 15000,
            "allow.auto.create.topics": False,
        }
    )
    rng = random.Random(seed)
    errors = []
    delivered = 0

    def on_delivery(error, message):
        nonlocal delivered
        if error:
            errors.append(str(error))
        else:
            delivered += 1
            print(
                f"Livré : {message.key().decode()} "
                f"partition={message.partition()} offset={message.offset()}",
                flush=True,
            )

    sent = 0
    try:
        while count == 0 or sent < count:
            if errors:
                break
            event = create_order(rng)
            producer.produce(
                topic,
                key=event["payload"]["order_id"].encode(),
                value=json.dumps(event).encode(),
                on_delivery=on_delivery,
            )
            sent += 1
            producer.poll(0)
            time.sleep(1 / rate)
    except KeyboardInterrupt:
        pass
    finally:
        remaining = producer.flush(20)
    if errors or remaining:
        raise RuntimeError(f"Échec de livraison : {errors}, messages en attente : {remaining}")
    return delivered


def main():
    parser = argparse.ArgumentParser(description="Produire des commandes fictives dans Kafka.")
    parser.add_argument(
        "--count", type=int, default=20, help="Nombre de commandes, 0 pour continuer."
    )
    parser.add_argument("--rate", type=float, default=2, help="Commandes par seconde.")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    if args.count < 0 or not math.isfinite(args.rate) or args.rate <= 0:
        parser.error("count doit être positif ou nul, rate doit être fini et strictement positif.")
    print(f"{publish_orders(BOOTSTRAP_SERVERS, ORDERS_TOPIC, **vars(args))} commandes livrées.")
