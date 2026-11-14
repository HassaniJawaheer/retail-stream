import argparse
import math
import random
import time

from confluent_kafka import Producer

from retail_stream.config import BOOTSTRAP_SERVERS, ORDERS_TOPIC
from retail_stream.scenarios import SCENARIOS, make_message


def publish_orders(
    bootstrap_servers: str, topic: str, count: int, rate: float, seed: int, scenario: str = "normal"
) -> int:
    producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": True,
            "delivery.timeout.ms": 15000,
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
            key, value = make_message(rng, scenario, sent)
            deadline = time.monotonic() + 15
            while True:
                try:
                    producer.produce(topic, key=key, value=value, on_delivery=on_delivery)
                    break
                except BufferError:
                    producer.poll(0.1)
                    if errors or time.monotonic() >= deadline:
                        raise RuntimeError("File du producteur saturée ou livraison en échec")
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
    parser.add_argument("--scenario", choices=(*SCENARIOS, "mixed"), default="normal")
    args = parser.parse_args()
    if args.count < 0 or not math.isfinite(args.rate) or args.rate <= 0:
        parser.error("count doit être positif ou nul, rate doit être fini et strictement positif.")
    print(f"{publish_orders(BOOTSTRAP_SERVERS, ORDERS_TOPIC, **vars(args))} commandes livrées.")
