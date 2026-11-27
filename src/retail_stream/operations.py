import argparse
import json
import random
import time
from uuid import uuid4

from retail_stream.catalog import Product, connect, list_products, save_product, seed
from retail_stream.config import DATABASE_URL
from retail_stream.inventory import (
    finish_order,
    place_order,
    receive_replenishment,
    request_replenishment,
    tick,
)


def simulate(count=20, rate=2, seed_value=42, dsn=DATABASE_URL):
    if count < 0 or rate <= 0:
        raise ValueError("Nombre ou cadence invalide")
    rng = random.Random(seed_value)
    for _ in range(count):
        with connect(dsn) as conn:
            products = conn.execute("""SELECT p.sku FROM products p JOIN inventory i USING(sku)
                WHERE p.status='active' AND i.physical-i.reserved-i.safety_stock>0
                ORDER BY p.sku""").fetchall()
        if not products:
            print("Aucun produit disponible", flush=True)
            return
        chosen = rng.sample(products, k=min(len(products), rng.randint(1, 3)))
        result = place_order(
            str(uuid4()),
            "CUST-DEMO",
            [(p["sku"], rng.randint(1, 3)) for p in chosen],
            dsn,
            action=rng.choice(["ship", "ship", "ship", "cancel"]),
            action_delay=rng.randint(10, 30),
        )
        print(f"{result['order_id']} : {result['status']}", flush=True)
        time.sleep(1 / rate)


def main():
    parser = argparse.ArgumentParser(description="Catalogue et opérations métier RetailStream.")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("init")
    sub.add_parser("products")
    sub.add_parser("stock")
    product = sub.add_parser("product")
    product.add_argument("file", help="Fiche produit RetailStream JSON")
    status = sub.add_parser("status")
    status.add_argument("sku")
    status.add_argument("status", choices=["active", "inactive", "discontinued"])
    order = sub.add_parser("order")
    order.add_argument("--item", action="append", required=True, help="SKU:quantité")
    order.add_argument("--request-id", required=True)
    order.add_argument("--customer", default="CUST-DEMO")
    order.add_argument("--ttl", type=int, default=120)
    for action in ("ship", "cancel"):
        command = sub.add_parser(action)
        command.add_argument("order_id")
    replenish = sub.add_parser("replenish")
    replenish.add_argument("sku")
    replenish.add_argument("quantity", type=int)
    replenish.add_argument("--request-id", required=True)
    replenish.add_argument("--delay", type=int, default=30)
    receive = sub.add_parser("receive")
    receive.add_argument("replenishment_id")
    worker = sub.add_parser("worker")
    worker.add_argument("--once", action="store_true")
    worker.add_argument("--auto-replenish", action="store_true")
    sim = sub.add_parser("simulate")
    sim.add_argument("--count", type=int, default=20)
    sim.add_argument("--rate", type=float, default=2)
    sim.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    try:
        if args.command == "init":
            seed()
            print("Catalogue et stocks initialisés sans remise à zéro.")
        elif args.command == "products":
            print(json.dumps(list_products(), default=str, ensure_ascii=False, indent=2))
        elif args.command == "stock":
            with connect() as conn:
                rows = conn.execute(
                    "SELECT *,physical-reserved AS available FROM inventory ORDER BY sku"
                ).fetchall()
            print(json.dumps(rows, default=str, indent=2))
        elif args.command == "product":
            with open(args.file) as handle:
                save_product(Product(**json.load(handle)))
        elif args.command == "status":
            with connect() as conn:
                cursor = conn.execute(
                    "UPDATE products SET status=%s,version=version+1,updated_at=now() WHERE sku=%s",
                    (args.status, args.sku),
                )
                if not cursor.rowcount:
                    raise ValueError("Produit inconnu")
        elif args.command == "order":
            items = [(sku, int(qty)) for sku, qty in (s.rsplit(":", 1) for s in args.item)]
            print(
                json.dumps(
                    place_order(args.request_id, args.customer, items, ttl_seconds=args.ttl),
                    default=str,
                )
            )
        elif args.command in ("ship", "cancel"):
            print(json.dumps(finish_order(args.order_id, args.command), default=str))
        elif args.command == "replenish":
            print(
                json.dumps(
                    request_replenishment(
                        args.request_id, args.sku, args.quantity, delay_seconds=args.delay
                    ),
                    default=str,
                )
            )
        elif args.command == "receive":
            print(json.dumps(receive_replenishment(args.replenishment_id), default=str))
        elif args.command == "simulate":
            simulate(args.count, args.rate, args.seed)
        elif args.command == "worker":
            while True:
                tick(auto_replenish=args.auto_replenish)
                if args.once:
                    break
                time.sleep(1)
    except KeyboardInterrupt:
        pass
    except (ValueError, TypeError) as error:
        parser.exit(1, f"{error}\n")
