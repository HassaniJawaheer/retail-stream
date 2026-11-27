import argparse
import json
import time

from confluent_kafka import Producer

from retail_stream.catalog import connect
from retail_stream.config import BOOTSTRAP_SERVERS, DATABASE_URL


def publish_pending(
    dsn=DATABASE_URL, bootstrap_servers=BOOTSTRAP_SERVERS, limit=100, producer=None
):
    producer = producer or Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": True,
            "delivery.timeout.ms": 15000,
            "allow.auto.create.topics": False,
        }
    )
    count = 0
    failure = None
    with connect(dsn) as conn:
        locked = conn.execute(
            "SELECT pg_try_advisory_xact_lock(hashtext(current_schema()),7085) AS locked"
        ).fetchone()["locked"]
        if not locked:
            raise RuntimeError("Un relais outbox est déjà actif")
        rows = conn.execute(
            "SELECT * FROM outbox WHERE published_at IS NULL ORDER BY sequence LIMIT %s FOR UPDATE",
            (limit,),
        ).fetchall()
        for row in rows:
            delivered = []
            try:
                producer.produce(
                    row["topic"],
                    key=row["message_key"],
                    value=json.dumps(row["event"]),
                    on_delivery=lambda error, message: delivered.append(error),
                )
                remaining = producer.flush(20)
                if remaining or len(delivered) != 1 or delivered[0] is not None:
                    raise RuntimeError(f"Livraison non confirmée : {delivered}")
            except Exception as error:
                failure = str(error)
                conn.execute(
                    "UPDATE outbox SET attempts=attempts+1,last_error=%s WHERE sequence=%s",
                    (failure, row["sequence"]),
                )
                break
            conn.execute(
                "UPDATE outbox SET published_at=now(),attempts=attempts+1,last_error=NULL "
                "WHERE sequence=%s",
                (row["sequence"],),
            )
            count += 1
    if failure:
        raise RuntimeError(failure)
    return count


def main():
    parser = argparse.ArgumentParser(description="Publier les événements métier enregistrés.")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    try:
        while True:
            try:
                count = publish_pending()
                if count:
                    print(f"{count} événements publiés", flush=True)
            except RuntimeError as error:
                if args.once:
                    parser.exit(1, f"{error}\n")
                print(f"Publication différée : {error}", flush=True)
                count = 0
            if args.once and count == 0:
                break
            time.sleep(1)
    except KeyboardInterrupt:
        pass
