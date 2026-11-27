from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

import pytest

from retail_stream.catalog import Product, connect, migrate, save_product, seed
from retail_stream.inventory import (
    finish_order,
    place_order,
    receive_replenishment,
    request_replenishment,
    tick,
)
from retail_stream.outbox import publish_pending

pytestmark = pytest.mark.integration


def stock(dsn, sku="SKU-001"):
    with connect(dsn) as conn:
        return conn.execute("SELECT * FROM inventory WHERE sku=%s", (sku,)).fetchone()


def test_seed_and_catalog_updates_preserve_stock(operations_database):
    order = place_order("one", "customer", [("SKU-001", 3)], operations_database)
    before = stock(operations_database)
    seed(operations_database)
    migrate(operations_database)
    assert stock(operations_database) == before
    product = Product("NEW-001", "Clavier", "Demo", "M1", "accessoires", "Clavier de test", 900)
    save_product(product, operations_database)
    save_product(replace(product, price_cents=1000, status="inactive"), operations_database)
    assert stock(operations_database, "NEW-001")["physical"] == 0
    with connect(operations_database) as conn:
        conn.execute("UPDATE products SET status='discontinued' WHERE sku='SKU-001'")
    rejected = place_order("two", "customer", [("SKU-001", 1)], operations_database)
    assert rejected["reason"] == "PRODUCT_NOT_SELLABLE"
    finish_order(order["order_id"], "ship", operations_database)
    assert stock(operations_database)["physical"] == 17
    assert stock(operations_database)["reserved"] == 0


def test_atomic_reservation_retries_and_concurrency(operations_database):
    first = place_order("first", "c", [("SKU-001", 3)], operations_database)
    assert (
        place_order("first", "c", [("SKU-001", 3)], operations_database)["order_id"]
        == first["order_id"]
    )
    with pytest.raises(ValueError):
        place_order("first", "c", [("SKU-001", 4)], operations_database)
    rejected = place_order("too-many", "c", [("SKU-001", 18), ("SKU-002", 1)], operations_database)
    assert rejected["status"] == "rejected"
    assert stock(operations_database)["reserved"] == 3
    assert stock(operations_database, "SKU-002")["reserved"] == 0
    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(
            pool.map(
                lambda key: place_order(key, "c", [("SKU-001", 10)], operations_database),
                ["race-a", "race-b"],
            )
        )
    assert sorted(r["status"] for r in outcomes) == ["rejected", "reserved"]
    assert stock(operations_database)["reserved"] == 13


def test_cancel_expire_ship_and_replenish_are_idempotent(operations_database):
    order = place_order("cancel", "c", [("SKU-001", 2)], operations_database)
    finish_order(order["order_id"], "cancel", operations_database)
    finish_order(order["order_id"], "ship", operations_database)
    assert stock(operations_database)["physical"] == 20
    assert stock(operations_database)["reserved"] == 0
    order = place_order("expire", "c", [("SKU-001", 2)], operations_database)
    with connect(operations_database) as conn:
        conn.execute("UPDATE reservations SET expires_at=now()-interval '1 second'")
    tick(operations_database)
    assert stock(operations_database)["reserved"] == 0
    replenishment = request_replenishment(
        "receipt", "SKU-001", 7, operations_database, delay_seconds=0
    )
    assert (
        request_replenishment("again", "SKU-001", 2, operations_database)["replenishment_id"]
        == replenishment["replenishment_id"]
    )
    with connect(operations_database) as conn:
        conn.execute("UPDATE products SET status='discontinued' WHERE sku='SKU-001'")
    receive_replenishment(replenishment["replenishment_id"], operations_database)
    receive_replenishment(replenishment["replenishment_id"], operations_database)
    assert stock(operations_database)["physical"] == 27
    with connect(operations_database) as conn:
        sums = conn.execute(
            "SELECT sum(physical_delta) AS p,sum(reserved_delta) AS r "
            "FROM stock_movements WHERE sku='SKU-001'"
        ).fetchone()
    assert (sums["p"], sums["r"]) == (27, 0)


def test_worker_auto_replenishes_once_and_runs_planned_actions(operations_database):
    order = place_order(
        "scheduled", "c", [("SKU-001", 16)], operations_database, action="ship", action_delay=0
    )
    tick(operations_database, auto_replenish=True)
    tick(operations_database, auto_replenish=True)
    assert stock(operations_database)["physical"] == 4
    with connect(operations_database) as conn:
        assert conn.execute("SELECT count(*) AS n FROM replenishments").fetchone()["n"] == 1
        assert (
            conn.execute(
                "SELECT status FROM orders WHERE order_id=%s", (order["order_id"],)
            ).fetchone()["status"]
            == "shipped"
        )


class FakeProducer:
    def __init__(self, fail=False):
        self.fail = fail
        self.sent = []

    def produce(self, topic, key, value, on_delivery):
        self.sent.append((topic, key, value))
        on_delivery("failure" if self.fail else None, None)

    def flush(self, timeout):
        return 0


def test_outbox_failure_retry_and_stable_events(operations_database):
    bad = FakeProducer(True)
    with pytest.raises(RuntimeError):
        publish_pending(operations_database, producer=bad)
    with connect(operations_database) as conn:
        assert (
            conn.execute(
                "SELECT count(*) AS n FROM outbox WHERE published_at IS NOT NULL"
            ).fetchone()["n"]
            == 0
        )
    good = FakeProducer()
    assert publish_pending(operations_database, producer=good) == 16
    assert good.sent[0] == bad.sent[0]
    assert publish_pending(operations_database, producer=good) == 0


def test_failed_transaction_leaves_no_order_stock_or_outbox(operations_database, monkeypatch):
    import retail_stream.inventory as inventory

    with connect(operations_database) as conn:
        before = conn.execute("SELECT count(*) AS n FROM outbox").fetchone()["n"]
    original = inventory.movement

    def fail_after_movement(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("Failure before commit")

    monkeypatch.setattr(inventory, "movement", fail_after_movement)
    with pytest.raises(RuntimeError, match="Failure before commit"):
        place_order("rollback", "c", [("SKU-001", 3)], operations_database)
    assert stock(operations_database)["reserved"] == 0
    with connect(operations_database) as conn:
        assert conn.execute("SELECT count(*) AS n FROM orders").fetchone()["n"] == 0
        assert conn.execute("SELECT count(*) AS n FROM outbox").fetchone()["n"] == before


def test_expired_reservation_cannot_ship(operations_database):
    order = place_order("expiry", "c", [("SKU-001", 3)], operations_database)
    with connect(operations_database) as conn:
        conn.execute("UPDATE reservations SET expires_at=now()-interval '1 second'")
    result = finish_order(order["order_id"], "ship", operations_database)
    assert result["status"] == "expired"
    assert stock(operations_database)["physical"] == 20
    assert stock(operations_database)["reserved"] == 0
