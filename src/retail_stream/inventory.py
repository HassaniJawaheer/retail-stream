import hashlib
import json
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from psycopg.types.json import Jsonb

from retail_stream.catalog import connect
from retail_stream.config import DATABASE_URL, INVENTORY_TOPIC, ORDERS_TOPIC, WAREHOUSE_ID

ORDER_STATES = {
    "order_created": "created",
    "order_reserved": "reserved",
    "order_rejected": "rejected",
    "order_cancelled": "cancelled",
    "reservation_expired": "expired",
    "order_shipped": "shipped",
}


def emit(conn, topic, key, kind, payload, version=1, entity_version=1):
    event_id = uuid4()
    event = dict(
        event_id=str(event_id),
        event_type=kind,
        event_time=datetime.now(timezone.utc).isoformat(),
        producer="retail-operations",
        version=version,
        entity_version=entity_version,
        payload=payload,
    )
    conn.execute(
        "INSERT INTO outbox (event_id,topic,message_key,event) VALUES (%s,%s,%s,%s)",
        (event_id, topic, key, Jsonb(event)),
    )
    return event_id


def movement(conn, sku, reason, operation_id, physical_delta=0, reserved_delta=0, initial=False):
    row = conn.execute(
        """UPDATE inventory SET physical=physical+%s, reserved=reserved+%s,
        version=version+%s WHERE warehouse_id=%s AND sku=%s RETURNING *""",
        (physical_delta, reserved_delta, 0 if initial else 1, WAREHOUSE_ID, sku),
    ).fetchone()
    if row is None:
        raise ValueError(f"Stock inconnu : {sku}")
    event_id = emit(
        conn,
        INVENTORY_TOPIC,
        f"{WAREHOUSE_ID}:{sku}",
        "inventory_changed",
        dict(
            warehouse_id=WAREHOUSE_ID,
            sku=sku,
            reason=reason,
            operation_id=str(operation_id),
            physical_delta=physical_delta,
            reserved_delta=reserved_delta,
            physical=row["physical"],
            reserved=row["reserved"],
            available=row["physical"] - row["reserved"],
            stock_version=row["version"],
        ),
        entity_version=row["version"],
    )
    conn.execute(
        """INSERT INTO stock_movements
        (movement_id,warehouse_id,sku,reason,operation_id,physical_delta,reserved_delta,
         physical_after,reserved_after,stock_version) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (
            event_id,
            WAREHOUSE_ID,
            sku,
            reason,
            str(operation_id),
            physical_delta,
            reserved_delta,
            row["physical"],
            row["reserved"],
            row["version"],
        ),
    )


def initialize_stock(conn, sku, quantity):
    conn.execute(
        "INSERT INTO inventory (warehouse_id,sku,physical) VALUES (%s,%s,0)", (WAREHOUSE_ID, sku)
    )
    movement(conn, sku, "initial", f"initial:{sku}", physical_delta=quantity, initial=True)


def order_event(conn, order, kind):
    items = conn.execute(
        "SELECT sku,quantity,unit_price_cents,product_version FROM order_items "
        "WHERE order_id=%s ORDER BY sku",
        (order["order_id"],),
    ).fetchall()
    emit(
        conn,
        ORDERS_TOPIC,
        str(order["order_id"]),
        kind,
        dict(
            order_id=str(order["order_id"]),
            customer_id=order["customer_id"],
            warehouse_id=order["warehouse_id"],
            currency="EUR",
            items=items,
            total_amount_cents=order["total_amount_cents"],
            status=order["status"],
            reason=order["reason"],
        ),
        version=3,
        entity_version=order["version"],
    )


def place_order(
    request_id, customer_id, items, dsn=DATABASE_URL, ttl_seconds=120, action=None, action_delay=30
):
    if not request_id or not customer_id.strip() or ttl_seconds <= 0 or action_delay < 0:
        raise ValueError("Identifiant, délai ou client invalide")
    if action not in (None, "ship", "cancel"):
        raise ValueError("Action invalide")
    quantities = {}
    for sku, qty in items:
        if type(qty) is not int or not 0 < qty <= 10000:
            raise ValueError("Quantité invalide")
        quantities[sku] = quantities.get(sku, 0) + qty
    if not quantities or len(quantities) > 100 or any(q > 10000 for q in quantities.values()):
        raise ValueError("Commande vide ou trop volumineuse")
    fingerprint = hashlib.sha256(
        json.dumps(
            [customer_id, sorted(quantities.items()), ttl_seconds, action, action_delay]
        ).encode()
    ).hexdigest()
    with connect(dsn) as conn:
        conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s),7083)", (request_id,))
        existing = conn.execute(
            "SELECT * FROM orders WHERE request_id=%s", (request_id,)
        ).fetchone()
        if existing:
            if existing["request_hash"] != fingerprint:
                raise ValueError("Identifiant de demande réutilisé avec un contenu différent")
            return existing
        products = conn.execute(
            "SELECT * FROM products WHERE sku=ANY(%s) ORDER BY sku FOR UPDATE",
            (sorted(quantities),),
        ).fetchall()
        if len(products) != len(quantities):
            raise ValueError("Référence produit inconnue")
        stock = conn.execute(
            "SELECT * FROM inventory WHERE warehouse_id=%s AND sku=ANY(%s) ORDER BY sku FOR UPDATE",
            (WAREHOUSE_ID, sorted(quantities)),
        ).fetchall()
        reason = None
        if any(p["status"] != "active" for p in products):
            reason = "PRODUCT_NOT_SELLABLE"
        elif len(stock) != len(products) or any(
            r["physical"] - r["reserved"] - r["safety_stock"] < quantities[r["sku"]] for r in stock
        ):
            reason = "INSUFFICIENT_STOCK"
        total = sum(p["price_cents"] * quantities[p["sku"]] for p in products)
        order_id = uuid4()
        order = conn.execute(
            """INSERT INTO orders
            (order_id,request_id,request_hash,customer_id,status,total_amount_cents)
            VALUES (%s,%s,%s,%s,'created',%s) RETURNING *""",
            (order_id, request_id, fingerprint, customer_id, total),
        ).fetchone()
        for p in products:
            conn.execute(
                "INSERT INTO order_items VALUES (%s,%s,%s,%s,%s)",
                (order_id, p["sku"], quantities[p["sku"]], p["price_cents"], p["version"]),
            )
        order_event(conn, order, "order_created")
        now = datetime.now(timezone.utc)
        order = conn.execute(
            """UPDATE orders SET status=%s,reason=%s,warehouse_id=%s,
            version=2,planned_action=%s,action_at=%s WHERE order_id=%s RETURNING *""",
            (
                "rejected" if reason else "reserved",
                reason,
                None if reason else WAREHOUSE_ID,
                None if reason else action,
                now + timedelta(seconds=action_delay) if action and not reason else None,
                order_id,
            ),
        ).fetchone()
        if not reason:
            reservation_id = uuid4()
            conn.execute(
                "INSERT INTO reservations VALUES (%s,%s,'active',%s)",
                (reservation_id, order_id, now + timedelta(seconds=ttl_seconds)),
            )
            for sku, qty in sorted(quantities.items()):
                conn.execute(
                    "INSERT INTO reservation_items VALUES (%s,%s,%s,%s)",
                    (reservation_id, WAREHOUSE_ID, sku, qty),
                )
                movement(conn, sku, "reservation", order_id, reserved_delta=qty)
        order_event(conn, order, "order_rejected" if reason else "order_reserved")
        return order


def finish_order(order_id, action, dsn=DATABASE_URL):
    mapping = {
        "ship": ("shipped", "consumed", "order_shipped"),
        "cancel": ("cancelled", "released", "order_cancelled"),
        "expire": ("expired", "expired", "reservation_expired"),
    }
    if action not in mapping:
        raise ValueError("Transition inconnue")
    with connect(dsn) as conn:
        order = conn.execute(
            "SELECT * FROM orders WHERE order_id=%s FOR UPDATE", (order_id,)
        ).fetchone()
        if order is None:
            raise ValueError("Commande inconnue")
        if order["status"] != "reserved":
            return order
        reservation = conn.execute(
            "SELECT * FROM reservations WHERE order_id=%s FOR UPDATE", (order_id,)
        ).fetchone()
        expired = reservation["expires_at"] <= datetime.now(timezone.utc)
        if action == "expire" and not expired:
            return order
        if expired:
            action = "expire"
        status, reservation_status, kind = mapping[action]
        lines = conn.execute(
            "SELECT * FROM reservation_items WHERE reservation_id=%s ORDER BY sku",
            (reservation["reservation_id"],),
        ).fetchall()
        for line in lines:
            movement(
                conn,
                line["sku"],
                action,
                order_id,
                physical_delta=-line["quantity"] if action == "ship" else 0,
                reserved_delta=-line["quantity"],
            )
        conn.execute(
            "UPDATE reservations SET status=%s WHERE reservation_id=%s",
            (reservation_status, reservation["reservation_id"]),
        )
        order = conn.execute(
            "UPDATE orders SET status=%s,version=version+1,action_at=NULL "
            "WHERE order_id=%s RETURNING *",
            (status, order_id),
        ).fetchone()
        order_event(conn, order, kind)
        return order


def request_replenishment(request_id, sku, quantity, dsn=DATABASE_URL, delay_seconds=30):
    if (
        not request_id
        or type(quantity) is not int
        or not 0 < quantity <= 1000000
        or delay_seconds < 0
    ):
        raise ValueError("Demande de réapprovisionnement invalide")
    with connect(dsn) as conn:
        conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s),7084)", (request_id,))
        existing = conn.execute(
            "SELECT * FROM replenishments WHERE request_id=%s", (request_id,)
        ).fetchone()
        if existing:
            if existing["sku"] != sku or existing["quantity"] != quantity:
                raise ValueError("Demande de réapprovisionnement contradictoire")
            return existing
        product = conn.execute("SELECT * FROM products WHERE sku=%s FOR UPDATE", (sku,)).fetchone()
        if product is None or product["status"] != "active":
            raise ValueError("Produit non réapprovisionnable")
        conn.execute(
            "SELECT * FROM inventory WHERE warehouse_id=%s AND sku=%s FOR UPDATE",
            (WAREHOUSE_ID, sku),
        )
        existing = conn.execute(
            "SELECT * FROM replenishments WHERE warehouse_id=%s AND sku=%s AND status='requested'",
            (WAREHOUSE_ID, sku),
        ).fetchone()
        if existing:
            return existing
        row = conn.execute(
            "INSERT INTO replenishments VALUES (%s,%s,%s,%s,%s,'requested',%s) RETURNING *",
            (
                uuid4(),
                request_id,
                WAREHOUSE_ID,
                sku,
                quantity,
                datetime.now(timezone.utc) + timedelta(seconds=delay_seconds),
            ),
        ).fetchone()
        emit(
            conn,
            INVENTORY_TOPIC,
            f"{WAREHOUSE_ID}:{sku}",
            "replenishment_requested",
            dict(
                warehouse_id=WAREHOUSE_ID,
                sku=sku,
                quantity=quantity,
                replenishment_id=str(row["replenishment_id"]),
                reason="manual_or_threshold",
            ),
        )
        return row


def receive_replenishment(replenishment_id, dsn=DATABASE_URL):
    with connect(dsn) as conn:
        row = conn.execute(
            "SELECT * FROM replenishments WHERE replenishment_id=%s FOR UPDATE", (replenishment_id,)
        ).fetchone()
        if row is None:
            raise ValueError("Réapprovisionnement inconnu")
        if row["status"] == "received":
            return row
        movement(conn, row["sku"], "receipt", replenishment_id, physical_delta=row["quantity"])
        row = conn.execute(
            "UPDATE replenishments SET status='received' WHERE replenishment_id=%s RETURNING *",
            (replenishment_id,),
        ).fetchone()
        emit(
            conn,
            INVENTORY_TOPIC,
            f"{WAREHOUSE_ID}:{row['sku']}",
            "replenishment_received",
            dict(
                warehouse_id=WAREHOUSE_ID,
                sku=row["sku"],
                quantity=row["quantity"],
                replenishment_id=str(replenishment_id),
                reason="receipt",
            ),
            entity_version=2,
        )
        return row


def tick(dsn=DATABASE_URL, auto_replenish=False):
    with connect(dsn) as conn:
        due = conn.execute("""SELECT o.order_id,o.planned_action,r.expires_at<=now() AS expired
            FROM orders o JOIN reservations r USING(order_id) WHERE o.status='reserved'
            AND (o.action_at<=now() OR r.expires_at<=now()) ORDER BY o.order_id""").fetchall()
        receipts = conn.execute(
            "SELECT replenishment_id FROM replenishments WHERE status='requested' AND due_at<=now()"
        ).fetchall()
    for row in due:
        finish_order(row["order_id"], "expire" if row["expired"] else row["planned_action"], dsn)
    for row in receipts:
        receive_replenishment(row["replenishment_id"], dsn)
    if auto_replenish:
        with connect(dsn) as conn:
            low = conn.execute("""SELECT i.* FROM inventory i JOIN products p USING(sku)
                WHERE p.status='active' AND i.physical-i.reserved<=i.reorder_point""").fetchall()
        for row in low:
            request_replenishment(
                f"auto:{row['sku']}:{row['version']}",
                row["sku"],
                row["target_stock"] - (row["physical"] - row["reserved"]),
                dsn,
            )
