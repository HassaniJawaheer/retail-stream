import hashlib
import json
import re
from dataclasses import asdict, dataclass
from importlib.resources import files
from uuid import NAMESPACE_URL, uuid5

import psycopg
from psycopg.rows import dict_row

from retail_stream.config import DATABASE_URL, WAREHOUSE_ID


@dataclass(frozen=True)
class Product:
    sku: str
    name: str
    manufacturer: str
    model: str
    category: str
    description: str
    price_cents: int
    currency: str = "EUR"
    status: str = "active"
    source: str | None = None
    source_ref: str | None = None

    def __post_init__(self):
        if not re.fullmatch(r"[A-Z0-9][A-Z0-9_-]{0,63}", self.sku):
            raise ValueError("SKU interne invalide")
        for name in ("name", "manufacturer", "model", "category", "description"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip() or value != value.strip():
                raise ValueError(f"Champ produit invalide : {name}")
        if type(self.price_cents) is not int or not 0 < self.price_cents <= 100000000:
            raise ValueError("Prix invalide : entier positif en centimes attendu")
        if self.currency != "EUR" or self.status not in ("active", "inactive", "discontinued"):
            raise ValueError("Devise ou statut invalide")
        if (self.source is None) != (self.source_ref is None):
            raise ValueError("Source et référence source doivent être renseignées ensemble")
        if self.source is not None and any(
            not isinstance(v, str) or not v.strip() or v != v.strip()
            for v in (self.source, self.source_ref)
        ):
            raise ValueError("Provenance invalide")

    @property
    def product_id(self):
        return uuid5(NAMESPACE_URL, f"retailstream:product:{self.sku}")


def connect(dsn=DATABASE_URL):
    return psycopg.connect(dsn, row_factory=dict_row, connect_timeout=10)


def migrate(dsn=DATABASE_URL):
    with connect(dsn) as conn:
        conn.execute("SELECT pg_advisory_xact_lock(hashtext(current_schema()), 7081)")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS retail_migrations "
            "(name TEXT PRIMARY KEY, checksum TEXT NOT NULL)"
        )
        for path in sorted(files("retail_stream").joinpath("migrations").iterdir(), key=str):
            if not path.name.endswith(".sql"):
                continue
            sql = path.read_text()
            digest = hashlib.sha256(sql.encode()).hexdigest()
            existing = conn.execute(
                "SELECT checksum FROM retail_migrations WHERE name=%s", (path.name,)
            ).fetchone()
            if existing:
                if existing["checksum"] != digest:
                    raise RuntimeError(f"Migration modifiée : {path.name}")
                continue
            conn.execute(sql)
            conn.execute("INSERT INTO retail_migrations VALUES (%s,%s)", (path.name, digest))


def save_product(product, dsn=DATABASE_URL):
    from retail_stream.inventory import initialize_stock

    values = asdict(product)
    values["product_id"] = product.product_id
    with connect(dsn) as conn:
        conn.execute(
            """INSERT INTO products
            (product_id,sku,name,manufacturer,model,category,description,price_cents,
             currency,status,source,source_ref)
            VALUES (%(product_id)s,%(sku)s,%(name)s,%(manufacturer)s,%(model)s,%(category)s,
                    %(description)s,%(price_cents)s,%(currency)s,%(status)s,%(source)s,%(source_ref)s)
            ON CONFLICT (sku) DO UPDATE SET name=EXCLUDED.name, manufacturer=EXCLUDED.manufacturer,
            model=EXCLUDED.model, category=EXCLUDED.category, description=EXCLUDED.description,
            price_cents=EXCLUDED.price_cents, currency=EXCLUDED.currency, status=EXCLUDED.status,
            source=EXCLUDED.source, source_ref=EXCLUDED.source_ref,
            version=products.version+1, updated_at=now()""",
            values,
        )
        existing = conn.execute(
            "SELECT 1 FROM inventory WHERE warehouse_id=%s AND sku=%s",
            (WAREHOUSE_ID, product.sku),
        ).fetchone()
        if not existing:
            initialize_stock(conn, product.sku, 0)


def list_products(dsn=DATABASE_URL):
    with connect(dsn) as conn:
        return conn.execute("SELECT * FROM products ORDER BY sku").fetchall()


def seed(dsn=DATABASE_URL):
    from retail_stream.inventory import initialize_stock

    migrate(dsn)
    with connect(dsn) as conn:
        conn.execute(
            "INSERT INTO warehouses VALUES (%s,%s) ON CONFLICT DO NOTHING",
            (WAREHOUSE_ID, "Corbeil-Essonnes"),
        )
    data = json.loads(files("retail_stream").joinpath("seed_products.json").read_text())
    for row in data:
        product = Product(**row)
        with connect(dsn) as conn:
            conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s),7082)", (product.sku,))
            exists = conn.execute("SELECT 1 FROM products WHERE sku=%s", (product.sku,)).fetchone()
            if exists:
                continue
            values = asdict(product)
            values["product_id"] = product.product_id
            conn.execute(
                """INSERT INTO products
                (product_id,sku,name,manufacturer,model,category,description,price_cents,
                 currency,status,source,source_ref)
                VALUES (%(product_id)s,%(sku)s,%(name)s,%(manufacturer)s,%(model)s,%(category)s,
                %(description)s,%(price_cents)s,%(currency)s,%(status)s,%(source)s,%(source_ref)s)""",
                values,
            )
            initialize_stock(conn, product.sku, 20)
