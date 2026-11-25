from decimal import Decimal

import pandas as pd
import psycopg
from psycopg.rows import dict_row

from retail_stream.config import DATABASE_URL, WAREHOUSE_ID

COLUMNS = ["window_start", "warehouse_id", "order_count", "total_amount_cents", "item_count"]


def load_metrics(since=None) -> pd.DataFrame:
    with psycopg.connect(DATABASE_URL, connect_timeout=3, row_factory=dict_row) as connection:
        connection.execute("SET TRANSACTION READ ONLY")
        connection.execute("SET LOCAL statement_timeout = '5s'")
        rows = connection.execute(
            "SELECT window_start, warehouse_id, order_count, total_amount_cents, item_count "
            "FROM order_metrics WHERE warehouse_id = %s "
            "AND (%s::timestamptz IS NULL OR window_start >= %s) "
            "ORDER BY window_start, warehouse_id",
            (WAREHOUSE_ID, since, since),
        ).fetchall()
    return pd.DataFrame(rows, columns=COLUMNS)


def summarize(frame):
    count = int(frame["order_count"].sum())
    amount = sum(frame["total_amount_cents"], Decimal(0)) / 100
    items = int(frame["item_count"].sum())
    average = amount / count if count else Decimal(0)
    return count, amount, items, average


def euros(value):
    return f"{value:,.2f}".replace(",", " ").replace(".", ",") + " €"
