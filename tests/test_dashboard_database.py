from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg.conninfo import make_conninfo
from psycopg.sql import SQL, Identifier
from streamlit.testing.v1 import AppTest

from retail_stream.config import DATABASE_URL
from retail_stream.dashboard_data import load_metrics
from retail_stream.storage import initialize_database


@pytest.mark.integration
def test_dashboard_reads_postgresql(monkeypatch):
    name = f"test_{uuid4().hex}"
    with psycopg.connect(DATABASE_URL) as connection:
        connection.execute(SQL("CREATE SCHEMA {}").format(Identifier(name)))
    dsn = make_conninfo(DATABASE_URL, options=f"-c search_path={name}")
    try:
        initialize_database(dsn)
        with psycopg.connect(dsn) as connection:
            connection.execute(
                "INSERT INTO order_metrics VALUES "
                "('2026-01-01 00:00Z', '2026-01-01 00:01Z', 'WH-PARIS', 2, 3000, 4, 1500), "
                "('2026-01-01 00:01Z', '2026-01-01 00:02Z', 'WH-LYON', 1, 900, 1, 900)"
            )
        monkeypatch.setattr("retail_stream.dashboard_data.DATABASE_URL", dsn)
        assert len(load_metrics()) == 2
        filtered = load_metrics(datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc))
        assert len(filtered) == 1
        assert filtered.iloc[0]["total_amount_cents"] == Decimal(900)
        app = AppTest.from_file(
            str(Path(__file__).parents[1] / "src/retail_stream/dashboard.py")
        ).run()
        assert not app.exception
        assert not app.error
        assert [metric.value for metric in app.metric] == ["3", "39,00 €", "5", "13,00 €"]
    finally:
        with psycopg.connect(DATABASE_URL) as connection:
            connection.execute(SQL("DROP SCHEMA {} CASCADE").format(Identifier(name)))
