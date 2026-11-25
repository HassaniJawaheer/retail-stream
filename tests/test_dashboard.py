from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pandas as pd
import psycopg
from streamlit.testing.v1 import AppTest

from retail_stream.dashboard_data import COLUMNS

APP = str(Path(__file__).parents[1] / "src/retail_stream/dashboard.py")


def test_dashboard_single_warehouse_and_weighted_average(monkeypatch):
    frame = pd.DataFrame(
        [
            (
                datetime(2026, 1, 1, tzinfo=timezone.utc),
                "WH-CORBEIL-01",
                1,
                Decimal(1000),
                Decimal(2),
            ),
            (
                datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc),
                "WH-CORBEIL-01",
                3,
                Decimal(9000),
                Decimal(6),
            ),
        ],
        columns=COLUMNS,
    )
    monkeypatch.setattr("retail_stream.dashboard_data.load_metrics", lambda since: frame)
    app = AppTest.from_file(APP).run()
    assert not app.exception
    assert [metric.value for metric in app.metric] == ["4", "100,00 €", "8", "25,00 €"]
    assert len(app.selectbox) == 1
    assert "Entrepôt" not in app.dataframe[0].value.columns
    assert len(app.dataframe[0].value) == 2


def test_dashboard_empty_period(monkeypatch):
    monkeypatch.setattr(
        "retail_stream.dashboard_data.load_metrics", lambda since: pd.DataFrame(columns=COLUMNS)
    )
    app = AppTest.from_file(APP).run()
    assert not app.exception
    assert "Aucune commande" in app.info[0].value


def test_dashboard_database_unavailable(monkeypatch):
    def fail(since):
        raise psycopg.OperationalError("private connection details")

    monkeypatch.setattr("retail_stream.dashboard_data.load_metrics", fail)
    app = AppTest.from_file(APP).run()
    assert not app.exception
    assert "PostgreSQL est indisponible" in app.error[0].value
    assert "private" not in app.error[0].value


def test_dashboard_period_is_sent_to_reader(monkeypatch):
    calls = []

    def load(since):
        calls.append(since)
        return pd.DataFrame(columns=COLUMNS)

    monkeypatch.setattr("retail_stream.dashboard_data.load_metrics", load)
    app = AppTest.from_file(APP).run()
    assert calls[-1] is None
    app.selectbox[0].select("Dernière heure").run()
    assert not app.exception
    delta = datetime.now(timezone.utc) - calls[-1]
    assert 3590 < delta.total_seconds() < 3610
