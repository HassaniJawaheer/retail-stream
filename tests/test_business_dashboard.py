from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

from retail_stream.business_dashboard import load_business

APP = str(Path(__file__).parents[1] / "src/retail_stream/dashboard.py")


def test_business_dashboard_empty(monkeypatch):
    monkeypatch.setattr(
        "retail_stream.business_dashboard.load_business", lambda: ([], [], [], [], 0)
    )
    app = AppTest.from_file(APP).run()
    app.radio[0].set_value("Activité métier").run()
    assert not app.exception
    assert [metric.value for metric in app.metric] == ["0", "0", "0", "0"]
    assert any("Aucun produit" in item.value for item in app.info)


@pytest.mark.integration
def test_business_dashboard_reports_unpublished_stock(operations_database, monkeypatch):
    monkeypatch.setattr(
        "retail_stream.business_dashboard.load_business", lambda: load_business(operations_database)
    )
    app = AppTest.from_file(APP).run()
    app.radio[0].set_value("Activité métier").run()
    assert not app.exception
    assert len(app.dataframe[0].value) == 16
    assert any("16 événements" in item.value for item in app.info)
    assert any("16 produits" in item.value for item in app.warning)
