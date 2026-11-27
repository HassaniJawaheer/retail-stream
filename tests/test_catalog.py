from dataclasses import replace

import pytest

from retail_stream.catalog import Product


def test_product_contract_is_stable_and_independent_of_source():
    product = Product("TEST-1", "Souris", "Demo", "M1", "accessoires", "Souris de test", 1900)
    assert product.product_id == replace(product, name="Autre nom", price_cents=2000).product_id
    for change in [
        dict(price_cents=0),
        dict(price_cents=1.2),
        dict(price_cents=True),
        dict(currency="USD"),
        dict(status="deleted"),
        dict(name=" "),
        dict(sku="invalid sku"),
        dict(source="bestbuy"),
    ]:
        with pytest.raises(ValueError):
            replace(product, **change)
