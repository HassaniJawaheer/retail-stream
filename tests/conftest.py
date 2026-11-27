from uuid import uuid4

import psycopg
import pytest
from psycopg.conninfo import make_conninfo
from psycopg.sql import SQL, Identifier

from retail_stream.catalog import seed
from retail_stream.config import DATABASE_URL


@pytest.fixture
def operations_database():
    name = f"ops_test_{uuid4().hex}"
    with psycopg.connect(DATABASE_URL) as conn:
        conn.execute(SQL("CREATE SCHEMA {}").format(Identifier(name)))
    dsn = make_conninfo(DATABASE_URL, options=f"-c search_path={name}")
    try:
        seed(dsn)
        yield dsn
    finally:
        with psycopg.connect(DATABASE_URL) as conn:
            conn.execute(SQL("DROP SCHEMA {} CASCADE").format(Identifier(name)))
