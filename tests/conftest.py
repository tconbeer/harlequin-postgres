from __future__ import annotations

import sys
from typing import Generator

import psycopg
import pytest
from harlequin_postgres.adapter import (
    HarlequinPostgresAdapter,
    HarlequinPostgresConnection,
)

if sys.version_info < (3, 10):
    pass
else:
    pass

TEST_DB_CONN = "postgresql://postgres:for-testing@localhost:5432"


@pytest.fixture
def connection() -> Generator[HarlequinPostgresConnection, None, None]:
    pgconn = psycopg.connect(conninfo=TEST_DB_CONN, dbname="postgres")
    pgconn.autocommit = True
    cur = pgconn.cursor()
    cur.execute("drop database if exists test;")
    cur.execute("create database test;")
    cur.close()
    pgconn.close()
    conn = HarlequinPostgresAdapter(
        conn_str=(f"{TEST_DB_CONN}",), dbname="test"
    ).connect()
    yield conn
    conn.close()
    pgconn = psycopg.connect(conninfo=TEST_DB_CONN, dbname="postgres")
    pgconn.autocommit = True
    cur = pgconn.cursor()
    cur.execute("drop database if exists test;")
    cur.close()
    pgconn.close()
