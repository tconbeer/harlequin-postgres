from __future__ import annotations

import sys
from typing import Callable, Generator

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


@pytest.fixture
def connect_again(
    connection: HarlequinPostgresConnection,
) -> Generator[Callable[..., HarlequinPostgresConnection], None, None]:
    """Opens more connections to the same test database, closed at teardown.

    A connection resolves its search path when it connects, so a test that
    changes the search path needs a new one.
    """
    connections: list[HarlequinPostgresConnection] = []

    def _connect(dsn_params: str = "") -> HarlequinPostgresConnection:
        conn_str = f"{TEST_DB_CONN}/?{dsn_params}" if dsn_params else TEST_DB_CONN
        conn = HarlequinPostgresAdapter(conn_str=(conn_str,), dbname="test").connect()
        connections.append(conn)
        return conn

    yield _connect

    for conn in connections:
        conn.close()


@pytest.fixture
def read_only_connection(
    connection: HarlequinPostgresConnection,
) -> Generator[HarlequinPostgresConnection, None, None]:
    """
    A read-only connection to the same (read-write provisioned) test database.
    """
    connection.execute("create table foo (a int)")
    connection.execute("insert into foo values (1)")
    ro_conn = HarlequinPostgresAdapter(
        conn_str=(f"{TEST_DB_CONN}",), dbname="test", read_only=True
    ).connect()
    yield ro_conn
    ro_conn.close()
