from __future__ import annotations

import sys
from datetime import date, datetime
from typing import cast

import pytest
from harlequin.adapter import HarlequinAdapter, HarlequinConnection, HarlequinCursor
from harlequin.catalog import Catalog, CatalogItem
from harlequin.exception import HarlequinConnectionError, HarlequinQueryError
from psycopg.pq import TransactionStatus
from textual_fastdatatable.backend import create_backend

import harlequin_postgres.adapter as postgres_adapter
from harlequin_postgres.adapter import (
    HarlequinPostgresAdapter,
    HarlequinPostgresConnection,
)

if sys.version_info < (3, 10):
    from importlib_metadata import entry_points
else:
    from importlib.metadata import entry_points

TEST_DB_CONN = "postgresql://postgres:for-testing@localhost:5432"


class FakeInfo:
    def __init__(
        self,
        dbname: str,
        transaction_status: TransactionStatus = TransactionStatus.IDLE,
    ) -> None:
        self.dbname = dbname
        self.transaction_status = transaction_status


class FakeCursor:
    description = None

    def __init__(self, result: list[tuple[str]] | None = None) -> None:
        self.result = result or []
        self.queries: list[tuple[str, tuple[str, ...] | None]] = []

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def execute(
        self,
        query: str,
        params: tuple[str, ...] | None = None,
    ) -> None:
        self.queries.append((query, params))

    def fetchall(self) -> list[tuple[str]]:
        return self.result

    def close(self) -> None:
        return None


class FakeConnection:
    def __init__(self, dbname: str) -> None:
        self.dbname = dbname
        self.autocommit = False
        self.info = FakeInfo(dbname)
        self.commits = 0
        self.rollbacks = 0
        self.closed = False
        self.cursor_calls = 0
        self.last_cursor: FakeCursor | None = None
        self.fail_commit = False

    def cursor(self) -> FakeCursor:
        self.cursor_calls += 1
        self.last_cursor = FakeCursor(result=[("public",)])
        return self.last_cursor

    def commit(self) -> None:
        if self.fail_commit:
            raise RuntimeError(f"cannot sync transaction mode for {self.dbname}")
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True


class FakePool:
    pools: list["FakePool"] = []
    fail_databases: set[str] = set()
    fail_getconn_databases: set[str] = set()
    fail_putconn_databases: set[str] = set()

    def __init__(
        self,
        conninfo: str,
        *_: object,
        kwargs: dict[str, object | None] | None = None,
        **__: object,
    ) -> None:
        self.conninfo = conninfo
        self.kwargs = kwargs or {}
        self.dbname = str(self.kwargs.get("dbname") or "env_db")
        if self.dbname in self.fail_databases:
            raise RuntimeError(f"cannot connect to {self.dbname}")
        self.connection = FakeConnection(self.dbname)
        self.getconn_calls = 0
        self.putconn_calls: list[FakeConnection] = []
        self.closed = False
        self.pools.append(self)

    def getconn(self) -> FakeConnection:
        if self.dbname in self.fail_getconn_databases:
            raise RuntimeError(f"cannot borrow connection to {self.dbname}")
        self.getconn_calls += 1
        return self.connection

    def putconn(self, conn: FakeConnection) -> None:
        if self.dbname in self.fail_putconn_databases:
            raise RuntimeError(f"cannot return connection to {self.dbname}")
        self.putconn_calls.append(conn)

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def fake_pools(monkeypatch: pytest.MonkeyPatch) -> type[FakePool]:
    FakePool.pools = []
    FakePool.fail_databases = set()
    FakePool.fail_getconn_databases = set()
    FakePool.fail_putconn_databases = set()
    monkeypatch.setattr(postgres_adapter, "ConnectionPool", FakePool)
    return FakePool


def make_connection() -> HarlequinPostgresConnection:
    return HarlequinPostgresConnection(
        ("postgresql://postgres:for-testing@localhost:5432/postgres",),
        options={},
    )


def make_env_connection() -> HarlequinPostgresConnection:
    return HarlequinPostgresConnection(
        ("postgresql://postgres:for-testing@localhost:5432",),
        options={},
    )


def test_plugin_discovery() -> None:
    PLUGIN_NAME = "postgres"
    eps = entry_points(group="harlequin.adapter")
    assert eps[PLUGIN_NAME]
    adapter_cls = eps[PLUGIN_NAME].load()
    assert issubclass(adapter_cls, HarlequinAdapter)
    assert adapter_cls == HarlequinPostgresAdapter


def test_connect() -> None:
    conn = HarlequinPostgresAdapter(conn_str=(TEST_DB_CONN,)).connect()
    assert isinstance(conn, HarlequinConnection)


def test_init_extra_kwargs() -> None:
    assert HarlequinPostgresAdapter(
        conn_str=(TEST_DB_CONN,), foo=1, bar="baz"
    ).connect()


@pytest.mark.parametrize(
    "conn_str",
    [
        ("foo",),
        ("host=foo",),
        ("postgresql://admin:pass@foo:5432/db",),
    ],
)
def test_connect_raises_connection_error(conn_str: tuple[str]) -> None:
    with pytest.raises(HarlequinConnectionError):
        _ = HarlequinPostgresAdapter(conn_str=conn_str, connect_timeout=0.1).connect()


@pytest.mark.parametrize(
    "conn_str,options,expected",
    [
        (("",), {}, "localhost:5432/postgres"),
        (("host=foo",), {}, "foo:5432/postgres"),
        (("postgresql://foo",), {}, "foo:5432/postgres"),
        (("postgresql://foo",), {"port": 5431}, "foo:5431/postgres"),
        (("postgresql://foo/mydb",), {"port": 5431}, "foo:5431/mydb"),
        (("postgresql://admin:pass@foo/mydb",), {"port": 5431}, "foo:5431/mydb"),
        (("postgresql://admin:pass@foo:5431/mydb",), {}, "foo:5431/mydb"),
    ],
)
def test_connection_id(
    conn_str: tuple[str], options: dict[str, int | float | str | None], expected: str
) -> None:
    adapter = HarlequinPostgresAdapter(
        conn_str=conn_str,
        **options,  # type: ignore[arg-type]
    )
    assert adapter.connection_id == expected


def test_switch_database_changes_active_connection_and_reuses_pool(
    fake_pools: type[FakePool],
) -> None:
    connection = make_connection()

    connection.switch_database("analytics")
    analytics_pool = fake_pools.pools[-1]
    connection.switch_database("postgres")
    connection.switch_database("analytics")

    assert cast(FakeConnection, connection._main_conn).dbname == "analytics"
    assert fake_pools.pools == [fake_pools.pools[0], analytics_pool]
    assert analytics_pool.getconn_calls == 2


def test_switch_database_preserves_active_connection_when_target_fails(
    fake_pools: type[FakePool],
) -> None:
    connection = make_connection()
    original_conn = connection._main_conn
    fake_pools.fail_databases.add("missing")

    with pytest.raises(HarlequinConnectionError):
        connection.switch_database("missing")

    assert connection._main_conn is original_conn
    assert cast(FakeConnection, connection._main_conn).dbname == "postgres"


def test_switch_database_preserves_active_connection_when_target_checkout_fails(
    fake_pools: type[FakePool],
) -> None:
    connection = make_connection()
    original_conn = connection._main_conn
    fake_pools.fail_getconn_databases.add("analytics")

    with pytest.raises(HarlequinConnectionError):
        connection.switch_database("analytics")

    assert cast(FakePool, connection.pool) is fake_pools.pools[0]
    assert connection._main_conn is original_conn
    assert connection._active_dbname == "postgres"


def test_switch_database_preserves_active_connection_when_sync_fails(
    fake_pools: type[FakePool],
) -> None:
    connection = make_connection()
    original_pool = cast(FakePool, connection.pool)
    original_conn = connection._main_conn
    connection.switch_database("analytics")
    analytics_pool = cast(FakePool, connection.pool)
    analytics_conn = cast(FakeConnection, connection._main_conn)
    analytics_conn.fail_commit = True
    connection.switch_database("postgres")

    with pytest.raises(HarlequinConnectionError):
        connection.switch_database("analytics")

    assert cast(FakePool, connection.pool) is original_pool
    assert connection._main_conn is original_conn
    assert connection._active_dbname == "postgres"
    assert analytics_pool.putconn_calls[-1] is analytics_conn


def test_switch_database_preserves_active_connection_when_returning_previous_fails(
    fake_pools: type[FakePool],
) -> None:
    connection = make_connection()
    original_pool = cast(FakePool, connection.pool)
    original_conn = connection._main_conn
    fake_pools.fail_putconn_databases.add("postgres")

    with pytest.raises(HarlequinConnectionError):
        connection.switch_database("analytics")

    analytics_pool = fake_pools.pools[-1]
    assert cast(FakePool, connection.pool) is original_pool
    assert connection._main_conn is original_conn
    assert connection._active_dbname == "postgres"
    assert analytics_pool.putconn_calls == [analytics_pool.connection]


def test_switch_database_rejects_active_manual_transaction(
    fake_pools: type[FakePool],
) -> None:
    connection = make_connection()
    connection.toggle_transaction_mode()
    cast(
        FakeConnection, connection._main_conn
    ).info.transaction_status = TransactionStatus.INTRANS

    with pytest.raises(HarlequinConnectionError):
        connection.switch_database("analytics")

    assert len(fake_pools.pools) == 1
    assert cast(FakeConnection, connection._main_conn).dbname == "postgres"


def test_initial_connection_does_not_force_postgres_when_dbname_not_explicit(
    fake_pools: type[FakePool],
) -> None:
    connection = make_env_connection()

    assert fake_pools.pools[0].kwargs == {}
    assert connection._active_dbname == "env_db"


def test_get_schemas_borrows_connection_for_requested_database(
    fake_pools: type[FakePool],
) -> None:
    connection = make_connection()

    schemas = connection._get_schemas("analytics")

    assert schemas == [("public",)]
    assert cast(FakeConnection, connection._main_conn).dbname == "postgres"
    analytics_pool = fake_pools.pools[-1]
    assert analytics_pool.dbname == "analytics"
    assert analytics_pool.getconn_calls == 1
    assert analytics_pool.putconn_calls == [analytics_pool.connection]


def test_get_catalog(connection: HarlequinPostgresConnection) -> None:
    catalog = connection.get_catalog()
    assert isinstance(catalog, Catalog)
    assert catalog.items
    assert isinstance(catalog.items[0], CatalogItem)


def test_get_schemas_includes_user_schema_named_pgmq(
    connection: HarlequinPostgresConnection,
) -> None:
    connection.execute("create schema pgmq")
    schemas = connection._get_schemas("test")
    assert ("pgmq",) in schemas


def test_get_completions(connection: HarlequinPostgresConnection) -> None:
    completions = connection.get_completions()
    test_labels = ["atomic", "greatest", "point_right", "autovacuum"]
    filtered = list(filter(lambda x: x.label in test_labels, completions))
    assert len(filtered) == 4
    value_filtered = list(filter(lambda x: x.value in test_labels, completions))
    assert len(value_filtered) == 4


def test_execute_ddl(connection: HarlequinPostgresConnection) -> None:
    cur = connection.execute("create table foo (a int)")
    assert cur is None


def test_execute_select(connection: HarlequinPostgresConnection) -> None:
    cur = connection.execute("select 1 as a")
    assert isinstance(cur, HarlequinCursor)
    assert cur.columns() == [("a", "#")]
    data = cur.fetchall()
    backend = create_backend(data)
    assert backend.column_count == 1
    assert backend.row_count == 1


def test_execute_select_dupe_cols(connection: HarlequinPostgresConnection) -> None:
    cur = connection.execute("select 1 as a, 2 as a, 3 as a")
    assert isinstance(cur, HarlequinCursor)
    assert len(cur.columns()) == 3
    data = cur.fetchall()
    backend = create_backend(data)
    assert backend.column_count == 3
    assert backend.row_count == 1


def test_set_limit(connection: HarlequinPostgresConnection) -> None:
    cur = connection.execute("select 1 as a union all select 2 union all select 3")
    assert isinstance(cur, HarlequinCursor)
    cur = cur.set_limit(2)
    assert isinstance(cur, HarlequinCursor)
    data = cur.fetchall()
    backend = create_backend(data)
    assert backend.column_count == 1
    assert backend.row_count == 2


def test_execute_raises_query_error(connection: HarlequinPostgresConnection) -> None:
    with pytest.raises(HarlequinQueryError):
        _ = connection.execute("sel;")


def test_inf_timestamps(connection: HarlequinPostgresConnection) -> None:
    cur = connection.execute(
        """select
            'infinity'::date,
            'infinity'::timestamp,
            'infinity'::timestamptz,
            '-infinity'::date,
            '-infinity'::timestamp,
            '-infinity'::timestamptz
        """
    )
    assert cur is not None
    data = cur.fetchall()
    assert data == [
        (
            date.max,
            datetime.max,
            datetime.max,
            date.min,
            datetime.min,
            datetime.min,
        )
    ]


def test_closed_conn_raises_right_error(
    connection: HarlequinPostgresConnection,
) -> None:
    connection._main_conn.close()

    with pytest.raises(HarlequinQueryError):
        connection.execute("select 1")
