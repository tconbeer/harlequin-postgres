from __future__ import annotations

import sys
from datetime import date, datetime

import pytest
from harlequin.adapter import HarlequinAdapter, HarlequinConnection, HarlequinCursor
from harlequin.catalog import Catalog, CatalogItem
from harlequin.exception import HarlequinConnectionError, HarlequinQueryError
from harlequin_postgres.adapter import (
    HarlequinPostgresAdapter,
    HarlequinPostgresConnection,
)
from textual_fastdatatable.backend import create_backend

if sys.version_info < (3, 10):
    from importlib_metadata import entry_points
else:
    from importlib.metadata import entry_points

TEST_DB_CONN = "postgresql://postgres:for-testing@localhost:5432"


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


def test_get_catalog(connection: HarlequinPostgresConnection) -> None:
    catalog = connection.get_catalog()
    assert isinstance(catalog, Catalog)
    assert catalog.items
    assert isinstance(catalog.items[0], CatalogItem)


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
