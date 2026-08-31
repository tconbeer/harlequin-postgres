from __future__ import annotations

import logging
import sys
from datetime import date, datetime

import psycopg
import pytest
from harlequin.adapter import HarlequinAdapter, HarlequinConnection, HarlequinCursor
from harlequin.catalog import Catalog, CatalogItem, InteractiveCatalogItem
from harlequin.exception import HarlequinConnectionError, HarlequinQueryError
from psycopg.pq import TransactionStatus
from textual_fastdatatable.backend import create_backend

from harlequin_postgres import adapter as adapter_module
from harlequin_postgres.adapter import (
    HarlequinPostgresAdapter,
    HarlequinPostgresConnection,
)

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


def test_pooled_queries_return_idle_connections(
    connection: HarlequinPostgresConnection,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Pooled connections are not autocommit, so a bare select leaves them INTRANS.
    psycopg_pool logs a warning and rolls back for every connection returned that
    way, so every helper that borrows one has to end its transaction first.
    """
    connection.execute("create schema one")
    connection.execute("create table one.foo as select 1 as a")

    with caplog.at_level(logging.WARNING, logger="psycopg.pool"):
        catalog = connection.get_catalog()
        [db_item] = [item for item in catalog.items if item.label == "test"]
        assert isinstance(db_item, InteractiveCatalogItem)
        [schema_item] = [
            item for item in db_item.fetch_children() if item.label == "one"
        ]
        assert isinstance(schema_item, InteractiveCatalogItem)
        [relation_item] = [
            item for item in schema_item.fetch_children() if item.label == "foo"
        ]
        assert isinstance(relation_item, InteractiveCatalogItem)
        relation_item.fetch_children()
        connection.search_catalog("foo")
        connection.get_completions()

    assert [
        record.getMessage()
        for record in caplog.records
        if record.name.startswith("psycopg")
    ] == []

    with connection.pool.connection() as conn:
        assert conn.info.transaction_status == TransactionStatus.IDLE


def test_failed_pooled_query_does_not_leak_connections(
    connection: HarlequinPostgresConnection,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    A borrowed connection has to go back to the pool when the query raises, too;
    otherwise a handful of failures would exhaust the pool.
    """

    def raise_error(conn: psycopg.Connection) -> None:
        raise ValueError("boom")

    monkeypatch.setattr(adapter_module, "_get_completions", raise_error)

    for _ in range(connection.pool.max_size + 5):
        with pytest.raises(ValueError):
            connection.get_completions()

    monkeypatch.undo()
    # the pool still has connections to give out
    assert connection.get_completions()


def test_closed_conn_raises_right_error(
    connection: HarlequinPostgresConnection,
) -> None:
    connection._main_conn.close()

    with pytest.raises(HarlequinQueryError):
        connection.execute("select 1")


def test_implements_read_only() -> None:
    assert HarlequinPostgresAdapter.IMPLEMENTS_READ_ONLY is True


def test_read_only_defaults_to_false() -> None:
    adapter = HarlequinPostgresAdapter(conn_str=(TEST_DB_CONN,))
    assert adapter.read_only is False
    conn = adapter.connect()
    assert conn.read_only is False
    conn.close()


def test_read_only_is_not_a_conn_info_option() -> None:
    """
    read_only is not a libpq conninfo key, so it must never be passed to psycopg.
    """
    adapter = HarlequinPostgresAdapter(conn_str=(TEST_DB_CONN,), read_only=True)
    assert adapter.read_only is True
    assert "read_only" not in adapter.options
    assert adapter.connection_id == "localhost:5432/postgres"

    conn = adapter.connect()
    # the conninfo passed to psycopg is unchanged; read-only is applied to each
    # connection after it is opened.
    assert "read_only" not in conn.conn_info
    assert "options" not in conn.conn_info
    conn.close()


def test_read_only_connection_can_read(
    read_only_connection: HarlequinPostgresConnection,
) -> None:
    assert read_only_connection.read_only is True
    cur = read_only_connection.execute("select * from foo")
    assert isinstance(cur, HarlequinCursor)
    assert cur.fetchall() == [(1,)]


@pytest.mark.parametrize(
    "query",
    [
        "insert into foo values (2)",
        "update foo set a = 2",
        "delete from foo",
        "create table bar (a int)",
        "drop table foo",
    ],
)
def test_read_only_connection_rejects_writes(
    read_only_connection: HarlequinPostgresConnection, query: str
) -> None:
    with pytest.raises(HarlequinQueryError):
        read_only_connection.execute(query)


def test_read_only_survives_transaction_mode_toggle(
    read_only_connection: HarlequinPostgresConnection,
) -> None:
    assert read_only_connection.transaction_mode.label == "Auto"
    for _ in range(4):
        with pytest.raises(HarlequinQueryError):
            read_only_connection.execute("insert into foo values (2)")
        read_only_connection.toggle_transaction_mode()


def test_read_only_applies_to_pooled_connections(
    read_only_connection: HarlequinPostgresConnection,
) -> None:
    # the catalog and completions run on connections from the pool, not on the
    # main connection.
    conn = read_only_connection.pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("select current_setting('default_transaction_read_only')")
            assert cur.fetchone() == ("on",)
            with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
                cur.execute("create table baz (a int)")
        conn.rollback()
    finally:
        read_only_connection.pool.putconn(conn)


def test_read_only_connection_get_catalog(
    read_only_connection: HarlequinPostgresConnection,
) -> None:
    catalog = read_only_connection.get_catalog()
    assert isinstance(catalog, Catalog)
    assert catalog.items


def test_read_only_refuses_to_connect_if_server_does_not_enforce(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    If the read-only setting does not make it to the server, connecting must fail
    loudly, rather than hand back a connection that would happily write.
    """
    monkeypatch.setattr(
        HarlequinPostgresConnection,
        "_configure_connection",
        lambda self, conn: None,
    )
    with pytest.raises(HarlequinConnectionError):
        HarlequinPostgresAdapter(conn_str=(TEST_DB_CONN,), read_only=True).connect()


def test_read_only_enforced_in_autocommit_session(
    read_only_connection: HarlequinPostgresConnection,
) -> None:
    """
    psycopg's Connection.read_only does not affect autocommit sessions, since
    psycopg never issues a BEGIN for them; the session characteristic does.
    """
    assert read_only_connection.transaction_mode.label == "Auto"
    assert read_only_connection._main_conn.autocommit is True
    with pytest.raises(HarlequinQueryError):
        read_only_connection.execute("insert into foo values (2)")
