from __future__ import annotations

import logging
import sys
import threading
import time
from datetime import date, datetime
from pathlib import Path

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
    threads_before_connect = set(threading.enumerate())
    with pytest.raises(HarlequinConnectionError):
        _ = HarlequinPostgresAdapter(conn_str=conn_str, connect_timeout=0.1).connect()
    # a pool left open keeps retrying the connection in its threads
    leaked_threads = set(threading.enumerate()) - threads_before_connect
    assert [thread.name for thread in leaked_threads] == []


@pytest.fixture
def no_stored_password(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Keeps libpq from finding a password in the environment or a .pgpass file."""
    monkeypatch.delenv("PGPASSWORD", raising=False)
    monkeypatch.setenv("PGPASSFILE", str(tmp_path / "nonexistent.pgpass"))


@pytest.mark.usefixtures("no_stored_password")
def test_connect_without_password_fails_fast_with_hint() -> None:
    """
    The server's error must be raised right away, not a PoolTimeout after the
    pool spends the whole timeout retrying (#73).
    """
    threads_before_connect = set(threading.enumerate())
    start = time.monotonic()
    with pytest.raises(HarlequinConnectionError) as exc_info:
        HarlequinPostgresAdapter(
            conn_str=("postgresql://postgres@localhost:5432",), connect_timeout=30
        ).connect()
    assert time.monotonic() - start < 5
    assert "no password supplied" in exc_info.value.msg
    assert "The server requires a password" in exc_info.value.msg
    assert "--password" in exc_info.value.msg
    leaked_threads = set(threading.enumerate()) - threads_before_connect
    assert [thread.name for thread in leaked_threads] == []


@pytest.mark.usefixtures("no_stored_password")
def test_connect_with_wrong_password_fails_fast_without_hint() -> None:
    start = time.monotonic()
    with pytest.raises(HarlequinConnectionError) as exc_info:
        HarlequinPostgresAdapter(
            conn_str=("postgresql://postgres:wrong@localhost:5432",),
            connect_timeout=30,
        ).connect()
    assert time.monotonic() - start < 5
    assert "password authentication failed" in exc_info.value.msg
    assert "The server requires a password" not in exc_info.value.msg


def test_connect_to_nonexistent_database_fails_fast() -> None:
    start = time.monotonic()
    with pytest.raises(HarlequinConnectionError) as exc_info:
        HarlequinPostgresAdapter(
            conn_str=(TEST_DB_CONN,), dbname="does_not_exist", connect_timeout=30
        ).connect()
    assert time.monotonic() - start < 5
    assert 'database "does_not_exist" does not exist' in exc_info.value.msg


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


def _psycopg_messages(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [
        record.getMessage()
        for record in caplog.records
        if record.name.startswith("psycopg")
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

    assert _psycopg_messages(caplog) == []

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


@pytest.mark.parametrize(
    "transaction_mode,query",
    [
        ("Auto", "begin; select 1"),
        ("Auto", "begin; create table not_committed (a int)"),
        ("Manual", "create table not_committed (a int)"),
    ],
)
def test_close_discards_open_transaction_quietly(
    connection: HarlequinPostgresConnection,
    caplog: pytest.LogCaptureFixture,
    transaction_mode: str,
    query: str,
) -> None:
    """
    Closing with a transaction open discards it without psycopg_pool logging a
    warning.
    """
    if connection.transaction_mode.label != transaction_mode:
        connection.toggle_transaction_mode()
    connection.execute(query)
    assert connection._main_conn.info.transaction_status == TransactionStatus.INTRANS
    main_backend_pid = connection._main_conn.info.backend_pid

    with caplog.at_level(logging.WARNING, logger="psycopg.pool"):
        connection.close()

    assert _psycopg_messages(caplog) == []
    with psycopg.connect(conninfo=TEST_DB_CONN, dbname="test") as conn:
        # the backend exits shortly after the client disconnects
        deadline = time.monotonic() + 5
        while conn.execute(
            "select 1 from pg_stat_activity where pid = %s", (main_backend_pid,)
        ).fetchone():
            assert time.monotonic() < deadline, "main connection's backend survived"
            time.sleep(0.05)
        result = conn.execute("select to_regclass('not_committed')").fetchone()
    assert result == (None,)


def test_close_after_loading_catalog_is_quiet(
    connection: HarlequinPostgresConnection,
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.WARNING, logger="psycopg.pool"):
        connection.get_catalog()
        connection.get_completions()
        connection.close()

    assert _psycopg_messages(caplog) == []


def test_close_is_idempotent(connection: HarlequinPostgresConnection) -> None:
    connection.close()
    connection.close()


def test_close_tolerates_broken_main_connection(
    connection: HarlequinPostgresConnection,
    caplog: pytest.LogCaptureFixture,
) -> None:
    with psycopg.connect(conninfo=TEST_DB_CONN, dbname="postgres") as conn:
        conn.execute(
            "select pg_terminate_backend(%s, 5000)",
            (connection._main_conn.info.backend_pid,),
        )
    with pytest.raises(HarlequinQueryError):
        connection.execute("select 1")

    with caplog.at_level(logging.WARNING, logger="psycopg.pool"):
        connection.close()

    assert _psycopg_messages(caplog) == []


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
