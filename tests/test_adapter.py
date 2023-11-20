from harlequin_myadapter.adapter import MyAdapter, MyConnection, MyCursor
from harlequin.catalog import Catalog, CatalogItem
from harlequin.adapter import HarlequinAdapter, HarlequinConnection, HarlequinCursor
from harlequin.exception import HarlequinConnectionError, HarlequinQueryError
from textual_fastdatatable.backend import AutoBackendType, create_backend
import pytest
import sys

if sys.version_info < (3, 10):
    from importlib_metadata import entry_points
else:
    from importlib.metadata import entry_points


def test_plugin_discovery() -> None:
    PLUGIN_NAME="my-adapter"
    eps = entry_points(group="harlequin.adapter")
    assert eps[PLUGIN_NAME]
    adapter_cls = eps[PLUGIN_NAME].load()
    assert issubclass(adapter_cls, HarlequinAdapter)
    assert adapter_cls == MyAdapter

def test_connect() -> None:
    conn = MyAdapter(conn_str=tuple()).connect()
    assert isinstance(conn, HarlequinConnection)

def test_init_extra_kwargs() -> None:
    assert MyAdapter(conn_str=tuple(), foo=1, bar="baz").connect()

def test_connect_raises_connection_error() -> None:
    with pytest.raises(HarlequinConnectionError):
        _ = MyAdapter(conn_str=("foo",)).connect()

@pytest.fixture
def connection() -> MyConnection:
    return MyAdapter(conn_str=tuple()).connect()

def test_get_catalog(connection: MyConnection) -> None:
    catalog = connection.get_catalog()
    assert isinstance(catalog, Catalog)
    assert catalog.items
    assert isinstance(catalog.items[0], CatalogItem)

def test_execute_ddl(connection: MyConnection) -> None:
    cur = connection.execute("create table foo (a int)")
    assert cur is None

def test_execute_select(connection: MyConnection) -> None:
    cur = connection.execute("select 1 as a")
    assert isinstance(cur, HarlequinCursor)
    assert cur.columns() == [("a", "##")]
    data = cur.fetchall()
    assert isinstance(data, AutoBackendType)
    backend = create_backend(data)
    assert backend.column_count == 1
    assert backend.row_count == 1

def test_execute_select_dupe_cols(connection: MyConnection) -> None:
    cur = connection.execute("select 1 as a, 2 as a, 3 as a")
    assert isinstance(cur, HarlequinCursor)
    assert len(cur.columns()) == 3
    data = cur.fetchall()
    assert isinstance(data, AutoBackendType)
    backend = create_backend(data)
    assert backend.column_count == 3
    assert backend.row_count == 1

def test_set_limit(connection: MyConnection) -> None:
    cur = connection.execute("select 1 as a union all select 2 union all select 3")
    assert isinstance(cur, HarlequinCursor)
    cur = cur.set_limit(2)
    assert isinstance(cur, HarlequinCursor)
    data = cur.fetchall()
    assert isinstance(data, AutoBackendType)
    backend = create_backend(data)
    assert backend.column_count == 1
    assert backend.row_count == 2

def test_execute_raises_query_error(connection: MyConnection) -> None:
    with pytest.raises(HarlequinQueryError):
        _ = connection.execute("select;")
