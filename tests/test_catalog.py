import pytest
from harlequin.catalog import InteractiveCatalogItem
from harlequin_postgres.adapter import HarlequinPostgresConnection
from harlequin_postgres.catalog import (
    ColumnCatalogItem,
    DatabaseCatalogItem,
    MaterializedViewCatalogItem,
    RelationCatalogItem,
    SchemaCatalogItem,
    TableCatalogItem,
    ViewCatalogItem,
)


@pytest.fixture
def connection_with_objects(
    connection: HarlequinPostgresConnection,
) -> HarlequinPostgresConnection:
    connection.execute("create schema one")
    connection.execute("create table one.foo as select 1 as a, '2' as b")
    connection.execute("create table one.bar as select 1 as a, '2' as b")
    connection.execute("create table one.baz as select 1 as a, '2' as b")
    connection.execute("create schema two")
    connection.execute("create view two.qux as select * from one.foo")
    connection.execute("create schema three")
    connection.execute("create schema four")
    connection.execute("create materialized view four.foo as select * from one.foo")
    # the original connection fixture will clean this up.
    return connection


def test_catalog(connection_with_objects: HarlequinPostgresConnection) -> None:
    conn = connection_with_objects

    catalog = conn.get_catalog()

    # at least two databases, postgres and test
    assert len(catalog.items) >= 2

    [test_db_item] = filter(lambda item: item.label == "test", catalog.items)
    assert isinstance(test_db_item, InteractiveCatalogItem)
    assert isinstance(test_db_item, DatabaseCatalogItem)
    assert not test_db_item.children
    assert not test_db_item.loaded

    schema_items = test_db_item.fetch_children()
    assert all(isinstance(item, SchemaCatalogItem) for item in schema_items)

    [schema_one_item] = filter(lambda item: item.label == "one", schema_items)
    assert isinstance(schema_one_item, SchemaCatalogItem)
    assert not schema_one_item.children
    assert not schema_one_item.loaded

    table_items = schema_one_item.fetch_children()
    assert all(isinstance(item, RelationCatalogItem) for item in table_items)

    [foo_item] = filter(lambda item: item.label == "foo", table_items)
    assert isinstance(foo_item, TableCatalogItem)
    assert not foo_item.children
    assert not foo_item.loaded

    foo_column_items = foo_item.fetch_children()
    assert all(isinstance(item, ColumnCatalogItem) for item in foo_column_items)

    [schema_two_item] = filter(lambda item: item.label == "two", schema_items)
    assert isinstance(schema_two_item, SchemaCatalogItem)
    assert not schema_two_item.children
    assert not schema_two_item.loaded

    view_items = schema_two_item.fetch_children()
    assert all(isinstance(item, ViewCatalogItem) for item in view_items)

    [qux_item] = filter(lambda item: item.label == "qux", view_items)
    assert isinstance(qux_item, ViewCatalogItem)
    assert not qux_item.children
    assert not qux_item.loaded

    qux_column_items = qux_item.fetch_children()
    assert all(isinstance(item, ColumnCatalogItem) for item in qux_column_items)

    assert [item.label for item in foo_column_items] == [
        item.label for item in qux_column_items
    ]

    # ensure calling fetch_children on cols doesn't raise
    children_items = foo_column_items[0].fetch_children()
    assert not children_items

    [schema_three_item] = filter(lambda item: item.label == "three", schema_items)
    assert isinstance(schema_two_item, SchemaCatalogItem)
    assert not schema_two_item.children
    assert not schema_two_item.loaded

    three_children = schema_three_item.fetch_children()
    assert not three_children

    [schema_four_item] = filter(lambda item: item.label == "four", schema_items)
    assert isinstance(schema_four_item, SchemaCatalogItem)
    assert not schema_four_item.children
    assert not schema_four_item.loaded

    mview_items = schema_four_item.fetch_children()
    assert all(isinstance(item, MaterializedViewCatalogItem) for item in mview_items)

    [foo_mv_item] = filter(lambda item: item.label == "foo", mview_items)
    assert isinstance(foo_mv_item, MaterializedViewCatalogItem)
    assert not foo_mv_item.children
    assert not foo_mv_item.loaded

    foo_mv_cols = foo_mv_item.fetch_children()
    assert foo_mv_cols
    assert all(isinstance(item, ColumnCatalogItem) for item in foo_mv_cols)
