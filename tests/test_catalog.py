import pytest
from harlequin.catalog import CatalogSearchResult, InteractiveCatalogItem
from harlequin.exception import HarlequinQueryError

from harlequin_postgres.adapter import (
    HarlequinPostgresAdapter,
    HarlequinPostgresConnection,
)
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


def test_catalog_items_carry_type_names(
    connection_with_objects: HarlequinPostgresConnection,
) -> None:
    """Every item the catalog walk builds names its full type, not just a label."""
    conn = connection_with_objects

    [db_item] = [
        i
        for i in conn.get_catalog().items
        if isinstance(i, DatabaseCatalogItem) and i.label == "test"
    ]
    schema_items = db_item.fetch_children()
    [schema_one_item] = [i for i in schema_items if i.label == "one"]
    [schema_two_item] = [i for i in schema_items if i.label == "two"]
    [schema_four_item] = [i for i in schema_items if i.label == "four"]

    [table_item] = [i for i in schema_one_item.fetch_children() if i.label == "foo"]
    [view_item] = [i for i in schema_two_item.fetch_children() if i.label == "qux"]
    [matview_item] = [i for i in schema_four_item.fetch_children() if i.label == "foo"]

    assert table_item.type_name == "BASE TABLE"
    assert view_item.type_name == "VIEW"
    assert matview_item.type_name == "MATERIALIZED VIEW"

    # a column's type_name is the full type its type_label is shortened from
    assert [
        (i.label, i.type_label, i.type_name) for i in table_item.fetch_children()
    ] == [
        ("a", "#", "integer"),
        ("b", "s", "text"),
    ]
    assert [
        (i.label, i.type_label, i.type_name) for i in matview_item.fetch_children()
    ] == [
        ("a", "#", "integer"),
        ("b", "s", "text"),
    ]


@pytest.fixture
def connection_for_search(
    connection_with_objects: HarlequinPostgresConnection,
) -> HarlequinPostgresConnection:
    conn = connection_with_objects
    conn.execute("create table one.orders as select 1 as customer_id, 2 as amount")
    # a schema, a relation, and a column that all match the same term, to check
    # that a search reports a parent before the items under it
    conn.execute("create schema five")
    conn.execute("create table five.five_a as select 1 as five_col")
    # names holding LIKE metacharacters, which a term must not be able to use
    conn.execute('create table one."a_b" as select 1 as a')
    conn.execute('create table one."axb" as select 1 as a')
    conn.execute('create table one."pct%tbl" as select 1 as a')
    # the original connection fixture will clean this up.
    return conn


def _labeled(results: list[CatalogSearchResult], label: str) -> CatalogSearchResult:
    [result] = [r for r in results if r.item.label == label]
    return result


def test_implements_catalog_search() -> None:
    assert HarlequinPostgresAdapter.IMPLEMENTS_CATALOG_SEARCH is True


def test_search_catalog_relations(
    connection_for_search: HarlequinPostgresConnection,
) -> None:
    conn = connection_for_search

    results = conn.search_catalog("foo", kind="relations")

    assert [(r.item.label, r.parents) for r in results] == [
        ("foo", ("test", "four")),
        ("foo", ("test", "one")),
    ]
    matview_result, table_result = results
    assert isinstance(matview_result.item, MaterializedViewCatalogItem)
    assert isinstance(table_result.item, TableCatalogItem)
    # a relation is built by the class fetch_children() would have used for it,
    # so its query name is the one --catalog shows for the same object
    assert table_result.item.query_name == '"one"."foo"'
    assert (table_result.item.type_label, table_result.item.type_name) == (
        "t",
        "BASE TABLE",
    )
    assert (matview_result.item.type_label, matview_result.item.type_name) == (
        "mv",
        "MATERIALIZED VIEW",
    )


def test_search_catalog_relations_finds_views(
    connection_for_search: HarlequinPostgresConnection,
) -> None:
    results = connection_for_search.search_catalog("qux", kind="relations")

    [result] = results
    assert isinstance(result.item, ViewCatalogItem)
    assert (result.item.type_label, result.item.type_name) == ("v", "VIEW")
    assert result.parents == ("test", "two")


def test_search_catalog_columns(
    connection_for_search: HarlequinPostgresConnection,
) -> None:
    results = connection_for_search.search_catalog("customer", kind="columns")

    [result] = results
    assert isinstance(result.item, ColumnCatalogItem)
    assert result.item.label == "customer_id"
    assert result.parents == ("test", "one", "orders")
    assert result.item.query_name == '"customer_id"'
    # type_name is the full type the label is shortened from
    assert (result.item.type_label, result.item.type_name) == ("#", "integer")


def test_search_catalog_columns_finds_matview_columns(
    connection_for_search: HarlequinPostgresConnection,
) -> None:
    # four.foo is a materialized view, which information_schema does not have
    results = connection_for_search.search_catalog("b", kind="columns")

    matview_results = [r for r in results if r.parents == ("test", "four", "foo")]
    [result] = matview_results
    assert isinstance(result.item, ColumnCatalogItem)
    assert result.item.label == "b"
    assert (result.item.type_label, result.item.type_name) == ("s", "text")


def test_search_catalog_kinds_are_scoped(
    connection_for_search: HarlequinPostgresConnection,
) -> None:
    conn = connection_for_search

    relations = conn.search_catalog("five", kind="relations")
    assert [r.item.label for r in relations] == ["five_a"]

    columns = conn.search_catalog("five", kind="columns")
    assert [r.item.label for r in columns] == ["five_col"]


def test_search_catalog_all_returns_every_level(
    connection_for_search: HarlequinPostgresConnection,
) -> None:
    results = connection_for_search.search_catalog("five", kind="all")

    # a parent arrives before the items under it
    assert [(r.item.label, r.parents) for r in results] == [
        ("five", ("test",)),
        ("five_a", ("test", "five")),
        ("five_col", ("test", "five", "five_a")),
    ]
    schema_result, relation_result, column_result = results
    assert isinstance(schema_result.item, SchemaCatalogItem)
    assert isinstance(relation_result.item, TableCatalogItem)
    assert isinstance(column_result.item, ColumnCatalogItem)


def test_search_catalog_matches_databases_by_name(
    connection_for_search: HarlequinPostgresConnection,
) -> None:
    conn = connection_for_search

    results = conn.search_catalog("postgres", kind="all")

    # the pool is bound to one database, but the others can still be matched
    # by name, which is all the catalog's top level shows for them
    result = _labeled(results, "postgres")
    assert isinstance(result.item, DatabaseCatalogItem)
    assert result.parents == ()
    assert result.item.query_name == '"postgres"'

    # a database is not a relation, so the narrower kinds do not report it
    assert not conn.search_catalog("postgres", kind="relations")


def test_search_catalog_is_case_insensitive(
    connection_for_search: HarlequinPostgresConnection,
) -> None:
    conn = connection_for_search

    assert [(r.item.label, r.parents) for r in conn.search_catalog("FOO")] == [
        (r.item.label, r.parents) for r in conn.search_catalog("foo")
    ]


def test_search_catalog_matches_a_substring(
    connection_for_search: HarlequinPostgresConnection,
) -> None:
    results = connection_for_search.search_catalog("rder", kind="relations")

    assert [r.item.label for r in results] == ["orders"]


@pytest.mark.parametrize(
    "term,expected",
    [
        ("a_b", ["a_b"]),
        ("%tbl", ["pct%tbl"]),
        ("pct%", ["pct%tbl"]),
    ],
)
def test_search_catalog_escapes_like_metacharacters(
    connection_for_search: HarlequinPostgresConnection,
    term: str,
    expected: list[str],
) -> None:
    results = connection_for_search.search_catalog(term, kind="relations")

    assert [r.item.label for r in results] == expected


def test_search_catalog_excludes_system_schemas(
    connection_for_search: HarlequinPostgresConnection,
) -> None:
    conn = connection_for_search

    # information_schema.columns and pg_catalog.pg_statistic exist, but the
    # catalog tree does not show them, so a search must not report them either
    assert not conn.search_catalog("information_schema", kind="all")
    assert not conn.search_catalog("pg_statistic", kind="relations")


def test_search_catalog_without_a_match_is_empty(
    connection_for_search: HarlequinPostgresConnection,
) -> None:
    assert connection_for_search.search_catalog("no-such-object") == []


def test_search_catalog_raises_query_error(
    connection_for_search: HarlequinPostgresConnection,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from harlequin_postgres import adapter

    monkeypatch.setitem(adapter._SEARCH_SQL, "all", "select not_a_column")

    with pytest.raises(HarlequinQueryError):
        connection_for_search.search_catalog("foo")

    # the connection went back to the pool, so the next search still works
    monkeypatch.undo()
    assert connection_for_search.search_catalog("foo")


def test_search_catalog_items_match_fetch_children(
    connection_for_search: HarlequinPostgresConnection,
) -> None:
    conn = connection_for_search

    [db_item] = [
        i
        for i in conn.get_catalog().items
        if isinstance(i, DatabaseCatalogItem) and i.label == "test"
    ]
    [schema_item] = [i for i in db_item.fetch_children() if i.label == "one"]
    [walked_relation] = [i for i in schema_item.fetch_children() if i.label == "orders"]
    [walked_column] = [
        i for i in walked_relation.fetch_children() if i.label == "customer_id"
    ]

    [searched_relation] = conn.search_catalog("orders", kind="relations")
    [searched_column] = conn.search_catalog("customer_id", kind="columns")

    for walked, searched in (
        (walked_relation, searched_relation.item),
        (walked_column, searched_column.item),
    ):
        assert type(searched) is type(walked)
        assert searched.label == walked.label
        assert searched.query_name == walked.query_name
        assert searched.qualified_identifier == walked.qualified_identifier
        assert searched.type_label == walked.type_label
        assert searched.type_name == walked.type_name
