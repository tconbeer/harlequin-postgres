from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Generator

import psycopg
import pytest
from harlequin.driver import HarlequinDriver
from psycopg import sql

from harlequin_postgres.adapter import HarlequinPostgresConnection
from harlequin_postgres.catalog import (
    ColumnCatalogItem,
    DatabaseCatalogItem,
    ForeignCatalogItem,
    MaterializedViewCatalogItem,
    SchemaCatalogItem,
    TableCatalogItem,
    ViewCatalogItem,
)
from harlequin_postgres.interactions import (
    execute_drop_database_statement,
    execute_drop_foreign_table_statement,
    execute_drop_schema_statement,
    execute_drop_table_statement,
    execute_drop_view_statement,
    execute_use_statement,
    insert_columns_at_cursor,
    show_describe_relation,
    show_describe_table_constraints,
    show_describe_table_indexes,
    show_list_indexes,
    show_list_objects,
    show_select_star,
    show_view_definition,
)

if TYPE_CHECKING:
    from textual.notifications import SeverityLevel

TEST_DB_CONN = "postgresql://postgres:for-testing@localhost:5432"

TRICKY_NAMES = [
    "Mixed Case",
    'dq"uote',
    "sq'uote",
    "back\\slash",
    "dot.ted",
    "semi;colon",
    "back`tick",
    "brack]et",
]


class RecordingDriver(HarlequinDriver):
    """Keeps the text an interaction inserts, and confirms every prompt."""

    def __init__(self) -> None:
        self.new_buffers: list[str] = []
        self.inserted_text: list[str] = []
        self.notifications: list[tuple[str, str]] = []

    def insert_text_in_new_buffer(self, text: str) -> None:
        self.new_buffers.append(text)

    def insert_text_at_selection(self, text: str) -> None:
        self.inserted_text.append(text)

    def confirm_and_execute(
        self, callback: Callable[[], None], instructions: str = "Are you sure?"
    ) -> None:
        callback()

    def notify(self, message: str, severity: SeverityLevel = "information") -> None:
        self.notifications.append((message, severity))

    def refresh_catalog(self) -> None:
        pass


@dataclass
class TrickyItems:
    schema: SchemaCatalogItem
    table: TableCatalogItem
    view: ViewCatalogItem
    matview: MaterializedViewCatalogItem
    foreign_table: ForeignCatalogItem


def _execute(conn: HarlequinPostgresConnection, query: sql.Composable) -> None:
    conn.execute(query.as_string())


def _rows(conn: HarlequinPostgresConnection, query: str) -> list[tuple]:
    cur = conn.execute(query)
    assert cur is not None
    return list(cur.fetchall())  # type: ignore[arg-type]


def _ident(name: str) -> str:
    return sql.Identifier(name).as_string()


@pytest.fixture(params=TRICKY_NAMES)
def tricky_name(request: pytest.FixtureRequest) -> str:
    name: str = request.param
    return name


@pytest.fixture
def tricky_items(
    connection: HarlequinPostgresConnection, tricky_name: str
) -> TrickyItems:
    """A schema, table, column, view, matview and foreign table all named
    tricky_name, beside a decoy schema and table with nearby names."""
    schema = sql.Identifier(tricky_name)
    name = sql.Identifier(tricky_name)
    for query in [
        sql.SQL("create schema {}").format(schema),
        sql.SQL("create table {}.{} ({} int primary key, other text)").format(
            schema, name, name
        ),
        sql.SQL("insert into {}.{} values (7, 'tricky')").format(schema, name),
        sql.SQL("create view {}.{} as select 42 as answer").format(
            schema, sql.Identifier(f"{tricky_name} view")
        ),
        sql.SQL("create materialized view {}.{} as select 43 as answer").format(
            schema, sql.Identifier(f"{tricky_name} mv")
        ),
        sql.SQL("create extension if not exists postgres_fdw"),
        sql.SQL("create server nowhere foreign data wrapper postgres_fdw"),
        sql.SQL("create foreign table {}.{} (a int) server nowhere").format(
            schema, sql.Identifier(f"{tricky_name} ft")
        ),
        sql.SQL("create schema decoy"),
        sql.SQL("create table decoy.{} (a int)").format(name),
        sql.SQL("insert into decoy.{} values (99)").format(name),
        sql.SQL("create table {}.decoy (a int)").format(schema),
    ]:
        _execute(connection, query)

    [db_item] = [
        item
        for item in connection.get_catalog().items
        if isinstance(item, DatabaseCatalogItem) and item.label == "test"
    ]
    [schema_item] = [i for i in db_item.fetch_children() if i.label == tricky_name]
    relations = {i.label: i for i in schema_item.fetch_children()}
    table_item = relations[tricky_name]
    view_item = relations[f"{tricky_name} view"]
    matview_item = relations[f"{tricky_name} mv"]
    foreign_item = relations[f"{tricky_name} ft"]
    assert isinstance(table_item, TableCatalogItem)
    assert isinstance(view_item, ViewCatalogItem)
    assert isinstance(matview_item, MaterializedViewCatalogItem)
    assert isinstance(foreign_item, ForeignCatalogItem)
    return TrickyItems(
        schema=schema_item,
        table=table_item,
        view=view_item,
        matview=matview_item,
        foreign_table=foreign_item,
    )


def test_catalog_items_name_their_objects(
    connection: HarlequinPostgresConnection,
    tricky_items: TrickyItems,
    tricky_name: str,
) -> None:
    [column_item, other_item] = tricky_items.table.fetch_children()
    assert isinstance(column_item, ColumnCatalogItem)
    assert (column_item.label, other_item.label) == (tricky_name, "other")

    table = tricky_items.table
    assert _rows(
        connection,
        f"select {column_item.qualified_identifier}, {column_item.query_name} "
        f"from {table.qualified_identifier}",
    ) == [(7, 7)]
    assert _rows(connection, f"select * from {tricky_items.view.query_name}") == [(42,)]
    assert _rows(connection, f"select * from {tricky_items.matview.query_name}") == [
        (43,)
    ]
    [matview_column] = tricky_items.matview.fetch_children()
    assert matview_column.label == "answer"


def test_search_catalog_finds_tricky_names(
    connection: HarlequinPostgresConnection,
    tricky_items: TrickyItems,
    tricky_name: str,
) -> None:
    results = connection.search_catalog(tricky_name, kind="relations")

    found = {(r.parents, r.item.label, r.item.query_name) for r in results}
    assert (("test", tricky_name), tricky_name, tricky_items.table.query_name) in found
    assert (
        ("test", tricky_name),
        f"{tricky_name} mv",
        tricky_items.matview.query_name,
    ) in found
    assert (("test", "decoy"), tricky_name, f'"decoy".{_ident(tricky_name)}') in found


def test_select_star_reads_the_relation(
    connection: HarlequinPostgresConnection, tricky_items: TrickyItems
) -> None:
    driver = RecordingDriver()
    show_select_star(tricky_items.table, driver)
    [select_star] = driver.new_buffers
    assert _rows(connection, select_star) == [(7, "tricky")]


def test_insert_columns_names_the_columns(
    connection: HarlequinPostgresConnection, tricky_items: TrickyItems
) -> None:
    driver = RecordingDriver()
    insert_columns_at_cursor(tricky_items.table, driver)
    [columns] = driver.inserted_text
    assert _rows(
        connection, f"select {columns} from {tricky_items.table.query_name}"
    ) == [(7, "tricky")]


def test_describe_relation_describes_the_relation(
    connection: HarlequinPostgresConnection,
    tricky_items: TrickyItems,
    tricky_name: str,
) -> None:
    driver = RecordingDriver()
    show_describe_relation(tricky_items.table, driver)
    [describe] = driver.new_buffers
    assert [row[0] for row in _rows(connection, describe)] == [tricky_name, "other"]


def test_describe_indexes_describes_the_table(
    connection: HarlequinPostgresConnection,
    tricky_items: TrickyItems,
    tricky_name: str,
) -> None:
    driver = RecordingDriver()
    show_describe_table_indexes(tricky_items.table, driver)
    [describe] = driver.new_buffers
    [index_row] = _rows(connection, describe)
    assert index_row[0] == tricky_name
    assert index_row[3] == tricky_name
    assert index_row[4] is True


def test_describe_constraints_describes_the_table(
    connection: HarlequinPostgresConnection,
    tricky_items: TrickyItems,
    tricky_name: str,
) -> None:
    driver = RecordingDriver()
    show_describe_table_constraints(tricky_items.table, driver)
    [describe] = driver.new_buffers
    primary_keys = [
        row for row in _rows(connection, describe) if row[2] == "Primary Key"
    ]
    assert [(row[0], row[3]) for row in primary_keys] == [(tricky_name, tricky_name)]


def test_list_objects_lists_the_schema(
    connection: HarlequinPostgresConnection,
    tricky_items: TrickyItems,
    tricky_name: str,
) -> None:
    driver = RecordingDriver()
    show_list_objects(tricky_items.schema, driver)
    [listing] = driver.new_buffers
    rows = _rows(connection, listing)
    assert {row[0] for row in rows} == {tricky_name}
    assert {row[1] for row in rows} >= {
        tricky_name,
        f"{tricky_name} view",
        f"{tricky_name} mv",
        f"{tricky_name} ft",
        "decoy",
    }


def test_list_indexes_lists_the_schema(
    connection: HarlequinPostgresConnection,
    tricky_items: TrickyItems,
    tricky_name: str,
) -> None:
    driver = RecordingDriver()
    show_list_indexes(tricky_items.schema, driver)
    [listing] = driver.new_buffers
    [index_row] = _rows(connection, listing)
    assert (index_row[0], index_row[4]) == (tricky_name, tricky_name)


def test_view_definition_reads_the_view(tricky_items: TrickyItems) -> None:
    driver = RecordingDriver()
    show_view_definition(tricky_items.view, driver)
    [definition] = driver.new_buffers
    assert "42 AS answer" in definition


def test_view_definition_of_a_missing_view_notifies(
    connection: HarlequinPostgresConnection, tricky_items: TrickyItems
) -> None:
    connection.execute(f"drop view {tricky_items.view.qualified_identifier}")
    driver = RecordingDriver()
    show_view_definition(tricky_items.view, driver)
    assert not driver.new_buffers
    [(_, severity)] = driver.notifications
    assert severity == "error"


def test_use_sets_the_search_path(
    connection: HarlequinPostgresConnection,
    tricky_items: TrickyItems,
    tricky_name: str,
) -> None:
    execute_use_statement(tricky_items.schema, RecordingDriver())
    assert _rows(connection, "select current_schemas(false)") == [([tricky_name],)]
    assert _rows(connection, "select a from decoy") == []


def _relation_names(connection: HarlequinPostgresConnection) -> set[tuple[str, str]]:
    rows = _rows(
        connection,
        "select n.nspname, c.relname from pg_class c "
        "join pg_namespace n on n.oid = c.relnamespace "
        "where n.nspname not like 'pg%' and n.nspname != 'information_schema' "
        "and c.relkind in ('r', 'v', 'm', 'f')",
    )
    return {(schema, relation) for schema, relation in rows}


@pytest.mark.parametrize(
    "drop,relation_attribute",
    [
        (execute_drop_table_statement, "table"),
        (execute_drop_view_statement, "view"),
        (execute_drop_foreign_table_statement, "foreign_table"),
    ],
)
def test_drop_relation_drops_only_the_relation(
    connection: HarlequinPostgresConnection,
    tricky_items: TrickyItems,
    tricky_name: str,
    drop: Callable[..., None],
    relation_attribute: str,
) -> None:
    relation_item = getattr(tricky_items, relation_attribute)
    before = _relation_names(connection)

    drop(relation_item, RecordingDriver())

    assert before - _relation_names(connection) == {(tricky_name, relation_item.label)}


def test_drop_schema_drops_only_the_schema(
    connection: HarlequinPostgresConnection,
    tricky_items: TrickyItems,
    tricky_name: str,
) -> None:
    execute_drop_schema_statement(tricky_items.schema, RecordingDriver())

    assert {schema for schema, _ in _relation_names(connection)} == {"decoy"}


@pytest.fixture
def tricky_database(tricky_name: str) -> Generator[str, None, None]:
    database_name = f"harlequin_pg {tricky_name}"
    drop_database = sql.SQL("drop database if exists {}").format(
        sql.Identifier(database_name)
    )
    with psycopg.connect(TEST_DB_CONN, autocommit=True) as conn:
        conn.execute(drop_database)
        conn.execute(
            sql.SQL("create database {}").format(sql.Identifier(database_name))
        )
    yield database_name
    with psycopg.connect(TEST_DB_CONN, autocommit=True) as conn:
        conn.execute(drop_database)


def test_drop_database_drops_the_database(
    connection: HarlequinPostgresConnection, tricky_database: str
) -> None:
    [database_item] = [
        item for item in connection.get_catalog().items if item.label == tricky_database
    ]
    assert isinstance(database_item, DatabaseCatalogItem)

    execute_drop_database_statement(database_item, RecordingDriver())

    assert tricky_database not in {
        item.label for item in connection.get_catalog().items
    }
    assert "test" in {item.label for item in connection.get_catalog().items}


def test_editor_text_runs_with_standard_conforming_strings_off(
    tricky_items: TrickyItems,
    tricky_name: str,
    connect_again: Callable[..., HarlequinPostgresConnection],
) -> None:
    connection = connect_again("options=-cstandard_conforming_strings%3Doff")
    driver = RecordingDriver()
    show_describe_relation(tricky_items.table, driver)
    show_list_objects(tricky_items.schema, driver)
    describe, listing = driver.new_buffers

    assert [row[0] for row in _rows(connection, describe)] == [tricky_name, "other"]
    assert {row[0] for row in _rows(connection, listing)} == {tricky_name}
