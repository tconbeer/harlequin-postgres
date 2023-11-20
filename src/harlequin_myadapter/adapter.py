from __future__ import annotations

from typing import Any, Sequence
from harlequin.adapter import HarlequinAdapter, HarlequinConnection, HarlequinCursor
from harlequin.catalog import Catalog, CatalogItem
from harlequin.exception import HarlequinConnectionError, HarlequinQueryError
from textual_fastdatatable.backend import AutoBackendType

from harlequin_myadapter.cli_options import MYADAPTER_OPTIONS


class MyCursor(HarlequinCursor):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.cur = args[0]
        self._limit: int | None = None

    def columns(self) -> list[tuple[str, str]]:
        names = self.cur.column_names
        types = self.cur.column_types
        return list(zip(names, types))

    def set_limit(self, limit: int) -> MyCursor:
        self._limit = limit

    def fetchall(self) -> AutoBackendType:
        try:
            if self._limit is not None:
                return self.cur.fetchall()
            else:
                return self.cur.fetchmany(self._limit)
        except Exception as e:
            raise HarlequinQueryError(
                msg=str(e),
                title="Harlequin encountered an error while executing your query.",
            )


class MyConnection(HarlequinConnection):
    def __init__(
        self, conn_str: Sequence[str], *args: Any, init_message: str = "", **kwargs: Any
    ) -> None:
        try:
            self.conn = "your database library's connect method goes here"
        except Exception as e:
            raise HarlequinConnectionError(
                msg=str(e), title="Harlequin could not connect to your database."
            )

    def execute(self, query: str) -> HarlequinCursor | None:
        try:
            cur = self.conn.execute(query)  # type: ignore
        except Exception as e:
            raise HarlequinQueryError(
                msg=str(e),
                title="Harlequin encountered an error while executing your query.",
            )
        else:
            if cur is not None:
                return MyCursor(cur)
            else:
                return None

    def get_catalog(self) -> Catalog:
        databases = self.conn.list_databases()
        db_items: list[CatalogItem] = []
        for db in databases:
            schemas = self.conn.list_schemas_in_db(db)
            schema_items: list[CatalogItem] = []
            for schema in schemas:
                relations = self.conn.list_relations_in_schema(schema)
                rel_items: list[CatalogItem] = []
                for rel, rel_type in relations:
                    cols = self.conn.list_columns_in_relation(rel)
                    col_items = [
                        CatalogItem(
                            qualified_identifier=f'"{db}"."{schema}"."{rel}"."{col}"',
                            query_name=f'"{col}"',
                            label=col,
                            type_label=col_type,
                        )
                        for col, col_type in cols
                    ]
                    rel_items.append(
                        CatalogItem(
                            qualified_identifier=f'"{db}"."{schema}"."{rel}"',
                            query_name=f'"{db}"."{schema}"."{rel}"',
                            label=rel,
                            type_label=rel_type,
                            children=col_items,
                        )
                    )
                schema_items.append(
                    CatalogItem(
                        qualified_identifier=f'"{db}"."{schema}"',
                        query_name=f'"{db}"."{schema}"',
                        label=schema,
                        type_label="s",
                        children=rel_items,
                    )
                )
            db_items.append(
                CatalogItem(
                    qualified_identifier=f'"{db}"',
                    query_name=f'"{db}"',
                    label=db,
                    type_label="db",
                    children=schema_items,
                )
            )
        return Catalog(items=db_items)


class MyAdapter(HarlequinAdapter):
    ADAPTER_OPTIONS = MYADAPTER_OPTIONS

    def __init__(self, conn_str: Sequence[str], **options: Any) -> None:
        self.conn_str = conn_str
        self.options = options

    def connect(self) -> MyConnection:
        conn = MyConnection(self.conn_str, self.options)
        return conn
