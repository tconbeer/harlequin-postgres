from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from harlequin.catalog import InteractiveCatalogItem

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
    from harlequin_postgres.adapter import HarlequinPostgresConnection


@dataclass
class ColumnCatalogItem(InteractiveCatalogItem["HarlequinPostgresConnection"]):
    parent: "RelationCatalogItem" | None = None

    @classmethod
    def from_parent(
        cls,
        parent: "RelationCatalogItem",
        label: str,
        type_label: str,
    ) -> "ColumnCatalogItem":
        column_qualified_identifier = f'{parent.qualified_identifier}."{label}"'
        column_query_name = f'"{label}"'
        return cls(
            qualified_identifier=column_qualified_identifier,
            query_name=column_query_name,
            label=label,
            type_label=type_label,
            connection=parent.connection,
            parent=parent,
            loaded=True,
        )


@dataclass
class RelationCatalogItem(InteractiveCatalogItem["HarlequinPostgresConnection"]):
    INTERACTIONS = [
        ("Insert Columns at Cursor", insert_columns_at_cursor),
        ("Preview Data", show_select_star),
        ("Describe Relation (\\d+)", show_describe_relation),
    ]
    parent: "SchemaCatalogItem" | None = None

    def fetch_children(self) -> list[ColumnCatalogItem]:
        if self.parent is None or self.parent.parent is None or self.connection is None:
            return []
        result = self.connection._get_columns(
            self.parent.parent.label, self.parent.label, self.label
        )
        return [
            ColumnCatalogItem.from_parent(
                parent=self,
                label=column_name,
                type_label=self.connection._short_column_type(column_type),
            )
            for column_name, column_type in result
        ]


class ViewCatalogItem(RelationCatalogItem):
    INTERACTIONS = RelationCatalogItem.INTERACTIONS + [
        ("Show View Definition", show_view_definition),
        ("Drop View", execute_drop_view_statement),
    ]

    @classmethod
    def from_parent(
        cls,
        parent: "SchemaCatalogItem",
        label: str,
    ) -> "ViewCatalogItem":
        relation_query_name = f'"{parent.label}"."{label}"'
        relation_qualified_identifier = f'{parent.qualified_identifier}."{label}"'
        return cls(
            qualified_identifier=relation_qualified_identifier,
            query_name=relation_query_name,
            label=label,
            type_label="v",
            connection=parent.connection,
            parent=parent,
        )


class TableCatalogItem(RelationCatalogItem):
    INTERACTIONS = RelationCatalogItem.INTERACTIONS + [
        ("Describe Indexes", show_describe_table_indexes),
        ("Describe Constraints", show_describe_table_constraints),
        ("Drop Table", execute_drop_table_statement),
    ]

    @classmethod
    def from_parent(
        cls,
        parent: "SchemaCatalogItem",
        label: str,
    ) -> "TableCatalogItem":
        relation_query_name = f'"{parent.label}"."{label}"'
        relation_qualified_identifier = f'{parent.qualified_identifier}."{label}"'
        return cls(
            qualified_identifier=relation_qualified_identifier,
            query_name=relation_query_name,
            label=label,
            type_label="t",
            connection=parent.connection,
            parent=parent,
        )


class TempTableCatalogItem(TableCatalogItem):
    @classmethod
    def from_parent(
        cls,
        parent: "SchemaCatalogItem",
        label: str,
    ) -> "TempTableCatalogItem":
        relation_query_name = f'"{parent.label}"."{label}"'
        relation_qualified_identifier = f'{parent.qualified_identifier}."{label}"'
        return cls(
            qualified_identifier=relation_qualified_identifier,
            query_name=relation_query_name,
            label=label,
            type_label="tmp",
            connection=parent.connection,
            parent=parent,
        )


class ForeignCatalogItem(TableCatalogItem):
    INTERACTIONS = RelationCatalogItem.INTERACTIONS + [
        ("Drop Table", execute_drop_foreign_table_statement),
    ]

    @classmethod
    def from_parent(
        cls,
        parent: "SchemaCatalogItem",
        label: str,
    ) -> "ForeignCatalogItem":
        relation_query_name = f'"{parent.label}"."{label}"'
        relation_qualified_identifier = f'{parent.qualified_identifier}."{label}"'
        return cls(
            qualified_identifier=relation_qualified_identifier,
            query_name=relation_query_name,
            label=label,
            type_label="f",
            connection=parent.connection,
            parent=parent,
        )


@dataclass
class SchemaCatalogItem(InteractiveCatalogItem["HarlequinPostgresConnection"]):
    INTERACTIONS = [
        ("Set Search Path", execute_use_statement),
        ("List Relations (\\d+)", show_list_objects),
        ("List Indexes (\\di+)", show_list_indexes),
        ("Drop Schema", execute_drop_schema_statement),
    ]
    parent: "DatabaseCatalogItem" | None = None

    @classmethod
    def from_parent(
        cls,
        parent: "DatabaseCatalogItem",
        label: str,
    ) -> "SchemaCatalogItem":
        schema_identifier = f'"{label}"'
        return cls(
            qualified_identifier=schema_identifier,
            query_name=schema_identifier,
            label=label,
            type_label="sch",
            connection=parent.connection,
            parent=parent,
        )

    def fetch_children(self) -> list[RelationCatalogItem]:
        if self.parent is None or self.connection is None:
            return []
        children: list[RelationCatalogItem] = []
        result = self.connection._get_relations(self.parent.label, self.label)
        for table_label, table_type in result:
            if table_type == "VIEW":
                children.append(
                    ViewCatalogItem.from_parent(
                        parent=self,
                        label=table_label,
                    )
                )
            elif table_type == "LOCAL TEMPORARY":
                children.append(
                    TempTableCatalogItem.from_parent(
                        parent=self,
                        label=table_label,
                    )
                )
            elif table_type == "FOREIGN":
                children.append(
                    ForeignCatalogItem.from_parent(
                        parent=self,
                        label=table_label,
                    )
                )
            else:
                children.append(
                    TableCatalogItem.from_parent(
                        parent=self,
                        label=table_label,
                    )
                )

        return children


class DatabaseCatalogItem(InteractiveCatalogItem["HarlequinPostgresConnection"]):
    INTERACTIONS = [
        ("List Relations (\\d+)", show_list_objects),
        ("List Indexes (\\di+)", show_list_indexes),
        ("Drop Database", execute_drop_database_statement),
    ]

    @classmethod
    def from_label(
        cls, label: str, connection: "HarlequinPostgresConnection"
    ) -> "DatabaseCatalogItem":
        database_identifier = f'"{label}"'
        return cls(
            qualified_identifier=database_identifier,
            query_name=database_identifier,
            label=label,
            type_label="db",
            connection=connection,
        )

    def fetch_children(self) -> list[SchemaCatalogItem]:
        if self.connection is None:
            return []
        schemas = self.connection._get_schemas(self.label)
        return [
            SchemaCatalogItem.from_parent(
                parent=self,
                label=schema_label,
            )
            for (schema_label,) in schemas
        ]
