from __future__ import annotations

from textwrap import dedent
from typing import TYPE_CHECKING, Literal, Sequence

from harlequin.catalog import CatalogItem
from harlequin.exception import HarlequinQueryError

if TYPE_CHECKING:
    from harlequin.driver import HarlequinDriver

    from harlequin_postgres.catalog import (
        ColumnCatalogItem,
        DatabaseCatalogItem,
        RelationCatalogItem,
        SchemaCatalogItem,
        ViewCatalogItem,
    )


def execute_use_statement(
    item: "SchemaCatalogItem",
    driver: "HarlequinDriver",
) -> None:
    if item.connection is None:
        return
    try:
        item.connection.execute(f"set search_path to {item.qualified_identifier}")
    except HarlequinQueryError:
        driver.notify("Could not switch context", severity="error")
        raise
    else:
        driver.notify(f"Editor context switched to {item.label}")


def execute_drop_schema_statement(
    item: "SchemaCatalogItem",
    driver: "HarlequinDriver",
) -> None:
    def _drop_schema() -> None:
        if item.connection is None:
            return
        try:
            item.connection.execute(f"drop schema {item.qualified_identifier} cascade")
        except HarlequinQueryError:
            driver.notify(f"Could not drop schema {item.label}", severity="error")
            raise
        else:
            driver.notify(f"Dropped schema {item.label}")
            driver.refresh_catalog()

    if item.children or item.fetch_children():
        driver.confirm_and_execute(callback=_drop_schema)
    else:
        _drop_schema()


def execute_drop_database_statement(
    item: "DatabaseCatalogItem",
    driver: "HarlequinDriver",
) -> None:
    def _drop_database() -> None:
        if item.connection is None:
            return
        try:
            item.connection.execute(f"drop database {item.qualified_identifier}")
        except HarlequinQueryError:
            driver.notify(f"Could not drop database {item.label}", severity="error")
            raise
        else:
            driver.notify(f"Dropped database {item.label}")
            driver.refresh_catalog()

    if item.children or item.fetch_children():
        driver.confirm_and_execute(callback=_drop_database)
    else:
        _drop_database()


def execute_drop_relation_statement(
    item: "RelationCatalogItem",
    driver: "HarlequinDriver",
    relation_type: Literal["view", "table", "foreign table"],
) -> None:
    def _drop_relation() -> None:
        if item.connection is None:
            return
        try:
            item.connection.execute(f"drop {relation_type} {item.qualified_identifier}")
        except HarlequinQueryError:
            driver.notify(
                f"Could not drop {relation_type} {item.label}", severity="error"
            )
            raise
        else:
            driver.notify(f"Dropped {relation_type} {item.label}")
            driver.refresh_catalog()

    driver.confirm_and_execute(callback=_drop_relation)


def execute_drop_table_statement(
    item: "RelationCatalogItem", driver: "HarlequinDriver"
) -> None:
    execute_drop_relation_statement(item=item, driver=driver, relation_type="table")


def execute_drop_foreign_table_statement(
    item: "RelationCatalogItem", driver: "HarlequinDriver"
) -> None:
    execute_drop_relation_statement(
        item=item, driver=driver, relation_type="foreign table"
    )


def execute_drop_view_statement(
    item: "RelationCatalogItem", driver: "HarlequinDriver"
) -> None:
    execute_drop_relation_statement(item=item, driver=driver, relation_type="view")


def show_select_star(
    item: "RelationCatalogItem",
    driver: "HarlequinDriver",
) -> None:
    driver.insert_text_in_new_buffer(
        dedent(
            f"""
            select *
            from {item.qualified_identifier}
            limit 100
            """.strip("\n")
        )
    )


def show_list_objects(
    item: "SchemaCatalogItem" | "DatabaseCatalogItem",
    driver: "HarlequinDriver",
) -> None:
    # sourced from psql with -E, then the following command:
    # \dtvmsE+ <schema>.*

    # can't use isinstance due to circular reference
    if type(item).__name__ == "SchemaCatalogItem":
        where_clause = f"and n.nspname = '{item.label}'"
    else:
        where_clause = (
            "and n.nspname not in ('pg_catalog', 'pg_toast', 'information_schema')"
        )
    driver.insert_text_in_new_buffer(
        dedent(
            f"""
            select 
                n.nspname as "Schema",
                c.relname as "Name",
                case c.relkind 
                    when 'r' then 'table'
                    when 'v' then 'view' 
                    when 'm' then 'materialized view'
                    when 'S'
                    then 'sequence'
                    when 't' then 'TOAST table' 
                    when 'f' then 'foreign table' 
                    when 'p' then 'partitioned table' 
                end as "Type",
                pg_catalog.pg_get_userbyid(c.relowner) as "Owner",
                case c.relpersistence 
                    when 'p' then 'permanent' 
                    when 't' then 'temporary' 
                    when 'u' then 'unlogged' 
                end as "Persistence",
                am.amname as "Access method",
                pg_catalog.pg_size_pretty(pg_catalog.pg_table_size(c.oid)) as "Size",
                pg_catalog.obj_description(c.oid, 'pg_class') as "Description"
            from pg_catalog.pg_class c
                left join pg_catalog.pg_namespace n on n.oid = c.relnamespace
                left join pg_catalog.pg_am am on am.oid = c.relam
            where
                c.relkind IN ('r','p','t','v','m', 's', 'S', 'f')
                {where_clause}
            order by 1,2;
            """.strip("\n")
        )
    )


def show_list_indexes(
    item: "SchemaCatalogItem" | "DatabaseCatalogItem",
    driver: "HarlequinDriver",
) -> None:
    # sourced from psql with -E, then the following command:
    # \dis+ <schema>.*

    # can't use isinstance due to circular reference
    if type(item).__name__ == "SchemaCatalogItem":
        where_clause = f"and n.nspname = '{item.label}'"
    else:
        where_clause = (
            "and n.nspname not in ('pg_catalog', 'pg_toast', 'information_schema')"
        )
    driver.insert_text_in_new_buffer(
        dedent(
            f"""
            select
                n.nspname as "Schema",
                c.relname as "Name",
                case
                    c.relkind
                    when 'i'
                    then 'index'
                    when 'I'
                    then 'partitioned index'
                end as "Type",
                pg_catalog.pg_get_userbyid(c.relowner) as "Owner",
                c2.relname as "Table",
                case
                    c.relpersistence
                    when 'p'
                    then 'permanent'
                    when 't'
                    then 'temporary'
                    when 'u'
                    then 'unlogged'
                end as "Persistence",
                am.amname as "Access method",
                pg_catalog.pg_size_pretty(pg_catalog.pg_table_size(c.oid)) as "Size",
                pg_catalog.obj_description(c.oid, 'pg_class') as "Description"
            from pg_catalog.pg_class c
            left join pg_catalog.pg_namespace n on n.oid = c.relnamespace
            left join pg_catalog.pg_am am on am.oid = c.relam
            left join pg_catalog.pg_index i on i.indexrelid = c.oid
            left join pg_catalog.pg_class c2 on i.indrelid = c2.oid
            where
                c.relkind in ('i', 'I')
                {where_clause}
            order by 1,2;
            """.strip("\n")
        )
    )


def show_describe_relation(
    item: "RelationCatalogItem",
    driver: "HarlequinDriver",
) -> None:
    # sourced from psql -E \d+ {my rel}
    # see https://stackoverflow.com/questions/60155968/using-results-of-d-command-in-psql
    if item.parent is None:
        driver.notify(
            f"Could not describe {item.label} due to missing schema reference.",
            severity="error",
        )
        return
    driver.insert_text_in_new_buffer(
        dedent(
            f"""
            with
                index_columns as (
                    select i.indexrelid, c.oid as rel_oid, unnest(i.indkey) as attnum
                    from pg_catalog.pg_index i
                    join pg_catalog.pg_class c on c.oid = i.indrelid
                    where c.relname = '{item.label}'
                ),
                index_column_counts as (
                    select rel_oid, attnum, count(*) as cnt 
                    from index_columns 
                    group by 1, 2
                ),
                constraint_columns as (
                    select con.oid, c.oid as rel_oid, unnest(con.conkey) as attnum
                    from pg_catalog.pg_constraint con
                    join pg_catalog.pg_class c on con.conrelid = c.oid
                    where c.relname = '{item.label}'
                ),
                constraint_column_counts as (
                    select rel_oid, attnum, count(*) as cnt
                    from constraint_columns
                    group by 1, 2
                ),
                fkey_columns as (
                    select
                        src.relname as src_name,
                        src.relnamespace::regnamespace as src_schema,
                        c.oid as rel_oid,
                        unnest(con.confkey) as attnum
                    from pg_catalog.pg_constraint con
                    join pg_catalog.pg_class c on con.confrelid = c.oid
                    join pg_catalog.pg_class src on con.conrelid = src.oid
                    where c.relname = '{item.label}'
                ),
                fkey_references as (
                    select
                        rel_oid, 
                        attnum, 
                        string_agg(src_schema || '.' || src_name, ', ') as sources
                    from fkey_columns
                    group by 1, 2
                )
            select
                a.attname as "Column",
                pg_catalog.format_type(a.atttypid, a.atttypmod) as "Type",
                coll.collname as "Collation",
                case
                    when a.attnotnull is true then 'not null' 
                    else '' 
                end as "Nullable",
                pg_catalog.pg_get_expr(d.adbin, d.adrelid, true) as "Default",
                case
                    a.attstorage
                    when 'p'
                    then 'plain'
                    when 'x'
                    then 'extended'
                    when 'e'
                    then 'external'
                    when 'm'
                    then 'main'
                    else a.attstorage::text
                end as "Storage",
                case
                    a.attcompression
                    when 'p'
                    then 'pglz'
                    when 'l'
                    then 'LZ4'
                    else a.attcompression::text
                end as "Compression",
                case 
                    when a.attstattarget = -1
                    then null
                    else a.attstattarget
                end as "Stats target",
                case 
                    when index_column_counts.cnt > 0 then true else false
                end as "Has Index",
                case
                    when constraint_column_counts.cnt > 0 then true else false
                end as "Has Constraint",
                fkey_references.sources as "Referenced by",
                pg_catalog.col_description(a.attrelid, a.attnum) as "Description"
            from pg_catalog.pg_attribute a
            join pg_catalog.pg_class c on a.attrelid = c.oid
            left join pg_catalog.pg_namespace n on n.oid = c.relnamespace
            left join pg_catalog.pg_collation coll on coll.oid = a.attcollation
            left join
                pg_catalog.pg_type t
                on (t.oid = a.atttypid and t.typcollation <> a.attcollation)
            left join
                pg_catalog.pg_attrdef d
                on (a.attrelid = d.adrelid and a.attnum = d.adnum and a.atthasdef)
            left join
                index_column_counts
                on a.attnum = index_column_counts.attnum
                and a.attrelid = index_column_counts.rel_oid
            left join
                constraint_column_counts
                on a.attnum = constraint_column_counts.attnum
                and a.attrelid = constraint_column_counts.rel_oid
            left join
                fkey_references
                on a.attnum = fkey_references.attnum
                and a.attrelid = fkey_references.rel_oid
            where
                c.relname = '{item.label}'
                and n.nspname = '{item.parent.label}'
                and a.attnum > 0
                and not a.attisdropped
            order by a.attnum
            """.strip("\n")
        )
    )


def show_describe_table_indexes(
    item: "RelationCatalogItem",
    driver: "HarlequinDriver",
) -> None:
    if item.parent is None:
        driver.notify(
            f"Could not describe {item.label} due to missing schema reference.",
            severity="error",
        )
        return
    driver.insert_text_in_new_buffer(
        dedent(
            f"""
            with
                index_columns as (
                    select
                        i.indexrelid, c.oid as rel_oid, unnest(i.indkey) as attnum
                    from pg_catalog.pg_index i
                    join pg_catalog.pg_class c on c.oid = i.indrelid
                    where c.relname = '{item.label}'
                ),
                index_column_names as (
                    select
                        index_columns.indexrelid, 
                        string_agg(pg_attribute.attname, ', ') as columns
                    from index_columns
                    join
                        pg_catalog.pg_attribute
                        on index_columns.rel_oid = pg_attribute.attrelid
                        and index_columns.attnum = pg_attribute.attnum
                    group by 1
                )

            select
                c.relname as "Table",
                i.indexrelid::regclass::text as "Index Name",
                pg_am.amname as "Index Type",
                index_column_names.columns as "Columns",
                i.indisprimary as "Is PK",
                i.indisunique as "Is Unique",
                i.indisclustered as "Is Clustered",
                i.indisvalid as "Is Valid"
            from pg_catalog.pg_index i
            join pg_catalog.pg_class c on c.oid = i.indrelid
            left join pg_catalog.pg_namespace n on n.oid = c.relnamespace
            join pg_catalog.pg_class ic on i.indexrelid = ic.oid
            join pg_catalog.pg_am on ic.relam = pg_am.oid
            left join
                index_column_names on i.indexrelid = index_column_names.indexrelid
            where
                c.relname = '{item.label}'
                and n.nspname = '{item.parent.label}'
            """.strip("\n")
        )
    )


def show_describe_table_constraints(
    item: "RelationCatalogItem",
    driver: "HarlequinDriver",
) -> None:
    if item.parent is None:
        driver.notify(
            f"Could not describe {item.label} due to missing schema reference.",
            severity="error",
        )
        return
    driver.insert_text_in_new_buffer(
        dedent(
            f"""
            with
                constraint_columns as (
                    select con.oid, c.oid as rel_oid, unnest(con.conkey) as attnum
                    from pg_catalog.pg_constraint con
                    join pg_catalog.pg_class c on con.conrelid = c.oid
                    where c.relname = '{item.label}'
                ),
                constraint_column_names as (
                    select
                        constraint_columns.oid,
                        string_agg(pg_attribute.attname, ', ') as columns
                    from constraint_columns
                    join
                        pg_catalog.pg_attribute
                        on constraint_columns.rel_oid = pg_attribute.attrelid
                        and constraint_columns.attnum = pg_attribute.attnum
                    group by 1
                ),
                constraint_foreign_columns as (
                    select con.oid, c.oid as rel_oid, unnest(con.confkey) as attnum
                    from pg_catalog.pg_constraint con
                    join pg_catalog.pg_class c on con.conrelid = c.oid
                    where c.relname = '{item.label}'
                ),
                constraint_foreign_column_names as (
                    select
                        constraint_foreign_columns.oid,
                        string_agg(pg_attribute.attname, ', ') as columns
                    from constraint_foreign_columns
                    join
                        pg_catalog.pg_attribute
                        on constraint_foreign_columns.rel_oid = pg_attribute.attrelid
                        and constraint_foreign_columns.attnum = pg_attribute.attnum
                    group by 1
                )

            select
                c.relname as "Table",
                con.conname as "Constraint name",
                case
                    con.contype
                    when 'c'
                    then 'Check'
                    when 'n'
                    then 'Not Null'
                    when 'p'
                    then 'Primary Key'
                    when 'f'
                    then 'Foreign Key'
                    when 'u'
                    then 'Unique'
                    when 't'
                    then 'Trigger'
                    when 'x'
                    then 'Exclusion'
                    else con.contype::text
                end as "Constraint Type",
                constraint_column_names.columns as "Columns",
                c.relnamespace::regnamespace
                || '.'
                || fc.relname
                || '('
                || constraint_foreign_column_names.columns
                || ')' as "References",
                con.conislocal as "Is Local",
                con.convalidated as "Is Validated",
                case
                    con.confupdtype
                    when 'a'
                    then 'no action'
                    when 'r'
                    then 'restrict'
                    when 'c'
                    then 'cascade'
                    when 'n'
                    then 'set null'
                    when 'd'
                    then 'set default'
                    else con.confupdtype::text
                end as "FK Update Type",
                case
                    con.confdeltype
                    when 'a'
                    then 'no action'
                    when 'r'
                    then 'restrict'
                    when 'c'
                    then 'cascade'
                    when 'n'
                    then 'set null'
                    when 'd'
                    then 'set default'
                    else con.confdeltype::text
                end as "FK Delete Type",
                case
                    con.confmatchtype
                    when 's'
                    then 'simple'
                    when 'f'
                    then 'full'
                    when 'p'
                    then 'partial'
                    else con.confmatchtype::text
                end as "FK Match Type"
            from pg_catalog.pg_constraint con
            join pg_catalog.pg_class c on con.conrelid = c.oid
            join pg_catalog.pg_namespace n on n.oid = c.relnamespace
            left join pg_catalog.pg_class fc on con.confrelid = fc.oid
            left join
                constraint_column_names on con.oid = constraint_column_names.oid
            left join
                constraint_foreign_column_names
                on con.oid = constraint_foreign_column_names.oid
            where
                c.relname = '{item.label}'
                and n.nspname = '{item.parent.label}'
            """.strip("\n")
        )
    )


def show_view_definition(
    item: "ViewCatalogItem",
    driver: "HarlequinDriver",
) -> None:
    if item.connection is None or item.parent is None:
        return
    view_def_query = f"""
        select pg_catalog.pg_get_viewdef(c.oid, true)
        from pg_catalog.pg_class as c
        left join pg_catalog.pg_namespace n on n.oid = c.relnamespace
        where c.relname = '{item.label}' and n.nspname = '{item.parent.label}'
        """.strip("\n")
    cur = item.connection.execute(view_def_query)
    if cur is None:
        return
    result = cur.fetchall()
    if result is None:
        return
    view_def: str = result[0][0]
    driver.insert_text_in_new_buffer(
        f"-- View definition for {item.query_name}\n" + view_def
    )


def insert_columns_at_cursor(
    item: "RelationCatalogItem",
    driver: "HarlequinDriver",
) -> None:
    if item.loaded:
        cols: Sequence["CatalogItem" | "ColumnCatalogItem"] = item.children
    else:
        cols = item.fetch_children()
    driver.insert_text_at_selection(text=",\n".join(c.query_name for c in cols))
