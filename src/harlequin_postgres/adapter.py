from __future__ import annotations

from itertools import cycle
from typing import Any, Sequence

from harlequin import (
    HarlequinAdapter,
    HarlequinCompletion,
    HarlequinConnection,
    HarlequinCursor,
    HarlequinTransactionMode,
)
from harlequin.catalog import (
    Catalog,
    CatalogItem,
    CatalogSearchKind,
    CatalogSearchResult,
)
from harlequin.exception import HarlequinConnectionError, HarlequinQueryError
from psycopg import Connection, Cursor, conninfo
from psycopg.errors import Error, QueryCanceled
from psycopg.pq import TransactionStatus
from psycopg_pool import ConnectionPool
from textual_fastdatatable.backend import AutoBackendType

from harlequin_postgres.catalog import (
    MATERIALIZED_VIEW,
    ColumnCatalogItem,
    DatabaseCatalogItem,
    ForeignCatalogItem,
    MaterializedViewCatalogItem,
    RelationCatalogItem,
    SchemaCatalogItem,
    TableCatalogItem,
    TempTableCatalogItem,
    ViewCatalogItem,
)
from harlequin_postgres.cli_options import POSTGRES_OPTIONS
from harlequin_postgres.completions import _get_completions
from harlequin_postgres.loaders import register_inf_loaders

_LIKE_ESCAPE = "\\"
"""What escapes a LIKE metacharacter in a term the caller typed."""


def _user_schemas(column: str) -> str:
    """The schemas the catalog shows, as a predicate on `column`.

    The same filter `_get_schemas()` applies, so a search only reports paths
    that `--catalog` also walks. The `%` is doubled because these queries are
    executed with parameters.
    """
    return (
        f"{column} != 'information_schema' and {column} not like 'pg\\_%%' escape '\\'"
    )


_SEARCH_DATABASES = """
select d.datname::text, null::text, null::text, null::text, null::text, null::text
from pg_database d
where
    d.datistemplate is false
    and d.datallowconn is true
    and d.datname ilike %s escape '\\'
"""
"""The databases on the server, which is the catalog's top level.

The pool is bound to one database, so every level below this one is the
connected database's; the others can still be matched by name, which is all
`_get_databases()` reads for them.
"""

_SEARCH_SCHEMAS = f"""
select
    s.catalog_name::text, s.schema_name::text,
    null::text, null::text, null::text, null::text
from information_schema.schemata s
where
    s.catalog_name = current_database()
    and {_user_schemas("s.schema_name")}
    and s.schema_name ilike %s escape '\\'
"""

_SEARCH_RELATIONS = f"""
select
    t.table_catalog::text, t.table_schema::text, t.table_name::text,
    t.table_type::text, null::text, null::text
from information_schema.tables t
where
    t.table_catalog = current_database()
    and {_user_schemas("t.table_schema")}
    and t.table_name ilike %s escape '\\'
"""

_SEARCH_MATERIALIZED_VIEWS = f"""
select
    current_database()::text, m.schemaname::text, m.matviewname::text,
    '{MATERIALIZED_VIEW}'::text, null::text, null::text
from pg_matviews m
where
    {_user_schemas("m.schemaname")}
    and m.matviewname ilike %s escape '\\'
"""
"""Matviews are not in information_schema, the same reason `_get_mvs()` exists."""

_SEARCH_COLUMNS = f"""
select
    c.table_catalog::text, c.table_schema::text, c.table_name::text,
    t.table_type::text, c.column_name::text, c.data_type::text
from information_schema.columns c
join information_schema.tables t
    on t.table_catalog = c.table_catalog
    and t.table_schema = c.table_schema
    and t.table_name = c.table_name
where
    c.table_catalog = current_database()
    and {_user_schemas("c.table_schema")}
    and c.column_name ilike %s escape '\\'
"""

_SEARCH_MATERIALIZED_VIEW_COLUMNS = f"""
select
    current_database()::text, s.nspname::text, t.relname::text,
    '{MATERIALIZED_VIEW}'::text, a.attname::text,
    pg_catalog.format_type(a.atttypid, a.atttypmod)::text
from pg_attribute a
join pg_class t on a.attrelid = t.oid
join pg_namespace s on t.relnamespace = s.oid
where
    t.relkind = 'm'
    and a.attnum > 0
    and not a.attisdropped
    and {_user_schemas("s.nspname")}
    and a.attname ilike %s escape '\\'
"""
"""The matview equivalent of `_SEARCH_COLUMNS`, shaped like `_get_mv_cols()`."""

_SEARCH_BRANCHES: dict[CatalogSearchKind, tuple[str, ...]] = {
    "relations": (_SEARCH_RELATIONS, _SEARCH_MATERIALIZED_VIEWS),
    "columns": (_SEARCH_COLUMNS, _SEARCH_MATERIALIZED_VIEW_COLUMNS),
    "all": (
        _SEARCH_DATABASES,
        _SEARCH_SCHEMAS,
        _SEARCH_RELATIONS,
        _SEARCH_MATERIALIZED_VIEWS,
        _SEARCH_COLUMNS,
        _SEARCH_MATERIALIZED_VIEW_COLUMNS,
    ),
}
"""Which levels each kind unions, every branch in the same six columns.

A row names one item by filling in the levels above it and leaving the rest
null, so `all` is every level of the catalog rather than the two at the bottom.
Each branch binds the pattern exactly once, in this order.
"""

_SEARCH_SQL = {
    kind: " union all ".join(branches)
    # `nulls first` explicitly, because Postgres sorts nulls last ascending:
    # without it a schema would arrive after the relations under it.
    + " order by 1, 2 nulls first, 3 nulls first, 5 nulls first"
    for kind, branches in _SEARCH_BRANCHES.items()
}
"""One query per kind, ordered so that an item arrives before its children."""


def _contains_pattern(term: str) -> str:
    """A term as the LIKE pattern that matches any label containing it."""
    escaped = term
    for character in (_LIKE_ESCAPE, "%", "_"):
        escaped = escaped.replace(character, f"{_LIKE_ESCAPE}{character}")
    return f"%{escaped}%"


class HarlequinPostgresCursor(HarlequinCursor):
    def __init__(self, conn: HarlequinPostgresConnection, cur: Cursor) -> None:
        self.conn = conn
        self.cur = cur
        # we need to copy the description from the cursor in case the results are
        # fetched and the cursor is closed before columns() is called.
        assert cur.description is not None
        self.description = cur.description.copy()
        self._limit: int | None = None

    def columns(self) -> list[tuple[str, str]]:
        return [
            (col.name, self.conn._short_column_type_from_oid(col.type_code))
            for col in self.description
        ]

    def set_limit(self, limit: int) -> HarlequinPostgresCursor:
        self._limit = limit
        return self

    def fetchall(self) -> AutoBackendType:
        try:
            if self._limit is None:
                return self.cur.fetchall()
            else:
                return self.cur.fetchmany(self._limit)
        except QueryCanceled:
            return []
        except Exception as e:
            raise HarlequinQueryError(
                msg=f"{e.__class__.__name__}: {e}",
                title="Harlequin encountered an error while executing your query.",
            ) from e
        finally:
            self.cur.close()


class HarlequinPostgresConnection(HarlequinConnection):
    def __init__(
        self,
        conn_str: Sequence[str],
        *_: Any,
        init_message: str = "",
        options: dict[str, Any],
        read_only: bool = False,
    ) -> None:
        self.init_message = init_message
        self.read_only = bool(read_only)
        try:
            self.conn_info = conninfo.conninfo_to_dict(
                conninfo=conn_str[0] if conn_str else "", **options
            )
        except Exception as e:
            raise HarlequinConnectionError(
                msg=str(e),
                title=(
                    "Harlequin could not connect to Postgres. "
                    "Invalid connection string."
                ),
            ) from e
        try:
            raw_timeout = self.conn_info.get("connect_timeout")
            timeout = float(raw_timeout) if raw_timeout is not None else 30.0
        except (TypeError, ValueError) as e:
            raise HarlequinConnectionError(
                msg=str(e),
                title=(
                    "Harlequin could not connect to Postgres. "
                    "Invalid value for connection_timeout."
                ),
            ) from e
        try:
            self.pool: ConnectionPool = ConnectionPool(
                conninfo=conn_str[0] if conn_str and conn_str[0] else "",
                min_size=2,
                max_size=5,
                kwargs=options,
                open=True,
                timeout=timeout,
                configure=self._configure_connection,
            )
            self._main_conn: Connection = self.pool.getconn()
        except Exception as e:
            raise HarlequinConnectionError(
                msg=str(e), title="Harlequin could not connect to Postgres."
            ) from e

        self._transaction_modes = cycle(
            [
                HarlequinTransactionMode(label="Auto"),
                HarlequinTransactionMode(
                    label="Manual",
                    commit=self.commit,
                    rollback=self.rollback,
                ),
            ]
        )
        self.toggle_transaction_mode()

        if self.read_only:
            try:
                self._assert_read_only()
            except HarlequinConnectionError:
                self.close()
                raise

    def _configure_connection(self, conn: Connection) -> None:
        """
        Called by the pool for every connection that it creates, so that read-only
        mode is enforced by every connection, not just the main one. Must leave
        the connection idle (outside of a transaction).
        """
        if not self.read_only:
            return
        # psycopg's conn.read_only only adds READ ONLY to the transactions that
        # psycopg itself begins, so it does nothing in an autocommit session,
        # where psycopg never issues a BEGIN. SET SESSION CHARACTERISTICS sets
        # default_transaction_read_only for the whole session instead, which the
        # server enforces in both of our transaction modes.
        conn.execute("set session characteristics as transaction read only;")
        conn.commit()
        # additionally declare the transactions psycopg begins READ ONLY, so
        # Manual mode is still read-only if the session setting is changed.
        conn.read_only = True

    def _assert_read_only(self) -> None:
        """
        Confirms the server is actually enforcing read-only transactions, so this
        adapter never claims to implement a guarantee that it did not deliver.
        """
        try:
            with self._main_conn.cursor() as cur:
                cur.execute("select current_setting('default_transaction_read_only');")
                result = cur.fetchone()
            setting = result[0] if result else None
        except Exception as e:
            raise HarlequinConnectionError(
                msg=str(e),
                title="Harlequin could not open a read-only connection to Postgres.",
            ) from e
        if setting != "on":
            raise HarlequinConnectionError(
                msg=(
                    "Harlequin requested a read-only connection, but this server "
                    "reports default_transaction_read_only is "
                    f"{setting!r}. Refusing to connect, since writes would not "
                    "be prevented."
                ),
                title="Harlequin could not open a read-only connection to Postgres.",
            )

    def execute(self, query: str) -> HarlequinCursor | None:
        if (
            self.transaction_mode.label != "Auto"
            and self._main_conn.info.transaction_status == TransactionStatus.IDLE
        ):
            cur = self._main_conn.cursor()
            cur.execute(query="begin;")
            cur.close()

        try:
            cur = self._main_conn.cursor()
            cur.execute(query=query)
        except QueryCanceled:
            cur.close()
            return None
        except Exception as e:
            msg_suffix = ""
            try:
                cur.close()
                self.rollback()
            except Exception:
                # likely connection is closed; error messages
                # can be cryptic, so help the user.
                msg_suffix = (
                    "\n\nYou may need to restart Harlequin to reconnect to the "
                    "database."
                )
            raise HarlequinQueryError(
                msg=f"{e}{msg_suffix}",
                title="Harlequin encountered an error while executing your query.",
            ) from e
        else:
            if cur.description is not None:
                return HarlequinPostgresCursor(self, cur)
            else:
                cur.close()
                return None

    def cancel(self) -> None:
        self._main_conn.cancel_safe()

    def commit(self) -> None:
        self._main_conn.commit()

    def rollback(self) -> None:
        self._main_conn.rollback()

    def get_catalog(self) -> Catalog:
        databases = self._get_databases()
        db_items: list[CatalogItem] = [
            DatabaseCatalogItem.from_label(label=db, connection=self)
            for (db,) in databases
        ]
        return Catalog(items=db_items)

    def search_catalog(
        self, term: str, kind: CatalogSearchKind = "all"
    ) -> list[CatalogSearchResult]:
        pattern = _contains_pattern(term)
        parameters = [pattern] * len(_SEARCH_BRANCHES[kind])
        try:
            with self.pool.connection() as conn, conn.cursor() as cur:
                cur.execute(_SEARCH_SQL[kind], parameters)
                found: list[
                    tuple[
                        str,
                        str | None,
                        str | None,
                        str | None,
                        str | None,
                        str | None,
                    ]
                ] = cur.fetchall()
        except Error as e:
            raise HarlequinQueryError(
                msg=str(e), title="Postgres raised an error searching the catalog:"
            ) from e

        databases: dict[str, DatabaseCatalogItem] = {}
        schemas: dict[tuple[str, str], SchemaCatalogItem] = {}
        relations: dict[tuple[str, str, str], RelationCatalogItem] = {}
        results: list[CatalogSearchResult] = []
        # a row names the deepest level it fills in, and carries its ancestors
        # so that each one is built once and the match knows its own path
        for catalog, schema, relation, relation_type, column, column_type in found:
            database_item = databases.setdefault(
                catalog, DatabaseCatalogItem.from_label(label=catalog, connection=self)
            )
            if schema is None:
                results.append(CatalogSearchResult(item=database_item))
                continue
            schema_item = schemas.setdefault(
                (catalog, schema),
                SchemaCatalogItem.from_parent(parent=database_item, label=schema),
            )
            if relation is None:
                results.append(
                    CatalogSearchResult(item=schema_item, parents=(catalog,))
                )
                continue
            relation_item = relations.setdefault(
                (catalog, schema, relation),
                self._relation_item(schema_item, relation, relation_type),
            )
            if column is None:
                results.append(
                    CatalogSearchResult(item=relation_item, parents=(catalog, schema))
                )
                continue
            results.append(
                CatalogSearchResult(
                    item=ColumnCatalogItem.from_parent(
                        parent=relation_item,
                        label=column,
                        type_label=self._short_column_type(column_type or ""),
                        type_name=column_type,
                    ),
                    parents=(catalog, schema, relation),
                )
            )
        return results

    @staticmethod
    def _relation_item(
        parent: SchemaCatalogItem, label: str, relation_type: str | None
    ) -> RelationCatalogItem:
        """A relation of the class `fetch_children()` would have built for it."""
        if relation_type == "VIEW":
            return ViewCatalogItem.from_parent(
                parent=parent, label=label, type_name=relation_type
            )
        if relation_type == "LOCAL TEMPORARY":
            return TempTableCatalogItem.from_parent(
                parent=parent, label=label, type_name=relation_type
            )
        if relation_type == "FOREIGN":
            return ForeignCatalogItem.from_parent(
                parent=parent, label=label, type_name=relation_type
            )
        if relation_type == MATERIALIZED_VIEW:
            return MaterializedViewCatalogItem.from_parent(
                parent=parent, label=label, type_name=relation_type
            )
        return TableCatalogItem.from_parent(
            parent=parent, label=label, type_name=relation_type
        )

    def get_completions(self) -> list[HarlequinCompletion]:
        with self.pool.connection() as conn:
            return _get_completions(conn)

    def close(self) -> None:
        self.pool.putconn(self._main_conn)
        self.pool.close()

    @property
    def transaction_mode(self) -> HarlequinTransactionMode:
        return self._transaction_mode

    def toggle_transaction_mode(self) -> HarlequinTransactionMode:
        self._transaction_mode = next(self._transaction_modes)
        self._sync_transaction_mode()
        return self._transaction_mode

    def _sync_transaction_mode(self) -> None:
        """
        Sync this class's transaction mode with the main connection
        """
        conn = self._main_conn
        if self.transaction_mode.label == "Auto":
            conn.autocommit = True
            conn.commit()
        else:
            conn.autocommit = False

    def _get_databases(self) -> list[tuple[str]]:
        with self.pool.connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                select datname
                from pg_database
                where
                    datistemplate is false
                    and datallowconn is true
                order by datname asc
                ;"""
            )
            results: list[tuple[str]] = cur.fetchall()
        return results

    def _get_schemas(self, dbname: str) -> list[tuple[str]]:
        with self.pool.connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                select schema_name
                from information_schema.schemata
                where
                    catalog_name = %s
                    and schema_name != 'information_schema'
                    and schema_name not like 'pg\\_%%' escape '\\'
                order by schema_name asc
                ;""",
                (dbname,),
            )
            results: list[tuple[str]] = cur.fetchall()
        return results

    def _get_relations(self, dbname: str, schema: str) -> list[tuple[str, str]]:
        with self.pool.connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                select table_name, table_type
                from information_schema.tables
                where
                    table_catalog = %s
                    and table_schema = %s
                order by table_name asc
                ;""",
                (dbname, schema),
            )
            results: list[tuple[str, str]] = cur.fetchall()
        return results

    # only works for the currently-connected db
    def _get_mvs(self, schema: str) -> list[tuple[str]]:
        with self.pool.connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                select matviewname
                from pg_matviews
                where
                    schemaname = %s
                order by matviewname asc
                ;""",
                (schema,),
            )
            results: list[tuple[str]] = cur.fetchall()
        return results

    def _get_columns(
        self, dbname: str, schema: str, relation: str
    ) -> list[tuple[str, str]]:
        with self.pool.connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                select column_name, data_type
                from information_schema.columns
                where
                    table_catalog = %s
                    and table_schema = %s
                    and table_name = %s
                order by ordinal_position asc
                ;""",
                (dbname, schema, relation),
            )
            results: list[tuple[str, str]] = cur.fetchall()
        return results

    def _get_mv_cols(self, schema: str, mv: str) -> list[tuple[str, str]]:
        with self.pool.connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                select 
                    a.attname,
                    pg_catalog.format_type(a.atttypid, a.atttypmod)
                from pg_attribute a
                join pg_class t on a.attrelid = t.oid
                join pg_namespace s on t.relnamespace = s.oid
                where 
                    a.attnum > 0 
                    and not a.attisdropped
                    and s.nspname = %s
                    and t.relname = %s
                order by a.attnum;
                ;""",
                (schema, mv),
            )
            results: list[tuple[str, str]] = cur.fetchall()
        return results

    @staticmethod
    def _short_column_type(type_name: str) -> str:
        MAPPING = {
            "bigint": "##",
            "bigserial": "##",
            "bit": "010",
            "boolean": "t/f",
            "box": "□",
            "bytea": "b",
            "character": "s",
            "cidr": "ip",
            "circle": "○",
            "date": "d",
            "double": "#.#",
            "inet": "ip",
            "integer": "#",
            "interval": "|-|",
            "json": "{}",
            "jsonb": "b{}",
            "line": "—",
            "lseg": "-",
            "macaddr": "mac",
            "macaddr8": "mac",
            "money": "$$",
            "numeric": "#.#",
            "path": "╭",
            "pg_lsn": "lsn",
            "pg_snapshot": "snp",
            "point": "•",
            "polygon": "▽",
            "real": "#.#",
            "smallint": "#",
            "smallserial": "#",
            "serial": "#",
            "text": "s",
            "time": "t",
            "timestamp": "ts",
            "tsquery": "tsq",
            "tsvector": "tsv",
            "txid_snapshot": "snp",
            "uuid": "uid",
            "xml": "xml",
            "array": "[]",
        }
        return MAPPING.get(type_name.split("(")[0].split(" ")[0], "?")

    @staticmethod
    def _short_column_type_from_oid(oid: int) -> str:
        MAPPING = {
            16: "t/f",
            17: "b",
            18: "s",
            19: "s",
            20: "##",
            21: "#",
            22: "[#]",
            23: "#",
            25: "s",
            26: "oid",
            114: "{}",
            142: "xml",
            600: "•",
            601: "-",
            602: "╭",
            603: "□",
            604: "▽",
            628: "—",
            651: "[ip]",
            700: "#.#",
            701: "#.#",
            704: "|-|",
            718: "○",
            790: "$$",
            829: "mac",
            869: "ip",
            650: "ip",
            774: "mac",
            1000: "[t/f]",
            1001: "[b]",
            1002: "[s]",
            1003: "[s]",
            1009: "[s]",
            1013: "[oid]",
            1014: "[s]",
            1015: "[s]",
            1016: "[#]",
            1021: "[#.#]",
            1022: "[#.#]",
            1028: "[oid]",
            1040: "[mac]",
            1041: "[ip]",
            1042: "s",
            1043: "s",
            1082: "d",
            1083: "t",
            1114: "ts",
            1115: "[ts]",
            1182: "[d]",
            1183: "[t]",
            1184: "ts",
            1185: "[ts]",
            1186: "|-|",
            1187: "[|-|]",
            1231: "[#.#]",
            1266: "t",
            1270: "[t]",
            1560: "010",
            1562: "010",
            1700: "#.#",
            2950: "uid",
            3614: "tsv",
            3615: "tsq",
            3802: "b{}",
        }
        return MAPPING.get(oid, "?")


class HarlequinPostgresAdapter(HarlequinAdapter):
    ADAPTER_OPTIONS = POSTGRES_OPTIONS
    IMPLEMENTS_CANCEL = True
    IMPLEMENTS_CATALOG_SEARCH = True
    IMPLEMENTS_READ_ONLY = True

    def __init__(
        self,
        conn_str: Sequence[str],
        read_only: bool = False,
        host: str | None = None,
        port: str | None = None,
        dbname: str | None = None,
        user: str | None = None,
        password: str | None = None,
        passfile: str | None = None,
        require_auth: str | None = None,
        channel_binding: str | None = None,
        connect_timeout: int | float | None = None,
        sslmode: str | None = None,
        sslcert: str | None = None,
        sslkey: str | None = None,
        **_: Any,
    ) -> None:
        self.conn_str = conn_str
        self.read_only = bool(read_only)
        self.options: dict[str, str | int | None] = {
            "host": host,
            "port": port,
            "dbname": dbname,
            "user": user,
            "password": password,
            "passfile": passfile,
            "require_auth": require_auth,
            "channel_binding": channel_binding,
            "connect_timeout": connect_timeout,  # type: ignore[dict-item]
            "sslmode": sslmode,
            "sslcert": sslcert,
            "sslkey": sslkey,
        }

    @property
    def connection_id(self) -> str | None:
        """
        Use a simplified connection string, with only the host, port, and database
        """
        try:
            conn_info = conninfo.conninfo_to_dict(
                conninfo=self.conn_str[0] if self.conn_str else "",
                **self.options,
            )
        except Exception:
            return None

        host = conn_info.get("host", "localhost")
        port = conn_info.get("port", "5432")
        dbname = conn_info.get("dbname", "postgres")
        return f"{host}:{port}/{dbname}"

    def connect(self) -> HarlequinPostgresConnection:
        if len(self.conn_str) > 1:
            raise HarlequinConnectionError(
                "Cannot provide multiple connection strings to the Postgres adapter. "
                f"{self.conn_str}"
            )
        # before creating the connection, register updated type adapters, so
        # all subsequent connections will use those adapters
        register_inf_loaders()
        conn = HarlequinPostgresConnection(
            self.conn_str, options=self.options, read_only=self.read_only
        )
        return conn
