from __future__ import annotations

from typing import Any, Sequence

from harlequin.adapter import HarlequinAdapter, HarlequinConnection, HarlequinCursor
from harlequin.catalog import Catalog, CatalogItem
from harlequin.exception import HarlequinConnectionError, HarlequinQueryError
from psycopg2.extensions import connection, cursor
from psycopg2.pool import ThreadedConnectionPool
from textual_fastdatatable.backend import AutoBackendType

from harlequin_postgres.cli_options import POSTGRES_OPTIONS


class HarlequinPostgresCursor(HarlequinCursor):
    def __init__(self, conn: HarlequinPostgresConnection, cur: cursor) -> None:
        self.conn = conn
        self.cur = cur
        self._limit: int | None = None

    def columns(self) -> list[tuple[str, str]]:
        assert self.cur.description is not None
        return [
            (col.name, self.conn._get_short_type_from_oid(col.type_code))
            for col in self.cur.description
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
        except Exception as e:
            raise HarlequinQueryError(
                msg=str(e),
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
    ) -> None:
        self.init_message = init_message
        try:
            if conn_str and conn_str[0]:
                self.pool: ThreadedConnectionPool = ThreadedConnectionPool(
                    1, 5, dsn=conn_str[0], **options
                )
            else:
                self.pool = ThreadedConnectionPool(1, 5, **options)
        except Exception as e:
            raise HarlequinConnectionError(
                msg=str(e), title="Harlequin could not connect to Postgres."
            ) from e

    def execute(self, query: str) -> HarlequinCursor | None:
        try:
            conn: connection = self.pool.getconn(key="main")
            with conn:  # autocommit transaction
                cur = conn.cursor()
                cur.execute(query=query)
        except Exception as e:
            cur.close()
            raise HarlequinQueryError(
                msg=str(e),
                title="Harlequin encountered an error while executing your query.",
            ) from e
        else:
            if cur.description is not None:
                return HarlequinPostgresCursor(self, cur)
            else:
                cur.close()
                return None
        finally:
            self.pool.putconn(conn, key="main")

    def get_catalog(self) -> Catalog:
        databases = self._get_databases()
        db_items: list[CatalogItem] = []
        for (db,) in databases:
            schemas = self._get_schemas(db)
            schema_items: list[CatalogItem] = []
            for (schema,) in schemas:
                relations = self._get_relations(db, schema)
                rel_items: list[CatalogItem] = []
                for rel, rel_type in relations:
                    cols = self._get_columns(db, schema, rel)
                    col_items = [
                        CatalogItem(
                            qualified_identifier=f'"{db}"."{schema}"."{rel}"."{col}"',
                            query_name=f'"{col}"',
                            label=col,
                            type_label=self._get_short_type(col_type),
                        )
                        for col, col_type in cols
                    ]
                    rel_items.append(
                        CatalogItem(
                            qualified_identifier=f'"{db}"."{schema}"."{rel}"',
                            query_name=f'"{db}"."{schema}"."{rel}"',
                            label=rel,
                            type_label="v" if rel_type == "VIEW" else "t",
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

    def _get_databases(self) -> list[tuple[str]]:
        conn: connection = self.pool.getconn()
        with conn.cursor() as cur:
            cur.execute(
                """
                select datname
                from pg_database
                where
                    datistemplate is false
                    and datallowconn is true
                ;"""
            )
            results = cur.fetchall()
        self.pool.putconn(conn)
        return results

    def _get_schemas(self, dbname: str) -> list[tuple[str]]:
        conn: connection = self.pool.getconn()
        with conn.cursor() as cur:
            cur.execute(
                f"""
                select schema_name
                from information_schema.schemata
                where
                    catalog_name = '{dbname}'
                    and schema_name != 'information_schema'
                    and schema_name not like 'pg_%'
                ;"""
            )
            results = cur.fetchall()
        self.pool.putconn(conn)
        return results

    def _get_relations(self, dbname: str, schema: str) -> list[tuple[str, str]]:
        conn: connection = self.pool.getconn()
        with conn.cursor() as cur:
            cur.execute(
                f"""
                select table_name, table_type
                from information_schema.tables
                where
                    table_catalog = '{dbname}'
                    and table_schema = '{schema}'
                ;"""
            )
            results = cur.fetchall()
        self.pool.putconn(conn)
        return results

    def _get_columns(
        self, dbname: str, schema: str, relation: str
    ) -> list[tuple[str, str]]:
        conn: connection = self.pool.getconn()
        with conn.cursor() as cur:
            cur.execute(
                f"""
                select column_name, data_type
                from information_schema.columns
                where
                    table_catalog = '{dbname}'
                    and table_schema = '{schema}'
                    and table_name = '{relation}'
                ;"""
            )
            results = cur.fetchall()
        self.pool.putconn(conn)
        return results

    @staticmethod
    def _get_short_type(type_name: str) -> str:
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
    def _get_short_type_from_oid(oid: int) -> str:
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

    def __init__(
        self,
        conn_str: Sequence[str],
        host: str | None = None,
        port: str | None = None,
        dbname: str | None = None,
        user: str | None = None,
        password: str | None = None,
        passfile: str | None = None,
        require_auth: str | None = None,
        channel_binding: str | None = None,
        connect_timeout: int | None = None,
        sslmode: str | None = None,
        sslcert: str | None = None,
        sslkey: str | None = None,
        **_: Any,
    ) -> None:
        self.conn_str = conn_str
        self.options = {
            "host": host,
            "port": port,
            "dbname": dbname,
            "user": user,
            "password": password,
            "passfile": passfile,
            "require_auth": require_auth,
            "channel_binding": channel_binding,
            "connect_timeout": connect_timeout,
            "sslmode": sslmode,
            "sslcert": sslcert,
            "sslkey": sslkey,
        }

    def connect(self) -> HarlequinPostgresConnection:
        if len(self.conn_str) > 1:
            raise HarlequinConnectionError(
                "Cannot provide multiple connection strings to the Postgres adapter. "
                f"{self.conn_str}"
            )
        conn = HarlequinPostgresConnection(self.conn_str, options=self.options)
        return conn
