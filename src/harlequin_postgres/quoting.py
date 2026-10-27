from __future__ import annotations

from psycopg import sql


def quote_identifier(name: str) -> str:
    """name as a double-quoted Postgres identifier, with any `"` in it doubled."""
    return sql.Identifier(name).as_string()


def quote_qualified(*names: str) -> str:
    """Each of names as a quoted identifier, joined with dots, like `"s"."t"`."""
    return sql.Identifier(*names).as_string()


def quote_literal(value: str) -> str:
    """value as a Postgres string literal that reads the same under either setting
    of `standard_conforming_strings`.

    psycopg doubles `'`, and writes a value holding a backslash as `E'...'` with the
    backslash doubled; it pads that form with a leading space, which is stripped.
    """
    return sql.Literal(value).as_string().lstrip(" ")
