from __future__ import annotations

import re

from psycopg import sql

_BARE_IDENTIFIER = re.compile(r"[a-z_][a-z0-9_$]*")


def quote_identifier(name: str) -> str:
    """name as a double-quoted Postgres identifier, with any `"` in it doubled."""
    return sql.Identifier(name).as_string()


def quote_identifier_if_needed(name: str) -> str:
    """name bare if it reads the same unquoted, otherwise quoted.

    Keywords stay bare, because a function with keyword syntax, like
    `extract(year from ...)`, does not parse quoted.
    """
    if _BARE_IDENTIFIER.fullmatch(name):
        return name
    return quote_identifier(name)


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
