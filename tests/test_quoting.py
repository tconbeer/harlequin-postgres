from __future__ import annotations

import psycopg
import pytest

from harlequin_postgres.quoting import quote_identifier, quote_literal, quote_qualified

TEST_DB_CONN = "postgresql://postgres:for-testing@localhost:5432"

NAMES = [
    "plain",
    "Mixed Case",
    'dq"uote',
    "sq'uote",
    "back\\slash",
    "dot.ted",
    "semi;colon",
    "back`tick",
    "brack]et",
    "new\nline",
    "ünïcode",
]


@pytest.mark.parametrize(
    "name,expected",
    [
        ("plain", '"plain"'),
        ("Mixed Case", '"Mixed Case"'),
        ('dq"uote', '"dq""uote"'),
        ('""', '""""""'),
        ("sq'uote", '"sq\'uote"'),
        ("back\\slash", '"back\\slash"'),
        ("dot.ted", '"dot.ted"'),
    ],
)
def test_quote_identifier(name: str, expected: str) -> None:
    assert quote_identifier(name) == expected


def test_quote_qualified() -> None:
    assert quote_qualified("my schema", 'a"b') == '"my schema"."a""b"'


@pytest.mark.parametrize(
    "value,expected",
    [
        ("plain", "'plain'"),
        ("", "''"),
        ("sq'uote", "'sq''uote'"),
        ('dq"uote', "'dq\"uote'"),
        ("back\\slash", "E'back\\\\slash'"),
        ("both\\'", "E'both\\\\'''"),
    ],
)
def test_quote_literal(value: str, expected: str) -> None:
    assert quote_literal(value) == expected


@pytest.mark.parametrize("standard_conforming_strings", ["on", "off"])
def test_quoting_round_trips_through_the_server(
    standard_conforming_strings: str,
) -> None:
    with psycopg.connect(TEST_DB_CONN, autocommit=True) as conn:
        conn.execute(
            "select set_config('standard_conforming_strings', %s, false)",
            (standard_conforming_strings,),
        )
        for name in NAMES:
            cur = conn.execute(
                f"select {quote_literal(name)} as {quote_identifier(name)}"
            )
            assert cur.description is not None
            assert [column.name for column in cur.description] == [name]
            assert cur.fetchone() == (name,)
