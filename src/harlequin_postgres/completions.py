from __future__ import annotations

import csv
from pathlib import Path

from harlequin import HarlequinCompletion
from psycopg import Connection


def _get_completions(conn: Connection) -> list[HarlequinCompletion]:
    completions: list[HarlequinCompletion] = []

    # source: https://www.postgresql.org/docs/current/sql-keywords-appendix.html
    keyword_path = Path(__file__).parent / "keywords.tsv"
    with keyword_path.open("r") as f:
        keyword_reader = csv.reader(
            f,
            delimiter="\t",
        )
        _header = next(keyword_reader)
        for keyword, kind, _, _, _ in keyword_reader:
            completions.append(
                HarlequinCompletion(
                    label=keyword.lower(),
                    type_label="kw",
                    value=keyword.lower(),
                    priority=100 if kind.startswith("reserved") else 1000,
                    context=None,
                )
            )

    with conn.cursor() as cur:
        cur.execute(
            """
            select distinct
                routine_name as label,
                -- quote_ident() also quotes keywords like extract and left,
                -- which only parse unquoted, so a name that needs no quotes
                -- is inserted bare
                case
                    when routine_name ~ '^[a-z_][a-z0-9_$]*$' then routine_name
                    else quote_ident(routine_name)
                end as value,
                case when routine_type is null then 'agg' else 'fn' end as type_label,
                case
                    when routine_schema = 'pg_catalog'  --
                    then null
                    else routine_schema
                end as context
            from information_schema.routines
            where
                length(routine_name) < 37
                and routine_name not ilike %s
                and routine_name not ilike %s
                and routine_name not ilike %s
            ;""",
            ("\\_%", "pg\\_%", "binary\\_upgrade\\_%"),
        )
        results = cur.fetchall()
    for label, value, type_label, context in results:
        completions.append(
            HarlequinCompletion(
                label=label,
                type_label=type_label,
                value=value,
                priority=1000,
                context=context,
            )
        )

    with conn.cursor() as cur:
        cur.execute("""select distinct name as label from pg_settings""")
        results = cur.fetchall()
    for (label,) in results:
        completions.append(
            HarlequinCompletion(
                label=label, type_label="set", value=label, priority=2000, context=None
            )
        )

    return sorted(completions)
