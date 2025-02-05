from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING

import psycopg
from psycopg.errors import DataError

# Subclass existing adapters so that the base case is handled normally.
from psycopg.types.datetime import (
    DateBinaryLoader,
    DateLoader,
    TimestampBinaryLoader,
    TimestampLoader,
    TimestamptzBinaryLoader,
    TimestamptzLoader,
)

if TYPE_CHECKING:
    from psycopg.adapt import Buffer, Loader


class InfDateLoader(DateLoader):
    def load(self, data: "Buffer") -> date:
        if data == b"infinity":
            return date.max
        elif data == b"-infinity":
            return date.min
        else:
            return super().load(data)


class InfDateBinaryLoader(DateBinaryLoader):
    def load(self, data: "Buffer") -> date:
        try:
            return super().load(data)
        except DataError as e:
            if "date too small" in str(e):
                return date.min
            elif "date too large" in str(e):
                return date.max
            raise e


class InfTimestampLoader(TimestampLoader):
    def load(self, data: "Buffer") -> datetime:
        if data == b"infinity":
            return datetime.max
        elif data == b"-infinity":
            return datetime.min
        else:
            return super().load(data)


class InfTimestampBinaryLoader(TimestampBinaryLoader):
    def load(self, data: "Buffer") -> datetime:
        try:
            return super().load(data)
        except DataError as e:
            if "timestamp too small" in str(e):
                return datetime.min
            elif "timestamp too large" in str(e):
                return datetime.max
            raise e


class InfTimestamptzLoader(TimestamptzLoader):
    def load(self, data: "Buffer") -> datetime:
        if data == b"infinity":
            return datetime.max
        elif data == b"-infinity":
            return datetime.min
        else:
            return super().load(data)


class InfTimestamptzBinaryLoader(TimestamptzBinaryLoader):
    def load(self, data: "Buffer") -> datetime:
        try:
            return super().load(data)
        except DataError as e:
            if "timestamp too small" in str(e):
                return datetime.min
            elif "timestamp too large" in str(e):
                return datetime.max
            raise e


INF_LOADERS: list[tuple[str, type["Loader"]]] = [
    ("date", InfDateLoader),
    ("date", InfDateBinaryLoader),
    ("timestamp", InfTimestampLoader),
    ("timestamp", InfTimestampBinaryLoader),
    ("timestamptz", InfTimestamptzLoader),
    ("timestamptz", InfTimestamptzBinaryLoader),
]


def register_inf_loaders() -> None:
    """
    Register updated date/datetime loaders in the global types
    registry, so that any connections created afterwards will use
    the updated loaders (that allow infinity date/timestamps)
    """
    for type_name, loader in INF_LOADERS:
        psycopg.adapters.register_loader(type_name, loader)
