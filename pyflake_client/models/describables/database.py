# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import Any, Callable, Dict

import dacite

from pyflake_client.models.describables.snowflake_describable_interface import (
    ISnowflakeDescribable,
)
from pyflake_client.models.entities.database import Database as DatabaseEntity


@dataclass(frozen=True)
class Database(ISnowflakeDescribable):
    """Database"""

    name: str

    def get_describe_statement(self) -> str:
        """get_describe_statement"""
        return f"SHOW DATABASES LIKE '{self.name}'".upper()

    def is_procedure(self) -> bool:
        """is_procedure"""
        return False

    @classmethod
    def get_deserializer(cls) -> Callable[[Dict[str, Any]], DatabaseEntity]:
        def deserialize(data: Dict[str, Any]) -> DatabaseEntity:
            return dacite.from_dict(DatabaseEntity, data, dacite.Config(type_hooks={int: lambda v: int(v)}))

        return deserialize
