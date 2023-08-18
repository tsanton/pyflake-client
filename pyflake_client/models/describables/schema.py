# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import Any, Callable, Dict

import dacite

from pyflake_client.models.describables.snowflake_describable_interface import (
    ISnowflakeDescribable,
)
from pyflake_client.models.entities.schema import Schema as SchemaEntity


@dataclass(frozen=True)
class Schema(ISnowflakeDescribable):
    name: str
    database_name: str

    def get_describe_statement(self) -> str:
        return f"SHOW SCHEMAS LIKE '{self.name}' IN DATABASE {self.database_name}".upper()

    def is_procedure(self) -> bool:
        return False

    @classmethod
    def get_deserializer(cls) -> Callable[[Dict[str, Any]], SchemaEntity]:
        def deserialize(data: Dict[str, Any]) -> SchemaEntity:
            return dacite.from_dict(SchemaEntity, data, dacite.Config(type_hooks={int: lambda v: int(v)}))

        return deserialize
