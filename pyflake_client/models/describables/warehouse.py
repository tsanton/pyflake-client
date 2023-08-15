# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import Any, Callable, Dict

import dacite

from pyflake_client.models.describables.snowflake_describable_interface import (
    ISnowflakeDescribable,
)
from pyflake_client.models.entities.warehouse import Warehouse as WarehouseEntity


@dataclass(frozen=True)
class Warehouse(ISnowflakeDescribable):
    name: str

    def get_describe_statement(self) -> str:
        return f"SHOW WAREHOUSES LIKE '{self.name}'".upper()

    def is_procedure(self) -> bool:
        return False

    @classmethod
    def get_deserializer(cls) -> Callable[[Dict[str, Any]], WarehouseEntity]:
        def deserialize(data: Dict[str, Any]) -> WarehouseEntity:
            return dacite.from_dict(
                WarehouseEntity,
                data,
                dacite.Config(
                    type_hooks={
                        int: lambda v: int(v),
                    }
                ),
            )

        return deserialize
