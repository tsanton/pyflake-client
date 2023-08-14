# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import Any, Callable, Dict

import dacite

from pyflake_client.models.describables.snowflake_describable_interface import (
    ISnowflakeDescribable,
)
from pyflake_client.models.entities.role import Role as RoleEntity


@dataclass(frozen=True)
class DatabaseRoles(ISnowflakeDescribable):
    """DatabaseRoles"""

    db_name: str

    def get_describe_statement(self) -> str:
        """get_describe_statement"""
        return f"SHOW DATABASE ROLES IN DATABASE {self.db_name};".upper()

    def is_procedure(self) -> bool:
        """is_procedure"""
        return False

    @classmethod
    def get_deserializer(cls) -> Callable[[Dict[str, Any]], RoleEntity]:
        def deserialize(data: Dict[str, Any]) -> RoleEntity:
            return dacite.from_dict(RoleEntity, data, None)

        return deserialize
