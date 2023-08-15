# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import Any, Callable, Dict

import dacite

from pyflake_client.models.describables.snowflake_describable_interface import (
    ISnowflakeDescribable,
)
from pyflake_client.models.describables.snowflake_grant_principal import (
    ISnowflakeGrantPrincipal,
)
from pyflake_client.models.entities.role import Role as RoleEntity


@dataclass(frozen=True)
class DatabaseRole(ISnowflakeDescribable, ISnowflakeGrantPrincipal):
    name: str
    db_name: str

    def get_describe_statement(self) -> str:
        return f"SHOW DATABASE ROLES LIKE '{self.name}' IN DATABASE {self.db_name};".upper()

    def is_procedure(self) -> bool:
        return False

    @classmethod
    def get_deserializer(cls) -> Callable[[Dict[str, Any]], RoleEntity]:
        def deserialize(data: Dict[str, Any]) -> RoleEntity:
            return dacite.from_dict(RoleEntity, data, None)

        return deserialize

    @staticmethod
    def get_snowflake_type() -> str:
        return "DATABASE ROLE"
