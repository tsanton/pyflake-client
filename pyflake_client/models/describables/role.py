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
class Role(ISnowflakeDescribable, ISnowflakeGrantPrincipal):
    name: str

    def get_describe_statement(self) -> str:
        return f"SHOW ROLES LIKE '{self.name}'".upper()

    def is_procedure(self) -> bool:
        return False

    @classmethod
    def get_deserializer(cls) -> Callable[[Dict[str, Any]], RoleEntity]:
        def deserialize(data: Dict[str, Any]) -> RoleEntity:
            return dacite.from_dict(RoleEntity, data, dacite.Config(type_hooks={int: lambda v: int(v)}))

        return deserialize

    @staticmethod
    def get_snowflake_type() -> str:
        return "ROLE"
