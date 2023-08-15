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
from pyflake_client.models.entities.user import User as UserEntity


@dataclass(frozen=True)
class User(ISnowflakeDescribable, ISnowflakeGrantPrincipal):
    name: str

    def get_describe_statement(self) -> str:
        return f"SHOW USERS LIKE '{self.name}'".upper()

    def is_procedure(self) -> bool:
        return False

    @classmethod
    def get_deserializer(cls) -> Callable[[Dict[str, Any]], UserEntity]:
        def deserialize(data: Dict[str, Any]) -> UserEntity:
            return dacite.from_dict(
                UserEntity,
                data,
                dacite.Config(
                    type_hooks={
                        int: lambda v: int(v),
                    }
                ),
            )

        return deserialize

    @staticmethod
    def get_snowflake_type() -> str:
        return "USER"
