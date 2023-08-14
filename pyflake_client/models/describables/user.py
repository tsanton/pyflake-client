# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import Union

import dacite

from pyflake_client.models.describables.snowflake_describable_interface import (
    ISnowflakeDescribable,
)
from pyflake_client.models.describables.snowflake_grant_principal import (
    ISnowflakeGrantPrincipal,
)


@dataclass(frozen=True)
class User(ISnowflakeDescribable, ISnowflakeGrantPrincipal):
    """User"""

    name: str

    def get_describe_statement(self) -> str:
        """get_describe_statement"""
        return f"SHOW USERS LIKE '{self.name}'".upper()

    def is_procedure(self) -> bool:
        """is_procedure"""
        return False

    def get_dacite_config(self) -> Union[dacite.Config, None]:
        """get_dacite_config"""
        return None

    @staticmethod
    def get_snowflake_type() -> str:
        """get_snowflake_type"""
        return "USER"
