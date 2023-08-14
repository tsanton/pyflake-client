# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import Union

import dacite

from pyflake_client.models.describables.snowflake_describable_interface import (
    ISnowflakeDescribable,
)


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

    def get_dacite_config(self) -> Union[dacite.Config, None]:
        """get_dacite_config"""
        return dacite.Config(type_hooks={int: lambda v: int(v)})
