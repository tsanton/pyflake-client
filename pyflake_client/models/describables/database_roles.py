# -*- coding: utf-8 -*-
from dataclasses import dataclass

from dacite import Config

from pyflake_client.models.describables.snowflake_describable_interface import (
    ISnowflakeDescribable,
)


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

    def get_dacite_config(self) -> Config:
        return None
