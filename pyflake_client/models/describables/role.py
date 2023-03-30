"""role"""
from dataclasses import dataclass

from dacite import Config

from pyflake_client.models.describables.snowflake_describable_interface import (
    ISnowflakeDescribable,
)


@dataclass(frozen=True)
class Role(ISnowflakeDescribable):
    """Role"""

    name: str

    def get_describe_statement(self) -> str:
        """get_describe_statement"""
        return f"SHOW ROLES LIKE '{self.name}'".upper()

    def is_procedure(self) -> bool:
        """is_procedure"""
        return False

    def get_dacite_config(self) -> Config:
        return None
