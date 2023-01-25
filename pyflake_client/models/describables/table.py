"""table"""
# pylint: disable=line-too-long

from dataclasses import dataclass

from dacite import Config

from pyflake_client.models.describables.snowflake_describable_interface import ISnowflakeDescribable


@dataclass(frozen=True)
class Table(ISnowflakeDescribable):
    """Table"""
    database_name: str
    schema_name: str
    name: str

    def get_describe_statement(self) -> str:
        """get_describe_statement"""
        return f"SHOW TABLES LIKE '{self.name}' IN SCHEMA {self.database_name}.{self.schema_name}".upper()

    def is_procedure(self) -> bool:
        """is_procedure"""
        return False

    def get_dacite_config(self) -> Config:
        return None
