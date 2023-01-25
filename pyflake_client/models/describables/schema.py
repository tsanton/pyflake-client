"""schema"""
from dataclasses import dataclass
from datetime import datetime

from dacite import Config

from pyflake_client.models.describables.snowflake_describable_interface import ISnowflakeDescribable


@dataclass(frozen=True)
class Schema(ISnowflakeDescribable):
    """Schema"""
    name: str
    database_name: str
    owner: str = None
    comment: str = None
    retention_time: str = None
    created_on: datetime = None

    def get_describe_statement(self) -> str:
        """get_describe_statement"""
        return f"SHOW SCHEMAS LIKE '{self.name}' IN DATABASE {self.database_name}".upper()

    def is_procedure(self) -> bool:
        """is_procedure"""
        return False

    def get_dacite_config(self) -> Config:
        return None
