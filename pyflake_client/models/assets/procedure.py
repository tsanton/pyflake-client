"""procedure"""
from dataclasses import dataclass
from typing import List


from pyflake_client.models.assets.snowflake_asset_interface import ISnowflakeAsset
from pyflake_client.models.enums.column_type import ColumnType  # TODO: Rename ColumnType to DataType


@dataclass(frozen=False)
class Procedure(ISnowflakeAsset):
    """Procedure"""

    database_name: str
    schema_name: str
    name: str
    args: List[ColumnType]
    definition: str

    def get_create_statement(self) -> str:
        """get_create_statement"""
        return self.definition

    def get_delete_statement(self) -> str:
        """get_delete_ddl"""
        return f"drop procedure {self.database_name}.{self.schema_name}.{self.name}({', '.join(self.args)});"
