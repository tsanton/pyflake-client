"""table"""
# pylint: disable=line-too-long
from dataclasses import dataclass
from typing import List, Union

from pyflake_client.models.assets.schema import Schema
from pyflake_client.models.assets.snowflake_asset_interface import ISnowflakeAsset
from pyflake_client.models.assets.table_columns import Column
from pyflake_client.models.assets.grants.snowflake_principle_interface import ISnowflakePrincipal


@dataclass(frozen=True)
class Table(ISnowflakeAsset):
    """Table"""
    schema: Schema
    table_name: str
    columns: List[Column]
    owner: ISnowflakePrincipal

    def get_create_statement(self) -> str:
        primary_keys = [c.name for c in self.columns if c.primary_key]

        table_definition = f"CREATE OR REPLACE TABLE {self.schema.database.db_name}.{self.schema.schema_name}.{self.table_name}"
        table_definition += f" ({','.join(c.get_definition() for c in self.columns)}"
        if len(primary_keys) > 0:
            table_definition += f", PRIMARY KEY({','.join(primary_keys)})"
        
        table_definition += ")"
        return table_definition


    def get_delete_statement(self) -> str:
        """get_delete_ddl"""
        return f"drop table if exists {self.schema.database.db_name}.{self.schema.schema_name}.{self.table_name}"
