"""table"""
# pylint: disable=line-too-long
from dataclasses import dataclass, field
from typing import List, Union

from pyflake_client.models.assets.schema import Schema
from pyflake_client.models.assets.snowflake_asset_interface import ISnowflakeAsset
from pyflake_client.models.assets.table_columns import Column, ClassificationTag
from pyflake_client.models.assets.snowflake_principal_interface import ISnowflakePrincipal
from pyflake_client.models.describables.snowflake_grant_principal import ISnowflakeGrantPrincipal

@dataclass(frozen=True)
class Table(ISnowflakeAsset, ISnowflakeGrantPrincipal):
    """Table"""

    schema: Schema
    table_name: str
    columns: List[Column]
    tags: List[ClassificationTag] = field(default_factory=list)
    data_retention_time_days: int = 1
    owner: Union[ISnowflakePrincipal, None] = None

    def get_create_statement(self) -> str:
        if self.owner is None:
            raise ValueError("Create statement not supported for owner-less tables")
        
        table_identifier = f"{self.schema.database.db_name}.{self.schema.schema_name}.{self.table_name}"
        primary_keys = [c.name for c in self.columns if c.primary_key]

        table_definition = f"CREATE OR REPLACE TABLE {table_identifier}"
        table_definition += f" ({','.join(c.get_definition() for c in self.columns)}"
        if len(primary_keys) > 0:
            table_definition += f", PRIMARY KEY({','.join(primary_keys)})"

        table_definition += ");"
        for tag in self.tags:
            val = tag.tag_value or ""
            table_definition += f"ALTER TABLE {table_identifier} SET TAG {tag.get_identifier()} = '{val}';"

        for c in self.columns:
            if len(c.tags) > 0:
                for ct in c.tags:
                    value = ct.tag_value or ""
                    table_definition += (
                        f" ALTER TABLE {table_identifier} ALTER COLUMN {c.name}"
                    )
                    table_definition += f" SET TAG {ct.get_identifier()} = '{value}';"

        return table_definition

    def get_delete_statement(self) -> str:
        """get_delete_ddl"""
        return f"drop table if exists {self.schema.database.db_name}.{self.schema.schema_name}.{self.table_name}"
