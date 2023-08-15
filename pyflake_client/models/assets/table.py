# -*- coding: utf-8 -*-
# pylint: disable=line-too-long
from dataclasses import dataclass, field
from typing import List, Union

from pyflake_client.models.assets.snowflake_asset_interface import ISnowflakeAsset
from pyflake_client.models.assets.snowflake_principal_interface import (
    ISnowflakePrincipal,
)
from pyflake_client.models.assets.table_columns import ClassificationTag, Column
from pyflake_client.models.describables.snowflake_grant_principal import (
    ISnowflakeGrantPrincipal,
)


@dataclass(frozen=True)
class Table(ISnowflakeAsset, ISnowflakeGrantPrincipal):
    db_name: str
    schema_name: str
    table_name: str
    columns: List[Column]
    tags: List[ClassificationTag] = field(default_factory=list)
    data_retention_time_days: int = 1
    owner: Union[ISnowflakePrincipal, None] = None

    def get_create_statement(self) -> str:
        table_identifier = f"{self.db_name}.{self.schema_name}.{self.table_name}"
        primary_keys = [c.name for c in self.columns if c.primary_key]

        table_definition = f"CREATE OR REPLACE TABLE {table_identifier}"
        table_definition += f" ({','.join(c.get_definition() for c in self.columns)}"
        if len(primary_keys) > 0:
            table_definition += f", PRIMARY KEY({','.join(primary_keys)})"

        table_definition += ");"

        if self.owner is not None:
            table_definition += f"GRANT OWNERSHIP ON TABLE {self.db_name}.{self.schema_name}.{self.table_name} TO {self.owner.get_snowflake_type()} {self.owner.get_identifier()};"

        for tag in self.tags:
            val = tag.tag_value or ""
            table_definition += f"ALTER TABLE {table_identifier} SET TAG {tag.get_identifier()} = '{val}';"

        for c in self.columns:
            if len(c.tags) > 0:
                for ct in c.tags:
                    value = ct.tag_value or ""
                    table_definition += f" ALTER TABLE {table_identifier} ALTER COLUMN {c.name}"
                    table_definition += f" SET TAG {ct.get_identifier()} = '{value}';"

        return table_definition

    def get_delete_statement(self) -> str:
        return f"drop table if exists {self.db_name}.{self.schema_name}.{self.table_name}"
