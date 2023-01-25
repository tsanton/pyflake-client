"""table"""
# pylint: disable=line-too-long
from dataclasses import dataclass
from typing import List, Any
import re
from datetime import date, datetime

from pyflake_client.models.enums.column_type import ColumnType
from pyflake_client.models.assets.schema import Schema
from pyflake_client.models.assets.snowflake_asset_interface import ISnowflakeAsset


@dataclass(frozen=True)
class Column:
    """Column"""
    name: str
    type: ColumnType
    not_null: bool = True
    primary_key: bool = False
    unique: bool = False
    identity: bool = False
    default_value: Any = None

    def to_snowflake_column(self) -> str:
        """to_snowflake_column"""
        output: str = f"{self.name} "
        default_value = None
        match self.type:
            # TODO: More type assertion?
            case ColumnType.INTEGER:
                output += f"INTEGER {'IDENTITY (1,1)' if self.identity else '' } %s %s %s"
                default_value = self.default_value
            case ColumnType.VARCHAR:
                output += "VARCHAR(16777216) %s %s %s"
                default_value = f"'{self.default_value}'" if self.default_value is not None else None
            case ColumnType.BOOLEAN:
                output += "BOOLEAN %s %s %s"
                default_value = self.default_value
            case ColumnType.NUMBER:
                output += "NUMBER(38,37) %s %s %s"
                default_value = self.default_value
            case ColumnType.FLOAT:
                output += "FLOAT %s %s %s"
                default_value = self.default_value
            case ColumnType.DATE:
                output += "DATE %s %s %s"
                default_value = None
                if self.default_value is not None:
                    assert isinstance(self.default_value, date)
                    default_value = f"'{self.default_value.strftime('%Y-%m-%d')}'::date"
            case ColumnType.TIMESTAMP:
                output += "TIMESTAMP_NTZ(2) %s %s %s"
                default_value = None
                if self.default_value is not None:
                    assert isinstance(self.default_value, datetime)
                    default_value = f"'{self.default_value.strftime('%Y-%m-%dT%H:%M:%S.%f%z')}'::TIMESTAMP_NTZ(2)"
            case ColumnType.ARRAY:
                raise NotImplementedError("ARRAY not tested and implemented")
            case ColumnType.OBJECT:
                raise NotImplementedError("OBJECT not tested and implemented")
            case ColumnType.VARIANT:
                output += "VARIANT %s %s %s"
                default_value = self.default_value

        return re.sub(" +", " ", output % (
            "NOT NULL" if self.not_null else "",
            "UNIQUE" if self.unique else "",
            "DEFAULT " + default_value if default_value is not None else ''
        )).rstrip()


@dataclass(frozen=True)
class Table(ISnowflakeAsset):
    """Table"""
    schema: Schema
    table_name: str
    columns: List[Column]
    owner: str = None

    def get_create_statement(self) -> str:
        base = f"create or replace table {self.schema.database.db_name}.{self.schema.schema_name}.{self.table_name}"
        columns = ", ".join(x.to_snowflake_column() for x in self.columns)
        pk_columns = [x.name for x in self.columns if x.primary_key]
        primary_key = "PRIMARY KEY(" + ",".join(pk_columns) + ")" if len(pk_columns) > 0 else None
        # TODO: add ownership if not none
        return base + "(" + columns + (f", {primary_key}" if primary_key is not None else "") + ")"

    def get_delete_statement(self) -> str:
        """get_delete_ddl"""
        return f"drop table if exists {self.schema.database.db_name}.{self.schema.schema_name}.{self.table_name}"
