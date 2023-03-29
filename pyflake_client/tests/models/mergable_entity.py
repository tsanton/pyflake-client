"""mergable_entity"""
# pylint: disable=line-too-long
# pylint: disable=invalid-name
# pylint: disable=too-many-locals

from dataclasses import dataclass
from datetime import datetime
from typing import Union

from pyflake_client.models.mergeables.snowflake_mergable_interface import (
    ISnowflakeMergable,
)
from pyflake_client.models.assets.table_columns import Number, Varchar, Identity

# --------------------------------------------------------
# --- Test dependencies
# --------------------------------------------------------

TABLE_NAME = "merge_table"
TABLE_COLUMN_DEFINITION = [
    Number("id", identity=Identity(1, 1)),
    Varchar("the_primary_key", primary_key=True),
    # TODO once table and columns is complete
    # Column("id", ColumnType.INTEGER, identity=True),
    # Column("the_primary_key", ColumnType.VARCHAR, primary_key=True),
    # Column("enabled", ColumnType.BOOLEAN),
    # Column("valid_from", ColumnType.TIMESTAMP),
    # Column("valid_to", ColumnType.TIMESTAMP),
]


@dataclass
class MergableEntity(ISnowflakeMergable):
    """RoleRelationshipEntity"""

    the_primary_key: str
    enabled: Union[bool, None] = None
    valid_from: Union[datetime, None] = None
    valid_to: Union[datetime, None] = None
    id: Union[int, None] = None

    def merge_into_statement(self, db_name: str, schema_name: str) -> str:
        """merge_into_statement"""
        now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f%z")
        return f"""
        merge into {db_name}.{schema_name}.{TABLE_NAME} tar using
        (
            select
                '{self.the_primary_key}' THE_PRIMARY_KEY,
                '{self.enabled}' ENABLED,
                '{now}'::timestamp_ntz(2)  VALID_FROM,
                '9999-12-31 23:59:59.99' VALID_TO
            union all
            select
                s.the_primary_key,
                s.enabled,
                s.valid_from,
                '{now}'::timestamp_ntz(2) valid_to
            from {db_name}.{schema_name}.{TABLE_NAME} s
            where
                s.THE_PRIMARY_KEY = '{self.the_primary_key}'
                and s.VALID_TO::date = '9999-12-31'
        )src on
            src.THE_PRIMARY_KEY = tar.THE_PRIMARY_KEY
            and src.VALID_FROM = tar.VALID_FROM
        WHEN MATCHED THEN UPDATE
            set tar.VALID_TO = '{now}'::timestamp_ntz(2)
        WHEN NOT MATCHED THEN INSERT (
            THE_PRIMARY_KEY,
            ENABLED,
            VALID_FROM,
            VALID_TO
        ) VALUES (
            src.THE_PRIMARY_KEY,
            src.ENABLED,
            src.VALID_FROM,
            src.VALID_TO
        );
        """

    def select_statement(self, db_name: str, schema_name: str) -> str:
        """select_statement"""
        return f"""
        select
            *
        from {db_name}.{schema_name}.{TABLE_NAME} s
        where s.the_primary_key = '{self.the_primary_key}' and valid_to::date = '9999-12-31'
        """
