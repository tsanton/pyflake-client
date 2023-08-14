# -*- coding: utf-8 -*-
# pylint: disable=line-too-long

from dataclasses import dataclass
from datetime import datetime
from typing import Union

from dacite import Config

from pyflake_client.models.describables.snowflake_describable_interface import (
    ISnowflakeDescribable,
)


@dataclass(frozen=True)
class Table(ISnowflakeDescribable):
    """Table"""

    database_name: str
    schema_name: str
    name: str

    def get_describe_statement(self) -> str:
        return """
with show_table_description as procedure(db_name varchar, schema_name varchar, table_name varchar)
    returns variant not null
    language python
    runtime_version = '3.8'
    packages = ('snowflake-snowpark-python')
    handler = 'show_table_description_py'
as $$
import json
def show_table_description_py(snowpark_session, db_name_py:str, schema_name_py:str, table_name_py:str):
    res = []

    table = snowpark_session.sql(f"SHOW TABLES like '{table_name_py}' IN SCHEMA {db_name_py}.{schema_name_py}").collect()[0].as_dict()

    table['tags'] = []
    tag_query: str = f"SELECT * from table(%(s1)s.INFORMATION_SCHEMA.TAG_REFERENCES('%(s1)s.%(s2)s.%(s3)s', 'TABLE'));"
    for tag in snowpark_session.sql(tag_query).to_local_iterator():
        table['tags'].append(tag.as_dict())

    for row in snowpark_session.sql(f'DESCRIBE TABLE {db_name_py}.{schema_name_py}.{table_name_py}').to_local_iterator():
        col = snowpark_session.sql(f"SHOW COLUMNS LIKE '{row['name']}' IN TABLE {db_name_py}.{schema_name_py}.{table_name_py}").collect()[0].as_dict()
        res.append({**row.as_dict(), **{'tags': [], 'auto_increment': col['autoincrement'] if col['autoincrement'] != '' else None, 'data_type': json.loads(col['data_type'])}})

    column_tags_query:str = f"SELECT * from table(%(s1)s.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS('%(s1)s.%(s2)s.%(s3)s', 'TABLE'));"

    for column_tag in snowpark_session.sql(column_tags_query).to_local_iterator():
        next(col for col in res if col['name'] == column_tag['COLUMN_NAME'])['tags'].append(column_tag.as_dict())

    table['columns'] = res
    return table
$$
call show_table_description('%(s1)s', '%(s2)s', '%(s3)s');
        """ % {
            "s1": self.database_name,
            "s2": self.schema_name,
            "s3": self.name,
        }

    def is_procedure(self) -> bool:
        """is_procedure"""
        return True

    def get_dacite_config(self) -> Union[Config, None]:
        return Config(
            type_hooks={datetime: lambda v: datetime.strptime(v, "%Y-%m-%d %H:%M:%S.%f%z"), int: lambda i: int(i)},
        )
