# -*- coding: utf-8 -*-
# pylint: disable=line-too-long

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict

import dacite
from dacite import Config

from pyflake_client.models.describables.snowflake_describable_interface import (
    ISnowflakeDescribable,
)
from pyflake_client.models.entities.classification_tag import (
    ClassificationTag as ClassificationTagEntity,
)
from pyflake_client.models.entities.column import Bool
from pyflake_client.models.entities.column import Column
from pyflake_client.models.entities.column import Column as ColumnEntity
from pyflake_client.models.entities.column import (
    Date,
    Number,
    Time,
    Timestamp,
    Varchar,
    Variant,
)
from pyflake_client.models.entities.table import Table as TableEntity


@dataclass(frozen=True)
class Table(ISnowflakeDescribable):
    database_name: str
    schema_name: str
    name: str

    def get_describe_statement(self) -> str:
        return """
with show_table_description as procedure(db_name varchar, schema_name varchar, table_name varchar)
    returns variant not null
    language python
    runtime_version = '3.10'
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
        return True

    @classmethod
    def get_deserializer(cls) -> TableEntity:
        def deserialize(data: Dict[str, Any]) -> TableEntity:
            columns = [column_deserializer(c) for c in data["columns"]]
            tags = [classification_tag_deserializer(t) for t in data["tags"]]
            return dacite.from_dict(
                TableEntity,
                {
                    "name": data["name"],
                    "database_name": data["database_name"],
                    "schema_name": data["schema_name"],
                    "kind": data["kind"],
                    "columns": columns,
                    "comment": data["comment"],
                    "tags": tags,
                    "rows": data["rows"],
                    "owner": data["owner"],
                    "retention_time": data["retention_time"],
                    "created_on": data["created_on"],
                },
                Config(
                    type_hooks={
                        datetime: lambda v: datetime.strptime(v, "%Y-%m-%d %H:%M:%S.%f%z"),
                        int: lambda i: int(i),
                    }
                ),
            )

        return deserialize


def column_deserializer(data: Dict[str, Any]) -> ColumnEntity:
    renaming = {
        "null?": "nullable",
        "policy name": "policy_name",
        "primary key": "primary_key",
        "unique key": "unique_key",
    }
    for old_key, new_key in renaming.items():
        data[new_key] = data.pop(old_key)
    data["tags"] = [classification_tag_deserializer(tag) for tag in data["tags"]]
    data["primary_key"] = data["primary_key"] == "Y"
    data["unique_key"] = data["unique_key"] == "Y"
    data["nullable"] = data["nullable"] == "Y"
    defaults = {k: data[k] for k in Column.__dataclass_fields__}
    data_type = data["type"]
    if re.match(r"NUMBER\([0-9]{1,2},[0-9]{1,2}\)", data_type):
        return Number(
            precision=data["data_type"]["precision"],
            scale=data["data_type"]["scale"],
            **defaults,
        )
    elif re.match(r"VARCHAR\([0-9]{1,8}\)", data_type):
        return Varchar(length=data["data_type"]["length"], **defaults)
    elif re.match(r"TIME\([0-9]\)", data_type):
        return Time(precision=data["data_type"]["precision"], **defaults)
    elif re.match(r"TIMESTAMP_NTZ\([0-9]\)", data_type):
        return Timestamp(precision=data["data_type"]["precision"], **defaults)
    elif data_type == "BOOLEAN":
        return Bool(**defaults)
    elif data_type == "DATE":
        return Date(**defaults)
    elif data_type == "VARIANT":
        return Variant(**defaults)
    raise ValueError(f"ColumnEntitiy received undefined data type: {data_type}")


def classification_tag_deserializer(data: Dict[str, Any]) -> ClassificationTagEntity:
    renaming = {
        "TAG_DATABASE": "tag_database_name",
        "TAG_SCHEMA": "tag_schema_name",
        "TAG_NAME": "tag_name",
        "DOMAIN": "domain_level",
        "TAG_VALUE": "tag_value",
    }
    for old_key, new_key in renaming.items():
        data[new_key] = data.pop(old_key)
    data["tag_value"] = None if data["tag_value"] == "" else data["tag_value"]
    return dacite.from_dict(ClassificationTagEntity, data, None)
