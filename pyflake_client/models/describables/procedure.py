# -*- coding: utf-8 -*-
# pylint: disable=line-too-long

from dataclasses import dataclass
from typing import Union

import dacite

from pyflake_client.models.describables.snowflake_describable_interface import (
    ISnowflakeDescribable,
)


@dataclass(frozen=True)
class Procedure(ISnowflakeDescribable):
    """Procedure"""

    database_name: str
    schema_name: str
    name: str

    def get_describe_statement(self) -> str:
        """get_describe_statement"""
        # TODO: flawed logic in case of polymorphic procedures -> only first is returned
        return """
with show_procedures as procedure(db_name varchar, schema_name varchar, procedure_name varchar)
    returns variant not null
    language python
    runtime_version = '3.8'
    packages = ('snowflake-snowpark-python')
    handler = 'show_procedure_py'
as $$
def show_procedure_py(snowpark_session, db_name_py: str, schema_name_py:str, procedure_name_py:str):
    for row in snowpark_session.sql(f"SHOW PROCEDURES LIKE '{procedure_name_py}' IN SCHEMA {db_name_py}.{schema_name_py}".upper()).to_local_iterator():
        #TODO: flawed logic in case of polymorphic procedures -> only first is returned
        data = row.as_dict()
        if data["min_num_arguments"] > 0:
            args = data["arguments"]
            start = args.find("(")+1
            end = args.find(")")
            out = args[start:end].split(",")
            out = [x.strip() for x in out]
        else:
            out = []
        return {**data, "procedure_args": out}
$$
call show_procedures('%(str1)s', '%(str2)s', '%(str3)s');
        """ % {
            "str1": self.database_name,
            "str2": self.schema_name,
            "str3": self.name,
        }

    def is_procedure(self) -> bool:
        """is_procedure"""
        return True

    def get_dacite_config(self) -> Union[dacite.Config, None]:
        """get_dacite_config"""
        return None
