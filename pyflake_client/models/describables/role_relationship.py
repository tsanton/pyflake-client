"""role_relationship"""
# pylint: disable=line-too-long
from dataclasses import dataclass
from typing import Union

import dacite

from pyflake_client.models.describables.snowflake_describable_interface import (
    ISnowflakeDescribable,
)


@dataclass(frozen=True)
class RoleRelationship(ISnowflakeDescribable):
    """RoleRelationship"""

    child_role_name: str
    parent_role_name: str

    def get_describe_statement(self) -> str:
        """get_describe_statement"""
        return """
with show_role_inheritance_relationship as procedure(child_role varchar, parent_role varchar)
    returns variant not null
    language python
    runtime_version = '3.8'
    packages = ('snowflake-snowpark-python')
    handler = 'show_role_inheritance_relationship_py'
as '
def show_role_inheritance_relationship_py(snowpark_session, child_role_py: str, parent_role_py:str):
    res = {}
    for row in snowpark_session.sql(f"SHOW GRANTS ON ROLE {child_role_py.upper()}").to_local_iterator():
        if row["granted_on"] == "ROLE" and row["privilege"] == "USAGE" and row["granted_to"] == "ROLE" and row["grantee_name"] == parent_role_py.upper():
                return {**row.as_dict(), **{"child_role_name": child_role_py.upper(), "parent_role_name": parent_role_py.upper()}}
    return res
'
call show_role_inheritance_relationship('%(str1)s', '%(str2)s');""" % {
            "str1": self.child_role_name,
            "str2": self.parent_role_name,
        }

    def is_procedure(self) -> bool:
        """is_procedure"""
        return True

    def get_dacite_config(self) -> Union[dacite.Config, None]:
        """get_dacite_config"""
        return None
