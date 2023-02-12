"""role_descendants"""
# pylint: disable=consider-using-f-string
from dataclasses import dataclass
from dacite import Config

from pyflake_client.models.describables.snowflake_describable_interface import ISnowflakeDescribable


@dataclass(frozen=True)
class RoleDescendants(ISnowflakeDescribable):
    """RoleDescendants: all roles that are one step lower in the hierarchy that the queried role"""
    role_name: str

    def get_describe_statement(self) -> str:
        """get_describe_statement"""
        return """
with show_all_roles_directly_inherited_by_source as procedure(role_name varchar)
    returns variant not null
    language python
    runtime_version = '3.8'
    packages = ('snowflake-snowpark-python')
    handler = 'main_py'
as '
from typing import List, Dict
def show_grants_to_role_py(snowpark_session, role_name: str, links_removed:int, res:List[Dict]):
    try:
        for row in snowpark_session.sql(f"SHOW GRANTS TO ROLE {role_name}").to_local_iterator():
            if row["privilege"] == "USAGE" and row["granted_on"] in ["ROLE", "DATABASE_ROLE"]:
                res.append({ **row.as_dict(), **{
						"distance_from_source": links_removed,
						"role_name": row["name"],
						"parent_role_name": row["grantee_name"]
                	}
				})
    except:
        return res
    return res
            
def main_py(snowpark_session, base_role_name_py:str):
    res = []
    show_grants_to_role_py(snowpark_session, base_role_name_py, 0, res)
    return {"name": base_role_name_py, "descendant_roles": res}
'
call show_all_roles_directly_inherited_by_source('%(s1)s');""" % {"s1": self.role_name}

    def is_procedure(self) -> bool:
        """is_procedure"""
        return True

    def get_dacite_config(self) -> Config:
        return None
