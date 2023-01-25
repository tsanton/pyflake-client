"""role_hierarchy"""
# pylint: disable=consider-using-f-string
from dataclasses import dataclass
from dacite import Config

from pyflake_client.models.describables.snowflake_describable_interface import ISnowflakeDescribable


@dataclass(frozen=True)
class RoleHierarchy(ISnowflakeDescribable):
    """RoleHierarchy: all roles higher in the role hierarchy with USAGE on this role"""
    role_name: str

    def get_describe_statement(self) -> str:
        """get_describe_statement"""
        return """
with show_all_roles_that_inherit_source as procedure(role_name varchar)
    returns variant not null
    language python
    runtime_version = '3.8'
    packages = ('snowflake-snowpark-python')
    handler = 'main_py'
as '
def show_grants_on_role_py(snowpark_session, role_name: str, links_removed:int):
    res = []
    try:
        for row in snowpark_session.sql(f"SHOW GRANTS ON ROLE {role_name}").to_local_iterator():
            if row["privilege"] == "USAGE":
                res.append({ **row.as_dict(), **{
						"distance_from_source": links_removed,
						"role_name": row["name"],
						"parent_role_name": row["grantee_name"]
                	}
				})
    except:
        return res
    return res

def show_all_roles_that_inherit_source_py(snowpark_session, role_name: str, links_removed:int, result: list, roles_shown:set = set()):
    roles = show_grants_on_role_py(snowpark_session, role_name, links_removed)
    show_inheritance = []
    for role in roles:
        if not role["grantee_name"] in roles_shown:
            result.append(role)
            roles_shown.add(role["grantee_name"].upper())
            show_inheritance.append(role["grantee_name"].upper())
    for role_name in show_inheritance:
        show_all_roles_that_inherit_source_py(snowpark_session, role_name, links_removed +1, result, roles_shown)
            
def main_py(snowpark_session, base_role_name_py:str):
    res = []
    show_all_roles_that_inherit_source_py(snowpark_session, base_role_name_py, 0, res)
    return {"name": base_role_name_py, "inheriting_roles": res}
'
call show_all_roles_that_inherit_source('%(s1)s');""" % {"s1": self.role_name}

    def is_procedure(self) -> bool:
        """is_procedure"""
        return True

    def get_dacite_config(self) -> Config:
        return None
