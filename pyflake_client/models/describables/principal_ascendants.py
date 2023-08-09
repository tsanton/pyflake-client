"""principal_ascendants"""
# pylint: disable=consider-using-f-string
from dataclasses import dataclass
from typing import Union
import dacite

from pyflake_client.models.describables.role import Role as RoleDescribable
from pyflake_client.models.describables.database_role import DatabaseRole as DatabaseRoleDescribable

from pyflake_client.models.describables.snowflake_describable_interface import ISnowflakeDescribable

from pyflake_client.models.describables.snowflake_grant_principal import ISnowflakeGrantPrincipal


@dataclass(frozen=True)
class PrincipalAscendants(ISnowflakeDescribable):
    """PrincipalAscendants: all roles higher in the role hierarchy with USAGE on this principal"""

    principal: ISnowflakeGrantPrincipal

    def get_describe_statement(self) -> str:
        """get_describe_statement"""
        if isinstance(self.principal, RoleDescribable):
            principal_type = "ROLE"
            principal_identifier = self.principal.name
        elif isinstance(self.principal, DatabaseRoleDescribable):
            principal_type = "DATABASE ROLE"
            principal_identifier = f"{self.principal.db_name}.{self.principal.name}"
        else:
            raise NotImplementedError()
        
        query =  """
with show_all_roles_that_inherit_source as procedure(principal_type varchar, principal_identifier varchar)
    returns variant not null
    language python
    runtime_version = '3.8'
    packages = ('snowflake-snowpark-python')
    handler = 'main_py'
as $$
def show_grants_on_py(snowpark_session, principal_type_py:str, principal_identifier_py:str, links_removed:int):
    res = []
    try:
        for row in snowpark_session.sql(f'SHOW GRANTS OF {principal_type_py} {principal_identifier_py}').to_local_iterator():
            if row['granted_to'] in ['ROLE', 'DATABASE_ROLE']:
                res.append({ 
                    **row.as_dict(), 
                    **{ 'distance_from_source': links_removed, 'granted_on' : principal_type_py if principal_type_py != 'DATABASE ROLE' else 'DATABASE_ROLE' }
                })
    except:
        return res
    return res

def show_all_roles_that_inherit_source_py(snowpark_session, principal_type: str, principal_identifier: str, links_removed:int, result:list, roles_shown:set = set()):
    roles = show_grants_on_py(snowpark_session, principal_type, principal_identifier, links_removed)
    show_inheritance = []
    for role in roles:
        result.append(role)
        if not role['grantee_name'] in roles_shown:
            roles_shown.add(role['grantee_name'].upper())
            show_inheritance.append(role)
    for role in show_inheritance:
        principal_type_iter:str = role['granted_to'] if role['granted_to'] != 'DATABASE_ROLE' else 'DATABASE ROLE'
        show_all_roles_that_inherit_source_py(snowpark_session, principal_type_iter, role['grantee_name'], links_removed +1, result, roles_shown)
            
def main_py(snowpark_session, principal_type_py:str, principal_identifier_py:str):
    res = []
    show_all_roles_that_inherit_source_py(snowpark_session, principal_type_py, principal_identifier_py, 0, res)
    return res
$$
call show_all_roles_that_inherit_source('%(s1)s', '%(s2)s');""" % {
            "s1": principal_type,
            "s2": principal_identifier
        }
        return query

    def is_procedure(self) -> bool:
        """is_procedure"""
        return True

    def get_dacite_config(self) -> Union[dacite.Config, None]:
        """get_dacite_config"""
        return None


        