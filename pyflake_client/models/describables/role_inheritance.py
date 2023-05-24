"""role_inheritance"""
# pylint: disable=line-too-long
from dataclasses import dataclass
from typing import Union

import dacite

from pyflake_client.models.describables.snowflake_describable_interface import ISnowflakeDescribable
from pyflake_client.models.describables.role import Role as RoleDescribable
from pyflake_client.models.describables.database_role import DatabaseRole as DatabaseRoleDescribable
from pyflake_client.models.describables.user import User as UserDescribable
from pyflake_client.models.describables.snowflake_grant_principal import ISnowflakeGrantPrincipal

@dataclass(frozen=True)
class RoleInheritance(ISnowflakeDescribable):
    """RoleInheritance"""

    inherited_principal: ISnowflakeGrantPrincipal
    parent_principal: ISnowflakeGrantPrincipal

    def is_procedure(self) -> bool:
        """is_procedure"""
        return True

    def get_dacite_config(self) -> Union[dacite.Config, None]:
        """get_dacite_config"""
        return None


    def get_describe_statement(self) -> str:
        """get_describe_statement"""
        inherited_type = None
        inherited_identifier = None
        if isinstance(self.inherited_principal, RoleDescribable):
            inherited_type = RoleDescribable
            inherited_identifier = self.inherited_principal.name
        elif isinstance(self.inherited_principal, DatabaseRoleDescribable):
            inherited_type = DatabaseRoleDescribable
            inherited_identifier = f"{self.inherited_principal.db_name}.{self.inherited_principal.name}"
        else:
            raise NotImplementedError()
        
        parent_type = None
        parent_identifier = None
        if isinstance(self.parent_principal, RoleDescribable):
            parent_type = RoleDescribable
            parent_identifier = self.parent_principal.name
        elif isinstance(self.parent_principal, DatabaseRoleDescribable):
            parent_type = DatabaseRoleDescribable
            parent_identifier = f"{self.parent_principal.db_name}.{self.parent_principal.name}"
        elif isinstance(self.parent_principal, UserDescribable):
            parent_type = UserDescribable
            parent_identifier = self.parent_principal.name
        else:
            raise NotImplementedError()
        
        if parent_type == DatabaseRoleDescribable and inherited_type == RoleDescribable:
            raise NotImplementedError("Account roles cannot be granted to database roles")
        
        if inherited_type == DatabaseRoleDescribable:
            inherited_type = "DATABASE_ROLE"
        elif inherited_type == RoleDescribable:
            inherited_type = "ROLE"
        elif inherited_type == UserDescribable:
            inherited_type = "USER"
        else:
            raise NotImplementedError()

        return """
with show_inherited_role as procedure(parent_principal_identifier varchar, parent_principal_type varchar, child_role_identifier varchar, child_role_type varchar)
    returns variant
    language python
    runtime_version = '3.8'
    packages = ('snowflake-snowpark-python')
    handler = 'show_inherited_role_py'
as $$
def show_inherited_role_py(snowpark_session, parent_principal_identifier_py:str, parent_principal_type_py:str, child_role_identifier_py:str, child_role_type_py:str):
    for row in snowpark_session.sql(f'SHOW GRANTS TO {parent_principal_type_py} {parent_principal_identifier_py}'.upper()).to_local_iterator():
        if row['granted_on'] == child_role_type_py and row['name'] == child_role_identifier_py and row['privilege'] == 'USAGE':
            return row.as_dict()
    raise ValueError('Role relationship does not exist or not authorized')
$$
call show_inherited_role('%(s1)s', '%(s2)s', '%(s3)s', '%(s4)s');
""" % {
            "s1": parent_identifier,
            "s2": parent_type.get_snowflake_type(),
            "s3": inherited_identifier,
            "s4": inherited_type,
        }