from dataclasses import dataclass
from typing import Union

import dacite

from pyflake_client.models.describables.snowflake_describable_interface import (
    ISnowflakeDescribable,
)
from pyflake_client.models.describables.snowflake_grant_principal import (
    ISnowflakeGrantPrincipal,
)

from pyflake_client.models.assets.database_role import (
    DatabaseRole as AssetsDatabaseRole,
)
from pyflake_client.models.assets.role import Role as AssetsRole


@dataclass
class FutureGrant(ISnowflakeDescribable):
    principal: ISnowflakeGrantPrincipal

    def get_describe_statement(self) -> str:
        if isinstance(self.principal, AssetsRole):
            query = """
with show_grants_to_role as procedure(role_name varchar)
    returns variant not null
    language python
    runtime_version = '3.8'
    packages = ('snowflake-snowpark-python')
    handler = 'show_grants_to_role_py'
as $$
def show_grants_to_role_py(snowpark_session, role_name_py:str):
    res = []
    for row in snowpark_session.sql(f'SHOW FUTURE GRANTS TO ROLE {role_name_py.upper()}').to_local_iterator():
        res.append(row.as_dict())
    return res
$$
call show_grants_to_role('%(s1)s');
""" % {
                "s1": self.principal.name
            }

        elif isinstance(self.principal, AssetsDatabaseRole):
            query = """
with show_grants_to_database_role as procedure(database_name varchar, database_role_name varchar)
    returns variant not null
    language python
    runtime_version = '3.8'
    packages = ('snowflake-snowpark-python')
    handler = 'show_grants_to_database_role_py'
as $$
def show_grants_to_database_role_py(snowpark_session, database_name_py:str, database_role_name_py:str):
    res = []
    for row in snowpark_session.sql(f'SHOW FUTURE GRANTS IN DATABASE {database_name_py.upper()}').to_local_iterator():
        if row['grant_to'] == 'DATABASE_ROLE' and row['grant_to'] == 'DATABASE_ROLE':
                res.append(row.as_dict())
    for schema_object in snowpark_session.sql(f'SHOW SCHEMAS IN DATABASE {database_name_py.upper()}').to_local_iterator():
        schema_name:str = schema_object['name']
        if schema_name not in('INFORMATION_SCHEMA', 'PUBLIC'):
            query:str = f'SHOW FUTURE GRANTS IN SCHEMA {database_name_py}.{schema_name}'.upper()
            for row in snowpark_session.sql(query).to_local_iterator():
                if row['grant_to'] == 'DATABASE_ROLE' and row['grantee_name'] == database_role_name_py:
                    res.append(row.as_dict())
    return res
$$
call show_grants_to_database_role('%(s1)s','%(s2)s');
        """ % {
                "s1": "DATABASE_ROLE",
                "s2": f"{self.principal.database_name}.{self.principal.name}",
            }
        else:
            raise NotImplementedError(
                f"Future Grant describe statement for {self.__class__} is not implemented"
            )

        return query

    def is_procedure(self) -> bool:
        return False

    def get_dacite_config(self) -> Union[dacite.Config, None]:
        return None
