# -*- coding: utf-8 -*-
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict

import dacite

from pyflake_client.models.describables.database_role import (
    DatabaseRole as DatabaseRoleDescribable,
)
from pyflake_client.models.describables.snowflake_describable_interface import (
    ISnowflakeDescribable,
)
from pyflake_client.models.describables.snowflake_grant_principal import (
    ISnowflakeGrantPrincipal,
)
from pyflake_client.models.entities.future_grant import FutureGrant as FutureGrantEntity
from pyflake_client.models.enums.privilege import Privilege


@dataclass
class FutureGrant(ISnowflakeDescribable):
    principal: ISnowflakeGrantPrincipal

    def get_describe_statement(self) -> str:
        if self.principal.get_snowflake_type() == "ROLE":
            return f"SHOW FUTURE GRANTS TO ROLE {self.principal.name}".upper()

        elif isinstance(self.principal, DatabaseRoleDescribable):
            query = """
with show_grants_to_database_role as procedure(database_name varchar, database_role_name varchar)
    returns variant not null
    language python
    runtime_version = '3.10'
    packages = ('snowflake-snowpark-python')
    handler = 'show_grants_to_database_role_py'
as $$
def show_grants_to_database_role_py(snowpark_session, database_name_py:str, database_role_name_py:str):
    res = []
    try:
        for row in snowpark_session.sql(f'SHOW FUTURE GRANTS IN DATABASE {database_name_py.upper()}').to_local_iterator():
            if row['grant_to'] == 'DATABASE_ROLE' and row['grantee_name'] == database_role_name_py:
                    res.append(row.as_dict())
        for schema_object in snowpark_session.sql(f'SHOW SCHEMAS IN DATABASE {database_name_py.upper()}').to_local_iterator():
            schema_name:str = schema_object['name']
            if schema_name not in('INFORMATION_SCHEMA', 'PUBLIC'):
                query:str = f'SHOW FUTURE GRANTS IN SCHEMA {database_name_py}.{schema_name}'.upper()
                for row in snowpark_session.sql(query).to_local_iterator():
                    if row['grant_to'] == 'DATABASE_ROLE' and row['grantee_name'] == database_role_name_py:
                        res.append(row.as_dict())
    except:
        pass
    return res
$$
call show_grants_to_database_role('%(s1)s','%(s2)s');
        """ % {
                "s1": self.principal.db_name,
                "s2": self.principal.name,
            }
        else:
            raise NotImplementedError(f"Future Grant describe statement for {self.__class__} is not implemented")
        return query

    def is_procedure(self) -> bool:
        if self.principal.get_snowflake_type() == "ROLE":
            return False
        return True

    @classmethod
    def get_deserializer(cls) -> Callable[[Dict[str, Any]], FutureGrantEntity]:
        def deserialize(data: Dict[str, Any]) -> FutureGrantEntity:
            renaming = {
                "grantee_name": "grantee_identifier",
                "grant_to": "grantee_type",
                "name": "grant_identifier",
            }
            for old_key, new_key in renaming.items():
                data[new_key] = data.pop(old_key)
            return dacite.from_dict(
                FutureGrantEntity,
                data,
                dacite.Config(
                    type_hooks={
                        int: lambda v: int(v),
                        datetime: lambda d: datetime.fromisoformat(d) if type(d) == str else d,
                        bool: lambda b: bool(b),
                        Privilege: lambda s: Privilege(s),
                    }
                ),
            )

        return deserialize
