# -*- coding: utf-8 -*-
# pylint: disable=consider-using-f-string
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, Union

import dacite

from pyflake_client.models.describables.database_role import (
    DatabaseRole as DatabaseRoleDescribable,
)
from pyflake_client.models.describables.role import Role as RoleDescribable
from pyflake_client.models.describables.snowflake_describable_interface import (
    ISnowflakeDescribable,
)
from pyflake_client.models.describables.snowflake_grant_principal import (
    ISnowflakeGrantPrincipal,
)

from pyflake_client.models.entities.role_grant import RoleGrant as RoleGrantEntity
from pyflake_client.models.enums.privilege import Privilege


@dataclass(frozen=True)
class PrincipalDescendants(ISnowflakeDescribable):
    """PrincipalDescendants: all roles that are one step lower in the hierarchy that the queried role"""

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

        query = """
with show_direct_descendants_from_principal as procedure(principal_type varchar, principal_identifier varchar)
    returns variant not null
    language python
    runtime_version = '3.10'
    packages = ('snowflake-snowpark-python')
    handler = 'show_direct_descendants_from_principal_py'
as $$
def show_direct_descendants_from_principal_py(snowpark_session, principal_type_py:str, principal_identifier_py:str):
    res = []
    for row in snowpark_session.sql(f'SHOW GRANTS TO {principal_type_py} {principal_identifier_py}').to_local_iterator():
        if row['privilege'] == 'USAGE' and row['granted_on'] in ['ROLE', 'DATABASE_ROLE']:
            res.append({
                **row.as_dict(),
                **{'distance_from_source': 0 }
            })
    return res
$$
call show_direct_descendants_from_principal('%(s1)s', '%(s2)s');""" % {
            "s1": principal_type,
            "s2": principal_identifier,
        }
        return query

    def is_procedure(self) -> bool:
        """is_procedure"""
        return True

    @classmethod
    def get_deserializer(cls) -> Callable[[Dict[str, Any]], RoleGrantEntity]:
        def deserialize(data:Dict[str, Any]) -> RoleGrantEntity:
            renaming = {
                "grantee_name": "grantee_identifier",
                "granted_to": "grantee_type",
                "name": "granted_identifier",
            }
            for old_key, new_key in renaming.items():
                data[new_key] = data.pop(old_key)
            return dacite.from_dict(RoleGrantEntity, data, dacite.Config(type_hooks={
                int: lambda v: int(v),
                datetime: lambda d: datetime.fromisoformat(d),
                bool: lambda b: bool(b),
                Privilege: lambda s: Privilege(s)
            }))

        return deserialize
