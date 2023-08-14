# -*- coding: utf-8 -*-
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict

import dacite

from pyflake_client.models.describables.database_role import (
    DatabaseRole as DescribablesDatabaseRole,
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


@dataclass
class RoleGrant(ISnowflakeDescribable, ISnowflakeGrantPrincipal):
    principal: ISnowflakeGrantPrincipal

    def get_describe_statement(self) -> str:
        if isinstance(self.principal, RoleDescribable):
            query = f"SHOW GRANTS TO ROLE {self.principal.name}"
        elif isinstance(self.principal, DescribablesDatabaseRole):
            query = f"SHOW GRANTS TO DATABASE ROLE {self.principal.db_name}.{self.principal.name}"
        else:
            raise NotImplementedError(f"Grant describe statement for {self.__class__} is not implemented")
        return query

    def is_procedure(self) -> bool:
        return False

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
                bool: lambda b: bool(b),
                Privilege: lambda s: Privilege(s)
            }))

        return deserialize
