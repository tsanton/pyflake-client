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
from pyflake_client.models.describables.user import User as UserDescribable

from pyflake_client.models.entities.user_grant import UserGrant as UserGrantEntity
from pyflake_client.models.enums.privilege import Privilege


@dataclass
class UserGrant(ISnowflakeDescribable, ISnowflakeGrantPrincipal):
    principal: ISnowflakeGrantPrincipal

    def get_describe_statement(self) -> str:
        if isinstance(self.principal, UserDescribable):
            query = f"SHOW GRANTS TO USER {self.principal.name}"
        else:
            raise NotImplementedError(f"Grant describe statement for {self.__class__} is not implemented")

        return query

    def is_procedure(self) -> bool:
        return False

    @classmethod
    def get_deserializer(cls) -> Callable[[Dict[str, Any]], UserGrantEntity]:
        def deserialize(data:Dict[str, Any]) -> UserGrantEntity:
            renaming = {
                "grantee_name": "grantee_identifier",
                "granted_to": "grantee_type",
                "role": "granted_identifier",
            }
            for old_key, new_key in renaming.items():
                data[new_key] = data.pop(old_key)
            return dacite.from_dict(UserGrantEntity, data, dacite.Config(type_hooks={
                Privilege: lambda s: Privilege(s)
            }))

        return deserialize
