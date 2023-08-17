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
            return f"SHOW FUTURE GRANTS TO DATABASE ROLE {self.principal.db_name}.{self.principal.name}".upper()
        else:
            raise NotImplementedError(f"Future Grant describe statement for {self.__class__} is not implemented")

    def is_procedure(self) -> bool:
        return False

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
