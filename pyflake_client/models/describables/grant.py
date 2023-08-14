# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import Union

import dacite

from pyflake_client.models.describables.database_role import (
    DatabaseRole as DescribablesDatabaseRole,
)
from pyflake_client.models.describables.role import Role as DescribablesRole
from pyflake_client.models.describables.snowflake_describable_interface import (
    ISnowflakeDescribable,
)
from pyflake_client.models.describables.snowflake_grant_principal import (
    ISnowflakeGrantPrincipal,
)
from pyflake_client.models.describables.user import User as DescribablesUser


@dataclass
class Grant(ISnowflakeDescribable, ISnowflakeGrantPrincipal):
    principal: ISnowflakeGrantPrincipal

    def get_describe_statement(self) -> str:
        if isinstance(self.principal, DescribablesRole):
            query = f"SHOW GRANTS TO ROLE {self.principal.name}"
        elif isinstance(self.principal, DescribablesDatabaseRole):
            query = f"SHOW GRANTS TO DATABASE ROLE {self.principal.db_name}.{self.principal.name}"
        elif isinstance(self.principal, DescribablesUser):
            query = f"SHOW GRANTS TO USER {self.principal.name}"
        else:
            raise NotImplementedError(f"Grant describe statement for {self.__class__} is not implemented")

        return query

    def is_procedure(self) -> bool:
        return False

    def get_dacite_config(self) -> Union[dacite.Config, None]:
        return None
