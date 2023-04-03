from dataclasses import dataclass
from typing import Union

import dacite

from pyflake_client.models.describables.snowflake_describable_interface import (
    ISnowflakeDescribable,
)
from pyflake_client.models.describables.snowflake_grant_principal import (
    ISnowflakeGrantPrincipal,
)

from pyflake_client.models.describables.role import Role as DescribablesRole
from pyflake_client.models.describables.database_role import (
    DatabaseRole as DescribablesDatabaseRole,
)


@dataclass
class Grant(ISnowflakeDescribable):
    principal: ISnowflakeGrantPrincipal

    def get_describe_statement(self) -> str:
        if isinstance(self.principal, DescribablesRole):
            principal_type = "ROLE"  # TODO: enum for this
            principal_identifier = self.principal.name
        elif isinstance(self.principal, DescribablesDatabaseRole):
            principal_type = "DATABASE ROLE"  # TODO: enum for this
            principal_identifier = f"{self.principal.db_name}.{self.principal.name}"
        else:
            raise NotImplementedError(
                f"Grant describe statement for {self.__class__} is not implemented"
            )

        query = f"SHOW GRANTS TO {principal_type} {principal_identifier};"
        return query

    def is_procedure(self) -> bool:
        return False

    def get_dacite_config(self) -> Union[dacite.Config, None]:
        return None
