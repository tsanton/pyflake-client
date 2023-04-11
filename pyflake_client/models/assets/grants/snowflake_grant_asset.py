from abc import ABC
from typing import List
from pyflake_client.models.assets.snowflake_principal_interface import (
    ISnowflakePrincipal,
)
from pyflake_client.models.enums.privilege import Privilege


class ISnowflakeGrantAsset(ABC):
    def get_grant_statement(
        self, principal: ISnowflakePrincipal, privileges: List[Privilege]
    ) -> str:
        ...

    def get_revoke_statement(
        self, principal: ISnowflakePrincipal, privileges: List[Privilege]
    ) -> str:
        ...

    def validate_privileges(self, privileges: List[Privilege]) -> str:
        ...
