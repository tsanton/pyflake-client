# -*- coding: utf-8 -*-
# pylint: disable=line-too-long
from dataclasses import dataclass
from typing import List

from pyflake_client.models.assets.grants.snowflake_grant_asset import (
    ISnowflakeGrantAsset,
)
from pyflake_client.models.assets.role import Role as RoleAsset
from pyflake_client.models.assets.snowflake_principal_interface import (
    ISnowflakePrincipal,
)
from pyflake_client.models.enums.privilege import Privilege


@dataclass(frozen=True)
class WarehouseGrant(ISnowflakeGrantAsset):
    warehouse_name: str

    def get_grant_statement(self, principal: ISnowflakePrincipal, privileges: List[Privilege]) -> str:
        privs = ", ".join(privileges)
        if isinstance(principal, RoleAsset):
            return f"GRANT {privs} ON WAREHOUSE {self.warehouse_name} TO ROLE {principal.get_identifier()}"
        else:
            raise NotImplementedError(f"Can't generate grant statement for asset of type {self.__class__}")

    def get_revoke_statement(self, principal: ISnowflakePrincipal, privileges: List[Privilege]) -> str:
        privs = ", ".join(privileges)
        if isinstance(principal, RoleAsset):
            return f"REVOKE {privs} ON WAREHOUSE {self.warehouse_name} FROM ROLE {principal.get_identifier()} CASCADE;"

    def validate_grants(self, privileges: List[Privilege]) -> bool:
        raise NotImplementedError("Grant validation is NYI")
