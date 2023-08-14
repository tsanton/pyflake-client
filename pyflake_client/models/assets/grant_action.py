# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import List

from pyflake_client.models.assets.grants.snowflake_grant_asset import (
    ISnowflakeGrantAsset,
)
from pyflake_client.models.assets.snowflake_asset_interface import ISnowflakeAsset
from pyflake_client.models.assets.snowflake_principal_interface import (
    ISnowflakePrincipal,
)
from pyflake_client.models.enums.privilege import Privilege


@dataclass
class GrantAction(ISnowflakeAsset):
    """GrantAction"""

    principal: ISnowflakePrincipal
    target: ISnowflakeGrantAsset
    privileges: List[Privilege]

    def get_create_statement(self) -> str:
        return self.target.get_grant_statement(self.principal, self.privileges)

    def get_delete_statement(self) -> str:
        return self.target.get_revoke_statement(self.principal, self.privileges)
