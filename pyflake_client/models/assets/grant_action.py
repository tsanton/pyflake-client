from typing import List

from pyflake_client.models.assets.grants.account_grant import AccountGrant
from pyflake_client.models.assets.snowflake_asset_interface import ISnowflakeAsset
from pyflake_client.models.enums.privilege import Privilege


class GrantAction(ISnowflakeAsset):
    principal: AccountGrant
    privileges: List[Privilege]

    def get_create_statement(self) -> str:
        return self.principal.get_grant_statement(self.privileges)

    def get_delete_statement(self) -> str:
        return self.principal.get_revoke_statement(self.privileges)
