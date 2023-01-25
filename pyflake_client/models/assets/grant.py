"""role_grant"""
# pylint: disable=line-too-long
from dataclasses import dataclass
from typing import TypeVar, List

from pyflake_client.models.assets.grants.snowflake_grant_interface import ISnowflakeGrant
from pyflake_client.models.assets.snowflake_asset_interface import ISnowflakeAsset

T = TypeVar("T", bound=ISnowflakeGrant)


@dataclass(frozen=True)
class Grant(ISnowflakeAsset):
    """Grant"""
    target: T
    privileges: List[str]

    def get_create_statement(self):
        """get_create_statement"""
        return self.target.get_grant_statement(self.privileges)

    def get_delete_statement(self):
        """get_delete_statement"""
        return self.target.get_revoke_statement(self.privileges)
