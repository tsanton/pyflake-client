# pylint: disable=line-too-long
"""role_account_grant"""
from dataclasses import dataclass
from typing import List

from pyflake_client.models.assets.grants.snowflake_grant_interface import ISnowflakeGrant


@dataclass(frozen=True)
class RoleAccountGrant(ISnowflakeGrant):
    """RoleAccountGrant"""
    role_name: str

    def get_grant_statement(self, privileges: List[str]) -> str:  # TODO: List[enum]
        """get_grant_statement"""
        privs = ", ".join(privileges)
        return f"GRANT {privs} ON ACCOUNT TO ROLE {self.role_name};"

    def get_revoke_statement(self, privileges: List[str]) -> str:
        """get_revoke_statement"""
        privs = ", ".join(privileges)
        return f"REVOKE {privs} ON ACCOUNT FROM ROLE {self.role_name} CASCADE;"

    def validate(self, privileges: List[str]) -> bool:
        return True
