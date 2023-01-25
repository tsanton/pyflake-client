"""role_database_grant"""
# pylint: disable=line-too-long
from dataclasses import dataclass
from typing import List

from pyflake_client.models.assets.grants.snowflake_grant_interface import ISnowflakeGrant


@dataclass(frozen=True)
class RoleDatabaseGrant(ISnowflakeGrant):
    """RoleDatabaseGrant"""
    role_name: str
    database_name: str

    def get_grant_statement(self, privileges: List[str]) -> str:  # TODO: List[enum]
        """get_grant_statement"""
        privs = ", ".join(privileges)
        return "GRANT %(s1)s ON DATABASE %(s2)s TO ROLE %(s3)s;" % {"s1": privs, "s2": self.database_name, "s3": self.role_name}

    def get_revoke_statement(self, privileges: List[str]) -> str:
        """get_revoke_statement"""
        privs = ", ".join(privileges)
        return "REVOKE %(s1)s ON DATABASE %(s2)s FROM ROLE %(s3)s CASCADE;" % {"s1": privs, "s2": self.database_name, "s3": self.role_name}

    def validate(self, privileges: List[str]) -> bool:
        return True
