"""role_schema_future_grant"""
# pylint: disable=line-too-long
from dataclasses import dataclass
from typing import List


from pyflake_client.models.enums.object_type import ObjectType
from pyflake_client.models.assets.grants.snowflake_grant_interface import ISnowflakeGrant


@dataclass(frozen=True)
class RoleSchemaFutureGrant(ISnowflakeGrant):
    """RoleSchemaFutureGrant"""
    role_name: str
    db_name: str
    schema_name: str
    object_type: ObjectType

    def get_grant_statement(self, privileges: List[str]) -> str:  # TODO: List[enum]
        """get_grant_statement"""
        return f"GRANT {', '.join(privileges)} ON FUTURE {self.object_type.pluralize()} IN SCHEMA {self.db_name}.{self.schema_name} TO ROLE {self.role_name};"

    def get_revoke_statement(self, privileges: List[str]) -> str:  # TODO: List[enum]
        """get_revoke_statement"""
        return f"REVOKE {', '.join(privileges)} ON FUTURE {self.object_type.pluralize()} IN SCHEMA {self.db_name}.{self.schema_name} FROM ROLE {self.role_name};"

    def validate(self, privileges: List[str]) -> bool:
        """validate"""
        return True  # TODO: Implement
