"""role_schema_object_grant"""
# pylint: disable=line-too-long
from dataclasses import dataclass
from typing import List

from pyflake_client.models.enums.object_type import ObjectType
from pyflake_client.models.assets.grants.snowflake_grant_interface import ISnowflakeGrant


@dataclass(frozen=True)
class RoleSchemaObjectGrant(ISnowflakeGrant):
    """RoleSchemaObjectGrant"""
    db_name: str
    schema_name: str
    object_type: ObjectType
    object_name: str

    def get_grant_statement(self, privileges: List[str]) -> str:  # TODO: List[enum]
        """get_grant_statement"""
        return f"GRANT {', '.join(privileges)} ON {self.object_type} {self.db_name}.{self.schema_name}.{self.object_name} TO ROLE {role.name};"

    def get_revoke_statement(self, privileges: List[str]) -> str:  # TODO: List[enum]
        """get_revoke_statement"""
        return f"REVOKE {', '.join(privileges)} ON {self.object_type} {self.db_name}.{self.schema_name}.{self.object_name} FROM ROLE {role.name};"

    def validate(self, privileges: List[str]) -> bool:
        return True  # TODO: Implement
