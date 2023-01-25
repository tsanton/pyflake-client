"""role_relationship"""
from dataclasses import dataclass
from pyflake_client.models.assets.snowflake_asset_interface import ISnowflakeAsset


@dataclass(frozen=True)
class RoleRelationship(ISnowflakeAsset):
    """RoleRelationship: the child role is granted to the parent"""
    child_role_name: str
    parent_role_name: str

    def get_create_statement(self):
        """get_create_statement"""
        return f"""GRANT ROLE {self.child_role_name} TO ROLE {self.parent_role_name};"""

    def get_delete_statement(self):
        """get_delete_statement"""
        return f"""REVOKE ROLE {self.child_role_name} FROM ROLE {self.parent_role_name};"""
