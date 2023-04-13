"""role_inheritance"""
from dataclasses import dataclass
from pyflake_client.models.assets.snowflake_asset_interface import ISnowflakeAsset
from pyflake_client.models.assets.snowflake_principal_interface import ISnowflakePrincipal

from pyflake_client.models.assets.role import Role as RoleAsset
from pyflake_client.models.assets.database_role import  DatabaseRole as DatabaseRoleAssets

from pyflake_client.models.enums.role_type import RoleType

@dataclass(frozen=True)
class RoleInheritance(ISnowflakeAsset):
    """RoleInheritance: the child role is granted to the parent"""
    child_principal: ISnowflakePrincipal
    parent_principal: ISnowflakePrincipal

    def get_create_statement(self):
        """get_create_statement"""
        child_principal_type = None
        parent_principal_type = None

        grant_statement = "GRANT"
        if isinstance(self.child_principal, RoleAsset):
            child_principal_type = RoleAsset
            grant_statement += f" {RoleType.ROLE} {self.child_principal.get_identifier()} TO";
        elif isinstance(self.child_principal, DatabaseRoleAssets):
            child_principal_type = DatabaseRoleAssets
            grant_statement += f" {RoleType.DATABASE_ROLE} {self.child_principal.get_identifier()} TO";
        else:
            raise NotImplementedError(f"Can't generate grant statement for asset of type {self.child_principal.__class__}")

        if isinstance(self.parent_principal, RoleAsset):
            parent_principal_type = RoleAsset
            grant_statement += f" {RoleType.ROLE} {self.parent_principal.get_identifier()};";
        elif isinstance(self.parent_principal, DatabaseRoleAssets):
            parent_principal_type = DatabaseRoleAssets
            grant_statement += f" {RoleType.DATABASE_ROLE} {self.parent_principal.get_identifier()};";
        else:
            raise NotImplementedError(f"Can't generate grant statement for asset of type {self.parent_principal.__class__}")

        if parent_principal_type == DatabaseRoleAssets:
            if child_principal_type == RoleAsset:
                raise ValueError("Account roles cannot be granted to database roles")
            elif child_principal_type == DatabaseRoleAssets and self.child_principal.database_name != self.parent_principal.database_name:
                raise ValueError("Can only grant database roles to other database roles in the same database")
        return grant_statement

    def get_delete_statement(self):
        """get_delete_statement"""
        revoke_statement = "REVOKE"
        if isinstance(self.child_principal, RoleAsset):
            revoke_statement += f" {RoleType.ROLE} {self.child_principal.get_identifier()} FROM"
        elif isinstance(self.child_principal, DatabaseRoleAssets):
            revoke_statement += f" {RoleType.DATABASE_ROLE} {self.child_principal.get_identifier()} FROM"
        else:
            raise NotImplementedError("get_identifier is not implemented for this interface type")
        if isinstance(self.parent_principal, RoleAsset):
            revoke_statement += f" {RoleType.ROLE} {self.parent_principal.get_identifier()};"
        elif isinstance(self.parent_principal, DatabaseRoleAssets):
            revoke_statement += f" {RoleType.DATABASE_ROLE} {self.parent_principal.get_identifier()};"
        else:
            raise NotImplementedError("get_identifier is not implemented for this interface type")
        return revoke_statement
