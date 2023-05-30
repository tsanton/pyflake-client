"""role_inheritance"""
from dataclasses import dataclass
from pyflake_client.models.assets.snowflake_asset_interface import ISnowflakeAsset
from pyflake_client.models.assets.snowflake_principal_interface import ISnowflakePrincipal

from pyflake_client.models.enums.principal import Principal


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
        child_principal_type = self.child_principal.get_snowflake_type()
        if child_principal_type == Principal.ROLE:
            grant_statement += f" ROLE {self.child_principal.get_identifier()} TO";
        elif child_principal_type == Principal.DATABASE_ROLE:
            grant_statement += f" DATABASE ROLE {self.child_principal.get_identifier()} TO";
        else:
            raise NotImplementedError(f"Can't generate grant statement for asset of type {self.child_principal.__class__}")

        parent_principal_type = self.parent_principal.get_snowflake_type()
        if parent_principal_type == Principal.ROLE:
            grant_statement += f" ROLE {self.parent_principal.get_identifier()};";
        elif parent_principal_type == Principal.DATABASE_ROLE:
            grant_statement += f" DATABASE ROLE {self.parent_principal.get_identifier()};";
        elif parent_principal_type == Principal.USER:
            grant_statement += f" USER {self.parent_principal.get_identifier()};"
        else:
            raise NotImplementedError(f"Can't generate grant statement for asset of type {self.parent_principal.__class__}")

        if parent_principal_type == Principal.DATABASE_ROLE:
            if child_principal_type == Principal.ROLE:
                raise ValueError("Account roles cannot be granted to database roles")
            elif child_principal_type == Principal.DATABASE_ROLE and self.child_principal.get_identifier().split(".")[0] != self.parent_principal.get_identifier().split(".")[0]:
                raise ValueError("Can only grant database roles to other database roles in the same database")
        return grant_statement

    def get_delete_statement(self):
        """get_delete_statement"""
        revoke_statement = "REVOKE"

        child_principal_type = self.child_principal.get_snowflake_type()
        if child_principal_type == Principal.ROLE:
            revoke_statement += f" ROLE {self.child_principal.get_identifier()} FROM"
        elif child_principal_type == Principal.DATABASE_ROLE:
            revoke_statement += f" DATABASE ROLE {self.child_principal.get_identifier()} FROM"
        else:
            raise NotImplementedError("get_delete_statement is not implemented for this interface type")
        
        parent_principal_type = self.parent_principal.get_snowflake_type()
        if parent_principal_type == Principal.ROLE:
            revoke_statement += f" ROLE {self.parent_principal.get_identifier()};"
        elif parent_principal_type == Principal.DATABASE_ROLE:
            revoke_statement += f" DATABASE ROLE {self.parent_principal.get_identifier()};"
        elif parent_principal_type == Principal.USER:
            revoke_statement += f" USER {self.parent_principal.get_identifier()};"
        else:
            raise NotImplementedError("get_delete_statement is not implemented for this interface type")
        return revoke_statement
