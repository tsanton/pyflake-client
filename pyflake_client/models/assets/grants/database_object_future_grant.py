"""database_object_future_grant"""
from dataclasses import dataclass
from typing import List, TypeVar

from pyflake_client.models.assets.grants.snowflake_grant_asset import ISnowflakeGrantAsset
from pyflake_client.models.assets.snowflake_principal_interface import ISnowflakePrincipal
from pyflake_client.models.assets.role import Role as RoleAsset
from pyflake_client.models.assets.database_role import DatabaseRole as DatabaseRoleAsset
from pyflake_client.models.enums.object_type import ObjectType
from pyflake_client.models.enums.role_type import RoleType
from pyflake_client.models.enums.privilege import Privilege

T = TypeVar("T", bound=ISnowflakePrincipal)


@dataclass
class DatabaseObjectFutureGrant(ISnowflakeGrantAsset):
    database_name: str
    grant_target: ObjectType

    def get_grant_statement(self, principal: ISnowflakePrincipal, privileges: List[Privilege]) -> str:
        privs = f"{', '.join(p.value for p in privileges)}"
        if isinstance(principal, RoleAsset):
            return f"GRANT {privs} ON FUTURE {self.grant_target.pluralize()} IN DATABASE {self.database_name} TO {RoleType.ROLE} {principal.get_identifier()}"
        if isinstance(principal, DatabaseRoleAsset):
            return f"GRANT {privs} ON FUTURE {self.grant_target.pluralize()} IN DATABASE {self.database_name} TO {RoleType.DATABASE_ROLE} {principal.get_identifier()}"
        raise NotImplementedError(f"Can't generate grant statement for asset of type {self.__class__}")

    def get_revoke_statement(self, principal: ISnowflakePrincipal, privileges: List[Privilege]) -> str:
        privs = f"{', '.join(p.value for p in privileges)}"
        if isinstance(principal, RoleAsset):
            return f"REVOKE {privs} ON FUTURE {self.grant_target.pluralize()} IN DATABASE {self.database_name} FROM {RoleType.ROLE} {principal.get_identifier()} CASCADE"
        if isinstance(principal, DatabaseRoleAsset):
            return f"REVOKE {privs} ON FUTURE {self.grant_target.pluralize()} IN DATABASE {self.database_name} FROM {RoleType.DATABASE_ROLE} {principal.get_identifier()} CASCADE"
        raise NotImplementedError(f"Can't generate revoke statement for asset of type {self.__class__}")

    def validate_grants(self, privileges: List[Privilege]) -> bool:
        raise NotImplementedError("Grant validation is NYI")
