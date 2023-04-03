from typing import List, Type, TypeVar
from pyflake_client.models.assets.grants.snowflake_principal_interface import (
    ISnowflakePrincipal,
)
from pyflake_client.models.assets.role import Role as AssetsRole
from pyflake_client.models.enums.privilege import Privilege

T = TypeVar("T", bound=ISnowflakePrincipal)


class AccountGrant(Type[T]):
    grantable: Type[T]

    def get_grant_statement(self, privileges: List[Privilege]) -> str:
        privs = f"{', '.join(p.value for p in privileges)}"
        if isinstance(self.grantable, AssetsRole):
            return f"GRANT {privs} ON ACCOUNT TO ROLE {self.grantable.get_identifier()}"

        raise NotImplementedError(
            f"Can't generate grant statement for asset of type {self.__class__}"
        )

    def get_revoke_statement(self, privileges: List[Privilege]) -> str:
        privs = f"{', '.join(p.value for p in privileges)}"
        if isinstance(self.grantable, AssetsRole):
            return (
                f"REVOKE {privs} ON ACCOUNT FROM ROLE {self.grantable.get_identifier()}"
            )

        raise NotImplementedError(
            f"Can't generate revoke statement for asset of type {self.__class__}"
        )

    def validate_grants(self, privileges: List[Privilege]) -> bool:
        raise NotImplementedError("Grant validation is NYI")
