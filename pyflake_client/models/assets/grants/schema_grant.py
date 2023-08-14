# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import List, TypeVar

from pyflake_client.models.assets.database_role import DatabaseRole as DatabaseRoleAsset
from pyflake_client.models.assets.grants.snowflake_grant_asset import (
    ISnowflakeGrantAsset,
)
from pyflake_client.models.assets.role import Role as RoleAsset
from pyflake_client.models.assets.snowflake_principal_interface import (
    ISnowflakePrincipal,
)
from pyflake_client.models.enums.privilege import Privilege

T = TypeVar("T", bound=ISnowflakePrincipal)


@dataclass
class SchemaGrant(ISnowflakeGrantAsset):
    database_name: str
    schema_name: str

    def get_grant_statement(self, principal: ISnowflakePrincipal, privileges: List[Privilege]) -> str:
        privs = f"{', '.join(p.value for p in privileges)}"
        if isinstance(principal, RoleAsset):
            return (
                f"GRANT {privs} ON SCHEMA {self.database_name}.{self.schema_name} TO ROLE {principal.get_identifier()}"
            )
        if isinstance(principal, DatabaseRoleAsset):
            return f"GRANT {privs} ON SCHEMA {self.database_name}.{self.schema_name} TO DATABASE ROLE {principal.get_identifier()}"

        raise NotImplementedError(f"Can't generate grant statement for asset of type {self.__class__}")

    def get_revoke_statement(self, principal: ISnowflakePrincipal, privileges: List[Privilege]) -> str:
        privs = f"{', '.join(p.value for p in privileges)}"
        if isinstance(principal, RoleAsset):
            return f"REVOKE {privs} ON SCHEMA {self.database_name}.{self.schema_name} FROM ROLE {principal.get_identifier()}"
        if isinstance(principal, DatabaseRoleAsset):
            return f"REVOKE {privs} ON SCHEMA {self.database_name}.{self.schema_name} FROM DATABASE ROLE {principal.get_identifier()} CASCADE"

        raise NotImplementedError(f"Can't generate revoke statement for asset of type {self.__class__}")

    def validate_grants(self, privileges: List[Privilege]) -> bool:
        raise NotImplementedError("Grant validation is NYI")
