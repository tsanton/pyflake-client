# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import Union

from pyflake_client.models.assets.snowflake_asset_interface import ISnowflakeAsset
from pyflake_client.models.assets.snowflake_principal_interface import (
    ISnowflakePrincipal,
)
from pyflake_client.models.describables.snowflake_grant_principal import (
    ISnowflakeGrantPrincipal,
)
from pyflake_client.models.enums.principal import Principal


@dataclass(frozen=True)
class User(ISnowflakeAsset, ISnowflakePrincipal, ISnowflakeGrantPrincipal):
    name: str
    comment: str = ""
    owner: Union[ISnowflakePrincipal, None] = None

    def get_create_statement(self) -> str:
        if self.owner is None:
            raise ValueError("Create statement not supported for owner-less users")

        snowflake_principal_type = self.owner.get_snowflake_type().snowflake_type()
        if snowflake_principal_type not in ["ROLE"]:
            raise NotImplementedError("Ownership is not implemented for asset of type {self.owner.__class__}")

        return f"""CREATE OR REPLACE USER {self.name} COMMENT = '{self.comment}';
                   GRANT OWNERSHIP ON USER {self.name} to {snowflake_principal_type} {self.owner.get_identifier()} REVOKE CURRENT GRANTS;"""

    def get_delete_statement(self):
        return f"DROP USER IF EXISTS {self.name};"

    def get_identifier(self):
        return self.name

    def get_snowflake_type(self) -> Principal:
        return Principal.USER
