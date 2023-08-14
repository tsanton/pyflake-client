# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import Union

from pyflake_client.models.assets.snowflake_asset_interface import ISnowflakeAsset
from pyflake_client.models.assets.snowflake_principal_interface import (
    ISnowflakePrincipal,
)
from pyflake_client.models.enums.principal import Principal


@dataclass(frozen=True)
class DatabaseRole(ISnowflakeAsset, ISnowflakePrincipal):
    """DatabaseRole"""

    name: str
    database_name: str
    comment: str = ""
    owner: Union[ISnowflakePrincipal, None] = None

    def get_create_statement(self) -> str:
        """get_create_statement"""
        if self.owner is None:
            raise ValueError("Create statement not supported for owner-less roles")

        snowflake_principal_type = self.owner.get_snowflake_type().snowflake_type()
        if snowflake_principal_type not in ["ROLE", "DATABASE ROLE"]:
            raise NotImplementedError("Ownership is not implemented for asset of type {self.owner.__class__}")

        return f"""CREATE OR REPLACE DATABASE ROLE {self.database_name}.{self.name} COMMENT = '{self.comment}';
                   GRANT OWNERSHIP ON DATABASE ROLE {self.database_name}.{self.name} TO {snowflake_principal_type} {self.owner.get_identifier()}
                   REVOKE CURRENT GRANTS;"""

    def get_delete_statement(self) -> str:
        """get_delete_statement"""
        return f"DROP DATABASE ROLE IF EXISTS {self.database_name}.{self.name}"

    def get_identifier(self) -> str:
        return f"{self.database_name}.{self.name}"

    def get_snowflake_type(self) -> Principal:
        return Principal.DATABASE_ROLE
