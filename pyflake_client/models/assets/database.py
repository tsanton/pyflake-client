# -*- coding: utf-8 -*-
from dataclasses import dataclass

from pyflake_client.models.assets.snowflake_asset_interface import ISnowflakeAsset
from pyflake_client.models.assets.snowflake_principal_interface import (
    ISnowflakePrincipal,
)


@dataclass(frozen=True)
class Database(ISnowflakeAsset):
    db_name: str
    comment: str
    owner: ISnowflakePrincipal

    def get_create_statement(self):
        if self.owner is None:
            raise ValueError("Create statement not supported for owner-less databases")
        snowflake_principal_type = self.owner.get_snowflake_type().snowflake_type()
        if snowflake_principal_type not in ["ROLE"]:
            raise NotImplementedError("Ownership is not implemented for asset of type {self.owner.__class__}")

        return f"""CREATE OR REPLACE DATABASE {self.db_name} COMMENT = '{self.comment}';
                   GRANT OWNERSHIP ON DATABASE {self.db_name} TO {snowflake_principal_type} {self.owner.get_identifier()};"""

    def get_delete_statement(self):
        return f"drop database if exists {self.db_name} CASCADE;"
