"""schema"""
# pylint: disable=line-too-long
from dataclasses import dataclass

from pyflake_client.models.assets.snowflake_principal_interface import ISnowflakePrincipal
from pyflake_client.models.assets.snowflake_asset_interface import ISnowflakeAsset


@dataclass(frozen=True)
class Schema(ISnowflakeAsset):
    """schema class"""
    db_name: str
    schema_name: str
    comment: str
    owner: ISnowflakePrincipal

    def get_create_statement(self):
        """get_create_statement"""
        if self.owner is None:
            raise ValueError("Create statement not supported for owner-less schemas")

        snowflake_principal_type = self.owner.get_snowflake_type().snowflake_type()
        if snowflake_principal_type not in ["ROLE", "DATABASE ROLE"]:
            raise NotImplementedError("Ownership is not implemented for asset of type {self.owner.__class__}")
        
        return f"""CREATE OR REPLACE SCHEMA {self.db_name}.{self.schema_name} WITH MANAGED ACCESS COMMENT = '{self.comment}';
                   GRANT OWNERSHIP ON SCHEMA {self.db_name}.{self.schema_name} to {snowflake_principal_type} {self.owner.get_identifier()} REVOKE CURRENT GRANTS;"""

    def get_delete_statement(self):
        """get_delete_statement"""
        return f"DROP SCHEMA IF EXISTS {self.db_name}.{self.schema_name} CASCADE;"
