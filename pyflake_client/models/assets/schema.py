"""schema"""
# pylint: disable=line-too-long
from dataclasses import dataclass

from pyflake_client.models.assets.role import Role as RoleAsset
from pyflake_client.models.assets.database_role import DatabaseRole as DatabaseRoleAsset
from pyflake_client.models.assets.database import Database
from pyflake_client.models.assets.snowflake_principal_interface import ISnowflakePrincipal
from pyflake_client.models.assets.snowflake_asset_interface import ISnowflakeAsset


@dataclass(frozen=True)
class Schema(ISnowflakeAsset):
    """schema class"""

    database: Database
    schema_name: str
    comment: str
    owner: ISnowflakePrincipal

    def get_create_statement(self):
        """get_create_statement"""
        if self.owner is None:
            raise ValueError("Create statement not supported for owner-less schemas")

        if isinstance(self.owner, RoleAsset):
            grantee_type = "ROLE"
        elif isinstance(self.owner, DatabaseRoleAsset):
            grantee_type = "DATABASE ROLE"
        else:
            raise NotImplementedError("Ownership is not implementer for asset of type {self.owner.__class__}")
        
        return f"""CREATE OR REPLACE SCHEMA {self.database.db_name}.{self.schema_name} WITH MANAGED ACCESS COMMENT = '{self.comment}';
                   GRANT OWNERSHIP ON SCHEMA {self.database.db_name}.{self.schema_name} to {grantee_type} {self.owner.get_identifier()} REVOKE CURRENT GRANTS;"""

    def get_delete_statement(self):
        """get_delete_statement"""
        return f"DROP SCHEMA IF EXISTS {self.database.db_name}.{self.schema_name} CASCADE;"
