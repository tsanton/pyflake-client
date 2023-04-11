"""schema"""
# pylint: disable=line-too-long
from dataclasses import dataclass
from pyflake_client.models.assets.database import Database
from pyflake_client.models.assets.grants.snowflake_principal_interface import (
    ISnowflakePrincipal,
)
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
        return f"""CREATE OR REPLACE SCHEMA {self.database.db_name}.{self.schema_name} WITH MANAGED ACCESS COMMENT = '{self.comment}';
                   GRANT OWNERSHIP ON SCHEMA {self.database.db_name}.{self.schema_name} to {self.owner.get_identifier()} REVOKE CURRENT GRANTS;"""

    def get_delete_statement(self):
        """get_delete_statement"""
        return (
            f"DROP SCHEMA IF EXISTS {self.database.db_name}.{self.schema_name} CASCADE;"
        )
