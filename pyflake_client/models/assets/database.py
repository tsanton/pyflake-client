"""database"""
from dataclasses import dataclass

from pyflake_client.models.assets.snowflake_asset_interface import ISnowflakeAsset
from pyflake_client.models.assets.snowflake_principal_interface import ISnowflakePrincipal


@dataclass(frozen=True)
class Database(ISnowflakeAsset):
    """Database class"""

    db_name: str
    comment: str
    owner: ISnowflakePrincipal

    def get_create_statement(self):
        """get_create_statement"""
        if self.owner is None:
            raise ValueError("Create statement not supported for owner-less databases")
        return f"""CREATE OR REPLACE DATABASE {self.db_name} COMMENT = '{self.comment}';
                   GRANT OWNERSHIP ON DATABASE {self.db_name} TO {self.owner.get_identifier()};"""

    def get_delete_statement(self):
        """get_delete_statement"""
        return f"drop database if exists {self.db_name} CASCADE;"
