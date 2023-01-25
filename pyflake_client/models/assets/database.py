"""database"""
from dataclasses import dataclass

from pyflake_client.models.assets.snowflake_asset_interface import ISnowflakeAsset


@dataclass(frozen=True)
class Database(ISnowflakeAsset):
    """Database class"""
    db_name: str
    comment: str
    owner: str = "SYSADMIN"

    def get_create_statement(self):
        """get_create_statement"""
        return f"""CREATE OR REPLACE DATABASE {self.db_name} COMMENT = '{self.comment}';
                   GRANT OWNERSHIP ON DATABASE {self.db_name} TO {self.owner};"""

    def get_delete_statement(self):
        """get_delete_statement"""
        return f"drop database if exists {self.db_name} CASCADE;"
