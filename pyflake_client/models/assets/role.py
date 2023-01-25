"""role"""
from dataclasses import dataclass
from pyflake_client.models.assets.snowflake_asset_interface import ISnowflakeAsset


@dataclass(frozen=True)
class Role(ISnowflakeAsset):
    """Role"""
    name: str
    owner: str
    comment: str

    def get_create_statement(self):
        """get_create_statement"""
        return f"""CREATE OR REPLACE ROLE {self.name} COMMENT = '{self.comment}';
                   GRANT OWNERSHIP ON ROLE {self.name} to {self.owner} REVOKE CURRENT GRANTS;"""

    def get_delete_statement(self):
        """get_delete_statement"""
        return f"DROP ROLE IF EXISTS {self.name};"
