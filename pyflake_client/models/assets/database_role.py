"""database_role"""
from dataclasses import dataclass
from typing import Union

from pyflake_client.models.assets.snowflake_asset_interface import ISnowflakeAsset
from pyflake_client.models.assets.grants.snowflake_principle_interface import (
    ISnowflakePrinciple,
)
from pyflake_client.models.assets.role import Role as AccountRole


@dataclass(frozen=True)
class DatabaseRole(ISnowflakeAsset, ISnowflakePrinciple):
    """DatabaseRole"""

    name: str
    database_name: str
    owner: Union[ISnowflakePrinciple, None] = None
    comment: str = ""

    def get_create_statement(self) -> str:
        """get_create_statement"""
        if self.owner is None:
            raise ValueError("Create statement not supported for owner-less roles")

        if isinstance(self.owner, DatabaseRole):
            role_type = "DATABASE ROLE"
        elif isinstance(self.owner, AccountRole):
            role_type = "ROLE"
        else:
            raise NotImplementedError(
                "Ownership is not implementer for this interface type"
            )

        return f"""CREATE OR REPLACE DATABASE ROLE  {self.database_name}.{self.name} COMMENT = '{self.comment}';
                   GRANT OWNERSHIP ON DATABASE ROLE {self.database_name}.{self.name} TO {role_type} {self.owner.get_identifier()}
                    REVOKE CURRENT GRANTS;"""

    def get_delete_statement(self) -> str:
        """get_delete_statement"""
        return f"DROP DATABASE ROLE IF EXISTS {self.database_name}.{self.name}"

    def get_identifier(self) -> str:
        return self.name
