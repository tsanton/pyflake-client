"""role"""
from dataclasses import dataclass
from typing import Union

from pyflake_client.models.assets.snowflake_asset_interface import ISnowflakeAsset
from pyflake_client.models.assets.snowflake_principal_interface import ISnowflakePrincipal
from pyflake_client.models.describables.snowflake_grant_principal import ISnowflakeGrantPrincipal


@dataclass(frozen=True)
class Role(ISnowflakeAsset, ISnowflakePrincipal, ISnowflakeGrantPrincipal):
    """Role"""

    name: str
    owner: Union[ISnowflakePrincipal, None] = None
    comment: str = ""

    def get_create_statement(self) -> str:
        """get_create_statement"""
        if self.owner is None:
            raise ValueError("Create statement not supported for owner-less roles")

        if isinstance(self.owner, Role):
            role_type = "ROLE"
        else:
            raise NotImplementedError(
                "Ownership is not implementer for this interface type"
            )

        return f"""CREATE OR REPLACE ROLE {self.name} COMMENT = '{self.comment}';
                   GRANT OWNERSHIP ON {role_type} {self.name} to {self.owner.get_identifier()} REVOKE CURRENT GRANTS;"""

    def get_delete_statement(self):
        """get_delete_statement"""
        return f"DROP ROLE IF EXISTS {self.name};"

    def get_identifier(self):
        return self.name

    def get_snowflake_type(self) -> str:
        return "ROLE"
