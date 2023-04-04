from dataclasses import dataclass
from typing import List, Union
from pyflake_client.models.assets.grants.snowflake_principal_interface import (
    ISnowflakePrincipal,
)
from pyflake_client.models.assets.role import Role
from pyflake_client.models.assets.snowflake_asset_interface import ISnowflakeAsset


@dataclass
class Tag(ISnowflakeAsset):
    database_name: Union[str, None]
    schema_name: Union[str, None]
    tag_name: Union[str, None]
    tag_values: List[str]
    owner: ISnowflakePrincipal = Role(name="SYSADMIN")
    comment: str = ""

    def get_create_statement(self) -> str:
        query = ""
        query += f"CREATE OR REPLACE TAG {self.database_name}.{self.schema_name}.{self.tag_name}"
        if len(self.tag_values) > 0:
            tag_values = ",".join(f"'{t}'" for t in self.tag_values)
            query += f" ALLOWED_VALUES {tag_values}"

        query += f" COMMENT = '{self.comment}';"
        query += f"\nGRANT OWNERSHIP ON TAG {self.database_name}.{self.schema_name}.{self.tag_name} TO {self.owner.get_snowflake_type()} {self.owner.get_identifier()}"
        return query

    def get_delete_statement(self) -> str:
        query = f"DROP TAG {self.database_name}.{self.schema_name}.{self.tag_name}"
        return query
