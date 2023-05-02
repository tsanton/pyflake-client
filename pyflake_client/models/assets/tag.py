from dataclasses import dataclass
from typing import List
from pyflake_client.models.assets.snowflake_principal_interface import ISnowflakePrincipal
from pyflake_client.models.assets.role import Role
from pyflake_client.models.assets.snowflake_asset_interface import ISnowflakeAsset


@dataclass
class Tag(ISnowflakeAsset):
    database_name: str
    schema_name: str
    tag_name: str
    tag_values: List[str]
    comment: str = ""
    owner: ISnowflakePrincipal = Role(name="SYSADMIN")

    def get_create_statement(self) -> str:
        if self.owner is None:
            raise ValueError("Create statement not supported for owner-less tags")
        snowflake_principal_type = self.owner.get_snowflake_type().snowflake_type()
        if snowflake_principal_type not in ["ROLE", "DATABASE ROLE"]:
            raise NotImplementedError("Ownership is not implementer for asset of type {self.owner.__class__}")
        
        query = ""
        query += f"CREATE OR REPLACE TAG {self.database_name}.{self.schema_name}.{self.tag_name}"
        if len(self.tag_values) > 0:
            tag_values = ",".join(f"'{t}'" for t in self.tag_values)
            query += f" ALLOWED_VALUES {tag_values}"

        query += f" COMMENT = '{self.comment}';"
        query += f"\nGRANT OWNERSHIP ON TAG {self.database_name}.{self.schema_name}.{self.tag_name} TO {snowflake_principal_type} {self.owner.get_identifier()}"
        return query

    def get_delete_statement(self) -> str:
        query = f"DROP TAG {self.database_name}.{self.schema_name}.{self.tag_name}"
        return query
