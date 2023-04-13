"""warehouse"""
# pylint: disable=line-too-long
from typing import Union

from dataclasses import dataclass
from pyflake_client.models.assets.snowflake_asset_interface import ISnowflakeAsset
from pyflake_client.models.assets.snowflake_principal_interface import ISnowflakePrincipal
from pyflake_client.models.assets.enums import WarehouseSize 


@dataclass(frozen=True)
class Warehouse(ISnowflakeAsset):
    """Warehouse"""
    warehouse_name: str
    comment: str
    size: WarehouseSize = WarehouseSize.XSMALL
    warehouse_type: str = "STANDARD"
    auto_resume: bool = True
    auto_suspend: int = 30
    init_suspend: bool = True
    owner: Union[ISnowflakePrincipal, None] = None

    def get_create_statement(self):
        """get_create_statement"""
        if self.owner is None:
            raise ValueError("Create statement not supported for owner-less warehouses")
        
        return f"""CREATE OR REPLACE WAREHOUSE {self.warehouse_name} WITH WAREHOUSE_SIZE = '{self.size.value}' WAREHOUSE_TYPE = '{self.warehouse_type}' AUTO_RESUME = {self.auto_resume} AUTO_SUSPEND = {self.auto_suspend} INITIALLY_SUSPENDED = {self.init_suspend} COMMENT = '{self.comment}';
                   GRANT OWNERSHIP ON WAREHOUSE {self.warehouse_name} to {self.owner.get_identifier()} REVOKE CURRENT GRANTS;"""

    def get_delete_statement(self):
        """get_delete_statement"""
        return f"DROP WAREHOUSE IF EXISTS {self.warehouse_name};"
