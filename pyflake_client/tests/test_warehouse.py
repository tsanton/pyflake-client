"""test_warehouse"""
# pylint: disable=line-too-long
import queue
import uuid
from datetime import date

from pyflake_client.client import PyflakeClient

from pyflake_client.models.assets.role import Role as RoleAsset
from pyflake_client.models.assets.warehouse import Warehouse as WarehouseAsset

from pyflake_client.models.entities.warehouse import Warehouse as WarehouseEntity

from pyflake_client.models.describables.warehouse import Warehouse as WarehouseDescribable


def test_create_warehouse(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_create_warehouse"""
    ### Arrange ###
    warehouse: WarehouseAsset = WarehouseAsset("IGT_DEMO_WH", f"pyflake_client_test_{uuid.uuid4()}", owner=RoleAsset("SYSADMIN"))

    try:
        flake.register_asset(warehouse, assets_queue)

        ### Act ###
        sf_wh = flake.describe_one(WarehouseDescribable(warehouse.warehouse_name), WarehouseEntity)
        
        ### Assert ###
        assert sf_wh is not None
        assert sf_wh.name == warehouse.warehouse_name
        assert sf_wh.comment == warehouse.comment
        assert sf_wh.owner == "SYSADMIN"
        assert sf_wh.created_on.date() == date.today()
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)
