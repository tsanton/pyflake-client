"""test_role_warehouse_grant"""


def test_come_back_to_this_later4():
    assert False


# import queue
# import uuid


# from pyflake_client.client import PyflakeClient
# from pyflake_client.models.assets.role import Role as AssetsRole
# from pyflake_client.models.assets.grant import Grant as GrantAsset
# from pyflake_client.models.assets.grants.role_warehouse_grant import RoleWarehouseGrant
# from pyflake_client.models.describables.grants.role_grant import RoleGrant as RoleGrantDescribable
# from pyflake_client.models.entities.grants.role_grant import RoleGrants as RoleGrantsEntity

# from pyflake_client.models.assets.warehouse import Warehouse as WarehouseAsset
# from pyflake_client.models.entities.warehouse import Warehouse as WarehouseEntity
# from pyflake_client.models.describables.warehouse import Warehouse as WarehouseDescribable

# def test_grant_role_warehouse_privileges(flake: PyflakeClient, assets_queue: queue.LifoQueue):
#     """test_grant_role_warehouse_privileges"""
#     ### Arrange ###
#     role = AssetsRole("IGT_CREATE_ROLE", AssetsRole("USERADMIN"), f"pyflake_client_TEST_{uuid.uuid4()}")
#     warehouse: WarehouseAsset = WarehouseAsset("IGT_DEMO_WH", f"pyflake_client_TEST_{uuid.uuid4()}")
#     privilege = GrantAsset(RoleWarehouseGrant(role.name, warehouse.warehouse_name), ["USAGE"])

#     try:
#         flake.register_asset(role, assets_queue)
#         flake.register_asset(warehouse, assets_queue)
#         flake.register_asset(privilege, assets_queue)

#         ### Act ###
#         granted: RoleGrantsEntity = flake.describe(RoleGrantDescribable(role.name), RoleGrantsEntity)

#         ### Assert ###
#         assert granted.role_name == role.name
#         assert len(granted.grants) == 1

#         wh_grant = next((r for r in granted.grants if r.granted_on == "WAREHOUSE" and r.privilege == "USAGE"), None)
#         assert wh_grant is not None
#         assert wh_grant.name == warehouse.warehouse_name
#         assert wh_grant.granted_by == "SYSADMIN"
#     finally:
#         ### Cleanup ###
#         flake.delete_assets(assets_queue)
