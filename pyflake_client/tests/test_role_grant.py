# -*- coding: utf-8 -*-
import queue
import uuid

from pyflake_client.client import PyflakeClient
from pyflake_client.models.assets.database import Database as DatabaseAsset
from pyflake_client.models.assets.grant_action import GrantAction
from pyflake_client.models.assets.grants.account_grant import AccountGrant
from pyflake_client.models.assets.grants.database_grant import DatabaseGrant
from pyflake_client.models.assets.grants.schema_grant import SchemaGrant
from pyflake_client.models.assets.grants.warehouse_grant import WarehouseGrant
from pyflake_client.models.assets.role import Role as RoleAsset
from pyflake_client.models.assets.schema import Schema as SchemaAsset
from pyflake_client.models.assets.warehouse import Warehouse as WarehouseAsset
from pyflake_client.models.describables.grant import Grant as GrantDescribable
from pyflake_client.models.describables.role import Role as RoleDescribable
from pyflake_client.models.entities.grant import Grant as GrantEntity
from pyflake_client.models.enums.privilege import Privilege


def test_describe_grant_for_non_existing_role(flake: PyflakeClient):
    """test_describe_grant_for_non_existing_role"""
    ### Act ###
    grants = flake.describe_async(
        describable=GrantDescribable(principal=RoleDescribable(name="NON_EXISTING_ROLE"))
    ).deserialize_many(GrantEntity)

    ### Assert ###
    assert grants == []


def test_role_account_grants(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_role_account_grants"""
    ### Arrange ###
    snowflake_comment: str = f"pyflake_client_test_{uuid.uuid4()}"
    role = RoleAsset("IGT_CREATE_ROLE", snowflake_comment, RoleAsset("USERADMIN"))
    grant = GrantAction(role, AccountGrant(), [Privilege.CREATE_DATABASE, Privilege.CREATE_USER])

    try:
        flake.register_asset_async(role, assets_queue).wait()
        flake.register_asset_async(grant, assets_queue).wait()

        ### Act ###
        grants = flake.describe_async(
            describable=GrantDescribable(principal=RoleDescribable(name=role.name))
        ).deserialize_many(GrantEntity)

        ### Assert ###
        assert grants is not None
        assert len(grants) == 2

        cdb = next((r for r in grants if r.privilege == Privilege.CREATE_DATABASE), None)
        assert cdb is not None
        assert cdb.granted_on == "ACCOUNT"
        assert cdb.granted_by == "SYSADMIN"
        assert cdb.grantee_identifier == role.name
        assert cdb.grantee_type == "ROLE"

        cu = next((r for r in grants if r.privilege == Privilege.CREATE_USER), None)
        assert cu is not None
        assert cu.granted_on == "ACCOUNT"
        assert cu.granted_by == "USERADMIN"
        assert cu.grantee_identifier == role.name
        assert cu.grantee_type == "ROLE"
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_role_database_grants(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_role_database_grant"""
    ### Arrange ###
    snowflake_comment: str = f"pyflake_client_test_{uuid.uuid4()}"
    database = DatabaseAsset("IGT_DEMO", snowflake_comment, owner=RoleAsset("SYSADMIN"))
    role = RoleAsset("IGT_CREATE_ROLE", snowflake_comment, RoleAsset("USERADMIN"))
    grant = GrantAction(
        role, DatabaseGrant(database_name=database.db_name), [Privilege.CREATE_DATABASE_ROLE, Privilege.CREATE_SCHEMA]
    )

    try:
        w1 = flake.register_asset_async(database, assets_queue)
        w2 = flake.register_asset_async(role, assets_queue)
        flake.wait_all([w1, w2])
        flake.register_asset_async(grant, assets_queue).wait()

        ### Act ###
        grants = flake.describe_async(
            describable=GrantDescribable(principal=RoleDescribable(name=role.name))
        ).deserialize_many(GrantEntity)

        ### Assert ###
        assert grants is not None
        assert len(grants) == 2

        cdr = next((r for r in grants if r.privilege == Privilege.CREATE_DATABASE_ROLE), None)
        assert cdr is not None
        assert cdr.granted_on == "DATABASE"
        assert cdr.granted_by == "SYSADMIN"
        assert cdr.grantee_identifier == role.name
        assert cdr.grantee_type == "ROLE"

        cs = next((r for r in grants if r.privilege == Privilege.CREATE_SCHEMA), None)
        assert cs is not None
        assert cs.granted_on == "DATABASE"
        assert cs.granted_by == "SYSADMIN"
        assert cs.grantee_identifier == role.name
        assert cs.grantee_type == "ROLE"
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_role_schema_grants(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_role_schema_grants"""
    snowflake_comment: str = f"pyflake_client_test_{uuid.uuid4()}"
    sysadmin_role = RoleAsset("SYSADMIN")
    database = DatabaseAsset("IGT_DEMO", snowflake_comment, owner=sysadmin_role)
    schema = SchemaAsset(
        db_name=database.db_name,
        schema_name="SOME_SCHEMA",
        comment=snowflake_comment,
        owner=sysadmin_role,
    )
    role = RoleAsset("IGT_CREATE_ROLE", snowflake_comment, RoleAsset("USERADMIN"))
    grant = GrantAction(
        role,
        SchemaGrant(database_name=database.db_name, schema_name=schema.schema_name),
        [Privilege.MONITOR, Privilege.USAGE],
    )

    try:
        w1 = flake.register_asset_async(database, assets_queue)
        w2 = flake.register_asset_async(role, assets_queue)
        flake.wait_all([w1, w2])
        flake.register_asset_async(schema, assets_queue).wait()
        flake.register_asset_async(grant, assets_queue).wait()

        ### Act ###
        grants = flake.describe_async(
            describable=GrantDescribable(principal=RoleDescribable(name=role.name))
        ).deserialize_many(GrantEntity)

        ### Assert ###
        assert grants is not None
        assert len(grants) == 2

        m = next((r for r in grants if r.privilege == Privilege.MONITOR), None)
        assert m is not None
        assert m.granted_on == "SCHEMA"
        assert m.granted_by == "SYSADMIN"
        assert m.grantee_identifier == role.name
        assert m.grantee_type == "ROLE"

        u = next((r for r in grants if r.privilege == Privilege.USAGE), None)
        assert u is not None
        assert u.granted_on == "SCHEMA"
        assert u.granted_by == "SYSADMIN"
        assert u.grantee_identifier == role.name
        assert u.grantee_type == "ROLE"
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_role_warehouse_grant(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_role_warehouse_grant"""
    ### Arrange ###
    snowflake_comment: str = f"pyflake_client_test_{uuid.uuid4()}"
    role = RoleAsset("IGT_CREATE_ROLE", snowflake_comment, RoleAsset("USERADMIN"))
    warehouse: WarehouseAsset = WarehouseAsset("IGT_DEMO_WH", snowflake_comment, owner=RoleAsset("SYSADMIN"))

    grant = GrantAction(
        role, WarehouseGrant(warehouse_name=warehouse.warehouse_name), [Privilege.MONITOR, Privilege.USAGE]
    )

    try:
        w1 = flake.register_asset_async(role, assets_queue)
        w2 = flake.register_asset_async(warehouse, assets_queue)
        flake.wait_all([w1, w2])
        flake.register_asset_async(grant, assets_queue).wait()

        ### Act ###
        grants = flake.describe_async(
            describable=GrantDescribable(principal=RoleDescribable(name=role.name))
        ).deserialize_many(GrantEntity)

        ### Assert ###
        assert grants is not None
        assert len(grants) == 2

        m = next((r for r in grants if r.privilege == Privilege.MONITOR), None)
        assert m is not None
        assert m.granted_on == "WAREHOUSE"
        assert m.granted_by == "SYSADMIN"
        assert m.grantee_identifier == role.name
        assert m.grantee_type == "ROLE"

        u = next((r for r in grants if r.privilege == Privilege.USAGE), None)
        assert u is not None
        assert u.granted_on == "WAREHOUSE"
        assert u.granted_by == "SYSADMIN"
        assert u.grantee_identifier == role.name
        assert u.grantee_type == "ROLE"
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)
