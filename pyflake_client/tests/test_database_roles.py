# -*- coding: utf-8 -*-
import queue
import uuid

from pyflake_client.client import PyflakeClient
from pyflake_client.models.assets.database import Database as DatabaseAsset
from pyflake_client.models.assets.database_role import DatabaseRole
from pyflake_client.models.assets.role import Role as RoleAsset
from pyflake_client.models.describables.database_roles import DatabaseRoles
from pyflake_client.models.entities.role import Role as RoleEntity
from pyflake_client.tests.utilities import find


def test_get_single_database_role(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_create_role"""
    snowflake_comment: str = f"pyflake_client_test_{uuid.uuid4()}"
    ### Arrange ###
    database = DatabaseAsset("IGT_DEMO", snowflake_comment, owner=RoleAsset("SYSADMIN"))
    role: DatabaseRole = DatabaseRole(
        name="IGT_ROLE",
        database_name=database.db_name,
        comment=snowflake_comment,
        owner=RoleAsset("USERADMIN"),
    )

    try:
        flake.register_asset_async(database, assets_queue).wait()
        flake.create_asset_async(role).wait()

        ### Act ###
        roles = flake.describe_async(DatabaseRoles(db_name=database.db_name)).deserialize_many(RoleEntity)

        ### Assert ###
        assert roles is not None
        assert len(roles) == 1
        db_role = find(roles, lambda x: x.name == role.name)
        assert db_role is not None
        assert db_role.owner == "USERADMIN"
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_get_database_roles(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_create_role"""
    snowflake_comment: str = f"pyflake_client_test_{uuid.uuid4()}"
    ### Arrange ###
    database = DatabaseAsset("IGT_DEMO", snowflake_comment, owner=RoleAsset("SYSADMIN"))
    role_1: DatabaseRole = DatabaseRole(
        name="IGT_ROLE_1",
        database_name=database.db_name,
        comment=snowflake_comment,
        owner=RoleAsset("USERADMIN"),
    )
    role_2: DatabaseRole = DatabaseRole(
        name="IGT_ROLE_2",
        database_name=database.db_name,
        comment=snowflake_comment,
        owner=RoleAsset("USERADMIN"),
    )

    try:
        flake.register_asset_async(database, assets_queue).wait()
        w1 = flake.create_asset_async(role_1)
        w2 = flake.create_asset_async(role_2)
        flake.wait_all([w1, w2])

        ### Act ###
        roles = flake.describe_async(DatabaseRoles(db_name=database.db_name)).deserialize_many(RoleEntity)

        ### Assert ###
        assert roles is not None
        assert len(roles) == 2

        db_role_1 = find(roles, lambda x: x.name == role_1.name)
        assert db_role_1 is not None
        assert db_role_1.owner == "USERADMIN"

        db_role_2 = find(roles, lambda x: x.name == role_2.name)
        assert db_role_2 is not None
        assert db_role_2.owner == "USERADMIN"
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_get_database_role_none_exist(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_create_role"""
    snowflake_comment: str = f"pyflake_client_test_{uuid.uuid4()}"
    ### Arrange ###
    database = DatabaseAsset("IGT_DEMO", snowflake_comment, owner=RoleAsset("SYSADMIN"))

    try:
        flake.register_asset_async(database, assets_queue).wait()
        ### Act ###
        roles = flake.describe_async(DatabaseRoles(db_name=database.db_name)).deserialize_many(RoleEntity)

        ### Assert ###
        assert roles == []
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_get_database_role_async(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_create_role"""
    snowflake_comment: str = f"pyflake_client_test_{uuid.uuid4()}"
    ### Arrange ###
    database = DatabaseAsset("IGT_DEMO", snowflake_comment, owner=RoleAsset("SYSADMIN"))
    role: DatabaseRole = DatabaseRole(
        name="IGT_ROLE",
        database_name=database.db_name,
        comment=snowflake_comment,
        owner=RoleAsset("USERADMIN"),
    )

    try:
        flake.register_asset_async(database, assets_queue).wait()
        flake.create_asset_async(role).wait()

        ### Act ###
        roles = flake.describe_async(DatabaseRoles(db_name=database.db_name)).deserialize_many(RoleEntity)

        ### Assert ###
        assert roles is not None
        assert len(roles) == 1
        assert find(roles, lambda x: x.name == role.name) is not None
        assert find(roles, lambda x: x.name == role.name).owner == "USERADMIN"
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)
