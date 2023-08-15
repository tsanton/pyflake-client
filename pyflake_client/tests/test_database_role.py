# -*- coding: utf-8 -*-
import queue
from datetime import date

from pyflake_client.client import PyflakeClient
from pyflake_client.models.assets.database import Database as DatabaseAsset
from pyflake_client.models.assets.database_role import DatabaseRole
from pyflake_client.models.assets.role import Role as RoleAsset
from pyflake_client.models.describables.database_role import (
    DatabaseRole as RoleDescribable,
)
from pyflake_client.models.entities.role import Role as RoleEntity


def test_create_database_role(flake: PyflakeClient, assets_queue: queue.LifoQueue, rand_str: str, comment: str):
    ### Arrange ###
    database = DatabaseAsset(f"PYFLAKE_CLIENT_TEST_DB_{rand_str}", comment, owner=RoleAsset("SYSADMIN"))
    role: DatabaseRole = DatabaseRole(
        name="PYFLAKE_CLIENT_TEST_DB_ROLE",
        database_name=database.db_name,
        comment=comment,
        owner=RoleAsset("USERADMIN"),
    )

    try:
        flake.register_asset_async(database, assets_queue).wait()
        flake.register_asset_async(role, assets_queue).wait()

        ### Act ###
        sf_role = flake.describe_async(RoleDescribable(name=role.name, db_name=database.db_name)).deserialize_one(
            RoleEntity
        )

        ### Assert ###
        assert sf_role is not None
        assert sf_role.name == role.name
        assert sf_role.comment == role.comment
        assert sf_role.owner == "USERADMIN"
        assert sf_role.created_on is not None
        assert sf_role.created_on.date() == date.today()
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_get_database_role_async(flake: PyflakeClient, assets_queue: queue.LifoQueue, rand_str: str, comment: str):
    ### Arrange ###
    database = DatabaseAsset(f"PYFLAKE_CLIENT_TEST_DB_{rand_str}", comment, owner=RoleAsset("SYSADMIN"))
    role: DatabaseRole = DatabaseRole(
        name="PYFLAKE_CLIENT_TEST_DB_ROLE",
        database_name=database.db_name,
        comment=comment,
        owner=RoleAsset("USERADMIN"),
    )

    try:
        flake.register_asset_async(database, assets_queue).wait()
        flake.create_asset_async(role).wait()

        ### Act ###
        r = flake.describe_async(RoleDescribable(name=role.name, db_name=database.db_name)).deserialize_one(RoleEntity)

        ### Assert ###
        assert r is not None
        assert r.name == role.name
        assert r.comment == role.comment
        assert r.owner == "USERADMIN"
        assert r.granted_roles == None
        assert r.created_on.date() == date.today()
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_get_database_role_from_db_not_exists(flake: PyflakeClient):
    sf_role = flake.describe_async(
        RoleDescribable(name="SNOWFLAKE", db_name="I_SURELY_DO_NOT_EXIST_DATABASE")
    ).deserialize_one(RoleEntity)
    assert sf_role is None


def test_get_database_role_not_exists(
    flake: PyflakeClient, assets_queue: queue.LifoQueue, rand_str: str, comment: str
):
    try:
        database = DatabaseAsset(f"PYFLAKE_CLIENT_TEST_DB_{rand_str}", comment, owner=RoleAsset("SYSADMIN"))

        flake.register_asset_async(database, assets_queue).wait()
        sf_role = flake.describe_async(
            RoleDescribable(name="I_SURELY_DO_NOT_EXIST_DATABASE_ROLE", db_name=database.db_name)
        ).deserialize_one(RoleEntity)
        assert sf_role is None
    finally:
        flake.delete_assets(assets_queue)
