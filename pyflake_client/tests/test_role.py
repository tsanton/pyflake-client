# -*- coding: utf-8 -*-
import queue
import uuid
from datetime import date

from pyflake_client.client import PyflakeClient
from pyflake_client.models.assets.role import Role as RoleAsset
from pyflake_client.models.describables.role import Role as DescribablesRole
from pyflake_client.models.entities.role import Role as EntitiesRole


def test_create_role(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_create_role"""
    ### Arrange ###
    role: RoleAsset = RoleAsset(
        name="IGT_CREATE_ROLE",
        owner=RoleAsset("USERADMIN"),
        comment=f"pyflake_client_test_{uuid.uuid4()}",
    )

    try:
        flake.register_asset_async(role, assets_queue).wait()

        ### Act ###
        sf_role = flake.describe_async(DescribablesRole(role.name)).deserialize_one(EntitiesRole)
        ### Assert ###
        assert sf_role is not None
        assert sf_role.name == role.name
        assert sf_role.comment == role.comment
        assert sf_role.owner == "USERADMIN"
        assert sf_role.created_on.date() == date.today()
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_get_role(flake: PyflakeClient):
    """test_get_role"""
    ### Act ###
    role = flake.describe_async(DescribablesRole("ACCOUNTADMIN")).deserialize_one(EntitiesRole)

    ### Assert ###
    assert role is not None
    assert role.name == "ACCOUNTADMIN"
    assert role.comment == "Account administrator can manage all aspects of the account."


def test_get_role_that_does_not_exist(flake: PyflakeClient):
    """test_get_role_that_does_not_exist"""
    ### Act ###
    role = flake.describe_async(DescribablesRole("I_SURELY_DO_NOT_EXIST")).deserialize_one(EntitiesRole)

    ### Assert ###
    assert role is None
