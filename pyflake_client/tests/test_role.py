# -*- coding: utf-8 -*-
import queue
from datetime import date

from pyflake_client.client import PyflakeClient
from pyflake_client.models.assets.role import Role as RoleAsset
from pyflake_client.models.describables.role import Role as RoleDescribable
from pyflake_client.models.entities.role import Role as RoleEntity


def test_create_role(flake: PyflakeClient, assets_queue: queue.LifoQueue, rand_str: str, comment: str):
    ### Arrange ###
    role = RoleAsset(
        name=f"PYFLAKE_CLIENT_TEST_ROLE_{rand_str}",
        owner=RoleAsset("USERADMIN"),
        comment=comment,
    )

    try:
        flake.register_asset_async(role, assets_queue).wait()

        ### Act ###
        sf_role = flake.describe_async(RoleDescribable(role.name)).deserialize_one(RoleEntity)
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
    ### Act ###
    role = flake.describe_async(RoleDescribable("ACCOUNTADMIN")).deserialize_one(RoleEntity)

    ### Assert ###
    assert role is not None
    assert role.name == "ACCOUNTADMIN"
    assert role.comment == "Account administrator can manage all aspects of the account."


def test_get_role_that_does_not_exist(flake: PyflakeClient):
    ### Act ###
    role = flake.describe_async(RoleDescribable("I_SURELY_DO_NOT_EXIST")).deserialize_one(RoleEntity)

    ### Assert ###
    assert role is None
