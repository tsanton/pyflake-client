"""test_user"""
import os
import queue
import uuid
from datetime import date


from pyflake_client.client import PyflakeClient
from pyflake_client.models.assets.user import User as UserAsset
from pyflake_client.models.assets.role import Role as RoleAsset
from pyflake_client.models.describables.user import User as DescribablesUser
from pyflake_client.models.entities.user import User as EntitiesUser


def test_create_user(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_create_role"""
    ### Arrange ###
    user = UserAsset(
        name="IGT_CREATE_USER",
        owner=RoleAsset("USERADMIN"),
        comment=f"pyflake_client_test_{uuid.uuid4()}",
    )

    try:
        flake.register_asset(user, assets_queue)

        ### Act ###
        sf_user = flake.describe_one(DescribablesUser(user.name), EntitiesUser)
        ### Assert ###
        assert sf_user is not None
        assert sf_user.name == user.name
        assert sf_user.comment == user.comment
        assert sf_user.owner == "USERADMIN"
        assert sf_user.created_on.date() == date.today()
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_get_user(flake: PyflakeClient):
    """test_get_role"""
    ### Act ###
    user_id = os.environ.get("SNOWFLAKE_UID")
    assert user_id is not None
    user = flake.describe_one(DescribablesUser(user_id), EntitiesUser)

    ### Assert ###
    assert user is not None
    assert user.name == user_id


def test_get_user_that_does_not_exist(flake: PyflakeClient):
    """test_get_role_that_does_not_exist"""
    ### Act ###
    user = flake.describe_one(DescribablesUser("I_SURELY_DO_NOT_EXIST"), EntitiesUser)

    ### Assert ###
    assert user is None
