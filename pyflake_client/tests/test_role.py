"""test_role"""
import queue
import uuid
from datetime import date


from pyflake_client.client import PyflakeClient
from pyflake_client.models.assets.role import Role as AssetsRole
from pyflake_client.models.describables.role import Role as DescribablesRole
from pyflake_client.models.entities.role import Role as EntitiesRole
from pyflake_client.models.assets.role import Role as AssetsRole


def test_create_role(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_create_role"""
    ### Arrange ###
    role: AssetsRole = AssetsRole(
        name="IGT_CREATE_ROLE",
        owner=AssetsRole("USERADMIN"),
        comment=f"pyflake_client_TEST_{uuid.uuid4()}",
    )

    try:
        flake.register_asset(role, assets_queue)

        ### Act ###
        sf_role: EntitiesRole = flake.describe(
            DescribablesRole(role.name), EntitiesRole
        )
        ### Assert ###
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
    role: EntitiesRole = flake.describe(DescribablesRole("ACCOUNTADMIN"), EntitiesRole)

    ### Assert ###
    assert role.name == "ACCOUNTADMIN"
    assert (
        role.comment == "Account administrator can manage all aspects of the account."
    )


def test_get_role_that_does_not_exist(flake: PyflakeClient):
    """test_get_role_that_does_not_exist"""
    ### Act ###
    role: EntitiesRole = flake.describe(
        DescribablesRole("I_SURELY_DO_NOT_EXIST"), EntitiesRole
    )

    ### Assert ###
    assert role is None
