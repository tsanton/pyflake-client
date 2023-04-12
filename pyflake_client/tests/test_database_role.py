"""test_role"""
import queue
import uuid
from datetime import date


from pyflake_client.client import PyflakeClient
from pyflake_client.models.assets.database_role import DatabaseRole
from pyflake_client.models.describables.database_role import DatabaseRole as DescribablesRole
from pyflake_client.models.entities.role import Role as EntitiesRole
from pyflake_client.models.assets.role import Role as RoleAsset
from pyflake_client.models.assets.database import Database as DatabaseAsset


def test_create_database_role(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_create_role"""
    ### Arrange ###
    database = DatabaseAsset("IGT_DEMO", f"pyflake_client_test_{uuid.uuid4()}", owner=RoleAsset("SYSADMIN"))
    role: DatabaseRole = DatabaseRole(
        name="IGT_CREATE_ROLE",
        database_name=database.db_name,
        owner=RoleAsset("USERADMIN"),
        comment=f"pyflake_client_test_{uuid.uuid4()}",
    )

    try:
        flake.register_asset(database, assets_queue)
        flake.register_asset(role, assets_queue)

        ### Act ###
        sf_role = flake.describe_one(DescribablesRole(name=role.name, db_name=database.db_name),EntitiesRole)

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


def test_get_database_role(flake: PyflakeClient,existing_database_role: str, existing_database: str):
    sf_role = flake.describe_one(DescribablesRole(name=existing_database_role, db_name=existing_database), EntitiesRole)
    assert sf_role is not None
    assert sf_role.name == existing_database_role
    assert sf_role.owner == ""
    assert sf_role.comment == ""


def test_get_database_role_from_db_not_exists(flake: PyflakeClient, existing_database_role: str):
    sf_role = flake.describe_one(DescribablesRole(name=existing_database_role, db_name="I_SURELY_DO_NOT_EXIST_DATABASE"), EntitiesRole)
    assert sf_role is None


def test_get_database_role_not_exists(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    try:
        database = DatabaseAsset("IGT_DEMO", f"pyflake_client_test_{uuid.uuid4()}", owner=RoleAsset("SYSADMIN"))

        flake.register_asset(database, assets_queue)
        sf_role = flake.describe_one(DescribablesRole(name="I_SURELY_DO_NOT_EXIST_DATABASE_ROLE", db_name=database.db_name), EntitiesRole)
        assert sf_role is None
    finally:
        flake.delete_assets(assets_queue)
