"""test_role"""
import queue
import uuid
from datetime import date


from pyflake_client.client import PyflakeClient
from pyflake_client.models.assets.database_role import DatabaseRole
from pyflake_client.models.describables.database_role import (
    DatabaseRole as DescribablesRole,
)
from pyflake_client.models.entities.role import Role as EntitiesRole
from pyflake_client.models.assets.role import Role as AssetsRole
from pyflake_client.models.assets.database import Database as AssetsDatabase


def test_create_database_role(
    flake: PyflakeClient,
    assets_queue: queue.LifoQueue,
    db_asset_fixture: AssetsDatabase,
):
    """test_create_role"""
    ### Arrange ###
    role: DatabaseRole = DatabaseRole(
        name="IGT_CREATE_ROLE",
        database_name=db_asset_fixture.db_name,
        owner=AssetsRole("USERADMIN"),
        comment=f"pyflake_client_TEST_{uuid.uuid4()}",
    )

    try:
        flake.register_asset(db_asset_fixture, assets_queue)
        flake.register_asset(role, assets_queue)

        ### Act ###
        sf_role: EntitiesRole = flake.describe_one(
            DescribablesRole(name=role.name, db_name=db_asset_fixture.db_name),
            EntitiesRole,
        )
        ### Assert ###
        assert sf_role.name == role.name
        assert sf_role.comment == role.comment
        assert sf_role.owner == "USERADMIN"
        assert sf_role.created_on.date() == date.today()
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_get_database_role(
    flake: PyflakeClient,
    existing_database_role: str,
    existing_database: str,
):
    sf_role: EntitiesRole = flake.describe_one(
        DescribablesRole(name=existing_database_role, db_name=existing_database),
        EntitiesRole,
    )
    assert sf_role.name == existing_database_role
    assert sf_role.owner == ""
    assert sf_role.comment == ""


def test_get_database_role_from_db_not_exists(
    flake: PyflakeClient,
    existing_database_role: str,
):
    sf_role: EntitiesRole = flake.describe_one(
        DescribablesRole(
            name=existing_database_role, db_name="I_SURELY_DO_NOT_EXIST_DATABASE"
        ),
        EntitiesRole,
    )
    assert sf_role is None


def test_get_database_role_not_exists(
    flake: PyflakeClient,
    assets_queue: queue.LifoQueue,
    db_asset_fixture: AssetsDatabase,
):
    try:
        flake.register_asset(db_asset_fixture, assets_queue)
        sf_role: EntitiesRole = flake.describe_one(
            DescribablesRole(
                name="I_SURELY_DO_NOT_EXIST_DATABASE_ROLE",
                db_name=db_asset_fixture.db_name,
            ),
            EntitiesRole,
        )
        assert sf_role is None
    finally:
        flake.delete_assets(assets_queue)
