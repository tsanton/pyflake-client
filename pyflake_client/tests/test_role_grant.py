"""test_role"""
import queue
import uuid


from pyflake_client.client import PyflakeClient
from pyflake_client.models.assets.database import Database as AssetsDatabase
from pyflake_client.models.assets.grant_action import GrantAction
from pyflake_client.models.describables.role import Role as DescribablesRole
from pyflake_client.models.describables.database_role import (
    DatabaseRole as DescribablesDatabaseRole,
)
from pyflake_client.models.entities.role import Role as EntitiesRole
from pyflake_client.models.entities.grant import Grant as EntitiesGrant
from pyflake_client.models.assets.role import Role as AssetsRole
from pyflake_client.models.assets.database_role import (
    DatabaseRole as AssetsDatabaseRole,
)
from pyflake_client.models.assets.grants.account_grant import AccountGrant
from pyflake_client.models.assets.grants.database_grant import DatabaseGrant
from pyflake_client.models.enums.privilege import Privilege
from pyflake_client.models.describables.grant import Grant as DescribablesGrant


def test_describe_grant_for_non_existing_role(
    flake: PyflakeClient, assets_queue: queue.LifoQueue
):
    """test_create_role"""
    ### Arrange ###
    describables_role = DescribablesRole(name="I_DO_NOT_EXIST_ROLE")
    try:
        sf_roles = flake.describe_one(
            describable=describables_role, entity=EntitiesRole
        )

        ### Act ###
        assert sf_roles is None
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_describe_grant_for_role(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    ### Arrange ###
    role_asset = AssetsRole(
        name="TEST_PYFLAKE_ROLE",
        comment="Integration test role from the pyflake test suite",
        owner=AssetsRole(name="USERADMIN"),
    )
    grant = GrantAction(
        principal=role_asset,
        target=AccountGrant(),
        privileges=[Privilege.CREATE_DATABASE, Privilege.CREATE_USER],
    )
    try:
        ### Act ###
        flake.register_asset(obj=role_asset, asset_queue=assets_queue)
        flake.register_asset(obj=grant, asset_queue=assets_queue)
        role_grants = flake.describe_many(
            describable=DescribablesGrant(
                principal=DescribablesRole(name=role_asset.name)
            ),
            entity=EntitiesGrant,
        )

        ### Assert ###
        assert role_grants is not None
        assert len(role_grants) == 2
        create_database_grant = next(
            g for g in role_grants if g.privilege == "CREATE DATABASE"
        )
        create_user_grant = next(g for g in role_grants if g.privilege == "CREATE USER")
        assert create_database_grant.granted_on == "ACCOUNT"
        assert create_database_grant.granted_by == "SYSADMIN"
        assert create_user_grant.granted_on == "ACCOUNT"
        assert create_user_grant.granted_by == "USERADMIN"
    finally:
        flake.delete_assets(assets_queue)


def test_describe_grant_for_database_role(
    flake: PyflakeClient, assets_queue: queue.LifoQueue
):
    ### Arrange ###
    database_asset = AssetsDatabase(
        db_name="IGT_DEMO",
        comment=f"pyflake_client_TEST_{uuid.uuid4()}",
        owner=AssetsRole(name="SYSADMIN"),
    )

    database_role_asset = AssetsDatabaseRole(
        name="TEST_PYFLAKE_DATABASE_ROLE",
        database_name=database_asset.db_name,
        comment=f"pyflake_client_TEST_{uuid.uuid4()}",
        owner=AssetsRole(name="SYSADMIN"),
    )
    grant = GrantAction(
        principal=database_role_asset,
        target=DatabaseGrant(database_name=database_asset.db_name),
        privileges=[Privilege.CREATE_DATABASE_ROLE, Privilege.CREATE_SCHEMA],
    )
    try:
        ### Act ###
        flake.register_asset(obj=database_asset, asset_queue=assets_queue)

        flake.register_asset(obj=database_role_asset, asset_queue=assets_queue)
        flake.register_asset(obj=grant, asset_queue=assets_queue)
        role_grants = flake.describe_many(
            describable=DescribablesGrant(
                principal=DescribablesDatabaseRole(
                    name=database_role_asset.name, db_name=database_asset.db_name
                )
            ),
            entity=EntitiesGrant,
        )

        ### Assert ###
        assert role_grants is not None
        assert len(role_grants) == 3
        create_database_role_grant = next(
            g for g in role_grants if g.privilege == "CREATE DATABASE ROLE"
        )
        create_schema_grant = next(
            g for g in role_grants if g.privilege == "CREATE SCHEMA"
        )
        assert create_database_role_grant.granted_on == "DATABASE"
        assert create_database_role_grant.granted_by == "SYSADMIN"
        assert create_schema_grant.granted_on == "DATABASE"
        assert create_schema_grant.granted_by == "SYSADMIN"
    finally:
        flake.delete_assets(assets_queue)
