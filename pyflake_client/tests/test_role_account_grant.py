"""test_database"""
import queue
import uuid

from pyflake_client.client import PyflakeClient
from pyflake_client.models.assets.role import Role as AssetsRole
from pyflake_client.models.assets.grant import Grant as AssetsGrant
from pyflake_client.models.assets.grants.role_account_grant import RoleAccountGrant
from pyflake_client.models.describables.grant import Grant as DescribableGrant
from pyflake_client.models.entities.grant import Grant as EntitiesGrant
from pyflake_client.models.describables.role import Role as DescribableRole


def test_grant_role_account_privilege(
    flake: PyflakeClient, assets_queue: queue.LifoQueue
):
    """test_grant_role_account_privilege"""
    ### Arrange ###
    role = AssetsRole(
        "IGT_CREATE_ROLE",
        AssetsRole("USERADMIN"),
        f"pyflake_client_TEST_{uuid.uuid4()}",
    )
    privilege = AssetsGrant(RoleAccountGrant(role.name), ["CREATE ACCOUNT"])

    try:
        flake.register_asset(role, assets_queue)
        flake.register_asset(privilege, assets_queue)

        ### Act ###
        grants = flake.describe_many(
            describable=DescribableGrant(principal=DescribableRole(name=role.name)),
            entity=EntitiesGrant,
        )

        ### Assert ###
        assert grants is not None
        assert len(grants) == 1

        priv_create_acc = next(
            (r for r in grants if r.privilege == "CREATE ACCOUNT"), None
        )
        assert priv_create_acc is not None
        assert priv_create_acc.granted_on == "ACCOUNT"
        assert priv_create_acc.granted_by == "ACCOUNTADMIN"
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_grant_role_account_privileges(
    flake: PyflakeClient, assets_queue: queue.LifoQueue
):
    """test_grant_role_account_privileges"""
    ### Arrange ###
    role = AssetsRole(
        "IGT_CREATE_ROLE",
        AssetsRole("USERADMIN"),
        f"pyflake_client_TEST_{uuid.uuid4()}",
    )
    privilege = AssetsGrant(
        RoleAccountGrant(role.name), ["CREATE ACCOUNT", "CREATE USER"]
    )

    try:
        flake.register_asset(role, assets_queue)
        flake.register_asset(privilege, assets_queue)

        ### Act ###
        grants = flake.describe_many(
            describable=DescribableGrant(principal=DescribableRole(name=role.name)),
            entity=EntitiesGrant,
        )

        ### Assert ###
        assert grants is not None
        assert len(grants) == 2
        priv_create_acc = next(g for g in grants if g.privilege == "CREATE ACCOUNT")
        assert priv_create_acc is not None
        assert priv_create_acc.granted_on == "ACCOUNT"
        assert priv_create_acc.granted_by == "ACCOUNTADMIN"

        priv_create_user = next(g for g in grants if g.privilege == "CREATE USER")
        assert priv_create_user is not None
        assert priv_create_user.granted_on == "ACCOUNT"
        assert priv_create_user.granted_by == "USERADMIN"
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)
