"""test_database"""
# pylint: disable=line-too-long
import queue
import uuid

from pyflake_client.pyflake_client import PyflakeClient
from pyflake_client.models.assets.role import Role as AssetsRole
from pyflake_client.models.assets.grant import Grant as GrantAsset
from pyflake_client.models.assets.grants.role_account_grant import RoleAccountGrant
from pyflake_client.models.describables.grants.role_grant import RoleGrant as RoleGrantDescribable
from pyflake_client.models.entities.grants.role_grant import RoleGrants as RoleGrantsEntity


def test_grant_role_account_privilege(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_grant_role_account_privilege"""
    ### Arrange ###
    role = AssetsRole("IGT_CREATE_ROLE", "USERADMIN", f"pyflake_client_TEST_{uuid.uuid4()}")
    privilege = GrantAsset(RoleAccountGrant(role.name), ["CREATE ACCOUNT"])

    try:
        flake.register_asset(role, assets_queue)
        flake.register_asset(privilege, assets_queue)

        ### Act ###
        granted: RoleGrantsEntity = flake.describe(RoleGrantDescribable(role.name), RoleGrantsEntity)

        ### Assert ###
        assert granted.role_name == role.name
        assert len(granted.grants) == 1

        priv_create_acc = next((r for r in granted.grants if r.privilege == "CREATE ACCOUNT"), None)
        assert priv_create_acc is not None
        assert priv_create_acc.granted_on == "ACCOUNT"
        assert priv_create_acc.granted_by == "ACCOUNTADMIN"
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_grant_role_account_privileges(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_grant_role_account_privileges"""
    ### Arrange ###
    role = AssetsRole("IGT_CREATE_ROLE", "USERADMIN", f"pyflake_client_TEST_{uuid.uuid4()}")
    privilege = GrantAsset(RoleAccountGrant(role.name), ["CREATE ACCOUNT", "CREATE USER"])

    try:
        flake.register_asset(role, assets_queue)
        flake.register_asset(privilege, assets_queue)

        ### Act ###
        granted: RoleGrantsEntity = flake.describe(RoleGrantDescribable(role.name), RoleGrantsEntity)

        ### Assert ###
        assert granted.role_name == role.name
        assert len(granted.grants) == 2

        priv_create_acc = next((r for r in granted.grants if r.privilege == "CREATE ACCOUNT"), None)
        assert priv_create_acc is not None
        assert priv_create_acc.granted_on == "ACCOUNT"
        assert priv_create_acc.granted_by == "ACCOUNTADMIN"

        priv_create_usr = next((r for r in granted.grants if r.privilege == "CREATE USER"), None)
        assert priv_create_usr is not None
        assert priv_create_usr.granted_on == "ACCOUNT"
        assert priv_create_usr.granted_by == "USERADMIN"
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)
