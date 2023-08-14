# -*- coding: utf-8 -*-
# pylint: disable=line-too-long
# pylint: disable=too-many-locals

import queue
import uuid

from pyflake_client.client import PyflakeClient
from pyflake_client.models.assets.database import Database as DatabaseAsset
from pyflake_client.models.assets.database_role import DatabaseRole as DatabaseRoleAsset
from pyflake_client.models.assets.role import Role as RoleAsset
from pyflake_client.models.assets.role_inheritance import RoleInheritance
from pyflake_client.models.describables.database_role import (
    DatabaseRole as DatabaseRoleDescribable,
)
from pyflake_client.models.describables.principal_descendants import (
    PrincipalDescendants as RoleDescendantsDescribable,
)
from pyflake_client.models.describables.role import Role as RoleDescribable
from pyflake_client.models.entities.grant import Grant as GrantEntity
from pyflake_client.tests.utilities import find


def test_get_descendant_roles(flake: PyflakeClient):
    """test_get_descendant_roles: we know that ACCOUNTADMIN is the parent of both SECURITYADMIN and SYSADMIN"""
    ### Act ###
    descendants = flake.describe_async(RoleDescendantsDescribable(RoleDescribable("ACCOUNTADMIN"))).deserialize_many(
        GrantEntity
    )

    assert descendants is not None

    sec_admin = find(descendants, lambda x: x.granted_identifier == "SECURITYADMIN")
    sys_admin = find(descendants, lambda x: x.granted_identifier == "SYSADMIN")

    ### Assert ###
    assert sec_admin is not None
    assert sec_admin.grantee_identifier == "ACCOUNTADMIN"
    assert sec_admin.grantee_type == "ROLE"

    assert sys_admin is not None
    assert sys_admin.grantee_identifier == "ACCOUNTADMIN"
    assert sys_admin.granted_on == "ROLE"


def test_role_to_role_descendants(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_role_to_role_descendants"""
    snowflake_comment: str = f"pyflake_client_test_{uuid.uuid4()}"
    user_admin_role = RoleAsset("USERADMIN")
    child_role = RoleAsset("IGT_CHILD_ROLE", snowflake_comment, user_admin_role)
    parent_role = RoleAsset("IGT_PARENT_ROLE", snowflake_comment, user_admin_role)
    rel = RoleInheritance(child_role, parent_role)

    try:
        w1 = flake.register_asset_async(child_role, assets_queue)
        w2 = flake.register_asset_async(parent_role, assets_queue)
        flake.wait_all([w1, w2])
        flake.register_asset_async(rel, assets_queue).wait()

        descendants = flake.describe_async(
            RoleDescendantsDescribable(RoleDescribable(parent_role.name))
        ).deserialize_many(GrantEntity)

        assert descendants is not None
        assert len(descendants) == 1
        child = find(descendants, lambda x: x.granted_identifier == child_role.name)
        assert child is not None
        assert child.grantee_identifier == parent_role.name
        assert child.grantee_type == "ROLE"
        assert child.granted_by == "USERADMIN"

    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_role_to_roles_descendants(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """ "test_role_to_roles_descendants"""
    snowflake_comment: str = f"pyflake_client_test_{uuid.uuid4()}"
    user_admin_role = RoleAsset("USERADMIN")
    child_role_1 = RoleAsset("IGT_CHILD_ROLE_1", snowflake_comment, user_admin_role)
    child_role_2 = RoleAsset("IGT_CHILD_ROLE_2", snowflake_comment, user_admin_role)
    parent_role = RoleAsset("IGT_PARENT_ROLE", snowflake_comment, user_admin_role)
    rel_1 = RoleInheritance(child_role_1, parent_role)
    rel_2 = RoleInheritance(child_role_2, parent_role)

    try:
        w1 = flake.register_asset_async(child_role_1, assets_queue)
        w2 = flake.register_asset_async(child_role_2, assets_queue)
        w3 = flake.register_asset_async(parent_role, assets_queue)
        flake.wait_all([w1, w2, w3])
        w4 = flake.register_asset_async(rel_1, assets_queue)
        w5 = flake.register_asset_async(rel_2, assets_queue)
        flake.wait_all([w4, w5])

        descendants = flake.describe_async(
            RoleDescendantsDescribable(RoleDescribable(parent_role.name))
        ).deserialize_many(GrantEntity)

        assert descendants is not None
        assert len(descendants) == 2
        child_1 = find(descendants, lambda x: x.granted_identifier == child_role_1.name)
        child_2 = find(descendants, lambda x: x.granted_identifier == child_role_2.name)

        assert child_1 is not None
        assert child_1.grantee_identifier == parent_role.name
        assert child_1.grantee_type == "ROLE"
        assert child_1.granted_by == "USERADMIN"
        assert child_1.granted_on == "ROLE"

        assert child_2 is not None
        assert child_2.grantee_identifier == parent_role.name
        assert child_2.grantee_type == "ROLE"
        assert child_2.granted_by == "USERADMIN"
        assert child_2.granted_on == "ROLE"

    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_role_to_role_and_database_role_descendants(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    snowflake_comment: str = f"pyflake_client_test_{uuid.uuid4()}"
    user_admin = RoleAsset("USERADMIN")
    db = DatabaseAsset("IGT_DEMO", snowflake_comment, owner=RoleAsset("SYSADMIN"))
    child_role_1 = RoleAsset("IGT_CHILD_ROLE_1", snowflake_comment, user_admin)
    child_role_2 = DatabaseRoleAsset("IGT_CHILD_ROLE_2", db.db_name, snowflake_comment, user_admin)
    parent_role = RoleAsset("IGT_PARENT_ROLE", snowflake_comment, user_admin)
    rel_1 = RoleInheritance(child_role_1, parent_role)
    rel_2 = RoleInheritance(child_role_2, parent_role)

    try:
        w1 = flake.register_asset_async(db, assets_queue)
        w2 = flake.register_asset_async(child_role_1, assets_queue)
        w3 = flake.register_asset_async(parent_role, assets_queue)
        flake.wait_all([w1, w2, w3])
        w4 = flake.register_asset_async(child_role_2, assets_queue)
        w5 = flake.register_asset_async(rel_1, assets_queue)
        flake.wait_all([w4, w5])
        flake.register_asset_async(rel_2, assets_queue).wait()

        descendants = flake.describe_async(
            RoleDescendantsDescribable(RoleDescribable(parent_role.name))
        ).deserialize_many(GrantEntity)

        assert descendants is not None
        assert len(descendants) == 2
        child_1 = find(descendants, lambda x: x.granted_identifier == child_role_1.name)
        child_2 = find(descendants, lambda x: x.granted_identifier == child_role_2.get_identifier())

        assert child_1 is not None
        assert child_1.grantee_identifier == parent_role.name
        assert child_1.grantee_type == "ROLE"
        assert child_1.granted_by == "USERADMIN"
        assert child_1.granted_on == "ROLE"

        assert child_2 is not None
        assert child_2.grantee_identifier == parent_role.name
        assert child_2.grantee_type == "ROLE"
        assert child_2.granted_by == "USERADMIN"
        assert child_2.granted_on == "DATABASE_ROLE"

    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_database_role_to_database_roles_descendants(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    snowflake_comment: str = f"pyflake_client_test_{uuid.uuid4()}"
    user_admin_role = RoleAsset("USERADMIN")
    db = DatabaseAsset("IGT_DEMO", snowflake_comment, RoleAsset("SYSADMIN"))
    child_role_1 = DatabaseRoleAsset("IGT_CHILD_ROLE_1", db.db_name, snowflake_comment, user_admin_role)
    child_role_2 = DatabaseRoleAsset("IGT_CHILD_ROLE_2", db.db_name, snowflake_comment, user_admin_role)
    parent_role = DatabaseRoleAsset("IGT_PARENT_ROLE", db.db_name, snowflake_comment, user_admin_role)
    rel_1 = RoleInheritance(child_role_1, parent_role)
    rel_2 = RoleInheritance(child_role_2, parent_role)

    try:
        flake.register_asset_async(db, assets_queue).wait()
        w1 = flake.register_asset_async(child_role_1, assets_queue)
        w2 = flake.register_asset_async(child_role_2, assets_queue)
        w3 = flake.register_asset_async(parent_role, assets_queue)
        flake.wait_all([w1, w2, w3])
        w4 = flake.register_asset_async(rel_1, assets_queue)
        w5 = flake.register_asset_async(rel_2, assets_queue)
        flake.wait_all([w4, w5])

        descendants = flake.describe_async(
            RoleDescendantsDescribable(DatabaseRoleDescribable(parent_role.name, parent_role.database_name))
        ).deserialize_many(GrantEntity)

        assert descendants is not None
        assert len(descendants) == 2
        child_1 = find(descendants, lambda x: x.granted_identifier == child_role_1.get_identifier())
        child_2 = find(descendants, lambda x: x.granted_identifier == child_role_2.get_identifier())

        assert child_1 is not None
        assert child_1.grantee_identifier == parent_role.name
        assert child_1.grantee_type == "DATABASE_ROLE"
        assert child_1.granted_by == "USERADMIN"
        assert child_1.granted_on == "DATABASE_ROLE"

        assert child_2 is not None
        assert child_2.grantee_identifier == parent_role.name
        assert child_2.grantee_type == "DATABASE_ROLE"
        assert child_2.granted_by == "USERADMIN"
        assert child_2.granted_on == "DATABASE_ROLE"

    finally:
        ### Cleanup ###
        flake.delete_assets
