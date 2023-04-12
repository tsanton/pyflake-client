"""test_descendant_roles"""
# pylint: disable=line-too-long
# pylint: disable=too-many-locals

import queue
import uuid
from pyflake_client.client import PyflakeClient

from pyflake_client.models.assets.database import Database as DatabaseAsset
from pyflake_client.models.assets.role import Role as RoleAsset
from pyflake_client.models.assets.database_role import DatabaseRole as DatabaseRoleAsset
from pyflake_client.models.assets.role_inheritance import RoleInheritance

from pyflake_client.models.describables.role import Role as RoleDescribable
from pyflake_client.models.describables.database_role import DatabaseRole as DatabaseRoleDescribable
from pyflake_client.models.describables.principal_descendants import PrincipalDescendants as RoleDescendantsDescribable

from pyflake_client.models.entities.principal_descendants import PrincipalDescendants as RoleDescendantsEntity
from pyflake_client.models.entities.grant import Grant as GrantEntity

def test_get_descendant_roles(flake: PyflakeClient):
    """test_get_descendant_roles: we know that ACCOUNTADMIN is the parent of both SECURITYADMIN and SYSADMIN"""
    ### Act ###
    hierarchy = flake.describe_one(RoleDescendantsDescribable(RoleDescribable("ACCOUNTADMIN")), RoleDescendantsEntity)

    assert hierarchy is not None
    sec_admin:GrantEntity = next((r for r in hierarchy.descendants if r.granted_identifier == "SECURITYADMIN"), None)
    sys_admin:GrantEntity = next((r for r in hierarchy.descendants if r.granted_identifier == "SYSADMIN"), None)

    ### Assert ###
    assert hierarchy.principal_identifier == "ACCOUNTADMIN"
    assert hierarchy.principal_type == "ROLE"

    assert sec_admin is not None
    assert sec_admin.grantee_identifier == "ACCOUNTADMIN"

    assert sys_admin is not None
    assert sys_admin.grantee_identifier == "ACCOUNTADMIN"



def test_role_to_role_descendants(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    user_admin_role = RoleAsset("USERADMIN")
    child_role = RoleAsset("IGT_CHILD_ROLE", user_admin_role, f"pyflake_client_test_{uuid.uuid4()}")
    parent_role = RoleAsset("IGT_PARENT_ROLE", user_admin_role, f"pyflake_client_test_{uuid.uuid4()}")
    rel = RoleInheritance(child_role, parent_role)

    try:
        flake.register_asset(child_role, assets_queue)
        flake.register_asset(parent_role, assets_queue)
        flake.register_asset(rel, assets_queue)

        hierarchy = flake.describe_one(RoleDescendantsDescribable(RoleDescribable(parent_role.name)), RoleDescendantsEntity)

        assert hierarchy is not None
        assert len(hierarchy.descendants) == 1
        child:GrantEntity = next((r for r in hierarchy.descendants if r.granted_identifier == child_role.name), None)
        assert child is not None
        assert child.grantee_identifier == parent_role.name
        assert child.grantee_type == "ROLE"
        assert child.granted_by == "USERADMIN"


    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_role_to_roles_descendants(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    user_admin_role = RoleAsset("USERADMIN")
    child_role_1 = RoleAsset("IGT_CHILD_ROLE_1", user_admin_role, f"pyflake_client_test_{uuid.uuid4()}")
    child_role_2 = RoleAsset("IGT_CHILD_ROLE_2", user_admin_role, f"pyflake_client_test_{uuid.uuid4()}")
    parent_role = RoleAsset("IGT_PARENT_ROLE", user_admin_role, f"pyflake_client_test_{uuid.uuid4()}")
    rel_1 = RoleInheritance(child_role_1, parent_role)
    rel_2 = RoleInheritance(child_role_2, parent_role)

    try:
        flake.register_asset(child_role_1, assets_queue)
        flake.register_asset(child_role_2, assets_queue)
        flake.register_asset(parent_role, assets_queue)
        flake.register_asset(rel_1, assets_queue)
        flake.register_asset(rel_2, assets_queue)

        hierarchy = flake.describe_one(RoleDescendantsDescribable(RoleDescribable(parent_role.name)), RoleDescendantsEntity)

        assert hierarchy is not None
        assert len(hierarchy.descendants) == 2
        child_1:GrantEntity = next((r for r in hierarchy.descendants if r.granted_identifier == child_role_1.name), None)
        child_2:GrantEntity = next((r for r in hierarchy.descendants if r.granted_identifier == child_role_2.name), None)

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
    user_admin_role = RoleAsset("USERADMIN")
    db = DatabaseAsset("IGT_DEMO", f"pyflake_client_test_{uuid.uuid4()}", owner=RoleAsset("SYSADMIN"))
    child_role_1 = RoleAsset("IGT_CHILD_ROLE_1", user_admin_role, f"pyflake_client_test_{uuid.uuid4()}")
    child_role_2 = DatabaseRoleAsset("IGT_CHILD_ROLE_2", db.db_name, user_admin_role, f"pyflake_client_test_{uuid.uuid4()}")
    parent_role = RoleAsset("IGT_PARENT_ROLE", user_admin_role, f"pyflake_client_test_{uuid.uuid4()}")
    rel_1 = RoleInheritance(child_role_1, parent_role)
    rel_2 = RoleInheritance(child_role_2, parent_role)

    try:
        flake.register_asset(db, assets_queue)
        flake.register_asset(child_role_1, assets_queue)
        flake.register_asset(child_role_2, assets_queue)
        flake.register_asset(parent_role, assets_queue)
        flake.register_asset(rel_1, assets_queue)
        flake.register_asset(rel_2, assets_queue)

        hierarchy = flake.describe_one(RoleDescendantsDescribable(RoleDescribable(parent_role.name)), RoleDescendantsEntity)

        assert hierarchy is not None
        assert len(hierarchy.descendants) == 2
        child_1:GrantEntity = next((r for r in hierarchy.descendants if r.granted_identifier == child_role_1.name), None)
        child_2:GrantEntity = next((r for r in hierarchy.descendants if r.granted_identifier == child_role_2.get_identifier()), None)

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
    user_admin_role = RoleAsset("USERADMIN")
    db = DatabaseAsset("IGT_DEMO", f"pyflake_client_test_{uuid.uuid4()}", owner=RoleAsset("SYSADMIN"))
    child_role_1 = DatabaseRoleAsset("IGT_CHILD_ROLE_1", db.db_name, user_admin_role, f"pyflake_client_test_{uuid.uuid4()}")
    child_role_2 = DatabaseRoleAsset("IGT_CHILD_ROLE_2", db.db_name, user_admin_role, f"pyflake_client_test_{uuid.uuid4()}")
    parent_role = DatabaseRoleAsset("IGT_PARENT_ROLE", db.db_name, user_admin_role, f"pyflake_client_test_{uuid.uuid4()}")
    rel_1 = RoleInheritance(child_role_1, parent_role)
    rel_2 = RoleInheritance(child_role_2, parent_role)

    try:
        flake.register_asset(db, assets_queue)
        flake.register_asset(child_role_1, assets_queue)
        flake.register_asset(child_role_2, assets_queue)
        flake.register_asset(parent_role, assets_queue)
        flake.register_asset(rel_1, assets_queue)
        flake.register_asset(rel_2, assets_queue)

        hierarchy = flake.describe_one(RoleDescendantsDescribable(DatabaseRoleDescribable(parent_role.name, parent_role.database_name)), RoleDescendantsEntity)

        assert hierarchy is not None
        assert len(hierarchy.descendants) == 2
        child_1:GrantEntity = next((r for r in hierarchy.descendants if r.granted_identifier == child_role_1.get_identifier()), None)
        child_2:GrantEntity = next((r for r in hierarchy.descendants if r.granted_identifier == child_role_2.get_identifier()), None)

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