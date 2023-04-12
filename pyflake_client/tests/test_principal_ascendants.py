"""test_role_ascendants"""
# pylint: disable=line-too-long
# pylint: disable=too-many-locals

import queue
import uuid

from pyflake_client.client import PyflakeClient

from pyflake_client.models.assets.database import Database as AssetsDatabase
from pyflake_client.models.assets.role import Role
from pyflake_client.models.assets.database_role import DatabaseRole
from pyflake_client.models.assets.role_inheritance import RoleInheritance

from pyflake_client.models.describables.role import Role as RoleDescribable
from pyflake_client.models.describables.database_role import DatabaseRole as DatabaseRoleDescribable
from pyflake_client.models.describables.principal_ascendants import PrincipalAscendants as RoleAscendantsDescribable

from pyflake_client.models.entities.principal_ascendants import PrincipalAscendants as RoleAscendantsEntity



def test_get_principal_ascendants(flake: PyflakeClient):
    """test_get_principal_ascendants: we know that USERADMIN -> SECURITYADMIN -> ACCOUNTADMIN"""
    ### Act ###
    hierarchy: RoleAscendantsEntity = flake.describe_one(
        RoleAscendantsDescribable(RoleDescribable("USERADMIN")), RoleAscendantsEntity
    )

    sec_admin = next((r for r in hierarchy.ascendants if r.grantee_identifier == "SECURITYADMIN"), None)
    acc_admin = next((r for r in hierarchy.ascendants if r.grantee_identifier == "ACCOUNTADMIN"), None)
    sys_admin = next((r for r in hierarchy.ascendants if r.grantee_identifier == "SYSADMIN"), None)

    ### Assert ###
    assert hierarchy.principal_identifier == "USERADMIN"
    assert hierarchy.principal_type == "ROLE"
    assert sys_admin is None

    assert sec_admin is not None
    assert sec_admin.distance_from_source == 0 # SECURITYADMIN is a direct parent of USERADMIN
    assert sec_admin.granted_identifier == "USERADMIN"
    assert sec_admin.grantee_identifier == "SECURITYADMIN"

    assert acc_admin is not None
    assert acc_admin.distance_from_source == 1  # ACCOUNTADMIN is a grantparent of USERADMIN
    assert acc_admin.granted_identifier == "SECURITYADMIN"
    assert acc_admin.grantee_identifier == "ACCOUNTADMIN"


def test_create_role_ascendants(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_create_role_ascendants"""
    ### Arrange ###
    user_admin_role = Role("USERADMIN")
    sys_admin_role = Role("SYSADMIN")
    child1_role = Role("IGT_CHILD1_ROLE", user_admin_role, f"pyflake_client_TEST_{uuid.uuid4()}")
    child2_role = Role("IGT_CHILD2_ROLE", user_admin_role, f"pyflake_client_TEST_{uuid.uuid4()}")
    parent_role = Role("IGT_PARENT_ROLE", user_admin_role, f"pyflake_client_TEST_{uuid.uuid4()}")
    parent_child1_relationship = RoleInheritance(child1_role, parent_role)
    parent_child2_relationship = RoleInheritance(child2_role, parent_role)
    grandparent1_role = Role("IGT_GRANDPARENT1_ROLE", user_admin_role, f"pyflake_client_TEST_{uuid.uuid4()}")
    grandparent2_role = Role("IGT_GRANDPARENT2_ROLE", user_admin_role, f"pyflake_client_TEST_{uuid.uuid4()}")
    grandparent1_parent_relationship = RoleInheritance(parent_role, grandparent1_role)
    grandparent2_parent_relationship = RoleInheritance(parent_role, grandparent2_role)
    great_grandparent_role = Role("IGT_GREAT_GRANDPARENT_ROLE", user_admin_role, f"pyflake_client_TEST_{uuid.uuid4()}")
    great_grandparent_grandparent_relationship = RoleInheritance(child_principal=grandparent1_role, parent_principal=great_grandparent_role)
    sysadmin_great_grandparent_relationship = RoleInheritance(child_principal=great_grandparent_role, parent_principal=sys_admin_role )
    try:
        flake.register_asset(child1_role, assets_queue)
        flake.register_asset(child2_role, assets_queue)
        flake.register_asset(parent_role, assets_queue)
        flake.register_asset(parent_child1_relationship, assets_queue)
        flake.register_asset(parent_child2_relationship, assets_queue)
        flake.register_asset(grandparent1_role, assets_queue)
        flake.register_asset(grandparent2_role, assets_queue)
        flake.register_asset(grandparent1_parent_relationship, assets_queue)
        flake.register_asset(grandparent2_parent_relationship, assets_queue)
        flake.register_asset(great_grandparent_role, assets_queue)
        flake.register_asset(great_grandparent_grandparent_relationship, assets_queue)
        flake.register_asset(sysadmin_great_grandparent_relationship, assets_queue)

        ### Act ###
        hierarchy: RoleAscendantsEntity = flake.describe_one(RoleAscendantsDescribable(RoleDescribable("IGT_CHILD1_ROLE")), RoleAscendantsEntity)
        parent = next((r for r in hierarchy.ascendants if r.grantee_identifier == parent_role.name), None)
        grandparent1 = next((r for r in hierarchy.ascendants if r.grantee_identifier == grandparent1_role.name), None)
        grandparent2 = next((r for r in hierarchy.ascendants if r.grantee_identifier == grandparent2_role.name), None)
        great_grandparent = next((r for r in hierarchy.ascendants if r.grantee_identifier == great_grandparent_role.name), None)
        sys_admin = next((r for r in hierarchy.ascendants if r.grantee_identifier == "SYSADMIN"), None)
        acc_admin = next((r for r in hierarchy.ascendants if r.grantee_identifier == "ACCOUNTADMIN"), None)

        ### Assert ###
        # {parent: 0, grandparent: 1, great_grandparent: 2, sysadmin: 3, accountadmin: 4}
        assert hierarchy.principal_identifier == "IGT_CHILD1_ROLE"
        assert hierarchy.principal_type == "ROLE"

        assert parent is not None
        assert parent.distance_from_source == 0
        assert parent.granted_identifier == child1_role.name

        assert grandparent1 is not None
        assert grandparent1.distance_from_source == 1
        assert grandparent1.granted_identifier == parent_role.name

        assert grandparent2 is not None
        assert grandparent2.distance_from_source == 1
        assert grandparent2.granted_identifier == parent_role.name

        assert great_grandparent is not None
        assert great_grandparent.distance_from_source == 2
        assert great_grandparent.granted_identifier == grandparent1_role.name

        assert sys_admin is not None
        assert sys_admin.distance_from_source == 3
        assert sys_admin.granted_identifier == great_grandparent_role.name

        assert acc_admin is not None
        assert acc_admin.distance_from_source == 4
        assert acc_admin.granted_identifier == sys_admin_role.name

    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_broken_role_ascendants(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_broken_role_ascendants"""
    ### Arrange ###
    user_admin_role = Role("USERADMIN")
    sys_admin_role = Role("SYSADMIN")
    child1_role = Role("IGT_CHILD1_ROLE", user_admin_role, f"pyflake_client_TEST_{uuid.uuid4()}")
    child2_role = Role("IGT_CHILD2_ROLE", user_admin_role, f"pyflake_client_TEST_{uuid.uuid4()}")
    parent_role = Role("IGT_PARENT_ROLE", user_admin_role, f"pyflake_client_TEST_{uuid.uuid4()}")
    parent_child1_relationship = RoleInheritance(child1_role, parent_role)
    parent_child2_relationship = RoleInheritance(child2_role, parent_role)
    grandparent_role = Role("IGT_GRANDPARENT_ROLE", user_admin_role, f"pyflake_client_TEST_{uuid.uuid4()}")
    grandparent_parent_relationship = RoleInheritance(
        child_principal=parent_role, 
        parent_principal=grandparent_role
    )
    great_grandparent_role = Role("IGT_GREAT_GRANDPARENT_ROLE", user_admin_role, f"pyflake_client_TEST_{uuid.uuid4()}")
    # Removing the link between great grandparent and grandparent #great_grandparent_grandparent_relationship = RoleRelationship(grandparent_role.name, great_grandparent_role.name)
    sysadmin_great_grandparent_relationship = RoleInheritance(
        child_principal=great_grandparent_role,
        parent_principal=user_admin_role
    )
    try:
        flake.register_asset(child1_role, assets_queue)
        flake.register_asset(child2_role, assets_queue)
        flake.register_asset(parent_role, assets_queue)
        flake.register_asset(parent_child1_relationship, assets_queue)
        flake.register_asset(parent_child2_relationship, assets_queue)
        flake.register_asset(grandparent_role, assets_queue)
        flake.register_asset(grandparent_parent_relationship, assets_queue)
        flake.register_asset(great_grandparent_role, assets_queue)
        # Removing the link between great grandparent and grandparent # flake.register_asset(great_grandparent_grandparent_relationship, assets_queue)
        flake.register_asset(sysadmin_great_grandparent_relationship, assets_queue)

        ### Act ###
        hierarchy: RoleAscendantsEntity = flake.describe_one(RoleAscendantsDescribable(RoleDescribable("IGT_CHILD1_ROLE")), RoleAscendantsEntity)
        parent = next((r for r in hierarchy.ascendants if r.grantee_identifier == parent_role.name), None)
        grandparent = next((r for r in hierarchy.ascendants if r.grantee_identifier == grandparent_role.name), None)
        great_grandparent = next((r for r in hierarchy.ascendants if r.grantee_identifier == great_grandparent_role.name), None)
        sys_admin = next((r for r in hierarchy.ascendants if r.grantee_identifier == "SYSADMIN"), None)
        acc_admin = next((r for r in hierarchy.ascendants if r.grantee_identifier == "ACCOUNTADMIN"), None)

        ### Assert ###
        # {parent: 0, grandparent: 1, great_grandparent: 2, sysadmin: 3, accountadmin: 4}
        assert hierarchy.principal_identifier == "IGT_CHILD1_ROLE"
        assert hierarchy.principal_type == "ROLE"

        assert parent is not None
        assert parent.distance_from_source == 0
        assert parent.granted_identifier == child1_role.name

        assert grandparent is not None
        assert grandparent.distance_from_source == 1
        assert grandparent.granted_identifier == parent_role.name

        assert great_grandparent is None

        assert sys_admin is None

        assert acc_admin is None

    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_ascendants_with_database_roles(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_create_role_ascendants"""
    ### Arrange ###
    user_admin_role = Role("USERADMIN")
    sys_admin_role = Role("SYSADMIN")
    database = AssetsDatabase("IGT_DEMO", f"pyflake_client_TEST_{uuid.uuid4()}", owner=sys_admin_role)
    dr_sys = DatabaseRole("IGT_DEMO_DB_SYS_ADMIN", database.db_name, user_admin_role, f"pyflake_client_TEST_{uuid.uuid4()}")
    dr_rwc = DatabaseRole("IGT_DEMO_RWC", database.db_name, user_admin_role, f"pyflake_client_TEST_{uuid.uuid4()}")
    dr_rw = DatabaseRole("IGT_DEMO_RW", database.db_name, user_admin_role, f"pyflake_client_TEST_{uuid.uuid4()}")
    dr_r = DatabaseRole("IGT_DEMO_R", database.db_name, user_admin_role, f"pyflake_client_TEST_{uuid.uuid4()}")
    
    rel1 = RoleInheritance(dr_sys, sys_admin_role)
    rel2 = RoleInheritance(dr_rwc, dr_sys)
    rel3 = RoleInheritance(dr_rw, dr_rwc)
    rel4 = RoleInheritance(dr_r, dr_rw)

    try:
        flake.register_asset(database, assets_queue)
        flake.register_asset(dr_sys, assets_queue)
        flake.register_asset(dr_rwc, assets_queue)
        flake.register_asset(dr_rw, assets_queue)
        flake.register_asset(dr_r, assets_queue)

        flake.register_asset(rel1, assets_queue)
        flake.register_asset(rel2, assets_queue)
        flake.register_asset(rel3, assets_queue)
        flake.register_asset(rel4, assets_queue)

        ### Act ###
        hierarchy: RoleAscendantsEntity = flake.describe_one(RoleAscendantsDescribable(DatabaseRoleDescribable(dr_r.name, dr_r.database_name)), RoleAscendantsEntity)
        db_rw = next((r for r in hierarchy.ascendants if r.grantee_identifier == dr_rw.get_identifier()), None)
        db_rwc = next((r for r in hierarchy.ascendants if r.grantee_identifier == dr_rwc.get_identifier()), None)
        db_sysadmin = next((r for r in hierarchy.ascendants if r.grantee_identifier == dr_sys.get_identifier()), None)
        sys_admin = next((r for r in hierarchy.ascendants if r.grantee_identifier == "SYSADMIN"), None)
        acc_admin = next((r for r in hierarchy.ascendants if r.grantee_identifier == "ACCOUNTADMIN"), None)

        ### Assert ###
        assert hierarchy.principal_identifier == dr_r.get_identifier()
        assert hierarchy.principal_type == "DATABASE_ROLE"

        assert db_rw is not None
        assert db_rw.distance_from_source == 0
        assert db_rw.granted_identifier == dr_r.get_identifier()
        assert db_rw.principal_type == "DATABASE_ROLE"

        assert db_rwc is not None
        assert db_rwc.distance_from_source == 1
        assert db_rwc.granted_identifier == dr_rw.get_identifier()
        assert db_rwc.principal_type == "DATABASE_ROLE"

        assert db_sysadmin is not None
        assert db_sysadmin.distance_from_source == 2
        assert db_sysadmin.granted_identifier == dr_rwc.get_identifier()
        assert db_sysadmin.principal_type == "DATABASE_ROLE"

        assert sys_admin is not None
        assert sys_admin.distance_from_source == 3
        assert sys_admin.granted_identifier == dr_sys.get_identifier()
        assert sys_admin.principal_type == "ROLE"

        assert acc_admin is not None
        assert acc_admin.distance_from_source == 4
        assert acc_admin.granted_identifier == sys_admin_role.get_identifier()
        assert acc_admin.principal_type == "ROLE"

    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)
