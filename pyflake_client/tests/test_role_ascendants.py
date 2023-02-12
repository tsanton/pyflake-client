"""test_role_ascendants"""
# pylint: disable=line-too-long
# pylint: disable=too-many-locals

import queue
import uuid

from pyflake_client.pyflake_client import PyflakeClient

from pyflake_client.models.entities.role_ascendants import RoleAscendants as RoleAscendantsEntity
from pyflake_client.models.describables.role_ascendants import RoleAscendants as RoleAscendantsDescribable
from pyflake_client.models.assets.role import Role
from pyflake_client.models.assets.role_relationship import RoleRelationship


def test_get_role_ascendants(flake: PyflakeClient):
    """test_get_role_ascendants: we know that USERADMIN -> SECURITYADMIN -> ACCOUNTADMIN"""
    ### Act ###
    hierarchy: RoleAscendantsEntity = flake.describe(RoleAscendantsDescribable("USERADMIN"), RoleAscendantsEntity)

    sec_admin = next((r for r in hierarchy.ascendant_roles if r.parent_role_name == "SECURITYADMIN"), None)
    acc_admin = next((r for r in hierarchy.ascendant_roles if r.parent_role_name == "ACCOUNTADMIN"), None)
    sys_admin = next((r for r in hierarchy.ascendant_roles if r.parent_role_name == "SYSADMIN"), None)

    ### Assert ###
    assert hierarchy.name == "USERADMIN"
    assert sys_admin is None
    assert sec_admin.distance_from_source == 0  # SECURITYADMIN is a direct parent of USERADMIN
    assert sec_admin.role_name == "USERADMIN"
    assert sec_admin.parent_role_name == "SECURITYADMIN"
    assert acc_admin.distance_from_source == 1  # ACCOUNTADMIN is a grantparent of USERADMIN
    assert acc_admin.role_name == "SECURITYADMIN"
    assert acc_admin.parent_role_name == "ACCOUNTADMIN"


def test_create_role_ascendants(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_create_role_ascendants"""
    ### Arrange ###
    child1_role = Role("IGT_CHILD1_ROLE", "USERADMIN", f"pyflake_client_TEST_{uuid.uuid4()}")
    child2_role = Role("IGT_CHILD2_ROLE", "USERADMIN", f"pyflake_client_TEST_{uuid.uuid4()}")
    parent_role = Role("IGT_PARENT_ROLE", "USERADMIN", f"pyflake_client_TEST_{uuid.uuid4()}")
    parent_child1_relationship = RoleRelationship(child1_role.name, parent_role.name)
    parent_child2_relationship = RoleRelationship(child2_role.name, parent_role.name)
    grandparent1_role = Role("IGT_GRANDPARENT1_ROLE", "USERADMIN", f"pyflake_client_TEST_{uuid.uuid4()}")
    grandparent2_role = Role("IGT_GRANDPARENT2_ROLE", "USERADMIN", f"pyflake_client_TEST_{uuid.uuid4()}")
    grandparent1_parent_relationship = RoleRelationship(parent_role.name, grandparent1_role.name)
    grandparent2_parent_relationship = RoleRelationship(parent_role.name, grandparent2_role.name)
    great_grandparent_role = Role("IGT_GREAT_GRANDPARENT_ROLE", "USERADMIN", f"pyflake_client_TEST_{uuid.uuid4()}")
    great_grandparent_grandparent_relationship = RoleRelationship(grandparent1_role.name, great_grandparent_role.name)
    sysadmin_great_grandparent_relationship = RoleRelationship(great_grandparent_role.name, "SYSADMIN")
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
        hierarchy: RoleAscendantsEntity = flake.describe(RoleAscendantsDescribable("IGT_CHILD1_ROLE"), RoleAscendantsEntity)
        parent = next((r for r in hierarchy.ascendant_roles if r.parent_role_name == parent_role.name), None)
        grandparent1 = next((r for r in hierarchy.ascendant_roles if r.parent_role_name ==
                             grandparent1_role.name), None)
        grandparent2 = next((r for r in hierarchy.ascendant_roles if r.parent_role_name ==
                             grandparent2_role.name), None)
        great_grandparent = next((r for r in hierarchy.ascendant_roles if r.parent_role_name ==
                                 great_grandparent_role.name), None)
        sys_admin = next((r for r in hierarchy.ascendant_roles if r.parent_role_name == "SYSADMIN"), None)
        acc_admin = next((r for r in hierarchy.ascendant_roles if r.parent_role_name == "ACCOUNTADMIN"), None)

        ### Assert ###
        # {parent: 0, grandparent: 1, great_grandparent: 2, sysadmin: 3, accountadmin: 4}
        assert hierarchy.name == "IGT_CHILD1_ROLE"

        assert parent.distance_from_source == 0
        assert parent.role_name == child1_role.name

        assert grandparent1.distance_from_source == 1
        assert grandparent1.role_name == parent_role.name

        assert grandparent2.distance_from_source == 1
        assert grandparent2.role_name == parent_role.name

        assert great_grandparent.distance_from_source == 2
        assert great_grandparent.role_name == grandparent1_role.name

        assert sys_admin.distance_from_source == 3
        assert sys_admin.role_name == great_grandparent_role.name

        assert acc_admin.distance_from_source == 4
        assert acc_admin.role_name == sys_admin.parent_role_name

    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_broken_role_ascendants(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_broken_role_ascendants"""
    ### Arrange ###
    child1_role = Role("IGT_CHILD1_ROLE", "USERADMIN", f"pyflake_client_TEST_{uuid.uuid4()}")
    child2_role = Role("IGT_CHILD2_ROLE", "USERADMIN", f"pyflake_client_TEST_{uuid.uuid4()}")
    parent_role = Role("IGT_PARENT_ROLE", "USERADMIN", f"pyflake_client_TEST_{uuid.uuid4()}")
    parent_child1_relationship = RoleRelationship(child1_role.name, parent_role.name)
    parent_child2_relationship = RoleRelationship(child2_role.name, parent_role.name)
    grandparent_role = Role("IGT_GRANDPARENT_ROLE", "USERADMIN", f"pyflake_client_TEST_{uuid.uuid4()}")
    grandparent_parent_relationship = RoleRelationship(parent_role.name, grandparent_role.name)
    great_grandparent_role = Role("IGT_GREAT_GRANDPARENT_ROLE", "USERADMIN", f"pyflake_client_TEST_{uuid.uuid4()}")
    # Removing the link between great grandparent and grandparent #great_grandparent_grandparent_relationship = RoleRelationship(grandparent_role.name, great_grandparent_role.name)
    sysadmin_great_grandparent_relationship = RoleRelationship(great_grandparent_role.name, "SYSADMIN")
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
        hierarchy: RoleAscendantsEntity = flake.describe(RoleAscendantsDescribable("IGT_CHILD1_ROLE"), RoleAscendantsEntity)
        parent = next((r for r in hierarchy.ascendant_roles if r.parent_role_name == parent_role.name), None)
        grandparent = next((r for r in hierarchy.ascendant_roles if r.parent_role_name == grandparent_role.name), None)
        great_grandparent = next((r for r in hierarchy.ascendant_roles if r.parent_role_name == great_grandparent_role.name), None)
        sys_admin = next((r for r in hierarchy.ascendant_roles if r.parent_role_name == "SYSADMIN"), None)
        acc_admin = next((r for r in hierarchy.ascendant_roles if r.parent_role_name == "ACCOUNTADMIN"), None)

        ### Assert ###
        # {parent: 0, grandparent: 1, great_grandparent: 2, sysadmin: 3, accountadmin: 4}
        assert hierarchy.name == "IGT_CHILD1_ROLE"

        assert parent.distance_from_source == 0
        assert parent.role_name == child1_role.name

        assert grandparent.distance_from_source == 1
        assert grandparent.role_name == parent_role.name

        assert great_grandparent is None

        assert sys_admin is None

        assert acc_admin is None

    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)
