# -*- coding: utf-8 -*-
# pylint: disable=line-too-long
# pylint: disable=too-many-locals

import queue
from typing import List

from pyflake_client.client import PyflakeClient
from pyflake_client.models.assets.database import Database as DatabaseAsset
from pyflake_client.models.assets.database_role import DatabaseRole
from pyflake_client.models.assets.role import Role
from pyflake_client.models.assets.role_inheritance import RoleInheritance
from pyflake_client.models.describables.database_role import (
    DatabaseRole as DatabaseRoleDescribable,
)
from pyflake_client.models.describables.principal_ascendants import (
    PrincipalAscendants as RoleAscendantsDescribable,
)
from pyflake_client.models.describables.role import Role as RoleDescribable
from pyflake_client.models.entities.principal_ascendant import PrincipalAscendant
from pyflake_client.tests.utilities import find


def test_get_principal_ascendants(flake: PyflakeClient):
    """test_get_principal_ascendants: we know that USERADMIN -> SECURITYADMIN -> ACCOUNTADMIN"""
    ### Act ###
    ascendants: List[PrincipalAscendant] = flake.describe_async(
        RoleAscendantsDescribable(RoleDescribable("USERADMIN"))
    ).deserialize_many(PrincipalAscendant)

    sec_admin = find(
        ascendants, lambda x: x.grantee_identifier == "SECURITYADMIN" and x.granted_identifier == "USERADMIN"
    )
    acc_admin = find(
        ascendants, lambda x: x.grantee_identifier == "ACCOUNTADMIN" and x.granted_identifier == "SECURITYADMIN"
    )
    sys_admin = find(ascendants, lambda x: x.grantee_identifier == "SYSADMIN")

    ### Assert ###

    assert sys_admin is None

    assert sec_admin is not None
    assert sec_admin.distance_from_source == 0  # SECURITYADMIN is a direct parent of USERADMIN
    assert sec_admin.granted_identifier == "USERADMIN"
    assert sec_admin.grantee_identifier == "SECURITYADMIN"

    assert acc_admin is not None
    assert acc_admin.distance_from_source == 1  # ACCOUNTADMIN is a grantparent of USERADMIN
    assert acc_admin.granted_identifier == "SECURITYADMIN"
    assert acc_admin.grantee_identifier == "ACCOUNTADMIN"


def test_create_role_ascendants(flake: PyflakeClient, assets_queue: queue.LifoQueue, rand_str: str, comment: str):
    ### Arrange ###
    user_admin_role = Role("USERADMIN")
    sys_admin_role = Role("SYSADMIN")
    child1_role = Role(f"PYFLAKE_CLIENT_TEST_ROLE_CHILD_1_{rand_str}", comment, user_admin_role)
    child2_role = Role(f"PYFLAKE_CLIENT_TEST_ROLE_CHILD_2_{rand_str}", comment, user_admin_role)
    parent_role = Role(f"PYFLAKE_CLIENT_TEST_ROLE_PARENT_{rand_str}", comment, user_admin_role)
    parent_child1_relationship = RoleInheritance(child1_role, parent_role)
    parent_child2_relationship = RoleInheritance(child2_role, parent_role)
    grandparent1_role = Role(f"PYFLAKE_CLIENT_TEST_ROLE_GRANDPARENT_1_{rand_str}", comment, user_admin_role)
    grandparent2_role = Role(f"PYFLAKE_CLIENT_TEST_ROLE_GRANDPARENT_2_{rand_str}", comment, user_admin_role)
    grandparent1_parent_relationship = RoleInheritance(parent_role, grandparent1_role)
    grandparent2_parent_relationship = RoleInheritance(parent_role, grandparent2_role)
    great_grandparent_role = Role(f"PYFLAKE_CLIENT_TEST_ROLE_GREAT_GRANDPARENT_{rand_str}", comment, user_admin_role)
    great_grandparent_grandparent_relationship = RoleInheritance(
        child_principal=grandparent1_role, parent_principal=great_grandparent_role
    )
    sysadmin_great_grandparent_relationship = RoleInheritance(
        child_principal=great_grandparent_role, parent_principal=sys_admin_role
    )
    try:
        w1 = flake.register_asset_async(child1_role, assets_queue)
        w2 = flake.register_asset_async(child2_role, assets_queue)
        w3 = flake.register_asset_async(parent_role, assets_queue)
        w4 = flake.register_asset_async(grandparent1_role, assets_queue)
        w5 = flake.register_asset_async(grandparent2_role, assets_queue)
        w6 = flake.register_asset_async(great_grandparent_role, assets_queue)
        flake.wait_all([w1, w2, w3, w4, w5, w6])
        w7 = flake.register_asset_async(parent_child1_relationship, assets_queue)
        w8 = flake.register_asset_async(parent_child2_relationship, assets_queue)
        w9 = flake.register_asset_async(grandparent1_parent_relationship, assets_queue)
        w10 = flake.register_asset_async(grandparent2_parent_relationship, assets_queue)
        w11 = flake.register_asset_async(great_grandparent_grandparent_relationship, assets_queue)
        w12 = flake.register_asset_async(sysadmin_great_grandparent_relationship, assets_queue)
        flake.wait_all([w7, w8, w9, w10, w11, w12])

        ### ctregister_asset_async
        ascendants: List[PrincipalAscendant] = flake.describe_async(
            RoleAscendantsDescribable(RoleDescribable(child1_role.name))
        ).deserialize_many(PrincipalAscendant)

        parent = find(ascendants, lambda x: x.grantee_identifier == parent_role.name)
        grandparent1 = find(ascendants, lambda x: x.grantee_identifier == grandparent1_role.name)
        grandparent2 = find(ascendants, lambda x: x.grantee_identifier == grandparent2_role.name)
        great_grandparent = find(ascendants, lambda x: x.grantee_identifier == great_grandparent_role.name)
        sys_admin = find(ascendants, lambda x: x.grantee_identifier == "SYSADMIN")
        acc_admin = find(
            ascendants, lambda x: x.grantee_identifier == "ACCOUNTADMIN" and x.granted_identifier == "SYSADMIN"
        )

        ### Assert ###
        # {parent: 0, grandparent: 1, great_grandparent: 2, sysadmin: 3, accountadmin: 4}
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


def test_broken_role_ascendants(flake: PyflakeClient, assets_queue: queue.LifoQueue, rand_str: str, comment: str):
    ### Arrange ###
    user_admin_role = Role("USERADMIN")
    # sys_admin_role = Role("SYSADMIN")
    child1_role = Role(f"PYFLAKE_CLIENT_TEST_ROLE_CHILD_1_{rand_str}", comment, user_admin_role)
    child2_role = Role(f"PYFLAKE_CLIENT_TEST_ROLE_CHILD_2_{rand_str}", comment, user_admin_role)
    parent_role = Role(f"PYFLAKE_CLIENT_TEST_ROLE_PARENT_{rand_str}", comment, user_admin_role)
    parent_child1_relationship = RoleInheritance(child1_role, parent_role)
    parent_child2_relationship = RoleInheritance(child2_role, parent_role)
    grandparent_role = Role(f"PYFLAKE_CLIENT_TEST_ROLE_GRANDPARENT_{rand_str}", comment, user_admin_role)
    grandparent_parent_relationship = RoleInheritance(child_principal=parent_role, parent_principal=grandparent_role)
    great_grandparent_role = Role(f"PYFLAKE_CLIENT_TEST_ROLE_GREAT_GRANDPARENT_{rand_str}", comment, user_admin_role)
    # Removing the link between great grandparent and grandparent #great_grandparent_grandparent_relationship = RoleRelationship(grandparent_role.name, great_grandparent_role.name)
    sysadmin_great_grandparent_relationship = RoleInheritance(
        child_principal=great_grandparent_role, parent_principal=user_admin_role
    )
    try:
        w1 = flake.register_asset_async(child1_role, assets_queue)
        w2 = flake.register_asset_async(child2_role, assets_queue)
        w3 = flake.register_asset_async(parent_role, assets_queue)
        w4 = flake.register_asset_async(grandparent_role, assets_queue)
        w5 = flake.register_asset_async(great_grandparent_role, assets_queue)
        flake.wait_all([w1, w2, w3, w4, w5])
        w6 = flake.register_asset_async(parent_child1_relationship, assets_queue)
        w7 = flake.register_asset_async(parent_child2_relationship, assets_queue)
        w8 = flake.register_asset_async(grandparent_parent_relationship, assets_queue)
        # Removing the link between great grandparent and grandparent # flake.register_asset(great_grandparent_grandparent_relationship, assets_queue)
        w9 = flake.register_asset_async(sysadmin_great_grandparent_relationship, assets_queue)
        flake.wait_all([w6, w7, w8, w9])

        ### Act ###
        ascendants: List[PrincipalAscendant] = flake.describe_async(
            RoleAscendantsDescribable(RoleDescribable(child1_role.name))
        ).deserialize_many(PrincipalAscendant)
        parent = find(ascendants, lambda x: x.grantee_identifier == parent_role.name)
        grandparent = find(ascendants, lambda x: x.grantee_identifier == grandparent_role.name)
        great_grandparent = find(ascendants, lambda x: x.grantee_identifier == great_grandparent_role.name)
        sys_admin = find(ascendants, lambda x: x.grantee_identifier == "SYSADMIN")
        acc_admin = find(
            ascendants, lambda x: x.grantee_identifier == "ACCOUNTADMIN" and x.granted_identifier == "SYSADMIN"
        )

        ### Assert ###
        # {parent: 0, grandparent: 1, great_grandparent: 2, sysadmin: 3, accountadmin: 4}
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


def test_ascendants_with_database_roles(
    flake: PyflakeClient, assets_queue: queue.LifoQueue, rand_str: str, comment: str
):
    ### Arrange ###
    user_admin_role = Role("USERADMIN")
    sys_admin_role = Role("SYSADMIN")
    database = DatabaseAsset(f"PYFLAKE_CLIENT_TEST_DB_{rand_str}", comment, sys_admin_role)
    dr_sys = DatabaseRole("PYFLAKE_CLIENT_TEST_DB_SYS_ADMIN", database.db_name, comment, user_admin_role)
    dr_rwc = DatabaseRole("PYFLAKE_CLIENT_TEST_DB_RWC", database.db_name, comment, user_admin_role)
    dr_rw = DatabaseRole("PYFLAKE_CLIENT_TEST_DB_RW", database.db_name, comment, user_admin_role)
    dr_r = DatabaseRole("PYFLAKE_CLIENT_TEST_DB_R", database.db_name, comment, user_admin_role)

    rel1 = RoleInheritance(dr_sys, sys_admin_role)
    rel2 = RoleInheritance(dr_rwc, dr_sys)
    rel3 = RoleInheritance(dr_rw, dr_rwc)
    rel4 = RoleInheritance(dr_r, dr_rw)

    try:
        flake.register_asset_async(database, assets_queue).wait()
        w1 = flake.register_asset_async(dr_sys, assets_queue)
        w2 = flake.register_asset_async(dr_rwc, assets_queue)
        w3 = flake.register_asset_async(dr_rw, assets_queue)
        w4 = flake.register_asset_async(dr_r, assets_queue)
        flake.wait_all([w1, w2, w3, w4])
        w5 = flake.register_asset_async(rel1, assets_queue)
        w6 = flake.register_asset_async(rel2, assets_queue)
        w7 = flake.register_asset_async(rel3, assets_queue)
        w8 = flake.register_asset_async(rel4, assets_queue)
        flake.wait_all([w5, w6, w7, w8])

        ### Act ###
        ascendants: List[PrincipalAscendant] = flake.describe_async(
            RoleAscendantsDescribable(DatabaseRoleDescribable(dr_r.name, dr_r.database_name))
        ).deserialize_many(PrincipalAscendant)
        db_rw = find(ascendants, lambda x: x.grantee_identifier == dr_rw.get_identifier())
        db_rwc = find(ascendants, lambda x: x.grantee_identifier == dr_rwc.get_identifier())
        db_sysadmin = find(ascendants, lambda x: x.grantee_identifier == dr_sys.get_identifier())
        sys_admin = find(ascendants, lambda x: x.grantee_identifier == "SYSADMIN")
        acc_admin = find(
            ascendants, lambda x: x.grantee_identifier == "ACCOUNTADMIN" and x.granted_identifier == "SYSADMIN"
        )

        ### Assert ###
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
