"""test_role_relationship"""
# pylint: disable=line-too-long
import queue
import uuid

from pyflake_client.pyflake_client import PyflakeClient

from pyflake_client.models.assets.role_relationship import RoleRelationship as RoleRelationshipAsset
from pyflake_client.models.assets.role import Role
from pyflake_client.models.describables.role_relationship import RoleRelationship as RoleRelationshipDescribable
from pyflake_client.models.entities.role_relationship import RoleRelationship as RoleRelationshipEntity


def test_get_role_relationship(flake: PyflakeClient):
    """test_get_role_relationship"""
    ### Act ###
    role: RoleRelationshipEntity = flake.describe(RoleRelationshipDescribable("SYSADMIN", "ACCOUNTADMIN"), RoleRelationshipEntity)

    ### Assert ###
    assert role.child_role_name == "SYSADMIN"
    assert role.parent_role_name == "ACCOUNTADMIN"
    assert role.granted_by == ""


def test_create_role_relationship(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_create_role_relationship"""
    ### Arrange ###
    child_role: Role = Role("IGT_CHILD_ROLE", "USERADMIN", f"pyflake_client_TEST_{uuid.uuid4()}")
    parent_role: Role = Role("IGT_PARENT_ROLE", "USERADMIN", f"pyflake_client_TEST_{uuid.uuid4()}")
    try:
        flake.register_asset(child_role, assets_queue)
        flake.register_asset(parent_role, assets_queue)
        flake.register_asset(RoleRelationshipAsset(child_role.name, parent_role.name), assets_queue)

        ### Act ###
        rel: RoleRelationshipEntity = flake.describe(RoleRelationshipDescribable(child_role.name, parent_role.name), RoleRelationshipEntity)

        ### Assert ###
        assert rel.child_role_name == child_role.name
        assert rel.parent_role_name == parent_role.name
        assert rel.granted_by == "USERADMIN"

    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_delete_role_relationship(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_create_role_relationship"""
    ### Arrange ###
    child_role: Role = Role("IGT_CHILD_ROLE", "USERADMIN", f"pyflake_client_TEST_{uuid.uuid4()}")
    parent_role: Role = Role("IGT_PARENT_ROLE", "USERADMIN", f"pyflake_client_TEST_{uuid.uuid4()}")
    relationship = RoleRelationshipAsset(child_role.name, parent_role.name)
    relationship_describable = RoleRelationshipDescribable(child_role.name, parent_role.name)
    try:
        flake.register_asset(child_role, assets_queue)
        flake.register_asset(parent_role, assets_queue)
        flake.create_asset(relationship)
        new_rel: RoleRelationshipEntity = flake.describe(relationship_describable, RoleRelationshipEntity)

        ### Act ###
        flake.delete_asset(relationship)
        del_rel: RoleRelationshipEntity = flake.describe(relationship_describable, RoleRelationshipEntity)

        ### Assert ###
        assert del_rel is None
        assert new_rel.child_role_name == child_role.name
        assert new_rel.parent_role_name == parent_role.name
        assert new_rel.granted_by == "USERADMIN"

    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_get_non_existing_role_relationship(flake: PyflakeClient):
    """test_get_role_relationship: both roles exists, but ACCOUNTADMIN is not granted to SYSADMIN; it's the other way around"""
    ### Act ###
    role: RoleRelationshipEntity = flake.describe(RoleRelationshipDescribable("ACCOUNTADMIN", "SYSADMIN"), RoleRelationshipEntity)

    ### Assert ###
    assert role is None


def test_get_role_relationship_non_existing_child_role(flake: PyflakeClient):
    """test_get_role_relationship_non_existing_child_role"""
    ### Act ###
    role: RoleRelationshipEntity = flake.describe(
        RoleRelationshipDescribable("I_MOST_CERTAINLY_DO_NOT_EXIST", "SYSADMIN"),
        RoleRelationshipEntity
    )

    ### Assert ###
    assert role is None


def test_get_role_relationship_non_existing_parent_role(flake: PyflakeClient):
    """test_get_role_relationship: both roles exists, but ACCOUNTADMIN is not granted to SYSADMIN; it's the other way around"""
    ### Act ###
    role: RoleRelationshipEntity = flake.describe(
        RoleRelationshipDescribable("SYSADMIN", "I_MOST_CERTAINLY_DO_NOT_EXIST"),
        RoleRelationshipEntity
    )

    ### Assert ###
    assert role is None
