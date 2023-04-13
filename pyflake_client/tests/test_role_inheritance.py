"""test_role_inheritance"""
# pylint: disable=line-too-long
import queue
import uuid

import pytest

from pyflake_client.client import PyflakeClient


from pyflake_client.models.assets.database import Database as DatabaseAsset
from pyflake_client.models.assets.role import Role as RoleAsset
from pyflake_client.models.assets.database_role import DatabaseRole as DatabaseRoleAsset
from pyflake_client.models.assets.role_inheritance import RoleInheritance as RoleInheritanceAsset

from pyflake_client.models.describables.role_inheritance import RoleInheritance as RoleInheritanceDescribable
from pyflake_client.models.describables.role import Role as RoleDescribable
from pyflake_client.models.describables.database_role import DatabaseRole as DatabaseRoleDescribable

from pyflake_client.models.entities.role_inheritance import RoleInheritance as RoleInheritanceEntity

def test_get_role_inheritance(flake: PyflakeClient):
    """test_get_role_inheritance"""
    ### Act ###
    role: RoleInheritanceEntity = flake.describe_one(
        RoleInheritanceDescribable(RoleDescribable("SYSADMIN"), RoleDescribable("ACCOUNTADMIN")), 
        RoleInheritanceEntity
    )

    ### Assert ###
    assert role.inherited_role_identifier == "SYSADMIN"
    assert role.inherited_role_type == "ROLE"
    assert role.principal_identifier == "ACCOUNTADMIN"
    assert role.principal_type == "ROLE"
    assert role.granted_by == ""


def test_create_role_inheritance(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_create_role_inheritance"""
    ### Arrange ###
    snowflake_comment:str = f"pyflake_client_test_{uuid.uuid4()}"
    child_role = RoleAsset("IGT_CHILD_ROLE", snowflake_comment, RoleAsset("USERADMIN"))
    parent_role = RoleAsset("IGT_PARENT_ROLE", snowflake_comment, RoleAsset("USERADMIN"))
    try:
        flake.register_asset(child_role, assets_queue)
        flake.register_asset(parent_role, assets_queue)
        flake.register_asset(RoleInheritanceAsset(child_role, parent_role), assets_queue)

        ### Act ###
        rel: RoleInheritanceEntity = flake.describe_one(
            RoleInheritanceDescribable(RoleDescribable(child_role.name), RoleDescribable(parent_role.name)), 
            RoleInheritanceEntity
        )

        ### Assert ###
        assert rel.inherited_role_identifier == child_role.name
        assert rel.inherited_role_type == "ROLE"
        assert rel.principal_identifier == parent_role.name
        assert rel.principal_type == "ROLE"
        assert rel.granted_by == "USERADMIN"

    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_delete_role_inheritance(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_create_role_inheritance"""
    ### Arrange ###
    snowflake_comment:str = f"pyflake_client_test_{uuid.uuid4()}"
    child_role = RoleAsset("IGT_CHILD_ROLE", snowflake_comment, RoleAsset("USERADMIN"))
    parent_role = RoleAsset("IGT_PARENT_ROLE", snowflake_comment, RoleAsset("USERADMIN"))
    relationship = RoleInheritanceAsset(child_role, parent_role)

    relationship_describable = RoleInheritanceDescribable(RoleDescribable(child_role.name), RoleDescribable(parent_role.name))

    try:
        flake.register_asset(child_role, assets_queue)
        flake.register_asset(parent_role, assets_queue)
        flake.create_asset(relationship)
        new_rel: RoleInheritanceEntity = flake.describe_one(
            relationship_describable, RoleInheritanceEntity
        )

        ### Act ###
        flake.delete_asset(relationship)
        del_rel: RoleInheritanceEntity = flake.describe_one(
            relationship_describable, RoleInheritanceEntity
        )

        ### Assert ###
        assert del_rel is None
        assert new_rel.inherited_role_identifier == child_role.name
        assert new_rel.inherited_role_type == "ROLE"
        assert new_rel.principal_identifier == parent_role.name
        assert new_rel.principal_type == "ROLE"
        assert new_rel.granted_by == "USERADMIN"

    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_get_non_existing_role_inheritance(flake: PyflakeClient):
    """test_get_role_inheritance: both roles exists, but ACCOUNTADMIN is not granted to SYSADMIN; it's the other way around"""
    ### Act ###
    role: RoleInheritanceEntity = flake.describe_one(
        RoleInheritanceDescribable(
            RoleDescribable("ACCOUNTADMIN"), 
            RoleDescribable("SYSADMIN")
        ),
        RoleInheritanceEntity
    )

    ### Assert ###
    assert role is None


def test_get_role_inheritance_non_existing_child_role(flake: PyflakeClient):
    """test_get_role_inheritance_non_existing_child_role"""
    ### Act ###
    role: RoleInheritanceEntity = flake.describe_one(
        RoleInheritanceDescribable(
            RoleDescribable("I_MOST_CERTAINLY_DO_NOT_EXIST"), 
            RoleDescribable("SYSADMIN")
        ),
        RoleInheritanceEntity,
    )

    ### Assert ###
    assert role is None


def test_get_role_inheritance_non_existing_parent_role(flake: PyflakeClient):
    """test_get_role_inheritance: both roles exists, but ACCOUNTADMIN is not granted to SYSADMIN; it's the other way around"""
    ### Act ###
    role: RoleInheritanceEntity = flake.describe_one(
        RoleInheritanceDescribable(
            RoleDescribable("SYSADMIN"), 
            RoleDescribable("I_MOST_CERTAINLY_DO_NOT_EXIST")
        ),
        RoleInheritanceEntity,
    )

    ### Assert ###
    assert role is None

#region role to database role inheritance

def test_show_role_to_database_role_inheritance_in_non_existing_database(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_show_role_to_database_role_inheritance_in_non_existing_database"""
    snowflake_comment: str = f"pyflake_client_test_{uuid.uuid4()}"
    r1 = RoleAsset("TEST_SNOWPLOW_ROLE_1", snowflake_comment, RoleAsset("USERADMIN"))
    relationship_describable = RoleInheritanceDescribable(DatabaseRoleDescribable("I_DONT_EXIST_ROLE", "I_DONT_EXIST_EITHER_DATABASE"), RoleDescribable(r1.name))
    try:
        flake.register_asset(r1, assets_queue)
        inheritance = flake.describe_one(relationship_describable, RoleInheritanceEntity)

        assert inheritance is None
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)

def test_show_role_to_database_role_inheritance(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_show_role_to_database_role_inheritance"""
    ### Arrange ###
    snowflake_comment: str = f"pyflake_client_test_{uuid.uuid4()}"
    database = DatabaseAsset("IGT_DEMO", snowflake_comment, RoleAsset("SYSADMIN"))
    r1 = RoleAsset("TEST_SNOWPLOW_ROLE", snowflake_comment, RoleAsset("USERADMIN"))
    dr1 = DatabaseRoleAsset(
        name="TEST_SNOWPLOW_DATABASE_ROLE",
        database_name=database.db_name,
        comment=snowflake_comment,
        owner=RoleAsset("USERADMIN"),
    )
    rel = RoleInheritanceAsset(r1, dr1)

    ### Act & Assert ###
    with pytest.raises(ValueError):
        flake.register_asset(rel, rel)

#endregion


#region database role to role inheritance

def test_show_database_role_to_role_inheritance(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_show_database_role_to_role_inheritance"""
    ### Arrange ###
    snowflake_comment: str = f"pyflake_client_test_{uuid.uuid4()}"
    database = DatabaseAsset("IGT_DEMO", snowflake_comment, owner=RoleAsset("SYSADMIN"))
    r1 = RoleAsset("TEST_SNOWPLOW_ROLE", snowflake_comment, RoleAsset("USERADMIN"))
    dr1 = DatabaseRoleAsset(
        name="TEST_SNOWPLOW_DATABASE_ROLE",
        database_name=database.db_name,
        owner=RoleAsset("USERADMIN"),
        comment=snowflake_comment,
    )
    rel = RoleInheritanceAsset(child_principal=dr1, parent_principal=r1)

    ### Act ###
    try:
        flake.register_asset(database, assets_queue)
        flake.register_asset(r1, assets_queue)
        flake.register_asset(dr1, assets_queue)
        flake.register_asset(rel, assets_queue)

        inheritance = flake.describe_one(RoleInheritanceDescribable( 
                inherited_principal=DatabaseRoleDescribable(dr1.name, dr1.database_name), 
                parent_principal=RoleDescribable(r1.name)
            ), 
            RoleInheritanceEntity
        )

        assert inheritance is not None
    finally:
            ### Cleanup ###
            flake.delete_assets(assets_queue)

#endregion


#region database role to database role inheritance

def test_show_database_role_to_database_role_same_database_inheritance(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_show_database_role_to_database_role_inheritance"""
    ### Arrange ###
    snowflake_comment: str = f"pyflake_client_test_{uuid.uuid4()}"
    database = DatabaseAsset("IGT_DEMO", snowflake_comment, owner=RoleAsset("SYSADMIN"))
    dr1 = DatabaseRoleAsset(
        name="TEST_SNOWPLOW_DATABASE_ROLE_1",
        database_name=database.db_name,
        comment=snowflake_comment,
        owner=RoleAsset("USERADMIN"),
    )
    dr2 = DatabaseRoleAsset(
        name="TEST_SNOWPLOW_DATABASE_ROLE_2",
        database_name=database.db_name,
        comment=snowflake_comment,
        owner=RoleAsset("USERADMIN"),
    )
    rel = RoleInheritanceAsset(child_principal=dr1, parent_principal=dr2)

    ### Act ###
    try:
        flake.register_asset(database, assets_queue)
        flake.register_asset(dr1, assets_queue)
        flake.register_asset(dr2, assets_queue)
        flake.register_asset(rel, assets_queue)

        inheritance = flake.describe_one(RoleInheritanceDescribable( 
                inherited_principal=DatabaseRoleDescribable(dr1.name, dr1.database_name),
                parent_principal=DatabaseRoleDescribable(dr2.name, dr2.database_name)
            ), 
            RoleInheritanceEntity
        )

        assert inheritance is not None
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)

def test_show_database_role_to_database_role_cross_database_inheritance(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_show_database_role_to_database_role_inheritance"""
    ### Arrange ###
    snowflake_comment: str = f"pyflake_client_test_{uuid.uuid4()}"
    database = DatabaseAsset("IGT_DEMO_1", snowflake_comment, owner=RoleAsset("SYSADMIN"))
    database2 = DatabaseAsset("IGT_DEMO_2", snowflake_comment, owner=RoleAsset("SYSADMIN"))
    dr1 = DatabaseRoleAsset(
        name="TEST_SNOWPLOW_DATABASE_ROLE_1",
        database_name=database.db_name,
        comment=snowflake_comment,
        owner=RoleAsset("USERADMIN"),
    )
    dr2 = DatabaseRoleAsset(
        name="TEST_SNOWPLOW_DATABASE_ROLE_2",
        database_name=database2.db_name,
        comment=snowflake_comment,
        owner=RoleAsset("USERADMIN"),
    )
    rel = RoleInheritanceAsset(child_principal=dr1, parent_principal=dr2)

    ### Act ###
    with pytest.raises(ValueError):
        flake.register_asset(rel, rel)

#endregion