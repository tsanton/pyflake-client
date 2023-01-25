"""test_role_schema_future_grant"""
# pylint: disable=line-too-long
# pylint: disable=invalid-name
import queue
import uuid

from pyflake_client.models.enums.object_type import ObjectType
from pyflake_client.models.assets.role import Role
from pyflake_client.models.assets.grants.role_schema_future_grant import RoleSchemaFutureGrant as RoleSchemaFutureGrantAsset
from pyflake_client.models.assets.grant import Grant as GrantAsset
from pyflake_client.models.entities.grants.role_schema_future_grant import RoleSchemaFutureGrants
from pyflake_client.models.describables.grants.role_schema_future_grant import RoleSchemaFutureGrant as RoleSchemaFutureGrantDescribable
from pyflake_client.pyflake_client import PyflakeClient
from pyflake_client.tests.utilities import compare, _spawn_database_and_schema

# region table


def test_create_future_schema_table_privilege(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_create_future_schema_table_privilege"""
    ### Arrange ###
    d, s1, _ = _spawn_database_and_schema(flake, assets_queue)
    role: Role = Role(f"{d.db_name}_{s1.schema_name}_ACCESS_ROLE_R", "USERADMIN", f"pyflake_client_TEST_{uuid.uuid4()}")
    privilege = GrantAsset(RoleSchemaFutureGrantAsset(role.name, d.db_name, s1.schema_name, ObjectType.TABLE), ["SELECT", "REFERENCES"])

    try:
        flake.register_asset(role, assets_queue)
        flake.register_asset(privilege, assets_queue)
        ### Act ###

        future: RoleSchemaFutureGrants = flake.describe(RoleSchemaFutureGrantDescribable(
            role.name,
            d.db_name,
            s1.schema_name,
        ), RoleSchemaFutureGrants)

        ### Assert ###
        assert future.role_name == role.name
        assert compare(next((x.privileges for x in future.future_grants if x.grant_target == ObjectType.TABLE), []), privilege.privileges)
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_create_multiple_future_schema_table_privilege(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_create_multiple_future_schema_table_privilege"""
    ### Arrange ###
    d, s1, s2 = _spawn_database_and_schema(flake, assets_queue)
    role: Role = Role(f"{d.db_name}_{s1.schema_name}_ACCESS_ROLE_R", "USERADMIN", f"pyflake_client_TEST_{uuid.uuid4()}")
    privilege1 = GrantAsset(RoleSchemaFutureGrantAsset(role.name, d.db_name, s1.schema_name, ObjectType.TABLE), ["SELECT", "REFERENCES"])
    privilege2 = GrantAsset(RoleSchemaFutureGrantAsset(role.name, d.db_name, s2.schema_name, ObjectType.TABLE), ["SELECT"])

    try:
        flake.register_asset(role, assets_queue)

        ### Act ###
        flake.register_asset(privilege1, assets_queue)
        flake.register_asset(privilege2, assets_queue)

        future1: RoleSchemaFutureGrants = flake.describe(RoleSchemaFutureGrantDescribable(
            role.name,
            d.db_name,
            s1.schema_name,
        ), RoleSchemaFutureGrants)

        future2: RoleSchemaFutureGrants = flake.describe(RoleSchemaFutureGrantDescribable(
            role.name,
            d.db_name,
            s2.schema_name,
        ), RoleSchemaFutureGrants)

        ### Assert ###
        assert future1.role_name == role.name
        future1_privileges = next((x.privileges for x in future1.future_grants if x.grant_target == ObjectType.TABLE), [])
        assert compare(future1_privileges, privilege1.privileges)
        assert len(future1_privileges) == 2

        assert future2.role_name == role.name
        future2_privileges = next((x.privileges for x in future2.future_grants if x.grant_target == ObjectType.TABLE), [])
        assert compare(future2_privileges, privilege2.privileges)
        assert len(future2_privileges) == 1
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)
# endregion

# region views
# TODO: test views & all other future privilege grants
# endregion
