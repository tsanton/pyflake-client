"""test_role_schema_object_privilege"""
# pylint: disable=line-too-long
# pylint: disable=invalid-name
import uuid
import queue

from pyflake_client.models.assets.grant import Grant as GrantAsset
from pyflake_client.models.assets.grants.role_schema_future_grant import RoleSchemaFutureGrant as RoleSchemaFutureGrantAsset
from pyflake_client.models.entities.grants.schema_object_grant import SchemaObjectGrants
from pyflake_client.tests.utilities import compare, _spawn_database_and_schema
from pyflake_client.client import PyflakeClient
from pyflake_client.models.assets.table import Column, Table
from pyflake_client.models.assets.role_relationship import RoleRelationship
from pyflake_client.models.describables.grants.schema_object_grant import SchemaObjectGrant
from pyflake_client.models.assets.role import Role
from pyflake_client.models.enums.object_type import ObjectType
from pyflake_client.models.enums.column_type import ColumnType


# region table


def test_create_table_without_future_privileges(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_create_table_without_future_privileges"""
    ### Arrange ###
    d, s1, _ = _spawn_database_and_schema(flake, assets_queue)
    table_columns = [
        Column("ID", ColumnType.INTEGER, identity=True),
        Column("SOME_VARCHAR", ColumnType.VARCHAR, primary_key=True)
    ]
    t = Table(s1, "TEST", table_columns)
    try:
        flake.register_asset(t, assets_queue)

        ### Act ###
        showable = SchemaObjectGrant(d.db_name, s1.schema_name, ObjectType.TABLE, t.table_name)
        tp: SchemaObjectGrants = flake.describe(showable, SchemaObjectGrants)

        ### Assert ###
        assert tp.object_name == t.table_name
        assert tp.schema_name == s1.schema_name
        assert tp.object_type == ObjectType.TABLE
        assert len(tp.grants) == 1
        assert tp.grants[0].role_name == "ACCOUNTADMIN"
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_create_table_with_future_privileges(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_create_table_with_future_privileges"""
    ### Arrange ###
    d, s1, _ = _spawn_database_and_schema(flake, assets_queue)
    role: Role = Role(f"{d.db_name}_{s1.schema_name}_ACCESS_ROLE_R", "USERADMIN", f"pyflake_client_TEST_{uuid.uuid4()}")
    role_privileges = ["SELECT", "REFERENCES", "OWNERSHIP"]
    role_grant = GrantAsset(RoleSchemaFutureGrantAsset(role.name, d.db_name, s1.schema_name, ObjectType.TABLE), role_privileges)
    role_relationship = RoleRelationship(role.name, "SYSADMIN")
    table_columns = [
        Column("ID", ColumnType.INTEGER, identity=True),
        Column("SOME_VARCHAR", ColumnType.VARCHAR, primary_key=True)
    ]
    t = Table(s1, "TEST", table_columns)

    try:
        flake.register_asset(role, assets_queue)
        flake.register_asset(role_relationship, assets_queue)
        flake.register_asset(role_grant, assets_queue)

        ### Act ###
        flake.register_asset(t, assets_queue)
        showable = SchemaObjectGrant(d.db_name, s1.schema_name, ObjectType.TABLE, t.table_name)
        tp: SchemaObjectGrants = flake.describe(showable, SchemaObjectGrants)

        ### Assert ###
        assert tp.object_name == t.table_name
        assert tp.object_type == ObjectType.TABLE
        assert len(tp.grants) == 1
        assert tp.grants[0].role_name == role.name
        assert compare(tp.grants[0].privileges, role_grant.privileges)
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)

# endregion
